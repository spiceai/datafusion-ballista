// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use ballista_core::JobId;
use ballista_core::execution_plans::create_shuffle_path;
use ballista_core::execution_plans::range_shuffle::{
    index_path as range_index_path, is_ipc_file, open_ipc_file,
};
use ballista_core::serde::scheduler::{ByteRange, ShuffleFileKind, ShuffleLayout};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use std::convert::TryFrom;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio_util::io::ReaderStream;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::global_shuffle_manager;
use ballista_core::execution_plans::sort_shuffle::{
    ShuffleIndex, get_index_path, is_sort_shuffle_output, stream_sort_shuffle_partition,
};
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use datafusion::arrow::ipc::CompressionType;

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, info};
use std::io::BufReader;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;
use tokio::{sync::mpsc::Sender, task};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

/// Arrow Flight service for transferring shuffle data between executors.
///
/// This service implements the Apache Arrow Flight protocol to enable efficient
/// transfer of intermediate query results (shuffle data) between executor nodes.
/// It supports both decoded streaming via `do_get` and optimized block transfer
/// via the `IO_BLOCK_TRANSPORT` action.
///
/// The Spice fork addresses shuffle data by the explicit `path` carried on each
/// fetch request (a local file, or a `memory://` key served from the in-memory
/// shuffle manager), so the service needs no work directory. A work directory
/// set with [`Self::with_work_dir`] is only used to resolve requests that carry
/// no `path`, from their job/stage/partition/file identifiers.
#[derive(Clone, Default)]
pub struct BallistaFlightService {
    work_dir: Option<String>,
}

impl BallistaFlightService {
    /// Creates a new BallistaFlightService instance.
    pub fn new() -> Self {
        Self { work_dir: None }
    }

    /// Resolve fetch requests that carry no explicit `path` against `work_dir`.
    pub fn with_work_dir(mut self, work_dir: String) -> Self {
        self.work_dir = Some(work_dir);
        self
    }

    /// Resolve a fetch request to the file it names.
    ///
    /// An explicit `path` wins. Otherwise the data file is derived from the
    /// request's identifiers under the configured work directory. The index's
    /// name follows from the layout, which is why the wire does not spell it: a
    /// sort-shuffle output's index is its offset table, a passthrough output's
    /// is the range shuffle's value index.
    #[allow(clippy::too_many_arguments)]
    fn resolve_fetch_path(
        &self,
        path: &str,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        file_id: Option<u64>,
        layout: ShuffleLayout,
        file_kind: ShuffleFileKind,
    ) -> Result<PathBuf, Status> {
        let data = if !path.is_empty() {
            PathBuf::from(path)
        } else if let Some(work_dir) = &self.work_dir {
            create_shuffle_path(
                work_dir,
                job_id,
                stage_id,
                partition_id,
                file_id,
                matches!(layout, ShuffleLayout::Sort),
            )
            .map_err(|e| {
                Status::internal(format!("I/O error, can't create shuffle path: {e}"))
            })?
        } else {
            return Err(Status::invalid_argument(format!(
                "fetch request for job {job_id} stage {stage_id} partition \
                 {partition_id} carries no path and this executor has no work \
                 directory to resolve it against"
            )));
        };

        Ok(match file_kind {
            ShuffleFileKind::Data => data,
            ShuffleFileKind::Index => match layout {
                ShuffleLayout::Sort => get_index_path(data.as_path()),
                ShuffleLayout::Passthrough => range_index_path(data.as_path()),
            },
        })
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// shuffle file block transfer size    
const BLOCK_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::FetchPartition {
                job_id,
                stage_id,
                partition_id,
                path,
                file_id,
                layout,
                file_kind,
                byte_ranges,
                ..
            } => {
                if !byte_ranges.is_empty() {
                    // Byte ranges come back as bytes, which `do_get` cannot
                    // express — it returns decoded FlightData. Serving them
                    // here would mean decoding on the executor, which is the
                    // work a ranged read exists to avoid.
                    return Err(Status::invalid_argument(
                        "byte ranges are served by the IO_BLOCK_TRANSPORT action, \
                         not do_get",
                    ));
                }

                // Check if this is an in-memory partition
                if let Some(key) = path.strip_prefix("memory://") {
                    // Fetch from in-memory shuffle manager
                    let shuffle_manager = global_shuffle_manager();
                    let data = shuffle_manager.get_partition(key).map_err(|e| {
                        Status::not_found(format!(
                            "In-memory partition not found: {key}: {e}"
                        ))
                    })?;

                    debug!(
                        "FetchPartition serving in-memory partition: {} ({} batches, {} rows, format: {:?})",
                        key, data.num_batches, data.num_rows, data.format
                    );

                    let (tx, rx) = channel(2);
                    let schema = data.schema.clone();

                    // Convert to batches (handles both Arrow and Vortex formats)
                    let batches = data.to_batches().map_err(|e| {
                        Status::internal(format!(
                            "Failed to convert in-memory partition to batches: {e}"
                        ))
                    })?;

                    // Stream the batches from memory
                    task::spawn(async move {
                        for batch in batches {
                            if tx.send(Ok(batch)).await.is_err() {
                                break;
                            }
                        }
                    });

                    let write_options: IpcWriteOptions = IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .map_err(|e| from_arrow_err(&e))?;
                    let flight_data_stream = FlightDataEncoderBuilder::new()
                        .with_schema(schema)
                        .with_options(write_options)
                        .build(ReceiverStream::new(rx))
                        .map_err(|err| Status::from_error(Box::new(err)));

                    return Ok(Response::new(
                        Box::pin(flight_data_stream) as Self::DoGetStream
                    ));
                }

                let path = self.resolve_fetch_path(
                    path,
                    job_id,
                    *stage_id,
                    *partition_id,
                    *file_id,
                    *layout,
                    *file_kind,
                )?;
                debug!("FetchPartition reading partition {partition_id} from {path:?}");

                // Check if this is a sort-based shuffle output
                if is_sort_shuffle_output(&path) {
                    debug!("Detected sort-based shuffle format for {path:?}");
                    let index_path = get_index_path(path.as_path());
                    let stream =
                        stream_sort_shuffle_partition(&path, &index_path, *partition_id)
                            .map_err(|e| from_ballista_err(&e))?;

                    let schema = stream.schema();
                    // Map DataFusionError to FlightError
                    let stream =
                        stream.map_err(|e| FlightError::from(ArrowError::from(e)));

                    let write_options: IpcWriteOptions = IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .map_err(|e| from_arrow_err(&e))?;
                    let flight_data_stream = FlightDataEncoderBuilder::new()
                        .with_schema(schema)
                        .with_options(write_options)
                        .build(stream)
                        .map_err(|err| Status::from_error(Box::new(err)));

                    return Ok(Response::new(
                        Box::pin(flight_data_stream) as Self::DoGetStream
                    ));
                }

                // Detect the single-file shuffle format from the file itself
                // (range shuffle writes the IPC file format, whose leading magic
                // an IPC stream decoder rejects) or its extension (Vortex).
                let is_vortex =
                    path.extension().map(|ext| ext == "vortex").unwrap_or(false);

                let (schema, rx) = if is_vortex {
                    debug!("FetchPartition reading {path:?} (format: vortex)");
                    #[cfg(feature = "vortex")]
                    {
                        read_vortex_partition(&path)?
                    }
                    #[cfg(not(feature = "vortex"))]
                    {
                        return Err(Status::unimplemented(
                            "Vortex format is not available. Enable the 'vortex' feature.",
                        ));
                    }
                } else if is_ipc_file(&path) {
                    debug!("Detected range shuffle format for {path:?}");
                    let reader =
                        open_ipc_file(&path).map_err(|e| from_ballista_err(&e))?;
                    let (tx, rx) = channel(2);
                    let schema = reader.schema();
                    task::spawn_blocking(move || {
                        if let Err(e) = read_partition(reader, tx) {
                            log::warn!("error streaming range shuffle partition: {e}");
                        }
                    });
                    (schema, rx)
                } else {
                    debug!("FetchPartition reading {path:?} (format: arrow-ipc)");
                    read_arrow_ipc_partition(&path)?
                };

                let write_options: IpcWriteOptions = IpcWriteOptions::default()
                    .try_with_compression(Some(CompressionType::LZ4_FRAME))
                    .map_err(|e| from_arrow_err(&e))?;
                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_options(write_options)
                    .build(ReceiverStream::new(rx))
                    .map_err(|err| Status::from_error(Box::new(err)));

                Ok(Response::new(
                    Box::pin(flight_data_stream) as Self::DoGetStream
                ))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let token = uuid::Uuid::new_v4();
        info!("do_handshake token={}", token);

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<
            Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>,
        > = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            // Block transfer will transfer arrow ipc file block by block
            // without decoding or decompressing, this will provide less resource utilization
            // as file are not decoded nor decompressed/compressed. Usually this would transfer less data across
            // as files are better compressed due to its size.
            //
            // For further discussion regarding performance implications, refer to:
            // https://github.com/apache/datafusion-ballista/issues/1315
            "IO_BLOCK_TRANSPORT" => {
                let action =
                    decode_protobuf(&action.body).map_err(|e| from_ballista_err(&e))?;

                match &action {
                    BallistaAction::FetchPartition {
                        job_id,
                        stage_id,
                        partition_id,
                        path,
                        file_id,
                        layout,
                        file_kind,
                        byte_ranges,
                        ..
                    } => {
                        // Check if this is an in-memory partition
                        // For in-memory partitions, we need to serialize to IPC format first
                        if let Some(key) = path.strip_prefix("memory://") {
                            if !byte_ranges.is_empty() {
                                return Err(Status::invalid_argument(format!(
                                    "byte ranges cannot be served for in-memory partition {key}"
                                )));
                            }
                            return serve_memory_partition_block(key);
                        }

                        let path = self.resolve_fetch_path(
                            path,
                            job_id,
                            *stage_id,
                            *partition_id,
                            *file_id,
                            *layout,
                            *file_kind,
                        )?;

                        debug!("FetchPartition reading {path:?}");

                        let stream = if !byte_ranges.is_empty() {
                            // The caller has read an index and knows which
                            // bytes it wants. Hand them over and resolve
                            // nothing — the same request an object store
                            // serves with a Range header.
                            stream_byte_ranges(&path, byte_ranges).await?
                        } else if is_sort_shuffle_output(&path) {
                            // Sort-shuffle asked for a partition and no ranges,
                            // so the executor resolves it through the index
                            // beside the data: one round trip, as ever. When
                            // that read moves to the caller this arm goes with
                            // it.
                            stream_sort_shuffle_block(&path, *partition_id).await?
                        } else {
                            // One partition per file, so the file is the answer.
                            stream_whole_file(&path).await?
                        };

                        Ok(Response::new(stream))
                    }
                }
            }
            action_type => Err(Status::unimplemented(format!(
                "do_action does not implement: {}",
                action_type
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "IO_BLOCK_TRANSFER".to_owned(),
            description: "optimized shuffle data transfer".to_owned(),
        })];

        Ok(Response::new(
            Box::pin(futures::stream::iter(actions)) as Self::ListActionsStream
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}

/// Stream the requested byte ranges of `path`, concatenated in request order.
///
/// The serving side resolves nothing here: a consumer that has read an index
/// knows which bytes it wants, and this hands them over. Ranges are clamped to
/// the file so a stale index cannot walk off the end.
///
/// Chunked at [`BLOCK_BUFFER_CAPACITY`] like a whole-file read, for two
/// reasons: a range can be far larger than a gRPC message may be, and buffering
/// it whole would hold a consumer's entire share of a partition in the serving
/// executor's memory.
async fn stream_byte_ranges(
    path: &std::path::Path,
    ranges: &[ByteRange],
) -> Result<<BallistaFlightService as FlightService>::DoActionStream, Status> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let len = tokio::fs::metadata(path)
        .await
        .map_err(|e| Status::internal(format!("Failed to stat {path:?}: {e}")))?
        .len();

    for range in ranges {
        if range.offset >= len {
            return Err(Status::out_of_range(format!(
                "range at {} is past the end of {path:?} ({len} bytes)",
                range.offset
            )));
        }
    }

    debug!(
        "serving {} byte ranges ({} bytes) from {path:?}",
        ranges.len(),
        ranges
            .iter()
            .map(|r| r.length.min(len - r.offset))
            .sum::<u64>(),
    );

    let path = path.to_owned();
    let ranges: Vec<ByteRange> = ranges.to_vec();
    let stream = futures::stream::iter(ranges)
        .then(move |range| {
            let path = path.clone();
            async move {
                let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
                    Status::internal(format!("Failed to open {path:?}: {e}"))
                })?;
                file.seek(std::io::SeekFrom::Start(range.offset))
                    .await
                    .map_err(|e| Status::internal(format!("seek {path:?}: {e}")))?;
                let take = range.length.min(len - range.offset);
                // Both levels carry `Status` so the flatten has one error type.
                Ok::<_, Status>(
                    ReaderStream::with_capacity(file.take(take), BLOCK_BUFFER_CAPACITY)
                        .map(|chunk| {
                            chunk
                                .map(|bytes| arrow_flight::Result { body: bytes })
                                .map_err(|e| Status::internal(format!("I/O error: {e}")))
                        }),
                )
            }
        })
        .try_flatten();

    Ok(Box::pin(stream))
}

/// Serve an in-memory shuffle partition over block transport by encoding it to
/// an Arrow IPC stream first.
fn serve_memory_partition_block(
    key: &str,
) -> Result<Response<<BallistaFlightService as FlightService>::DoActionStream>, Status> {
    let shuffle_manager = global_shuffle_manager();
    let data = shuffle_manager.get_partition(key).map_err(|e| {
        Status::not_found(format!("In-memory partition not found: {key}: {e}"))
    })?;

    debug!(
        "FetchPartition serving in-memory partition via block transfer: {} ({} batches, format: {:?})",
        key, data.num_batches, data.format
    );

    // Convert to batches (handles both Arrow and Vortex formats)
    let batches = data.to_batches().map_err(|e| {
        Status::internal(format!(
            "Failed to convert in-memory partition to batches: {e}"
        ))
    })?;

    // Serialize batches to IPC format in memory
    let mut buffer = Vec::new();
    {
        use datafusion::arrow::ipc::writer::StreamWriter;
        let mut writer = StreamWriter::try_new_with_options(
            &mut buffer,
            &data.schema,
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .map_err(|e| from_arrow_err(&e))?,
        )
        .map_err(|e| from_arrow_err(&e))?;

        for batch in &batches {
            writer.write(batch).map_err(|e| from_arrow_err(&e))?;
        }
        writer.finish().map_err(|e| from_arrow_err(&e))?;
    }

    let bytes = bytes::Bytes::from(buffer);
    let result_stream =
        futures::stream::once(async move { Ok(arrow_flight::Result { body: bytes }) });

    Ok(Response::new(
        Box::pin(result_stream)
            as <BallistaFlightService as FlightService>::DoActionStream,
    ))
}

async fn stream_whole_file(
    path: &Path,
) -> Result<<BallistaFlightService as FlightService>::DoActionStream, Status> {
    let file = tokio::fs::File::open(path).await.map_err(|e| {
        // A 0-row partition is never written to disk (the writer creates
        // partition files lazily), so a missing file means an empty partition,
        // not a failure. Report NotFound so the client treats it as an empty
        // partition.
        if e.kind() == std::io::ErrorKind::NotFound {
            Status::not_found(format!("partition file not found (empty partition): {e}"))
        } else {
            Status::internal(format!("Failed to open file: {e}"))
        }
    })?;
    debug!(
        "streaming file: {:?} with size: {}",
        path,
        file.metadata().await?.len()
    );
    let reader = tokio::io::BufReader::with_capacity(BLOCK_BUFFER_CAPACITY, file);
    let file_stream = ReaderStream::with_capacity(reader, BLOCK_BUFFER_CAPACITY);
    Ok(Box::pin(file_stream.map(|result| {
        result
            .map(|bytes| arrow_flight::Result { body: bytes })
            .map_err(|e| Status::internal(format!("I/O error: {e}")))
    })))
}

async fn stream_sort_shuffle_block(
    data_path: &Path,
    partition_id: usize,
) -> Result<<BallistaFlightService as FlightService>::DoActionStream, Status> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let index_path = get_index_path(data_path);
    let index =
        ShuffleIndex::read_from_file(&index_path).map_err(|e| from_ballista_err(&e))?;

    if partition_id >= index.partition_count() {
        return Err(Status::out_of_range(format!(
            "partition_id {partition_id} not found in index (max: {})",
            index.partition_count()
        )));
    }

    // The leading [0, header_end) bytes hold the schema-header IPC stream.
    // We always prepend it to the partition's byte range so the receiver
    // recovers the schema even when the partition is empty.
    let header_end = index.header_end_offset() as u64;
    let (start, end) = index.get_partition_range(partition_id);
    let (start, end) = (start as u64, end as u64);

    // One open + dup gives us two independent file cursors over the same
    // inode: one reads the header from offset 0, the other seeks to the
    // partition. `chain` and `take` consume their readers by value, so two
    // cursors are unavoidable here.
    let header_file = tokio::fs::File::open(data_path)
        .await
        .map_err(|e| Status::internal(format!("Failed to open file: {e}")))?;
    let mut partition_file = header_file
        .try_clone()
        .await
        .map_err(|e| Status::internal(format!("dup file handle: {e}")))?;
    partition_file
        .seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|e| Status::internal(format!("seek partition: {e}")))?;

    let combined = header_file
        .take(header_end)
        .chain(partition_file.take(end - start));
    let file_stream = ReaderStream::with_capacity(combined, BLOCK_BUFFER_CAPACITY);
    Ok(Box::pin(file_stream.map(|result| {
        result
            .map(|bytes| arrow_flight::Result { body: bytes })
            .map_err(|e| Status::internal(format!("I/O error: {e}")))
    })))
}

/// Read an Arrow IPC stream partition file and return the schema and a
/// receiver for its record batches.
fn read_arrow_ipc_partition(
    path: &Path,
) -> Result<
    (
        SchemaRef,
        tokio::sync::mpsc::Receiver<Result<RecordBatch, FlightError>>,
    ),
    Status,
> {
    let file = File::open(path).map_err(|e| {
        // Missing file == empty partition (writer skips 0-row partitions).
        // Report NotFound so the client treats it as an empty partition.
        if e.kind() == std::io::ErrorKind::NotFound {
            Status::not_found(format!(
                "partition file not found (empty partition) at {path:?}: {e}"
            ))
        } else {
            from_ballista_err(&BallistaError::General(format!(
                "Failed to open partition file at {path:?}: {e:?}"
            )))
        }
    })?;
    let file = BufReader::new(file);
    // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
    let reader = unsafe {
        StreamReader::try_new(file, None)
            .map_err(|e| from_arrow_err(&e))?
            .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
    };

    let (tx, rx) = channel(2);
    let schema = reader.schema();
    task::spawn_blocking(move || {
        if let Err(e) = read_partition(reader, tx) {
            log::warn!("error streaming Arrow IPC shuffle partition: {e}");
        }
    });

    Ok((schema, rx))
}

/// Read Vortex partition file and return the schema and a receiver for record batches
#[cfg(feature = "vortex")]
fn read_vortex_partition(
    path: &Path,
) -> Result<
    (
        SchemaRef,
        tokio::sync::mpsc::Receiver<Result<RecordBatch, FlightError>>,
    ),
    Status,
> {
    use std::io::Cursor;
    use std::sync::Arc;
    use vortex_array::ArrayRef;
    use vortex_array::LEGACY_SESSION;
    use vortex_array::iter::ArrayIterator;
    use vortex_ipc::iterator::SyncIPCReader;

    let file = File::open(path)
        .map_err(|e| {
            BallistaError::General(format!(
                "Failed to open Vortex partition file at {path:?}: {e:?}"
            ))
        })
        .map_err(|e| from_ballista_err(&e))?;

    let mut buf_reader = BufReader::new(file);
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut buf_reader, &mut data).map_err(|e| {
        from_ballista_err(&BallistaError::General(format!(
            "Failed to read Vortex file at {path:?}: {e:?}"
        )))
    })?;

    // Create default session with all canonical encodings
    let session = &*LEGACY_SESSION;

    // Read IPC data
    let cursor = Cursor::new(data);
    let reader = SyncIPCReader::try_new(cursor, session).map_err(|e| {
        from_ballista_err(&BallistaError::General(format!(
            "Failed to create Vortex IPC reader at {path:?}: {e:?}"
        )))
    })?;

    // Get schema from IPC header via ArrayIterator::dtype() method
    // This is stored in the Vortex IPC format header, not inferred from data
    let dtype = reader.dtype().clone();
    let arrow_schema = dtype.to_arrow_schema().map_err(|e| {
        from_ballista_err(&BallistaError::General(format!(
            "Failed to convert Vortex DType to Arrow schema: {e:?}"
        )))
    })?;
    let schema = Arc::new(arrow_schema);

    let arrays: Vec<ArrayRef> = reader
        .map(|r| {
            r.map_err(|e| {
                from_ballista_err(&BallistaError::General(format!(
                    "Failed to read Vortex array: {e:?}"
                )))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let (tx, rx) = channel(2);
    task::spawn_blocking(move || {
        if let Err(e) = read_vortex_batches(arrays, tx) {
            log::warn!("error streaming Vortex shuffle partition: {e}");
        }
    });

    Ok((schema, rx))
}

/// Read Vortex arrays and send them as record batches
#[cfg(feature = "vortex")]
#[allow(deprecated)]
fn read_vortex_batches(
    arrays: Vec<vortex_array::ArrayRef>,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError> {
    use vortex_array::arrow::IntoArrowArray;

    if tx.is_closed() {
        return Err(FlightError::Tonic(Box::new(Status::internal(
            "Can't send a batch, channel is closed",
        ))));
    }

    for array in arrays {
        let arrow_array = array
            .into_arrow_preferred()
            .map_err(|e| FlightError::Arrow(ArrowError::ExternalError(Box::new(e))))?;

        let struct_array = arrow_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StructArray>()
            .ok_or_else(|| {
                FlightError::Arrow(ArrowError::InvalidArgumentError(
                    "Expected StructArray from Vortex".to_string(),
                ))
            })?;

        let batch = RecordBatch::from(struct_array);

        tx.blocking_send(Ok(batch)).map_err(|err| {
            if let SendError(Err(err)) = err {
                err
            } else {
                FlightError::Tonic(Box::new(Status::internal(format!(
                    "Can't send a batch, something went wrong: {err:?}"
                ))))
            }
        })?;
    }
    Ok(())
}

/// Pump every batch a shuffle-file decoder yields into `tx`.
///
/// Generic over the decoder: the two shuffle formats need different ones
/// (`StreamReader` for IPC stream, `FileReader` for IPC file) and share no
/// arrow trait past `Iterator`.
fn read_partition<R>(
    reader: R,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError>
where
    R: Iterator<Item = Result<RecordBatch, ArrowError>>,
{
    if tx.is_closed() {
        return Err(FlightError::Tonic(Box::new(Status::internal(
            "Can't send a batch, channel is closed",
        ))));
    }

    for batch in reader {
        tx.blocking_send(batch.map_err(|err| err.into()))
            .map_err(|err| {
                if let SendError(Err(err)) = err {
                    err
                } else {
                    FlightError::Tonic(Box::new(Status::internal(format!(
                        "Can't send a batch, something went wrong: {err:?}"
                    ))))
                }
            })?
    }
    Ok(())
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {e:?}"))
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
}
