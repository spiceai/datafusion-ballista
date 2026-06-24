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

use async_trait::async_trait;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::task::{Context, Poll};

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use url::Url;

use crate::client::BallistaClient;
use crate::execution_plans::shuffle_manager::global_shuffle_manager;
use crate::execution_plans::sort_shuffle::{
    get_index_path, is_sort_shuffle_output, stream_sort_shuffle_partition,
};
use crate::extension::{
    BallistaConfigGrpcEndpoint, SessionConfigExt, ShuffleReadMetricsCallback,
};
use crate::serde::scheduler::{PartitionLocation, PartitionStats};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt, ready};

use crate::error::BallistaError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use itertools::Itertools;
use log::{debug, error, trace};
use rand::prelude::SliceRandom;
use rand::rng;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// The query stage id to read from
    pub stage_id: usize,
    pub(crate) schema: SchemaRef,
    /// Each partition of a shuffle can read data from multiple locations
    pub partition: Vec<Vec<PartitionLocation>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        stage_id: usize,
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Ok(Self {
            stage_id,
            schema,
            partition,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ShuffleReaderExec: partitioning: {}",
                    self.properties.partitioning,
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.properties.partitioning)
            }
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn name(&self) -> &str {
        "ShuffleReaderExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Ballista ShuffleReaderExec does not support children plans".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = context.task_id().unwrap_or_else(|| partition.to_string());
        debug!("ShuffleReaderExec::execute({task_id})");

        let config = context.session_config();

        let max_request_num =
            config.ballista_shuffle_reader_maximum_concurrent_requests();
        let max_message_size = config.ballista_grpc_client_max_message_size();
        let force_remote_read = config.ballista_shuffle_reader_force_remote_read();
        let prefer_flight = config.ballista_shuffle_reader_remote_prefer_flight();
        let batch_size = config.batch_size();
        let customize_endpoint = config.ballista_override_create_grpc_client_endpoint();
        let use_tls = config.ballista_use_tls();
        let metrics_callback = config.ballista_shuffle_read_metrics_callback();

        if force_remote_read {
            debug!(
                "All shuffle partitions will be read as remote partitions! To disable this behavior set: `{}=false`",
                crate::config::BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ
            );
        }

        log::debug!(
            "ShuffleReaderExec::execute({task_id}) max_request_num: {max_request_num}, max_message_size: {max_message_size}"
        );
        let mut partition_locations = HashMap::new();
        for p in &self.partition[partition] {
            partition_locations
                .entry(p.executor_meta.id.clone())
                .or_insert_with(Vec::new)
                .push(p.clone());
        }
        // Sort partitions for evenly send fetching partition requests to avoid hot executors within one task
        let mut partition_locations: Vec<PartitionLocation> = partition_locations
            .into_values()
            .flat_map(|ps| ps.into_iter().enumerate())
            .sorted_by(|(p1_idx, _), (p2_idx, _)| Ord::cmp(p1_idx, p2_idx))
            .map(|(_, p)| p)
            .collect();
        // Shuffle partitions for evenly send fetching partition requests to avoid hot executors within multiple tasks
        partition_locations.shuffle(&mut rng());
        let response_receiver = send_fetch_partitions(
            partition_locations,
            max_request_num,
            max_message_size,
            force_remote_read,
            prefer_flight,
            customize_endpoint,
            use_tls,
            metrics_callback,
            context.runtime_env(),
        );

        let input_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            response_receiver.try_flatten(),
        ));

        Ok(Box::pin(CoalescedShuffleReaderStream::new(
            input_stream,
            batch_size,
            None,
            &self.metrics,
            partition,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<Statistics>> {
        if let Some(idx) = partition {
            let partition_count = self.properties().partitioning.partition_count();
            if idx >= partition_count {
                return datafusion::common::internal_err!(
                    "Invalid partition index: {}, the partition count is {}",
                    idx,
                    partition_count
                );
            }
            let stat_for_partition =
                stats_for_partition(idx, self.schema.fields().len(), &self.partition);

            trace!(
                "shuffle reader at stage: {} and partition {} returned statistics: {:?}",
                self.stage_id, idx, stat_for_partition
            );
            stat_for_partition.map(Arc::new)
        } else {
            let stats_for_partitions = stats_for_partitions(
                self.schema.fields().len(),
                self.partition
                    .iter()
                    .flatten()
                    .map(|loc| loc.partition_stats),
            );
            trace!(
                "shuffle reader at stage: {} returned statistics for all partitions: {:?}",
                self.stage_id, stats_for_partitions
            );
            Ok(Arc::new(stats_for_partitions))
        }
    }
}
/// Calculates stats for partition
pub fn stats_for_partition(
    partition: usize,
    num_fields: usize,
    partition_locations: &[Vec<PartitionLocation>],
) -> Result<Statistics> {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) = partition_locations
        .iter()
        .map(|location| {
            // extract requested partitions
            location
                .get(partition)
                .map(|p| p.partition_stats)
                .map(|p| (p.num_rows, p.num_bytes))
                .unwrap_or_default()
        })
        .fold(
            (Some(0), Some(0)),
            |(num_rows, total_byte_size), (rows, bytes)| {
                (
                    num_rows.zip(rows).map(|(a, b)| a + b as usize),
                    total_byte_size.zip(bytes).map(|(a, b)| a + b as usize),
                )
            },
        );

    Ok(Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    })
}

/// Calculates stats for partitions
pub fn stats_for_partitions(
    num_fields: usize,
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) =
        partition_stats.fold((Some(0), Some(0)), |(num_rows, total_byte_size), part| {
            // if any statistic is unkown it makes the entire statistic unkown
            let num_rows = num_rows.zip(part.num_rows).map(|(a, b)| a + b as usize);
            let total_byte_size = total_byte_size
                .zip(part.num_bytes)
                .map(|(a, b)| a + b as usize);
            (num_rows, total_byte_size)
        });
    Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    }
}

struct LocalShuffleStream {
    reader: StreamReader<BufReader<File>>,
}

impl LocalShuffleStream {
    pub fn new(reader: StreamReader<BufReader<File>>) -> Self {
        LocalShuffleStream { reader }
    }
}

impl Stream for LocalShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch.map_err(|e| e.into())));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Adapter for a tokio ReceiverStream that implements the SendableRecordBatchStream interface
struct AbortableReceiverStream {
    inner: ReceiverStream<result::Result<SendableRecordBatchStream, BallistaError>>,

    #[allow(dead_code)]
    drop_helper: Vec<SpawnedTask<()>>,
}

impl AbortableReceiverStream {
    /// Construct a new SendableRecordBatchReceiverStream which will send batches of the specified schema from inner
    pub fn create(
        rx: tokio::sync::mpsc::Receiver<
            result::Result<SendableRecordBatchStream, BallistaError>,
        >,
        spawned_tasks: Vec<SpawnedTask<()>>,
    ) -> AbortableReceiverStream {
        let inner = ReceiverStream::new(rx);
        Self {
            inner,
            drop_helper: spawned_tasks,
        }
    }
}

impl Stream for AbortableReceiverStream {
    type Item = result::Result<SendableRecordBatchStream, ArrowError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}
/// Splits the provided partition locations into local and remote partitions.
/// Local partitions are read directly from local Arrow IPC files,
/// while remote partitions are fetched using the Arrow Flight client.
/// If `force_remote_read` is true, all partitions are treated as remote.
#[allow(dead_code)]
fn local_remote_read_split(
    partition_locations: Vec<PartitionLocation>,
    force_remote_read: bool,
) -> (Vec<PartitionLocation>, Vec<PartitionLocation>) {
    if !force_remote_read {
        partition_locations
            .into_iter()
            .partition(check_is_local_location)
    } else {
        (vec![], partition_locations)
    }
}

/// Partition locations split into categories for different fetch strategies.
#[derive(Debug, Default)]
struct SplitPartitionLocations {
    /// Partitions stored in memory (fastest path)
    memory: Vec<PartitionLocation>,
    /// Partitions stored on local disk
    local: Vec<PartitionLocation>,
    /// Partitions stored in object stores (S3, Azure, GCS)
    object_store: Vec<PartitionLocation>,
    /// Partitions requiring remote fetch via Flight
    remote: Vec<PartitionLocation>,
}

/// Splits partition locations into memory, local disk, object store, and remote categories.
fn split_partition_locations(
    partition_locations: Vec<PartitionLocation>,
    force_remote_read: bool,
) -> SplitPartitionLocations {
    let mut result = SplitPartitionLocations::default();

    for loc in partition_locations {
        if check_is_memory_location(&loc) {
            // Memory locations should only be read locally if they exist in this executor's
            // shuffle manager. If the partition doesn't exist locally, it means the partition
            // was written by another executor and we need to fetch it remotely via Flight.
            if check_is_local_memory_location(&loc) {
                result.memory.push(loc);
            } else {
                // Partition is in memory on another executor, fetch remotely
                debug!(
                    "Memory partition {} not found locally, will fetch remotely from executor {}",
                    loc.path, loc.executor_meta.id
                );
                result.remote.push(loc);
            }
        } else if check_is_object_store_location(&loc) {
            // Object store locations are handled via the runtime_env's registered object stores
            result.object_store.push(loc);
        } else if !force_remote_read && check_is_local_location(&loc) {
            result.local.push(loc);
        } else {
            result.remote.push(loc);
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
fn send_fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_request_num: usize,
    max_message_size: usize,
    force_remote_read: bool,
    flight_transport: bool,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
    metrics_callback: Option<Arc<dyn ShuffleReadMetricsCallback>>,
    runtime_env: Arc<RuntimeEnv>,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) = mpsc::channel(max_request_num);
    let semaphore = Arc::new(Semaphore::new(max_request_num));
    let mut spawned_tasks: Vec<SpawnedTask<()>> = vec![];

    let locations = split_partition_locations(partition_locations, force_remote_read);

    debug!(
        "memory shuffle partition count: {}, local shuffle file counts: {}, object store shuffle file count: {}, remote shuffle file count: {}.",
        locations.memory.len(),
        locations.local.len(),
        locations.object_store.len(),
        locations.remote.len()
    );

    // Read memory partitions first (fastest path)
    let response_sender_m = response_sender.clone();
    let memory_locations = locations.memory;
    spawned_tasks.push(SpawnedTask::spawn(async move {
        for p in memory_locations {
            let r = PartitionReaderEnum::Memory
                .fetch_partition(&p, max_message_size, flight_transport, None, false)
                .await;
            if let Err(e) = response_sender_m.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
        }
    }));

    // keep local shuffle files reading in serial order for memory control.
    let response_sender_c = response_sender.clone();
    let customize_endpoint_c = customize_endpoint.clone();
    let metrics_callback_c = metrics_callback.clone();
    let local_locations = locations.local;
    spawned_tasks.push(SpawnedTask::spawn(async move {
        for p in local_locations {
            let start_time = std::time::Instant::now();
            let r = PartitionReaderEnum::Local
                .fetch_partition(
                    &p,
                    max_message_size,
                    flight_transport,
                    customize_endpoint_c.clone(),
                    use_tls,
                )
                .await;

            // Record local read metrics if callback is set and read succeeded
            if r.is_ok()
                && let Some(ref callback) = metrics_callback_c
            {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                let bytes = p.partition_stats.num_bytes().unwrap_or(0);
                let rows = p.partition_stats.num_rows().unwrap_or(0);
                callback.record_local_read(
                    &p.partition_id.job_id,
                    p.partition_id.stage_id,
                    p.partition_id.partition_id,
                    &p.executor_meta.id,
                    bytes,
                    rows,
                    duration_ms,
                );
            }

            if let Err(e) = response_sender_c.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
        }
    }));

    // Read object store partitions using the RuntimeEnv's registered object stores
    let response_sender_os = response_sender.clone();
    let runtime_env_clone = Arc::clone(&runtime_env);
    let object_store_locations = locations.object_store;
    spawned_tasks.push(SpawnedTask::spawn(async move {
        for p in object_store_locations {
            let r = fetch_partition_object_store_with_runtime(
                &p,
                Arc::clone(&runtime_env_clone),
            )
            .await;

            if let Err(e) = response_sender_os.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
        }
    }));

    for p in locations.remote.into_iter() {
        let semaphore = semaphore.clone();
        let response_sender = response_sender.clone();
        let customize_endpoint_c = customize_endpoint.clone();
        let metrics_callback_c = metrics_callback.clone();
        spawned_tasks.push(SpawnedTask::spawn(async move {
            // Block if exceeds max request number.
            let permit = semaphore.acquire_owned().await.unwrap();
            let start_time = std::time::Instant::now();
            let r = PartitionReaderEnum::FlightRemote
                .fetch_partition(
                    &p,
                    max_message_size,
                    flight_transport,
                    customize_endpoint_c,
                    use_tls,
                )
                .await;

            // Record remote read metrics if callback is set and read succeeded
            if r.is_ok()
                && let Some(ref callback) = metrics_callback_c
            {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                let bytes = p.partition_stats.num_bytes().unwrap_or(0);
                let rows = p.partition_stats.num_rows().unwrap_or(0);
                callback.record_remote_read(
                    &p.partition_id.job_id,
                    p.partition_id.stage_id,
                    p.partition_id.partition_id,
                    &p.executor_meta.id,
                    bytes,
                    rows,
                    duration_ms,
                );
            }

            // Block if the channel buffer is full.
            if let Err(e) = response_sender.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
            // Increase semaphore by dropping existing permits.
            drop(permit);
        }));
    }

    AbortableReceiverStream::create(response_receiver, spawned_tasks)
}

fn check_is_local_location(location: &PartitionLocation) -> bool {
    std::path::Path::new(location.path.as_str()).exists()
}

/// Check if the partition location is stored in memory
fn check_is_memory_location(location: &PartitionLocation) -> bool {
    location.path.starts_with("memory://")
}

/// Check if a memory:// partition actually exists in the local shuffle manager.
/// This is used to determine whether to read the partition locally or fetch it remotely.
fn check_is_local_memory_location(location: &PartitionLocation) -> bool {
    if let Some(key) = location.path.strip_prefix("memory://") {
        global_shuffle_manager().contains_partition(key)
    } else {
        false
    }
}

/// Partition reader Trait, different partition reader can have
#[async_trait]
trait PartitionReader: Send + Sync + Clone {
    // Read partition data from PartitionLocation
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
        max_message_size: usize,
        flight_transport: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
        use_tls: bool,
    ) -> result::Result<SendableRecordBatchStream, BallistaError>;
}

#[derive(Clone)]
enum PartitionReaderEnum {
    Local,
    Memory,
    FlightRemote,
    #[allow(dead_code)]
    ObjectStoreRemote,
}

#[async_trait]
impl PartitionReader for PartitionReaderEnum {
    // Notice return `BallistaError::FetchFailed` will let scheduler re-schedule the task.
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
        max_message_size: usize,
        flight_transport: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
        use_tls: bool,
    ) -> result::Result<SendableRecordBatchStream, BallistaError> {
        match self {
            PartitionReaderEnum::FlightRemote => {
                fetch_partition_remote(
                    location,
                    max_message_size,
                    flight_transport,
                    customize_endpoint,
                    use_tls,
                )
                .await
            }
            PartitionReaderEnum::Local => fetch_partition_local(location).await,
            PartitionReaderEnum::Memory => fetch_partition_memory(location).await,
            PartitionReaderEnum::ObjectStoreRemote => {
                fetch_partition_object_store(location).await
            }
        }
    }
}

/// A shuffle partition with no rows is never written to disk — the writer creates
/// partition files lazily, only when a partition actually receives a batch. A fetch
/// for such a partition finds no file; that is an empty partition, not a failure.
/// Represent it as a zero-batch stream so the reducer reads no rows for it.
fn empty_partition_stream() -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::new(datafusion::arrow::datatypes::Schema::empty()),
        futures::stream::empty::<datafusion::error::Result<RecordBatch>>(),
    ))
}

async fn fetch_partition_remote(
    location: &PartitionLocation,
    max_message_size: usize,
    flight_transport: bool,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
    // TODO for shuffle client connections, we should avoid creating new connections again and again.
    // And we should also avoid to keep alive too many connections for long time.
    let host = metadata.host.as_str();
    let port = metadata.port;
    let mut ballista_client = BallistaClient::try_new(
        host,
        port,
        max_message_size,
        use_tls,
        customize_endpoint,
    )
    .await
    .map_err(|error| match error {
        // map grpc connection error to partition fetch error.
        BallistaError::GrpcConnectionError(msg) => BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            msg,
        ),
        other => other,
    })?;

    ballista_client
        .fetch_partition(
            &metadata.id,
            partition_id,
            &location.path,
            host,
            port,
            flight_transport,
        )
        .await
}

async fn fetch_partition_local(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let path = &location.path;
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
    let data_path = std::path::Path::new(path);

    // Detect format from file extension
    let is_vortex = path.ends_with(".vortex");

    if is_vortex {
        #[cfg(feature = "vortex")]
        {
            let stream = fetch_partition_local_vortex(path).map_err(|e| {
                BallistaError::FetchFailed(
                    metadata.id.clone(),
                    partition_id.stage_id,
                    partition_id.partition_id,
                    e.to_string(),
                )
            })?;
            return Ok(stream);
        }
        #[cfg(not(feature = "vortex"))]
        {
            return Err(BallistaError::General(
                "Vortex format files found but 'vortex' feature is not enabled"
                    .to_string(),
            ));
        }
    }

    // Check if this is a sort-based shuffle output (has index file)
    if is_sort_shuffle_output(data_path) {
        debug!(
            "Reading sort-based shuffle for partition {} from {:?}",
            partition_id.partition_id, data_path
        );
        let index_path = get_index_path(data_path);
        return stream_sort_shuffle_partition(
            data_path,
            &index_path,
            partition_id.partition_id,
        )
        .map_err(|e| {
            BallistaError::FetchFailed(
                metadata.id.clone(),
                partition_id.stage_id,
                partition_id.partition_id,
                e.to_string(),
            )
        });
    }

    // A 0-row partition is never written to disk (the writer creates partition
    // files lazily), so a missing file here means an empty partition, not a
    // failure. Return an empty stream rather than failing the whole stage.
    if !data_path.exists() {
        return Ok(empty_partition_stream());
    }

    // Standard hash-based shuffle - read the file directly
    let reader = fetch_partition_local_arrow(path).map_err(|e| {
        BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            e.to_string(),
        )
    })?;
    Ok(Box::pin(LocalShuffleStream::new(reader)))
}

/// Fetch partition from local Arrow IPC file
fn fetch_partition_local_arrow(
    path: &str,
) -> result::Result<StreamReader<BufReader<File>>, BallistaError> {
    let file = File::open(path).map_err(|e| {
        BallistaError::General(format!("Failed to open partition file at {path}: {e:?}"))
    })?;
    let file = BufReader::new(file);
    // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
    let reader = unsafe {
        StreamReader::try_new(file, None)
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to create Arrow IPC reader at {path}: {e:?}"
                ))
            })?
            .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
    };
    Ok(reader)
}

/// Fetch partition from local Vortex file
#[cfg(feature = "vortex")]
fn fetch_partition_local_vortex(
    path: &str,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    use super::vortex_shuffle::LocalVortexShuffleStream;

    // Vortex IPC format is self-describing, but we need a schema for the stream interface.
    // For now, use an empty schema - the actual data schema will come from the Vortex arrays.
    // TODO: Consider storing schema metadata in the Vortex file or a sidecar file.
    let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty());

    // Create the stream - it handles reading and converting Vortex arrays to Arrow
    let stream = LocalVortexShuffleStream::try_new(path, schema)?;
    Ok(Box::pin(stream))
}

/// Fetch partition data from in-memory shuffle storage.
///
/// After successfully fetching the data, the partition is removed from memory
/// to allow for immediate memory reclamation. This is safe because each shuffle
/// partition is typically read only once by the consuming stage.
async fn fetch_partition_memory(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let path = &location.path;
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;

    // Extract the key from the "memory://{key}" path format
    let key = path.strip_prefix("memory://").ok_or_else(|| {
        BallistaError::General(format!("Invalid in-memory partition path format: {path}"))
    })?;

    let shuffle_manager = global_shuffle_manager();

    // Remove and retrieve the partition data in one atomic operation
    // This ensures the memory is reclaimed as soon as the data is read
    let data = shuffle_manager
        .remove_partition(key)
        .ok_or_else(|| {
            // If remove fails, try a regular get (for retry scenarios)
            shuffle_manager.get_partition(key).map_err(|e| {
                BallistaError::FetchFailed(
                    metadata.id.clone(),
                    partition_id.stage_id,
                    partition_id.partition_id,
                    e.to_string(),
                )
            })
        })
        .or_else(|result| result)?;

    debug!(
        "Fetched and removed partition {} from memory: {} batches, {} rows",
        key, data.num_batches, data.num_rows
    );

    let batches = data.to_batches().map_err(|e| {
        BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            format!("Failed to convert in-memory partition to batches: {e}"),
        )
    })?;

    Ok(Box::pin(InMemoryShuffleStream::new(data.schema, batches)))
}

/// Stream that reads from in-memory shuffle data
struct InMemoryShuffleStream {
    schema: SchemaRef,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl InMemoryShuffleStream {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }
}

impl Stream for InMemoryShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.batches.next() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for InMemoryShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Fetch partition from object store using the RuntimeEnv's registered object stores.
/// This uses the credentials and configuration from the runtime environment.
///
/// This implementation streams data from the object store and decodes record batches
/// incrementally using Arrow's `StreamDecoder`, avoiding buffering the entire partition
/// in memory.
async fn fetch_partition_object_store_with_runtime(
    location: &PartitionLocation,
    runtime_env: Arc<RuntimeEnv>,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    use object_store::path::Path as ObjectPath;

    let path = &location.path;
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;

    debug!("Fetching shuffle partition from object store using runtime_env: {path}");

    let url = Url::parse(path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to parse object store URL '{path}': {e:?}"
        ))
    })?;

    // Get the object store from the RuntimeEnv's registry
    // This uses the credentials configured in the runtime (e.g., SpiceObjectStoreRegistry)
    let object_store_url = ObjectStoreUrl::parse(&url).map_err(|e| {
        BallistaError::General(format!(
            "Failed to parse object store URL '{path}': {e:?}"
        ))
    })?;

    let store = runtime_env.object_store(&object_store_url).map_err(|e| {
        BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            format!("Failed to get object store for URL '{path}': {e:?}"),
        )
    })?;

    // Extract the object path from the URL
    let object_path = ObjectPath::from(url.path().trim_start_matches('/'));

    debug!("Reading object from path: {object_path:?}");

    let get_result = store.get(&object_path).await.map_err(|e| {
        BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            format!("Failed to read object from {path}: {e:?}"),
        )
    })?;

    // Convert to a streaming byte stream instead of loading all bytes into memory
    let byte_stream = get_result.into_stream();

    // Create the streaming decoder
    let stream = ObjectStoreShuffleStream::try_new(byte_stream, path.clone()).await?;

    Ok(Box::pin(stream))
}

/// Streams a shuffle / result partition directly from object store. Used by the
/// driver-side final-stage fetch in `distributed_query` (via
/// [`crate::client::BallistaClient::fetch_partition`]), which has no
/// `RuntimeEnv` on hand and only knows the path and identifiers, so this helper
/// takes plain args and builds the client from the environment (same
/// credentials the writer side reads).
///
/// Streams the response body so partitions are decoded incrementally rather than
/// buffered in memory.
pub(crate) async fn fetch_object_store_partition_stream(
    path: &str,
    executor_id: &str,
    stage_id: usize,
    partition_id: usize,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    use object_store::path::Path as ObjectPath;

    debug!("Fetching shuffle partition from object store: {path}");

    let url = Url::parse(path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to parse object store URL '{path}': {e:?}"
        ))
    })?;

    let store = build_shuffle_object_store(&url).map_err(|e| {
        BallistaError::FetchFailed(
            executor_id.to_owned(),
            stage_id,
            partition_id,
            format!("Failed to build object store client for '{path}': {e:?}"),
        )
    })?;

    let object_path = ObjectPath::from(url.path().trim_start_matches('/'));

    debug!("Reading object from path: {object_path:?}");

    let get_result = store.get(&object_path).await.map_err(|e| {
        BallistaError::FetchFailed(
            executor_id.to_owned(),
            stage_id,
            partition_id,
            format!("Failed to read object from {path}: {e:?}"),
        )
    })?;

    let byte_stream = get_result.into_stream();
    let stream = ObjectStoreShuffleStream::try_new(byte_stream, path.to_owned()).await?;

    Ok(Box::pin(stream))
}

/// Builds an S3 object store client for a shuffle URL. Credentials come from
/// the environment (same as the writer in practice). Non-S3 schemes return an
/// error — Azure shuffle is on the same architectural footing but its writer-
/// side path doesn't go through this code today; rather than add a divergent
/// reader, we leave it to the registry-based follow-up to handle both.
fn build_shuffle_object_store(
    url: &Url,
) -> result::Result<Arc<dyn ObjectStore>, BallistaError> {
    let scheme = url.scheme();
    match scheme {
        "s3" => {
            let bucket = url.host_str().ok_or_else(|| {
                BallistaError::General(format!("No bucket in S3 URL: {url}"))
            })?;
            let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
            let store = builder.build().map_err(|e| {
                BallistaError::General(format!("Failed to create S3 client: {e:?}"))
            })?;
            Ok(Arc::new(store))
        }
        _ => Err(BallistaError::General(format!(
            "Unsupported object store scheme for shuffle reader: {scheme}. Only 's3' is supported."
        ))),
    }
}

/// Maximum length of message with schema definition for object store streaming.
const OBJECT_STORE_MAX_SCHEMA_BUFFER_SIZE: usize = 8_388_608;

/// A streaming reader for Arrow IPC data from object stores.
///
/// This stream incrementally decodes record batches as data chunks arrive from
/// the object store, avoiding the need to buffer the entire partition in memory.
/// It uses Arrow's `StreamDecoder` to decode IPC messages from the byte stream.
struct ObjectStoreShuffleStream {
    /// The Arrow IPC stream decoder
    decoder: datafusion::arrow::ipc::reader::StreamDecoder,
    /// Buffer holding partially received IPC messages
    state_buffer: datafusion::arrow::buffer::Buffer,
    /// The underlying byte stream from the object store
    byte_stream: Pin<Box<dyn Stream<Item = object_store::Result<bytes::Bytes>> + Send>>,
    /// The schema of the data being streamed
    schema: SchemaRef,
    /// Path for error messages
    path: String,
}

impl ObjectStoreShuffleStream {
    /// Creates a new `ObjectStoreShuffleStream` from an object store byte stream.
    ///
    /// This reads the schema from the stream header and initializes the decoder.
    async fn try_new(
        byte_stream: impl Stream<Item = object_store::Result<bytes::Bytes>> + Send + 'static,
        path: String,
    ) -> result::Result<Self, BallistaError> {
        use datafusion::arrow::buffer::Buffer;
        use datafusion::arrow::ipc::convert::try_schema_from_ipc_buffer;
        use datafusion::arrow::ipc::reader::StreamDecoder;

        let mut byte_stream: Pin<
            Box<dyn Stream<Item = object_store::Result<bytes::Bytes>> + Send>,
        > = Box::pin(byte_stream);
        let mut state_buffer = Buffer::default();

        // Read chunks until we have enough data to parse the schema
        loop {
            if state_buffer.len() > OBJECT_STORE_MAX_SCHEMA_BUFFER_SIZE {
                return Err(BallistaError::General(format!(
                    "Schema buffer length exceeded maximum buffer size for {path}, \
                    expected {} actual: {}",
                    OBJECT_STORE_MAX_SCHEMA_BUFFER_SIZE,
                    state_buffer.len()
                )));
            }

            match byte_stream.next().await {
                Some(Ok(blob)) => {
                    state_buffer = Self::combine_buffers(&state_buffer, &blob);

                    match try_schema_from_ipc_buffer(state_buffer.as_slice()) {
                        Ok(schema) => {
                            return Ok(Self {
                                decoder: StreamDecoder::new(),
                                state_buffer,
                                byte_stream,
                                schema: Arc::new(schema),
                                path,
                            });
                        }
                        Err(datafusion::arrow::error::ArrowError::ParseError(_)) => {
                            // Parse errors are ignored as we may not have received the
                            // whole message yet, so the schema cannot be extracted
                        }
                        Err(e) => {
                            return Err(BallistaError::General(format!(
                                "Failed to parse schema from {path}: {e:?}"
                            )));
                        }
                    }
                }
                Some(Err(e)) => {
                    return Err(BallistaError::General(format!(
                        "Error reading from object store {path}: {e:?}"
                    )));
                }
                None => {
                    return Err(BallistaError::General(format!(
                        "Premature end of stream while reading schema from {path}"
                    )));
                }
            }
        }
    }

    fn combine_buffers(
        first: &datafusion::arrow::buffer::Buffer,
        second: &bytes::Bytes,
    ) -> datafusion::arrow::buffer::Buffer {
        use datafusion::arrow::buffer::MutableBuffer;
        let mut combined = MutableBuffer::new(first.len() + second.len());
        combined.extend_from_slice(first.as_slice());
        combined.extend_from_slice(second);
        combined.into()
    }

    fn decode(
        &mut self,
    ) -> result::Result<Option<RecordBatch>, datafusion::arrow::error::ArrowError> {
        self.decoder.decode(&mut self.state_buffer)
    }

    fn extend_bytes(&mut self, blob: bytes::Bytes) {
        self.state_buffer = Self::combine_buffers(&self.state_buffer, &blob);
    }
}

impl Stream for ObjectStoreShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // First, try to decode a batch from the current buffer
        match self.decode() {
            Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
            Ok(None) => {
                // No complete batch in buffer, need more data
            }
            Err(e) => {
                return Poll::Ready(Some(Err(DataFusionError::ArrowError(
                    Box::new(e),
                    None,
                ))));
            }
        }

        // Poll the underlying byte stream for more data
        match self.byte_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(blob))) => {
                self.extend_bytes(blob);

                // Try to decode again with the new data
                match self.decode() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        // Still not enough data, wake ourselves to poll again
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Some(Err(DataFusionError::ArrowError(
                        Box::new(e),
                        None,
                    )))),
                }
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DataFusionError::External(
                    format!("Error reading from object store {}: {e:?}", self.path)
                        .into(),
                ))))
            }
            Poll::Ready(None) => {
                // End of stream - try one more decode in case there's remaining data
                match self.decode() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(DataFusionError::ArrowError(
                        Box::new(e),
                        None,
                    )))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ObjectStoreShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns true when the given path is an object-store URL the streaming shuffle
/// reader can handle. Exposed so non-shuffle-reader callers (e.g. the driver-side
/// final-stage fetch via [`crate::client::BallistaClient::fetch_partition`]) can
/// route around the gRPC `FetchPartition` path, which only understands local files
/// and `memory://`.
///
/// Scoped to `s3://` to match [`build_shuffle_object_store`]: the writer-side
/// `ObjectStoreShuffleStorage::new_azure` exists but is not wired through the
/// registry-based credential path yet, so `abfs://` / `az://` / `gs://` URLs would
/// be routed away from gRPC and then fail in `build_shuffle_object_store`.
/// Broaden this alongside `build_shuffle_object_store` once those backends are
/// supported.
pub(crate) fn path_is_object_store(path: &str) -> bool {
    path.starts_with("s3://")
}

/// Check if the location is an object store path (S3 or Azure).
fn check_is_object_store_location(location: &PartitionLocation) -> bool {
    let path = location.path.as_str();
    path.starts_with("s3://")
        || path.starts_with("abfs://")
        || path.starts_with("az://")
        || path.starts_with("gs://")
}

async fn fetch_partition_object_store(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    let path = &location.path;
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;

    debug!("Fetching shuffle partition from object store: {path}");

    let batches = fetch_partition_object_store_inner(path)
        .await
        .map_err(|e| {
            // return BallistaError::FetchFailed may let scheduler retry this task.
            BallistaError::FetchFailed(
                metadata.id.clone(),
                partition_id.stage_id,
                partition_id.partition_id,
                e.to_string(),
            )
        })?;

    if batches.is_empty() {
        return Err(BallistaError::General(format!(
            "No batches found in shuffle partition at {path}"
        )));
    }

    let schema = batches[0].schema();
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

async fn fetch_partition_object_store_inner(
    path: &str,
) -> result::Result<Vec<RecordBatch>, BallistaError> {
    use object_store::path::Path as ObjectPath;

    let url = Url::parse(path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to parse object store URL '{path}': {e:?}"
        ))
    })?;

    let scheme = url.scheme();
    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" => {
            let bucket = url.host_str().ok_or_else(|| {
                BallistaError::General(format!("No bucket in S3 URL: {path}"))
            })?;
            let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
            Arc::new(builder.build().map_err(|e| {
                BallistaError::General(format!("Failed to create S3 client: {e:?}"))
            })?)
        }
        "abfs" | "az" => {
            // Parse Azure URL: abfs://container@account.dfs.core.windows.net/path
            let host = url.host_str().ok_or_else(|| {
                BallistaError::General(format!("No host in Azure URL: {path}"))
            })?;

            // Extract container from username portion
            let container = url.username();
            if container.is_empty() {
                return Err(BallistaError::General(format!(
                    "No container in Azure URL. Expected format: abfs://container@account.dfs.core.windows.net/path. Got: {path}"
                )));
            }

            // Extract account from host (account.dfs.core.windows.net)
            let account = host.split('.').next().ok_or_else(|| {
                BallistaError::General(format!("No account in Azure URL: {path}"))
            })?;

            let builder = MicrosoftAzureBuilder::from_env()
                .with_account(account)
                .with_container_name(container);
            Arc::new(builder.build().map_err(|e| {
                BallistaError::General(format!("Failed to create Azure client: {e:?}"))
            })?)
        }
        _ => {
            return Err(BallistaError::General(format!(
                "Unsupported object store scheme: {scheme}. Supported: s3, abfs, az"
            )));
        }
    };

    // Extract the object path from the URL
    let object_path = ObjectPath::from(url.path().trim_start_matches('/'));

    debug!("Reading object from path: {object_path:?}");

    let get_result = store.get(&object_path).await.map_err(|e| {
        BallistaError::General(format!("Failed to read object from {path}: {e:?}"))
    })?;

    let bytes = get_result.bytes().await.map_err(|e| {
        BallistaError::General(format!("Failed to read bytes from {path}: {e:?}"))
    })?;

    let cursor = Cursor::new(bytes.to_vec());
    let stream_reader = StreamReader::try_new(cursor, None).map_err(|e| {
        BallistaError::General(format!(
            "Failed to create Arrow stream reader for {path}: {e:?}"
        ))
    })?;

    let mut batches = Vec::new();
    for batch_result in stream_reader {
        batches.push(batch_result.map_err(|e| {
            BallistaError::General(format!("Failed to read batch from {path}: {e:?}"))
        })?);
    }

    Ok(batches)
}

struct CoalescedShuffleReaderStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    coalescer: LimitedBatchCoalescer,
    completed: bool,
    baseline_metrics: BaselineMetrics,
}

impl CoalescedShuffleReaderStream {
    pub fn new(
        input: SendableRecordBatchStream,
        batch_size: usize,
        limit: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let schema = input.schema();
        Self {
            schema: schema.clone(),
            input,
            coalescer: LimitedBatchCoalescer::new(schema, batch_size, limit),
            completed: false,
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        }
    }
}

impl Stream for CoalescedShuffleReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        loop {
            // If there is already a completed batch ready, return it directly
            if let Some(batch) = self.coalescer.next_completed_batch() {
                self.baseline_metrics.record_output(batch.num_rows());
                return Poll::Ready(Some(Ok(batch)));
            }

            // If the upstream is completed, then it is completed for this stream too
            if self.completed {
                return Poll::Ready(None);
            }

            // Pull from upstream
            match ready!(self.input.poll_next_unpin(cx)) {
                // If upstream is completed, then flush remaning buffered batches
                None => {
                    self.completed = true;
                    if let Err(e) = self.coalescer.finish() {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                // If upstream is not completed, then push to coalescer
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        // Try to push to coalescer
                        match self.coalescer.push_batch(batch) {
                            // If push is successful, then continue
                            Ok(PushBatchStatus::Continue) => {
                                continue;
                            }
                            // If limit is reached, then finish coalescer and set completed to true
                            Ok(PushBatchStatus::LimitReached) => {
                                self.completed = true;
                                if let Err(e) = self.coalescer.finish() {
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

impl RecordBatchStream for CoalescedShuffleReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::ShuffleWriterExec;
    use crate::serde::scheduler::{ExecutorMetadata, ExecutorSpecification, PartitionId};
    use crate::utils;
    use datafusion::arrow::array::{Int32Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common;

    use datafusion::prelude::SessionContext;
    use tempfile::{TempDir, tempdir};

    #[tokio::test]
    async fn test_stats_for_partitions_empty() {
        let result = stats_for_partitions(0, std::iter::empty());

        let exptected = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_full() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: Some(4),
                num_bytes: Some(65),
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Exact(14),
            total_byte_size: Precision::Exact(149),
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_missing() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: None,
                num_bytes: None,
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }
    #[tokio::test]
    async fn test_stats_for_partition_statistics_no_specific_partition() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(None)?;
        assert_eq!(8, *stats.num_rows.get_value().unwrap());
        assert_eq!(80, *stats.total_byte_size.get_value().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_for_partition_statistics_specific_partition() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(Some(3))?;
        assert_eq!(2, *stats.num_rows.get_value().unwrap());
        assert_eq!(20, *stats.total_byte_size.get_value().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_for_partition_statistics_specific_partition_out_of_range()
    -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(Some(4));
        assert!(stats.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_partitions_error_mapping() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: Default::default(),
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;
        let mut stream = shuffle_reader_exec.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream).await;

        assert!(batches.is_err());

        // BallistaError::FetchFailed -> ArrowError::ExternalError -> ballistaError::FetchFailed
        let ballista_error = batches.unwrap_err();
        assert!(matches!(
            ballista_error,
            BallistaError::FetchFailed(_, _, _, _)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_1() {
        test_send_fetch_partitions(1, 10).await;
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_n() {
        test_send_fetch_partitions(4, 10).await;
    }

    #[tokio::test]
    async fn test_read_local_shuffle() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let work_dir = TempDir::new().unwrap();
        let input = ShuffleWriterExec::try_new(
            "local_file".to_owned(),
            1,
            create_test_data_plan().unwrap(),
            work_dir.path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1)),
        )
        .unwrap();

        let mut stream = input.execute(0, task_ctx).unwrap();

        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        let path = batches[0].columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // from to input partitions test the first one with two batches
        let file_path = path.value(0);
        let reader = fetch_partition_local_arrow(file_path).unwrap();

        let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
            async { Box::pin(LocalShuffleStream::new(reader)) }.await;

        let result = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        assert_eq!(result.len(), 2);
        for b in result {
            assert_eq!(b, create_test_batch())
        }
    }

    // tests if force remote read configuration option will
    // qualify all partitions as remote
    #[tokio::test]
    async fn test_remote_local_read() {
        let schema = get_test_partition_schema();
        let data_array = Int32Array::from(vec![1]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
                .unwrap();
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("shuffle_data");
        let file = File::create(&file_path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let partition_locations =
            get_test_partition_locations(1, file_path.to_str().unwrap().to_string());

        let (local, remote) = local_remote_read_split(partition_locations.clone(), false);

        assert!(!local.is_empty());
        assert!(remote.is_empty());

        let (local, remote) = local_remote_read_split(partition_locations, true);

        assert!(local.is_empty());
        assert!(!remote.is_empty());
    }

    async fn test_send_fetch_partitions(max_request_num: usize, partition_num: usize) {
        let schema = get_test_partition_schema();
        let data_array = Int32Array::from(vec![1]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
                .unwrap();
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("shuffle_data");
        let file = File::create(&file_path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let partition_locations = get_test_partition_locations(
            partition_num,
            file_path.to_str().unwrap().to_string(),
        );

        let response_receiver = send_fetch_partitions(
            partition_locations,
            max_request_num,
            4 * 1024 * 1024,
            false,
            true,
            None,
            false,
            None, // No metrics callback in tests
            Arc::new(RuntimeEnv::default()),
        );

        let stream = RecordBatchStreamAdapter::new(
            Arc::new(schema),
            response_receiver.try_flatten(),
        );

        let result = common::collect(Box::pin(stream)).await.unwrap();
        assert_eq!(partition_num, result.len());
    }

    fn get_test_partition_locations(n: usize, path: String) -> Vec<PartitionLocation> {
        (0..n)
            .map(|partition_id| PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: "job".to_string(),
                    stage_id: 1,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: format!("exec{partition_id}"),
                    host: "localhost".to_string(),
                    port: 50051,
                    grpc_port: 50052,
                    specification: ExecutorSpecification { task_slots: 12 },
                },
                partition_stats: Default::default(),
                path: path.clone(),
            })
            .collect()
    }

    fn get_test_partition_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    // create two partitions each has two same batches
    fn create_test_data_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let batch = create_test_batch();
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            create_test_schema(),
            None,
        )?);

        Ok(Arc::new(DataSourceExec::new(memory_data_source)))
    }

    fn create_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("rust"),
                    Some("datafusion"),
                    Some("ballista"),
                ])),
            ],
        )
        .unwrap()
    }

    fn create_custom_test_batch(rows: usize) -> RecordBatch {
        let schema = create_test_schema();

        // 1. Create number column (0, 1, 2, ..., rows-1)
        let number_vec: Vec<u32> = (0..rows as u32).collect();
        let number_array = UInt32Array::from(number_vec);

        // 2. Create string column ("s0", "s1", ..., "s{rows-1}")
        // Just to fill data, the content is not important
        let string_vec: Vec<String> = (0..rows).map(|i| format!("s{}", i)).collect();
        let string_array = StringArray::from(string_vec);

        RecordBatch::try_new(schema, vec![Arc::new(number_array), Arc::new(string_array)])
            .unwrap()
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt32, true),
            Field::new("str", DataType::Utf8, true),
        ]))
    }

    /// Test that ObjectStoreShuffleStream correctly decodes Arrow IPC data
    /// delivered in chunks, simulating streaming from an object store.
    #[tokio::test]
    async fn test_object_store_shuffle_stream() {
        use bytes::Bytes;
        use datafusion::arrow::ipc::writer::StreamWriter;

        // Create test batches
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(StringArray::from(vec![Some("d"), None, Some("f")])),
            ],
        )
        .unwrap();

        // Write batches to IPC format in memory
        let mut ipc_data = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_data, &schema).unwrap();
            writer.write(&batch1).unwrap();
            writer.write(&batch2).unwrap();
            writer.finish().unwrap();
        }

        // Split IPC data into small chunks to simulate streaming
        let chunk_size = 64; // Small chunks to test incremental decoding
        let chunks: Vec<Bytes> = ipc_data
            .chunks(chunk_size)
            .map(Bytes::copy_from_slice)
            .collect();

        // Create a stream that yields chunks with small delays to simulate network
        let byte_stream =
            futures::stream::iter(chunks.into_iter().map(Ok::<_, object_store::Error>));

        // Create the ObjectStoreShuffleStream
        let mut stream =
            ObjectStoreShuffleStream::try_new(byte_stream, "test://path".to_string())
                .await
                .expect("Failed to create ObjectStoreShuffleStream");

        // Verify the schema was correctly parsed
        assert_eq!(stream.schema().fields().len(), 2);
        assert_eq!(stream.schema().field(0).name(), "a");
        assert_eq!(stream.schema().field(1).name(), "b");

        // Collect all batches from the stream
        let mut collected_batches = Vec::new();
        while let Some(result) = stream.next().await {
            collected_batches.push(result.expect("Failed to read batch"));
        }

        // Verify we got both batches
        assert_eq!(collected_batches.len(), 2);

        // Verify batch1 contents
        assert_eq!(collected_batches[0].num_rows(), 3);
        let col_a = collected_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col_a.values(), &[1, 2, 3]);

        // Verify batch2 contents
        assert_eq!(collected_batches[1].num_rows(), 3);
        let col_a = collected_batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col_a.values(), &[4, 5, 6]);
    }

    /// Test ObjectStoreShuffleStream with single-byte chunks (extreme fragmentation)
    #[tokio::test]
    async fn test_object_store_shuffle_stream_single_byte_chunks() {
        use bytes::Bytes;
        use datafusion::arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![42, 43, 44]))],
        )
        .unwrap();

        // Write to IPC format
        let mut ipc_data = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_data, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Split into single-byte chunks (extreme case)
        let chunks: Vec<Bytes> = ipc_data.iter().map(|&b| Bytes::from(vec![b])).collect();

        let byte_stream =
            futures::stream::iter(chunks.into_iter().map(Ok::<_, object_store::Error>));

        let mut stream =
            ObjectStoreShuffleStream::try_new(byte_stream, "test://single".to_string())
                .await
                .expect("Failed to create stream with single-byte chunks");

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.expect("Failed to read batch");
            count += 1;
        }
        assert_eq!(count, 1);
    }

    /// Test ObjectStoreShuffleStream handles errors from the byte stream
    #[tokio::test]
    async fn test_object_store_shuffle_stream_error_handling() {
        use bytes::Bytes;
        use datafusion::arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Write to IPC format
        let mut ipc_data = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_data, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Create chunks but inject an error partway through
        let chunk_size = 64;
        let chunks: Vec<_> = ipc_data.chunks(chunk_size).collect();
        let mid = chunks.len() / 2;

        let items: Vec<object_store::Result<Bytes>> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == mid {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: "simulated error".into(),
                    })
                } else {
                    Ok(Bytes::copy_from_slice(c))
                }
            })
            .collect();

        let byte_stream = futures::stream::iter(items);

        // The stream creation might fail if the error occurs before schema is parsed,
        // or reading might fail later
        let stream_result =
            ObjectStoreShuffleStream::try_new(byte_stream, "test://error".to_string())
                .await;

        // Either creation fails or reading fails - both are acceptable
        match stream_result {
            Err(_) => {
                // Error during schema parsing is fine
            }
            Ok(mut stream) => {
                // Error should occur while reading batches
                let mut found_error = false;
                while let Some(result) = stream.next().await {
                    if result.is_err() {
                        found_error = true;
                        break;
                    }
                }
                assert!(found_error, "Expected an error while reading batches");
            }
        }
    }

    use datafusion::physical_plan::memory::MemoryStream;

    #[tokio::test]
    async fn test_coalesce_stream_logic() -> Result<()> {
        // 1. Create test data - 10 small batches, each with 3 rows
        let schema = create_test_schema();
        let small_batch = create_test_batch();
        let batches = vec![small_batch.clone(); 10];

        // 2. Create mock upstream stream (Input Stream)
        let input_stream = MemoryStream::try_new(batches, schema.clone(), None)?;
        let input_stream = Box::pin(input_stream) as SendableRecordBatchStream;

        // 3. Configure Coalescer: target batch size to 10 rows
        let target_batch_size = 10;

        // 4. Manually build the CoalescedShuffleReaderStream
        let coalesced_stream = CoalescedShuffleReaderStream::new(
            input_stream,
            target_batch_size,
            None,
            &ExecutionPlanMetricsSet::new(),
            0,
        );

        // 5. Execute stream and collect results
        let output_batches = common::collect(Box::pin(coalesced_stream)).await?;

        // 6. Assertions
        // Assert A: Data total not lost (30 rows)
        let total_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 30);

        // Assert B: Batch count reduced (10 -> 3)
        assert_eq!(output_batches.len(), 3);

        // Assert C: Each batch size is correct (all should be 10)
        assert_eq!(output_batches[0].num_rows(), 10);
        assert_eq!(output_batches[1].num_rows(), 10);
        assert_eq!(output_batches[2].num_rows(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_coalesce_stream_remainder_flush() -> Result<()> {
        let schema = create_test_schema();
        // Create 10 small batch, each with 3 rows. Total 30 rows.
        let small_batch = create_test_batch();
        let batches = vec![small_batch.clone(); 10];

        let input_stream = MemoryStream::try_new(batches, schema.clone(), None)?;
        let input_stream = Box::pin(input_stream) as SendableRecordBatchStream;

        // Target set to 100 rows.
        // Because 30 < 100, it can never be filled. Must depend on the `finish()` mechanism to flush out these 30 rows at the end of the stream.
        let target_batch_size = 100;

        let coalesced_stream = CoalescedShuffleReaderStream::new(
            input_stream,
            target_batch_size,
            None,
            &ExecutionPlanMetricsSet::new(),
            0,
        );

        let output_batches = common::collect(Box::pin(coalesced_stream)).await?;

        // Assertions
        assert_eq!(output_batches.len(), 1); // Should only have 1 batch
        assert_eq!(output_batches[0].num_rows(), 30); // Should contain all 30 rows

        Ok(())
    }

    #[tokio::test]
    async fn test_coalesce_stream_large_batch() -> Result<()> {
        let schema = create_test_schema();

        // 1. Create a large batch (20 rows)
        let big_batch = create_custom_test_batch(20);
        let batches = vec![big_batch.clone(); 10]; // Total 200 rows

        let input_stream = MemoryStream::try_new(batches, schema.clone(), None)?;
        let input_stream = Box::pin(input_stream) as SendableRecordBatchStream;

        // 2. Target set to small size, 10 rows
        let target_batch_size = 10;

        let coalesced_stream = CoalescedShuffleReaderStream::new(
            input_stream,
            target_batch_size,
            None,
            &ExecutionPlanMetricsSet::new(),
            0,
        );

        let output_batches = common::collect(Box::pin(coalesced_stream)).await?;

        // 3. Validation: It should not split the large batch, but directly output it
        // Coalescer will not split the batch if size > (max_batch_size / 2)
        assert_eq!(output_batches.len(), 10);
        assert_eq!(output_batches[0].num_rows(), 20);

        Ok(())
    }

    use futures::stream;

    #[tokio::test]
    async fn test_coalesce_stream_error_propagation() -> Result<()> {
        let schema = create_test_schema();
        let small_batch = create_test_batch(); // 3行

        // 1. Construct a stream with error
        let batches = vec![
            Ok(small_batch),
            Err(DataFusionError::Execution(
                "Network connection failed".to_string(),
            )),
        ];

        // 2. Construct a stream with error
        let stream = stream::iter(batches);
        let input_stream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream));

        // 3. Configure Coalescer
        let target_batch_size = 10;

        let coalesced_stream = CoalescedShuffleReaderStream::new(
            input_stream,
            target_batch_size,
            None,
            &ExecutionPlanMetricsSet::new(),
            0,
        );

        // 4. Execute stream
        let result = common::collect(Box::pin(coalesced_stream)).await;

        // 5. Validation
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Network connection failed")
        );

        Ok(())
    }

    /// `build_shuffle_object_store` must accept an S3 URL that includes a path
    /// component (the shuffle key) — earlier versions of the reader funneled
    /// the full URL through `ObjectStoreUrl::parse`, which rejected anything
    /// beyond scheme+authority and failed with
    /// "ObjectStoreUrl must only contain scheme and authority".
    #[test]
    fn test_build_shuffle_object_store_accepts_s3_with_path() {
        let url = Url::parse("s3://my-bucket/shuffle/job-id/1/4i1vaNv/1/0/data-35.arrow")
            .unwrap();

        build_shuffle_object_store(&url)
            .expect("a fully-qualified S3 shuffle URL must build a client");
    }

    #[test]
    fn test_build_shuffle_object_store_rejects_non_s3_schemes() {
        for url in [
            "file:///tmp/shuffle/data.arrow",
            "abfs://container@account.dfs.core.windows.net/data.arrow",
            "az://container/data.arrow",
            "gs://bucket/data.arrow",
        ] {
            let parsed = Url::parse(url).unwrap();
            assert!(
                build_shuffle_object_store(&parsed).is_err(),
                "expected {url} to be rejected — only s3 is supported on the reader today"
            );
        }
    }
}
