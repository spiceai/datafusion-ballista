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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format (default) or Vortex format.
//! The shuffle format is configurable. Future stages of the query will use the ShuffleReaderExec
//! to read these results.

use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;

use datafusion::arrow::ipc::writer::StreamWriter;
use std::any::Any;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::future::Future;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::config::ShuffleFormat;
use crate::error::BallistaError;
use crate::execution_plans::shuffle_manager::{
    InMemoryShuffleManager, ShufflePartitionData, global_shuffle_manager,
};
use crate::extension::SessionConfigExt;
use crate::shuffle_storage::ShuffleStorageType;
use crate::utils;

use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::PartitionStats;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};

use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, displayable,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};

use datafusion::arrow::error::ArrowError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use log::{debug, info};

use super::shuffle_writer_trait::ShuffleWriter;

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Optional shuffle output partitioning.
    /// If it's none, it means there's no need to do repartitioning.
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: Arc<PlanProperties>,
}

impl std::fmt::Display for ShuffleWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let printable_plan = displayable(self.plan.as_ref())
            .set_show_statistics(true)
            .indent(false);
        write!(
            f,
            "ShuffleWriterExec: job={} stage={} work_dir={} partitioning={:?} plan: \n {}",
            self.job_id,
            self.stage_id,
            self.work_dir,
            self.shuffle_output_partitioning,
            printable_plan
        )
    }
}

/// Writer for Arrow IPC format
pub struct ArrowIpcWriter {
    writer: StreamWriter<File>,
}

impl ArrowIpcWriter {
    pub fn try_new(
        file: File,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Self> {
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = StreamWriter::try_new_with_options(file, schema, options)?;
        Ok(Self { writer })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish()?;
        Ok(())
    }
}

/// Format-agnostic shuffle writer enum
pub enum ShuffleFileWriter {
    ArrowIpc(ArrowIpcWriter),
    #[cfg(feature = "vortex")]
    Vortex(super::vortex_shuffle::VortexWriteTracker),
}

impl ShuffleFileWriter {
    pub fn try_new_arrow_ipc(
        path: PathBuf,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Self> {
        let file = File::create(&path)?;
        Ok(Self::ArrowIpc(ArrowIpcWriter::try_new(file, schema)?))
    }

    #[cfg(feature = "vortex")]
    pub fn try_new_vortex(
        path: PathBuf,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<Self> {
        let tracker = super::vortex_shuffle::VortexWriteTracker::try_new(path, schema)?;
        Ok(Self::Vortex(tracker))
    }

    pub fn try_new(
        path: PathBuf,
        schema: datafusion::arrow::datatypes::SchemaRef,
        format: ShuffleFormat,
    ) -> Result<Self> {
        match format {
            ShuffleFormat::ArrowIpc => Self::try_new_arrow_ipc(path, schema.as_ref()),
            #[cfg(feature = "vortex")]
            ShuffleFormat::Vortex => Self::try_new_vortex(path, schema),
            #[cfg(not(feature = "vortex"))]
            ShuffleFormat::Vortex => Err(DataFusionError::NotImplemented(
                "Vortex format requires the 'vortex' feature to be enabled".to_string(),
            )),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::ArrowIpc(w) => w.write(batch),
            #[cfg(feature = "vortex")]
            Self::Vortex(w) => w.write(batch),
        }
    }

    pub fn finish(self) -> Result<()> {
        match self {
            Self::ArrowIpc(mut w) => w.finish(),
            #[cfg(feature = "vortex")]
            Self::Vortex(w) => w.finish(),
        }
    }
}

/// Tracks write progress for a partition
pub struct WriteTracker {
    pub num_batches: usize,
    pub num_rows: usize,
    pub writer: ShuffleFileWriter,
    pub path: PathBuf,
}

/// Tracker for in-memory shuffle writes.
/// Collects record batches in memory instead of writing to disk.
pub struct InMemoryWriteTracker {
    pub num_batches: usize,
    pub num_rows: usize,
    pub num_bytes: usize,
    pub batches: Vec<RecordBatch>,
    pub key: String,
}

#[derive(Debug, Clone)]
struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    write_time: metrics::Time,
    repart_time: metrics::Time,
    input_rows: metrics::Count,
    output_rows: metrics::Count,
}

impl ShuffleWriteMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let write_time = MetricBuilder::new(metrics).subset_time("write_time", partition);
        let repart_time =
            MetricBuilder::new(metrics).subset_time("repart_time", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            write_time,
            repart_time,
            input_rows,
            output_rows,
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        // If [`shuffle_output_partitioning`] is none, then there's no need to do repartitioning.
        // Therefore, the partition is the same as its input plan's.
        let partitioning = shuffle_output_partitioning
            .clone()
            .unwrap_or_else(|| plan.properties().output_partitioning().clone());
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the input partition count
    pub fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    /// Get the true output partitioning
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    /// Executes the shuffle write operation for a single input partition.
    pub fn execute_shuffle_write(
        self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<Vec<ShuffleWritePartition>>> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(&self.job_id);
        path.push(format!("{}", self.stage_id));

        let write_metrics = ShuffleWriteMetrics::new(input_partition, &self.metrics);
        let output_partitioning = self.shuffle_output_partitioning.clone();
        let plan = self.plan.clone();
        let job_id = self.job_id.clone();
        let stage_id = self.stage_id;

        // Check if memory mode is enabled and this is not the final stage
        // Final stages always write to disk to ensure proper cleanup via existing mechanisms
        let memory_mode = context.session_config().ballista_shuffle_memory_mode();
        let is_final_stage = context.session_config().ballista_is_final_stage();

        // Use memory mode only for intermediate stages, not for the final output stage
        let use_memory = memory_mode && !is_final_stage;

        // Check for object store shuffle configuration
        let storage_type_str = context.session_config().ballista_shuffle_storage_type();
        let storage_type: ShuffleStorageType = storage_type_str
            .parse()
            .unwrap_or(ShuffleStorageType::Local);
        let storage_url = context.session_config().ballista_shuffle_storage_url();
        let use_object_store = !use_memory
            && matches!(
                storage_type,
                ShuffleStorageType::S3 | ShuffleStorageType::Azure
            );

        // Get shuffle format from session config
        let shuffle_format = context.session_config().ballista_shuffle_format();
        let file_ext = utils::shuffle_file_extension(shuffle_format);

        async move {
            let now = Instant::now();
            let mut stream = plan.execute(input_partition, context)?;

            if use_memory {
                // Use in-memory shuffle storage with configurable format
                Self::execute_shuffle_write_memory(
                    &job_id,
                    stage_id,
                    input_partition,
                    &mut stream,
                    output_partitioning,
                    write_metrics,
                    now,
                    shuffle_format,
                )
                .await
            } else if use_object_store {
                // Use object store (S3 or Azure) for shuffle data
                Self::execute_shuffle_write_object_store(
                    &job_id,
                    stage_id,
                    input_partition,
                    &mut stream,
                    output_partitioning,
                    write_metrics,
                    now,
                    storage_type,
                    storage_url,
                    shuffle_format,
                    file_ext,
                )
                .await
            } else {
                // Use disk-based shuffle storage with configurable format
                // This is used for:
                // 1. When memory_mode is disabled
                // 2. For final stages (even if memory_mode is enabled)
                Self::execute_shuffle_write_disk(
                    path,
                    input_partition,
                    &mut stream,
                    output_partitioning,
                    write_metrics,
                    now,
                    shuffle_format,
                    file_ext,
                )
                .await
            }
        }
    }

    /// Executes shuffle write to disk (original behavior).
    #[allow(clippy::too_many_arguments)]
    async fn execute_shuffle_write_disk(
        mut path: PathBuf,
        input_partition: usize,
        stream: &mut std::pin::Pin<
            Box<dyn datafusion::physical_plan::RecordBatchStream + Send>,
        >,
        output_partitioning: Option<Partitioning>,
        write_metrics: ShuffleWriteMetrics,
        now: Instant,
        shuffle_format: ShuffleFormat,
        file_ext: &str,
    ) -> Result<Vec<ShuffleWritePartition>> {
        match output_partitioning {
            None => {
                let timer = write_metrics.write_time.timer();
                path.push(format!("{input_partition}"));
                std::fs::create_dir_all(&path)?;
                path.push(format!("data.{file_ext}"));
                let path = path.to_str().unwrap();
                debug!("Writing results to {path} (format: {shuffle_format})");

                // stream results to disk using configured format
                let stats = utils::write_stream_to_disk_with_format(
                    stream,
                    path,
                    &write_metrics.write_time,
                    shuffle_format,
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

                write_metrics
                    .input_rows
                    .add(stats.num_rows.unwrap_or(0) as usize);
                write_metrics
                    .output_rows
                    .add(stats.num_rows.unwrap_or(0) as usize);
                timer.done();

                info!(
                    "Executed partition {} in {} seconds. Statistics: {}",
                    input_partition,
                    now.elapsed().as_secs(),
                    stats
                );

                Ok(vec![ShuffleWritePartition {
                    partition_id: input_partition as u64,
                    path: path.to_owned(),
                    num_batches: stats.num_batches.unwrap_or(0),
                    num_rows: stats.num_rows.unwrap_or(0),
                    num_bytes: stats.num_bytes.unwrap_or(0),
                }])
            }

            Some(Partitioning::Hash(exprs, num_output_partitions)) => {
                // we won't necessary produce output for every possible partition, so we
                // create writers on demand
                let mut writers: Vec<Option<WriteTracker>> = vec![];
                for _ in 0..num_output_partitions {
                    writers.push(None);
                }

                let mut partitioner = BatchPartitioner::try_new(
                    Partitioning::Hash(exprs, num_output_partitions),
                    write_metrics.repart_time.clone(),
                    input_partition,
                    1,
                )?;

                let schema = stream.schema();

                while let Some(result) = stream.next().await {
                    let input_batch = result?;

                    write_metrics.input_rows.add(input_batch.num_rows());

                    partitioner.partition(
                        input_batch,
                        |output_partition, output_batch| {
                            // partition func in datafusion make sure not write empty output_batch.
                            let timer = write_metrics.write_time.timer();
                            match &mut writers[output_partition] {
                                Some(w) => {
                                    w.num_batches += 1;
                                    w.num_rows += output_batch.num_rows();
                                    w.writer.write(&output_batch)?;
                                }
                                None => {
                                    let mut file_path = path.clone();
                                    file_path.push(format!("{output_partition}"));
                                    std::fs::create_dir_all(&file_path)?;

                                    file_path.push(format!(
                                        "data-{input_partition}.{file_ext}"
                                    ));
                                    debug!("Writing results to {file_path:?} (format: {shuffle_format})");

                                    let mut writer = ShuffleFileWriter::try_new(
                                        file_path.clone(),
                                        schema.clone(),
                                        shuffle_format,
                                    )?;

                                    writer.write(&output_batch)?;
                                    writers[output_partition] = Some(WriteTracker {
                                        num_batches: 1,
                                        num_rows: output_batch.num_rows(),
                                        writer,
                                        path: file_path,
                                    });
                                }
                            }
                            write_metrics.output_rows.add(output_batch.num_rows());
                            timer.done();
                            Ok(())
                        },
                    )?;
                }

                let mut part_locs = vec![];

                for (i, w) in writers.into_iter().enumerate() {
                    if let Some(w) = w {
                        let num_bytes = fs::metadata(&w.path)?.len();
                        w.writer.finish()?;
                        debug!(
                            "Finished writing shuffle partition {} at {:?}. Batches: {}. Rows: {}. Bytes: {}.",
                            i, w.path, w.num_batches, w.num_rows, num_bytes
                        );

                        part_locs.push(ShuffleWritePartition {
                            partition_id: i as u64,
                            path: w.path.to_string_lossy().to_string(),
                            num_batches: w.num_batches as u64,
                            num_rows: w.num_rows as u64,
                            num_bytes,
                        });
                    }
                }
                Ok(part_locs)
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }

    /// Executes shuffle write to an object store (S3 or Azure).
    ///
    /// Supports Arrow IPC and Vortex shuffle formats. Arrow IPC data is streamed
    /// to the object store using multipart uploads to minimize memory pressure — each
    /// batch is serialized to IPC bytes and written to the upload as it arrives.
    /// Vortex data is buffered in memory and serialized at the end, since the Vortex
    /// IPC format requires all arrays to be available before serialization.
    #[allow(clippy::too_many_arguments)]
    async fn execute_shuffle_write_object_store(
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        stream: &mut std::pin::Pin<
            Box<dyn datafusion::physical_plan::RecordBatchStream + Send>,
        >,
        output_partitioning: Option<Partitioning>,
        write_metrics: ShuffleWriteMetrics,
        now: Instant,
        storage_type: ShuffleStorageType,
        storage_url: Option<String>,
        shuffle_format: ShuffleFormat,
        file_ext: &str,
    ) -> Result<Vec<ShuffleWritePartition>> {
        use crate::shuffle_storage::{ObjectStoreShuffleStorage, ShuffleStorageConfig};

        // Validate Vortex availability at compile time
        #[cfg(not(feature = "vortex"))]
        if shuffle_format == ShuffleFormat::Vortex {
            return Err(DataFusionError::NotImplemented(
                "Vortex format requires the 'vortex' feature to be enabled".to_string(),
            ));
        }

        let base_url = storage_url.ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Shuffle storage URL must be set when using {storage_type} storage type. Set the 'ballista.shuffle.storage_url' configuration."
            ))
        })?;

        let config = ShuffleStorageConfig::from_type_and_url(storage_type, &base_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let storage = ObjectStoreShuffleStorage::from_config(&config)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema = stream.schema();

        match output_partitioning {
            None => {
                // No repartitioning — stream batches directly to a multipart upload
                let (writer, full_url) = storage
                    .start_multipart_write(
                        job_id,
                        stage_id,
                        input_partition,
                        input_partition,
                        file_ext,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let (num_rows, num_batches, num_bytes) = match shuffle_format {
                    ShuffleFormat::ArrowIpc => {
                        // Single StreamWriter for the whole partition so the EOS
                        // marker is emitted once at the end; the per-batch path
                        // emitted one EOS per batch and the reader stopped at the
                        // first marker.
                        let mut uploader = StreamingMultipartIpcUploader::try_new(
                            schema.as_ref(),
                            writer,
                            full_url.clone(),
                        )?;
                        while let Some(result) = stream.next().await {
                            let batch = result?;
                            write_metrics.input_rows.add(batch.num_rows());
                            write_metrics.output_rows.add(batch.num_rows());
                            let timer = write_metrics.write_time.timer();
                            uploader.write_batch(&batch)?;
                            timer.done();
                        }
                        let timer = write_metrics.write_time.timer();
                        let (_url, batches, rows, bytes) = uploader.finish().await?;
                        timer.done();
                        (rows, batches, bytes)
                    }
                    #[cfg(feature = "vortex")]
                    ShuffleFormat::Vortex => {
                        let mut writer = writer;
                        let mut vortex_buffer: Vec<vortex_array::ArrayRef> = Vec::new();
                        let mut num_rows: u64 = 0;
                        let mut num_batches: u64 = 0;
                        while let Some(result) = stream.next().await {
                            let batch = result?;
                            write_metrics.input_rows.add(batch.num_rows());
                            write_metrics.output_rows.add(batch.num_rows());
                            num_rows += batch.num_rows() as u64;
                            num_batches += 1;
                            let timer = write_metrics.write_time.timer();
                            use vortex_array::arrow::FromArrowArray;
                            let vortex_array =
                                vortex_array::ArrayRef::from_arrow(&batch, false)
                                    .map_err(|e| {
                                        DataFusionError::External(Box::new(e))
                                    })?;
                            vortex_buffer.push(vortex_array);
                            timer.done();
                        }
                        let mut num_bytes: u64 = 0;
                        if !vortex_buffer.is_empty() {
                            let timer = write_metrics.write_time.timer();
                            let buf = serialize_vortex_arrays_to_bytes(vortex_buffer)?;
                            num_bytes = buf.len() as u64;
                            writer.put(bytes::Bytes::from(buf));
                            timer.done();
                        }
                        let timer = write_metrics.write_time.timer();
                        writer.finish().await.map_err(|e| {
                            DataFusionError::External(Box::new(BallistaError::General(
                                format!(
                                    "Failed to complete multipart upload to {}: {:?}",
                                    full_url, e
                                ),
                            )))
                        })?;
                        timer.done();
                        (num_rows, num_batches, num_bytes)
                    }
                    #[cfg(not(feature = "vortex"))]
                    _ => unreachable!(),
                };

                let stats = PartitionStats::new(
                    Some(num_rows),
                    Some(num_batches),
                    Some(num_bytes),
                );

                info!(
                    "Executed partition {} ({shuffle_format}) to object store in {} seconds. Statistics: {}",
                    input_partition,
                    now.elapsed().as_secs(),
                    stats
                );

                Ok(vec![ShuffleWritePartition {
                    partition_id: input_partition as u64,
                    path: full_url,
                    num_batches: stats.num_batches.unwrap_or(0),
                    num_rows: stats.num_rows.unwrap_or(0),
                    num_bytes: stats.num_bytes.unwrap_or(0),
                }])
            }

            Some(Partitioning::Hash(exprs, num_output_partitions)) => {
                match shuffle_format {
                    ShuffleFormat::ArrowIpc => {
                        // Arrow IPC: stream serialized batches to per-partition multipart uploads
                        Self::execute_hash_repart_object_store_ipc(
                            job_id,
                            stage_id,
                            input_partition,
                            stream,
                            exprs,
                            num_output_partitions,
                            &schema,
                            &storage,
                            &write_metrics,
                            file_ext,
                        )
                        .await
                    }
                    #[cfg(feature = "vortex")]
                    ShuffleFormat::Vortex => {
                        // Vortex: buffer arrays per partition, serialize at end
                        Self::execute_hash_repart_object_store_vortex(
                            job_id,
                            stage_id,
                            input_partition,
                            stream,
                            exprs,
                            num_output_partitions,
                            &schema,
                            &storage,
                            &write_metrics,
                            file_ext,
                        )
                        .await
                    }
                    // Non-vortex build: already returned error above
                    #[cfg(not(feature = "vortex"))]
                    _ => unreachable!(),
                }
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }

    /// Hash-repartition to object store using Arrow IPC format.
    ///
    /// Maintains lazy per-partition multipart writers. Each repartitioned batch
    /// is serialized to IPC bytes and streamed directly to the corresponding
    /// partition's multipart upload.
    #[allow(clippy::too_many_arguments)]
    async fn execute_hash_repart_object_store_ipc(
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        stream: &mut std::pin::Pin<
            Box<dyn datafusion::physical_plan::RecordBatchStream + Send>,
        >,
        exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        num_output_partitions: usize,
        schema: &SchemaRef,
        storage: &crate::shuffle_storage::ObjectStoreShuffleStorage,
        write_metrics: &ShuffleWriteMetrics,
        file_ext: &str,
    ) -> Result<Vec<ShuffleWritePartition>> {
        // One StreamingMultipartIpcUploader per output partition — emits the IPC
        // header on construction, appends each batch as it arrives, and writes
        // the EOS marker exactly once at finish(). Previously this path used
        // `serialize_batch_to_ipc_bytes` per batch (one complete stream per batch,
        // each with its own EOS marker) and concatenated them; the reader's
        // StreamReader stopped at the first marker so any multi-batch partition
        // came back as `Unexpected EOS`.
        let mut writers: Vec<Option<StreamingMultipartIpcUploader>> =
            (0..num_output_partitions).map(|_| None).collect();

        let mut partitioner = BatchPartitioner::try_new(
            Partitioning::Hash(exprs, num_output_partitions),
            write_metrics.repart_time.clone(),
            input_partition,
            1,
        )?;

        // The BatchPartitioner callback is synchronous — collect repartitioned
        // batches into a Vec and process (lazy multipart-start + write) on the
        // async side after each input batch.
        while let Some(result) = stream.next().await {
            let input_batch = result?;
            write_metrics.input_rows.add(input_batch.num_rows());

            let mut batch_pending: Vec<(usize, RecordBatch)> = Vec::new();
            partitioner.partition(input_batch, |output_partition, output_batch| {
                let rows = output_batch.num_rows();
                write_metrics.output_rows.add(rows);
                batch_pending.push((output_partition, output_batch));
                Ok(())
            })?;

            for (output_partition, output_batch) in batch_pending {
                let timer = write_metrics.write_time.timer();
                let uploader = match &mut writers[output_partition] {
                    Some(u) => u,
                    None => {
                        let (multipart_writer, full_url) = storage
                            .start_multipart_write(
                                job_id,
                                stage_id,
                                output_partition,
                                input_partition,
                                file_ext,
                            )
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let uploader = StreamingMultipartIpcUploader::try_new(
                            schema.as_ref(),
                            multipart_writer,
                            full_url,
                        )?;
                        writers[output_partition] = Some(uploader);
                        writers[output_partition].as_mut().expect("just inserted")
                    }
                };
                uploader.write_batch(&output_batch)?;
                timer.done();
            }
        }

        // Finalize all multipart uploads
        let mut part_locs = Vec::new();
        for (output_partition, writer_opt) in writers.into_iter().enumerate() {
            if let Some(uploader) = writer_opt {
                let timer = write_metrics.write_time.timer();
                let (full_url, num_batches, num_rows, num_bytes) =
                    uploader.finish().await?;
                timer.done();

                debug!(
                    "Finished writing shuffle partition {} (Arrow IPC) to object store. Batches: {}, Bytes: {}.",
                    output_partition, num_batches, num_bytes
                );

                part_locs.push(ShuffleWritePartition {
                    partition_id: output_partition as u64,
                    path: full_url,
                    num_batches,
                    num_rows,
                    num_bytes,
                });
            }
        }
        Ok(part_locs)
    }

    /// Hash-repartition to object store using Vortex format.
    ///
    /// Buffers Vortex arrays per output partition during repartitioning, then
    /// serializes each partition's arrays to Vortex IPC bytes and uploads via
    /// multipart at the end.
    #[cfg(feature = "vortex")]
    #[allow(clippy::too_many_arguments)]
    async fn execute_hash_repart_object_store_vortex(
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        stream: &mut std::pin::Pin<
            Box<dyn datafusion::physical_plan::RecordBatchStream + Send>,
        >,
        exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        num_output_partitions: usize,
        _schema: &SchemaRef,
        storage: &crate::shuffle_storage::ObjectStoreShuffleStorage,
        write_metrics: &ShuffleWriteMetrics,
        file_ext: &str,
    ) -> Result<Vec<ShuffleWritePartition>> {
        use vortex_array::arrow::FromArrowArray;

        struct VortexPartitionBuffer {
            arrays: Vec<vortex_array::ArrayRef>,
            num_batches: u64,
            num_rows: u64,
        }

        let mut buffers: Vec<Option<VortexPartitionBuffer>> =
            (0..num_output_partitions).map(|_| None).collect();

        let mut partitioner = BatchPartitioner::try_new(
            Partitioning::Hash(exprs, num_output_partitions),
            write_metrics.repart_time.clone(),
            input_partition,
            1,
        )?;

        while let Some(result) = stream.next().await {
            let input_batch = result?;
            write_metrics.input_rows.add(input_batch.num_rows());

            partitioner.partition(input_batch, |output_partition, output_batch| {
                let timer = write_metrics.write_time.timer();
                let batch_rows = output_batch.num_rows() as u64;

                let vortex_array =
                    vortex_array::ArrayRef::from_arrow(&output_batch, false)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match &mut buffers[output_partition] {
                    Some(buf) => {
                        buf.arrays.push(vortex_array);
                        buf.num_batches += 1;
                        buf.num_rows += batch_rows;
                    }
                    None => {
                        buffers[output_partition] = Some(VortexPartitionBuffer {
                            arrays: vec![vortex_array],
                            num_batches: 1,
                            num_rows: batch_rows,
                        });
                    }
                }

                write_metrics.output_rows.add(batch_rows as usize);
                timer.done();
                Ok(())
            })?;
        }

        // Serialize and upload each partition
        let mut part_locs = Vec::new();

        for (output_partition, buf_opt) in buffers.into_iter().enumerate() {
            if let Some(partition_buf) = buf_opt {
                let timer = write_metrics.write_time.timer();

                // Serialize all arrays for this partition to Vortex IPC bytes
                let ipc_bytes = serialize_vortex_arrays_to_bytes(partition_buf.arrays)?;
                let num_bytes = ipc_bytes.len() as u64;

                // Start multipart upload and write all bytes
                let (mut writer, full_url) = storage
                    .start_multipart_write(
                        job_id,
                        stage_id,
                        output_partition,
                        input_partition,
                        file_ext,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                writer.put(bytes::Bytes::from(ipc_bytes));
                writer.finish().await.map_err(|e| {
                    DataFusionError::External(Box::new(BallistaError::General(format!(
                        "Failed to complete multipart upload to {}: {:?}",
                        full_url, e
                    ))))
                })?;
                timer.done();

                debug!(
                    "Finished writing shuffle partition {} (Vortex) to object store. Batches: {}, Bytes: {}.",
                    output_partition, partition_buf.num_batches, num_bytes
                );

                part_locs.push(ShuffleWritePartition {
                    partition_id: output_partition as u64,
                    path: full_url,
                    num_batches: partition_buf.num_batches,
                    num_rows: partition_buf.num_rows,
                    num_bytes,
                });
            }
        }
        Ok(part_locs)
    }

    /// Executes shuffle write to in-memory storage.
    #[allow(clippy::too_many_arguments)]
    async fn execute_shuffle_write_memory(
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        stream: &mut std::pin::Pin<
            Box<dyn datafusion::physical_plan::RecordBatchStream + Send>,
        >,
        output_partitioning: Option<Partitioning>,
        write_metrics: ShuffleWriteMetrics,
        now: Instant,
        shuffle_format: ShuffleFormat,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let shuffle_manager = global_shuffle_manager();
        let schema = stream.schema();

        match output_partitioning {
            None => {
                let timer = write_metrics.write_time.timer();

                // Collect all batches into memory
                let mut batches = Vec::new();
                let mut num_rows = 0usize;
                let mut num_bytes = 0usize;

                while let Some(result) = stream.next().await {
                    let batch = result?;
                    num_rows += batch.num_rows();
                    num_bytes += batch.get_array_memory_size();
                    write_metrics.input_rows.add(batch.num_rows());
                    write_metrics.output_rows.add(batch.num_rows());
                    batches.push(batch);
                }

                let num_batches = batches.len();
                let key = InMemoryShuffleManager::partition_key(
                    job_id,
                    stage_id,
                    input_partition,
                );

                // Store in the global shuffle manager using the configured format
                let data =
                    Self::create_partition_data(schema.clone(), batches, shuffle_format)?;
                shuffle_manager.store_partition(key.clone(), data);

                timer.done();

                info!(
                    "Executed partition {} to memory ({shuffle_format}) in {} seconds. Batches: {}, Rows: {}, Bytes: {}",
                    input_partition,
                    now.elapsed().as_secs(),
                    num_batches,
                    num_rows,
                    num_bytes
                );

                // Use special "memory://" prefix to indicate in-memory storage
                Ok(vec![ShuffleWritePartition {
                    partition_id: input_partition as u64,
                    path: format!("memory://{key}"),
                    num_batches: num_batches as u64,
                    num_rows: num_rows as u64,
                    num_bytes: num_bytes as u64,
                }])
            }

            Some(Partitioning::Hash(exprs, num_output_partitions)) => {
                // We collect batches per output partition in memory
                let mut mem_writers: Vec<Option<InMemoryWriteTracker>> = vec![];
                for _ in 0..num_output_partitions {
                    mem_writers.push(None);
                }

                let mut partitioner = BatchPartitioner::try_new(
                    Partitioning::Hash(exprs, num_output_partitions),
                    write_metrics.repart_time.clone(),
                    input_partition,
                    1,
                )?;

                while let Some(result) = stream.next().await {
                    let input_batch = result?;
                    write_metrics.input_rows.add(input_batch.num_rows());

                    partitioner.partition(
                        input_batch,
                        |output_partition, output_batch| {
                            let timer = write_metrics.write_time.timer();
                            let batch_bytes = output_batch.get_array_memory_size();
                            let batch_rows = output_batch.num_rows();

                            match &mut mem_writers[output_partition] {
                                Some(w) => {
                                    w.num_batches += 1;
                                    w.num_rows += batch_rows;
                                    w.num_bytes += batch_bytes;
                                    w.batches.push(output_batch);
                                }
                                None => {
                                    let key = InMemoryShuffleManager::hash_partition_key(
                                        job_id,
                                        stage_id,
                                        output_partition,
                                        input_partition,
                                    );
                                    mem_writers[output_partition] =
                                        Some(InMemoryWriteTracker {
                                            num_batches: 1,
                                            num_rows: batch_rows,
                                            num_bytes: batch_bytes,
                                            batches: vec![output_batch],
                                            key,
                                        });
                                }
                            }
                            write_metrics.output_rows.add(batch_rows);
                            timer.done();
                            Ok(())
                        },
                    )?;
                }

                let mut part_locs = vec![];

                for (i, w) in mem_writers.into_iter().enumerate() {
                    if let Some(w) = w {
                        debug!(
                            "Finished writing shuffle partition {} to memory ({shuffle_format}). Batches: {}. Rows: {}. Bytes: {}.",
                            i, w.num_batches, w.num_rows, w.num_bytes
                        );

                        // Store in the global shuffle manager using the configured format
                        let data = Self::create_partition_data(
                            schema.clone(),
                            w.batches,
                            shuffle_format,
                        )?;
                        shuffle_manager.store_partition(w.key.clone(), data);

                        part_locs.push(ShuffleWritePartition {
                            partition_id: i as u64,
                            path: format!("memory://{}", w.key),
                            num_batches: w.num_batches as u64,
                            num_rows: w.num_rows as u64,
                            num_bytes: w.num_bytes as u64,
                        });
                    }
                }
                Ok(part_locs)
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }

    /// Creates partition data in the specified format (Arrow or Vortex).
    fn create_partition_data(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        format: ShuffleFormat,
    ) -> Result<ShufflePartitionData> {
        match format {
            ShuffleFormat::ArrowIpc => Ok(ShufflePartitionData::new(schema, batches)),
            #[cfg(feature = "vortex")]
            ShuffleFormat::Vortex => {
                use vortex_array::ArrayRef;
                use vortex_array::arrow::FromArrowArray;

                let mut arrays = Vec::with_capacity(batches.len());
                let mut total_rows = 0u64;
                let mut total_bytes = 0u64;

                for batch in batches {
                    total_rows += batch.num_rows() as u64;
                    // Convert Arrow RecordBatch to Vortex Array
                    let vortex_array = ArrayRef::from_arrow(&batch, false)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    total_bytes += vortex_array.nbytes();
                    arrays.push(vortex_array);
                }

                Ok(ShufflePartitionData::new_vortex(
                    schema,
                    arrays,
                    total_rows,
                    total_bytes,
                ))
            }
            #[cfg(not(feature = "vortex"))]
            ShuffleFormat::Vortex => Err(DataFusionError::NotImplemented(
                "Vortex format requires the 'vortex' feature to be enabled".to_string(),
            )),
        }
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ShuffleWriterExec: partitioning: {}",
                    self.shuffle_output_partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or("None".to_string())
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "partitioning={}",
                    self.shuffle_output_partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or("None".to_string())
                )
            }
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let input = children.pop().ok_or_else(|| {
                DataFusionError::Plan(
                    "Ballista ShuffleWriterExec expects single child".to_owned(),
                )
            })?;

            Ok(Arc::new(ShuffleWriterExec::try_new(
                self.job_id.clone(),
                self.stage_id,
                input,
                self.work_dir.clone(),
                self.shuffle_output_partitioning.clone(),
            )?))
        } else {
            Err(DataFusionError::Plan(
                "Ballista ShuffleWriterExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let schema_captured = schema.clone();
        let fut_stream = self
            .clone()
            .execute_shuffle_write(partition, context)
            .and_then(|part_loc| async move {
                // build metadata result batch
                let num_writers = part_loc.len();
                let mut partition_builder = UInt32Builder::with_capacity(num_writers);
                let mut path_builder =
                    StringBuilder::with_capacity(num_writers, num_writers * 100);
                let mut num_rows_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_batches_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_bytes_builder = UInt64Builder::with_capacity(num_writers);

                for loc in &part_loc {
                    path_builder.append_value(loc.path.clone());
                    partition_builder.append_value(loc.partition_id as u32);
                    num_rows_builder.append_value(loc.num_rows);
                    num_batches_builder.append_value(loc.num_batches);
                    num_bytes_builder.append_value(loc.num_bytes);
                }

                // build arrays
                let partition_num: ArrayRef = Arc::new(partition_builder.finish());
                let path: ArrayRef = Arc::new(path_builder.finish());
                let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
                    Box::new(num_rows_builder),
                    Box::new(num_batches_builder),
                    Box::new(num_bytes_builder),
                ];
                let mut stats_builder = StructBuilder::new(
                    PartitionStats::default().arrow_struct_fields(),
                    field_builders,
                );
                for _ in 0..num_writers {
                    stats_builder.append(true);
                }
                let stats = Arc::new(stats_builder.finish());

                // build result batch containing metadata
                let batch = RecordBatch::try_new(
                    schema_captured.clone(),
                    vec![partition_num, path, stats],
                )?;

                debug!("RESULTS METADATA:\n{batch:?}");

                MemoryStream::try_new(vec![batch], schema_captured, None)
            })
            .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut_stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.plan.partition_statistics(partition)
    }
}

impl ShuffleWriter for ShuffleWriterExec {
    fn job_id(&self) -> &str {
        &self.job_id
    }

    fn stage_id(&self) -> usize {
        self.stage_id
    }

    fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    fn clone_box(&self) -> Arc<dyn ShuffleWriter> {
        Arc::new(self.clone())
    }
}

fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        stats.arrow_struct_repr(),
    ]))
}

/// Builds an [`IpcWriteOptions`] with LZ4_FRAME compression for shuffle writes.
fn ipc_write_options() -> Result<IpcWriteOptions> {
    Ok(IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?)
}

/// Maintains a single Arrow IPC `StreamWriter` whose lifetime spans every batch
/// written to one shuffle output partition, and drains its bytes into a
/// `WriteMultipart` upload as they accumulate.
///
/// The previous implementation called `serialize_batch_to_ipc_bytes` per batch
/// — each call wrote a complete IPC stream **with** an EOS marker via
/// `StreamWriter::finish()` — and concatenated the streams into one S3 object.
/// `StreamReader` on the reader side stops at the first EOS, so any partition
/// holding more than one batch came back truncated (or as `Unexpected EOS`).
///
/// This wrapper writes the schema header once on construction, appends each
/// batch's bytes to the multipart upload after `StreamWriter::write`, and emits
/// the EOS marker exactly once at the end via [`Self::finish`].
struct StreamingMultipartIpcUploader {
    stream_writer: StreamWriter<std::io::Cursor<Vec<u8>>>,
    multipart_writer: object_store::WriteMultipart,
    full_url: String,
    num_batches: u64,
    num_rows: u64,
    num_bytes: u64,
}

impl StreamingMultipartIpcUploader {
    fn try_new(
        schema: &Schema,
        multipart_writer: object_store::WriteMultipart,
        full_url: String,
    ) -> Result<Self> {
        let options = ipc_write_options()?;
        let stream_writer = StreamWriter::try_new_with_options(
            std::io::Cursor::new(Vec::new()),
            schema,
            options,
        )?;
        let mut uploader = Self {
            stream_writer,
            multipart_writer,
            full_url,
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
        };
        // Flush the schema header bytes that `try_new_with_options` already wrote
        // so they land in the multipart upload before any batch bytes.
        uploader.drain_buffer();
        Ok(uploader)
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = batch.num_rows() as u64;
        self.stream_writer.write(batch)?;
        self.drain_buffer();
        self.num_batches += 1;
        self.num_rows += rows;
        Ok(())
    }

    /// Move whatever bytes the `StreamWriter` has emitted since the last drain
    /// from its internal cursor into the multipart upload and reset the cursor.
    ///
    /// Replaces the cursor's buffer with a fresh `Vec` sized at the previous
    /// capacity rather than `Vec::new()` so the writer can reuse the allocation
    /// between drains on shuffle-heavy workloads.
    fn drain_buffer(&mut self) {
        let cursor = self.stream_writer.get_mut();
        let buf = cursor.get_mut();
        let capacity = buf.capacity();
        let bytes = std::mem::replace(buf, Vec::with_capacity(capacity));
        cursor.set_position(0);
        if !bytes.is_empty() {
            self.num_bytes += bytes.len() as u64;
            self.multipart_writer.put(bytes::Bytes::from(bytes));
        }
    }

    /// Emit the final EOS marker, push any remaining bytes, and complete the
    /// multipart upload. Returns `(full_url, num_batches, num_rows, num_bytes)`.
    async fn finish(mut self) -> Result<(String, u64, u64, u64)> {
        self.stream_writer.finish()?;
        self.drain_buffer();
        self.multipart_writer.finish().await.map_err(|e| {
            DataFusionError::External(Box::new(BallistaError::General(format!(
                "Failed to complete multipart upload to {}: {:?}",
                self.full_url, e
            ))))
        })?;
        Ok((
            self.full_url,
            self.num_batches,
            self.num_rows,
            self.num_bytes,
        ))
    }
}

/// Serialize buffered Vortex arrays to IPC bytes.
#[cfg(feature = "vortex")]
fn serialize_vortex_arrays_to_bytes(
    arrays: Vec<vortex_array::ArrayRef>,
) -> Result<Vec<u8>> {
    use vortex_array::iter::ArrayIteratorAdapter;
    use vortex_error::VortexResult;
    use vortex_ipc::iterator::ArrayIteratorIPC;

    if arrays.is_empty() {
        return Ok(Vec::new());
    }

    let dtype = arrays[0].dtype().clone();
    let iter = arrays
        .into_iter()
        .map(|a| Ok(a) as VortexResult<vortex_array::ArrayRef>);
    let array_iter = ArrayIteratorAdapter::new(dtype, iter);
    let ipc_data = array_iter
        .into_ipc(&vortex_array::LEGACY_SESSION)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .collect_to_buffer()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(ipc_data.to_vec())
}

#[cfg(test)]
#[cfg(not(feature = "force_hash_collisions"))]
#[allow(dead_code, unused_imports)] // clippy false positive with local imports
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let file0 = path.value(0);
        assert!(
            file0.ends_with("/jobOne/1/0/data-0.arrow")
                || file0.ends_with("\\jobOne\\1\\0\\data-0.arrow")
        );
        let file1 = path.value(1);
        assert!(
            file1.ends_with("/jobOne/1/1/data-0.arrow")
                || file1.ends_with("\\jobOne\\1\\1\\data-0.arrow")
        );

        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(4, num_rows.value(0));
        assert_eq!(4, num_rows.value(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_rows.value(0));
        assert_eq!(2, num_rows.value(1));

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(3)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];

        let memory_data_source =
            Arc::new(MemorySourceConfig::try_new(&partitions, schema, None)?);

        Ok(Arc::new(DataSourceExec::new(memory_data_source)))
    }

    /// End-to-end regression test for [`StreamingMultipartIpcUploader`]:
    /// writes multiple Arrow batches to the in-memory object store via the
    /// streaming multipart path and reads them back, asserting all batches
    /// survive without `Unexpected EOS`. Before the per-partition StreamWriter
    /// refactor, every batch was serialised as a standalone IPC stream with its
    /// own EOS marker and the reader stopped at the first one.
    #[tokio::test]
    async fn streaming_multipart_ipc_uploader_round_trips_multiple_batches() -> Result<()>
    {
        use crate::shuffle_storage::{ObjectStoreShuffleStorage, ShuffleStorage};
        use datafusion::arrow::ipc::reader::StreamReader;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
        use object_store::memory::InMemory;
        use object_store::{ObjectStore, ObjectStoreExt};

        // Hand-build an `ObjectStoreShuffleStorage` over an in-memory store so the
        // test exercises the multipart write path without needing S3.
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = ObjectStoreShuffleStorage::new_for_test(
            Arc::clone(&inner),
            "s3://test-bucket/shuffle/prefix".to_string(),
            "shuffle/prefix".to_string(),
            ShuffleStorageType::S3,
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let write_time =
            metrics::MetricBuilder::new(&metrics).subset_time("write_time", 0);

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                let values: Vec<u32> = (0..3).map(|j| i * 10 + j).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(datafusion::arrow::array::UInt32Array::from(
                        values,
                    ))],
                )
                .unwrap()
            })
            .collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let (multipart_writer, full_url) = storage
            .start_multipart_write("job_a", 1, 0, 0, "arrow")
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut uploader = StreamingMultipartIpcUploader::try_new(
            schema.as_ref(),
            multipart_writer,
            full_url.clone(),
        )?;
        for batch in &batches {
            let timer = write_time.timer();
            uploader.write_batch(batch)?;
            timer.done();
        }
        let (returned_url, num_batches, num_rows, num_bytes) = uploader.finish().await?;
        assert_eq!(returned_url, full_url);
        assert_eq!(num_batches, batches.len() as u64);
        assert_eq!(num_rows, total_rows as u64);
        assert!(num_bytes > 0, "uploader should have written some bytes");

        // Read the object straight back from the underlying in-memory store and
        // decode with a standard Arrow `StreamReader` — this is what the
        // shuffle reader does for object-store partitions.
        let key = object_store::path::Path::from("shuffle/prefix/job_a/1/0/data.arrow");
        let bytes = inner
            .get(&key)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)?;

        let read_batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "unexpected error reading shuffle stream back: {e}"
                ))
            })?;

        assert_eq!(
            read_batches.len(),
            batches.len(),
            "every batch should round-trip through one Arrow IPC stream"
        );
        let read_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(read_rows, total_rows);

        Ok(())
    }
}
