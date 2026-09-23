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

//! Execution engine abstraction for query stage execution.
//!
//! This module provides traits and default implementations for executing
//! query stages in a distributed setting. The execution engine is responsible
//! for creating query stage executors from physical plans.

use async_trait::async_trait;
use ballista_core::JobId;
use ballista_core::client_pool::BallistaClientPool;
use ballista_core::execution_plans::sort_shuffle::SortShuffleWriterExec;
use ballista_core::execution_plans::{ShuffleReaderExec, ShuffleWriterExec};
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::utils;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::serde::scheduler::PartitionStats;
use ballista_core::{JobId, utils};
use datafusion::arrow::array::{
    Array, StringArray, StructArray, UInt32Array, UInt64Array,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

/// Extension point for customizing query stage execution.
///
/// Implement this trait to provide a custom execution engine that can
/// transform physical plans into query stage executors. This allows
/// for custom execution strategies beyond the default DataFusion-based
/// execution.
pub trait ExecutionEngine: Sync + Send {
    /// Creates a query stage executor from a physical plan.
    ///
    /// The returned executor will be responsible for executing the given
    /// plan partition and writing shuffle output to the specified work directory.
    #[allow(clippy::too_many_arguments)]
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
        global_output_partition_ids: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>>;
}

/// Restrict a file-backed `DataSourceExec` to the file group for `partition_id`,
/// emptying the others (partition count preserved). Ballista runs one partition
/// per task on its own plan instance, so without this the task's lone stream
/// drains the scan's shared work-queue and reads the whole table. Returns `None`
/// for non-file scans or a `partition_id` outside the source's file groups.
/// See apache/datafusion-ballista#1907.
fn restrict_scan_to_partition(
    plan: &Arc<dyn ExecutionPlan>,
    partition_id: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = plan.downcast_ref::<DataSourceExec>()?;
    let source: &dyn Any = exec.data_source().as_ref();
    let config = source.downcast_ref::<FileScanConfig>()?;
    if partition_id >= config.file_groups.len() {
        return None;
    }
    // Empty (not dropped) for the other partitions so the source's partition count is
    // preserved and `execute(partition_id)` still maps to its own group.
    let file_groups: Vec<FileGroup> = config
        .file_groups
        .iter()
        .enumerate()
        .map(|(i, group)| {
            if i == partition_id {
                group.clone()
            } else {
                FileGroup::new(vec![])
            }
        })
        .collect();
    let config = FileScanConfigBuilder::from(config.clone())
        .with_file_groups(file_groups)
        .build();
    Some(DataSourceExec::from_data_source(config))
}

/// Restrict every file scan in `plan` to `partition_id`'s file group.
fn restrict_scans(
    plan: Arc<dyn ExecutionPlan>,
    partition_id: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_down(|node| {
            Ok(match restrict_scan_to_partition(&node, partition_id) {
                Some(rewritten) => Transformed::yes(rewritten),
                None => Transformed::no(node),
            })
        })?
        .data)
}

/// Fix ParquetSource metadata_size_hint that is lost during protobuf
/// serialization. The hint is preserved in TableParquetOptions but not
/// transferred back to ParquetSource.metadata_size_hint on deserialization.
/// Without this, each parquet file open requires 2 HTTP round trips instead
/// of 1 for remote (S3/object store) files.
fn fix_parquet_metadata_size_hint(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(dse) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some((file_scan_config, parquet_source)) =
            dse.downcast_to_file_source::<ParquetSource>()
        else {
            return Ok(Transformed::no(node));
        };
        // Recover metadata_size_hint from the table parquet options.
        // During protobuf round-trip, the hint is preserved in
        // TableParquetOptions but not transferred to the source-level field.
        let Some(hint) = parquet_source
            .table_parquet_options()
            .global
            .metadata_size_hint
        else {
            return Ok(Transformed::no(node));
        };
        let new_source = parquet_source.clone().with_metadata_size_hint(hint);
        let new_config = FileScanConfigBuilder::from(file_scan_config.clone())
            .with_source(Arc::new(new_source))
            .build();
        Ok(Transformed::yes(
            DataSourceExec::from_data_source(new_config) as Arc<dyn ExecutionPlan>,
        ))
    })
    .map(|t| t.data)
}

/// Executor for a single query stage in a distributed query.
///
/// A query stage is a section of a query plan that has consistent partitioning
/// and can be executed as one unit with each partition running in parallel.
/// The output of each partition is re-partitioned and written to disk in
/// Arrow IPC format. Subsequent stages read these results via ShuffleReaderExec.
#[async_trait]
pub trait QueryStageExecutor: Sync + Send + Debug + Display {
    /// Executes this query stage's assigned partition slice.
    ///
    /// Returns metadata about the shuffle partitions written to disk,
    /// including file paths and statistics.
    async fn execute_query_stage(
        &self,
        task_id: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>>;

    /// Collects execution metrics from all operators in the plan.
    fn collect_plan_metrics(&self) -> Vec<MetricsSet>;

    /// Returns a reference to the underlying execution plan.
    ///
    /// This is used to walk the plan tree and extract metrics from specific
    /// operators like ShuffleReaderExec.
    fn plan(&self) -> &dyn ExecutionPlan;
}

/// Default execution engine using DataFusion's ShuffleWriterExec.
///
/// This implementation expects the input plan to be wrapped in a
/// ShuffleWriterExec and creates a DefaultQueryStageExec to execute it.
#[derive(Default)]
pub struct DefaultExecutionEngine {
    client_pool: Option<Arc<dyn BallistaClientPool>>,
}

impl DefaultExecutionEngine {
    /// Creates new Default Execution Engine without client pooling
    pub fn new() -> Self {
        Self { client_pool: None }
    }
    /// Creates new Default Execution Engine with client pooling
    pub fn with_client_pool(client_pool: Arc<dyn BallistaClientPool>) -> Self {
        Self {
            client_pool: Some(client_pool),
        }
    }
}

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
        global_output_partition_ids: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        // Fix ParquetSource metadata_size_hint lost during serialization
        let plan = fix_parquet_metadata_size_hint(plan)?;

        // Route remote shuffle fetches through the executor's client pool when
        // one is configured (upstream #1951); without a pool, readers connect
        // per fetch.
        let plan = match &self.client_pool {
            Some(client_pool) => {
                plan.transform(|p| {
                    if let Some(reader) = p.downcast_ref::<ShuffleReaderExec>() {
                        Ok(Transformed::yes(Arc::new(
                            reader.with_client_pool(client_pool.clone()),
                        )
                            as Arc<dyn ExecutionPlan>))
                    } else {
                        Ok(Transformed::no(p))
                    }
                })?
                .data
            }
            None => plan,
        };

        // the query plan created by the scheduler always starts with a shuffle writer
        // (either ShuffleWriterExec or SortShuffleWriterExec)
        if let Some(shuffle_writer) = plan.downcast_ref::<ShuffleWriterExec>() {
            // recreate the shuffle writer with the correct working directory,
            // restricting any file scan to this task's partition
            let exec = ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                restrict_scans(plan.children()[0].clone(), partition_id)?,
                work_dir.to_string(),
            )?
            .with_task_id(task_id)
            .with_global_output_partition_ids(global_output_partition_ids);
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Passthrough(exec),
            )))
        } else if plan.downcast_ref::<RangeShuffleWriterExec>().is_some() {
            let exec = RangeShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
            )?
            .with_task_id(task_id);
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Range(exec),
            )))
        } else if let Some(sort_shuffle_writer) =
            plan.downcast_ref::<SortShuffleWriterExec>()
        {
            // recreate the sort shuffle writer with the correct working directory,
            // restricting any file scan to this task's partition
            let exec = SortShuffleWriterExec::try_new(
                job_id,
                stage_id,
                restrict_scans(plan.children()[0].clone(), partition_id)?,
                work_dir.to_string(),
                sort_shuffle_writer.shuffle_output_partitioning().clone(),
                sort_shuffle_writer.config().clone(),
            )?
            .with_task_id(task_id)
            .with_global_output_partition_ids(global_output_partition_ids);
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Sort(exec),
            )))
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec, \
                 RangeShuffleWriterExec, or SortShuffleWriterExec"
                    .to_string(),
            ))
        }
    }
}

/// Enum representing the different shuffle writer implementations.
#[derive(Debug, Clone)]
pub enum ShuffleWriterVariant {
    /// Passthrough shuffle writer: preserves its input partitioning,
    /// one file per output partition.
    Passthrough(ShuffleWriterExec),
    /// Passthrough shuffle writer emitting the seekable Arrow IPC file
    /// format, for stages read back in value-range order.
    Range(RangeShuffleWriterExec),
    /// Sort-based shuffle writer.
    Sort(SortShuffleWriterExec),
}

/// Default query stage executor that wraps a shuffle writer.
///
/// This executor delegates to the underlying shuffle writer to perform the actual
/// shuffle write operation, which partitions the data and writes it to disk.
#[derive(Debug)]
pub struct DefaultQueryStageExec {
    /// The underlying shuffle writer execution plan.
    shuffle_writer: ShuffleWriterVariant,
}

impl DefaultQueryStageExec {
    /// Creates a new DefaultQueryStageExec wrapping the given shuffle writer.
    pub fn new(shuffle_writer: ShuffleWriterVariant) -> Self {
        Self { shuffle_writer }
    }
}

impl Display for DefaultQueryStageExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Passthrough(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Passthrough): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
            ShuffleWriterVariant::Range(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Range): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
            ShuffleWriterVariant::Sort(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Sort): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
        }
    }
}

#[async_trait]
impl QueryStageExecutor for DefaultQueryStageExec {
    async fn execute_query_stage(
        &self,
        task_id: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let (plan_arc, is_sort_shuffle): (Arc<dyn ExecutionPlan>, bool) =
            match &self.shuffle_writer {
                ShuffleWriterVariant::Passthrough(writer) => {
                    (Arc::new(writer.clone()), false)
                }
                ShuffleWriterVariant::Range(writer) => (Arc::new(writer.clone()), false),
                ShuffleWriterVariant::Sort(writer) => (Arc::new(writer.clone()), true),
            };
        debug!(
            "executor plan pre-run (task_id={task_id}):\n{}",
            DisplayableExecutionPlan::new(plan_arc.as_ref()).indent(true)
        );

        // Both variants share the same coordinator+oneshot handoff shape via
        // `execute(N)` — drive K parallel calls so every oneshot receiver is
        // taken concurrently and each output partition's summaries flow out
        // as soon as its files are closed.
        let result =
            drive_shuffle_writer_stage(plan_arc.clone(), context, is_sort_shuffle).await;

        debug!(
            "executor plan post-run (task_id={task_id}, ok={}):\n{}",
            result.is_ok(),
            DisplayableExecutionPlan::with_metrics(plan_arc.as_ref()).indent(true)
        );
        result
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Passthrough(writer) => {
                utils::collect_plan_metrics(writer)
            }
            ShuffleWriterVariant::Range(writer) => utils::collect_plan_metrics(writer),
            ShuffleWriterVariant::Sort(writer) => utils::collect_plan_metrics(writer),
        }
    }

    fn plan(&self) -> &dyn ExecutionPlan {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => writer,
            ShuffleWriterVariant::Sort(writer) => writer,
        }
    }
}

    fn collect_runtime_stats_reports(
        &self,
    ) -> Vec<ballista_core::serde::protobuf::RuntimeStatsReport> {
        // Walk from the shuffle writer's plan through the whitelist. If
        // no `RuntimeStatsExec` sits within reach, we return an empty
        // Vec — the majority of plans (anything not on the parallel-
        // window path today). Serialization errors are logged and the
        // report dropped rather than failing the task; the task's data
        // was already produced correctly, telemetry loss shouldn't tank
        // the query.
        let plan: Arc<dyn ExecutionPlan> = match &self.shuffle_writer {
            ShuffleWriterVariant::Passthrough(writer) => Arc::new(writer.clone()),
            ShuffleWriterVariant::Range(writer) => Arc::new(writer.clone()),
            ShuffleWriterVariant::Sort(writer) => Arc::new(writer.clone()),
        };
        match ballista_core::execution_plans::collect_runtime_stats_reports(&plan) {
            Ok(reports) => reports,
            Err(e) => {
                log::warn!(
                    "collect_runtime_stats_reports failed, task will report empty stats: {e}"
                );
                Vec::new()
            }
        }
    }

    /// Number of files in each file group of a `DataSourceExec`.
    fn group_file_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source: &dyn Any = exec.data_source().as_ref();
        let config = source.downcast_ref::<FileScanConfig>().unwrap();
        config.file_groups.iter().map(|g| g.len()).collect()
    }

    #[test]
    fn restrict_scan_keeps_only_its_own_group() {
        let plan = scan_with_file_groups(4);
        let restricted = restrict_scan_to_partition(&plan, 2).expect("scan rewritten");
        assert_eq!(group_file_counts(&restricted), vec![0, 0, 1, 0]);
    }

    #[test]
    fn restrict_scan_partition_out_of_range_is_left_untouched() {
        let plan = scan_with_file_groups(3);
        assert!(restrict_scan_to_partition(&plan, 3).is_none());
    }

    #[test]
    fn restrict_scan_ignores_non_file_scans() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        assert!(restrict_scan_to_partition(&plan, 0).is_none());
    }
}

/// Spawn K parallel `plan.execute(N, ctx)` calls against a shuffle writer,
/// collect metadata batches from each, and turn them back into
/// `Vec<ShuffleWritePartition>`. All K streams must be driven concurrently
/// so the writer's internal coordinator sees every oneshot receiver taken.
///
/// `is_sort_shuffle` is stamped onto every summary produced from the batches
/// — the metadata schema doesn't carry the flag (it's a handoff-only shape),
/// but the reader side needs it in `PartitionLocation` to pick the right
/// on-disk layout. The caller knows the variant from `ShuffleWriterVariant`.
async fn drive_shuffle_writer_stage(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    is_sort_shuffle: bool,
) -> Result<Vec<ShuffleWritePartition>> {
    let k = plan.properties().output_partitioning().partition_count();

    let mut stream_futures = Vec::with_capacity(k);
    for n in 0..k {
        let plan = plan.clone();
        let ctx = context.clone();
        stream_futures.push(tokio::spawn(async move {
            let mut stream = plan.execute(n, ctx)?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                batches.push(batch);
            }
            metadata_batches_to_summaries(batches, is_sort_shuffle)
        }));
    }

    let mut summaries = Vec::with_capacity(k);
    for handle in stream_futures {
        let per_partition = handle.await.map_err(|e| {
            DataFusionError::Execution(format!("shuffle writer drain panicked: {e}"))
        })??;
        summaries.extend(per_partition);
    }
    // Drop summaries for output slots that produced no data. The coordinator
    // uses zero-content entries as sentinels so `execute(N)` streams don't
    // stall on an unfilled oneshot; those must not become PartitionLocations
    // the scheduler tries to fetch.
    summaries.retain(|s| s.num_bytes > 0);
    Ok(summaries)
}

/// Convert the writer's metadata batches (one per output partition, each
/// with a single row) back into `ShuffleWritePartition` summaries.
fn metadata_batches_to_summaries(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    is_sort_shuffle: bool,
) -> Result<Vec<ShuffleWritePartition>> {
    let stats_fields = PartitionStats::default().arrow_struct_fields();
    let num_rows_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_rows")
        .expect("num_rows field present in PartitionStats");
    let num_batches_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_batches")
        .expect("num_batches field present in PartitionStats");
    let num_bytes_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_bytes")
        .expect("num_bytes field present in PartitionStats");

    let mut out = Vec::new();
    for batch in batches {
        let partition_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: partition column not UInt32".into(),
                )
            })?;
        let _path_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: path column not Utf8".into(),
                )
            })?;
        let file_id_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: file_id column not UInt64".into(),
                )
            })?;
        let stats_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: stats column not Struct".into(),
                )
            })?;
        let num_rows_arr = stats_col
            .column(num_rows_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_rows not UInt64".into(),
                )
            })?;
        let num_batches_arr = stats_col
            .column(num_batches_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_batches not UInt64".into(),
                )
            })?;
        let num_bytes_arr = stats_col
            .column(num_bytes_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_bytes not UInt64".into(),
                )
            })?;

        for row in 0..batch.num_rows() {
            let file_id = if file_id_col.is_null(row) {
                None
            } else {
                Some(file_id_col.value(row))
            };
            out.push(ShuffleWritePartition {
                partition_id: partition_col.value(row) as u64,
                num_batches: num_batches_arr.value(row),
                num_rows: num_rows_arr.value(row),
                num_bytes: num_bytes_arr.value(row),
                file_id,
                is_sort_shuffle,
            });
        }
    }
    Ok(out)
}

// TODO: port these tests to scheduler/src/state/task_builder.rs (they used
// to cover the executor-side restrict function that has moved scheduler-side).
