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
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::execution_plans::sort_shuffle::SortShuffleWriterExec;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::utils;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
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
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        partition_id: usize,
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

/// Executor for a single query stage in a distributed query.
///
/// A query stage is a section of a query plan that has consistent partitioning
/// and can be executed as one unit with each partition running in parallel.
/// The output of each partition is re-partitioned and written to disk in
/// Arrow IPC format. Subsequent stages read these results via ShuffleReaderExec.
#[async_trait]
pub trait QueryStageExecutor: Sync + Send + Debug + Display {
    /// Executes a single partition of this query stage.
    ///
    /// Returns metadata about the shuffle partitions written to disk,
    /// including file paths and statistics.
    async fn execute_query_stage(
        &self,
        input_partition: usize,
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
pub struct DefaultExecutionEngine {}

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
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
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Hash(exec),
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
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Sort(exec),
            )))
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec or SortShuffleWriterExec"
                    .to_string(),
            ))
        }
    }
}

/// Enum representing the different shuffle writer implementations.
#[derive(Debug, Clone)]
pub enum ShuffleWriterVariant {
    /// Hash-based shuffle writer (original implementation).
    Hash(ShuffleWriterExec),
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
            ShuffleWriterVariant::Hash(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Hash): ({})\n{}",
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
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
            ShuffleWriterVariant::Sort(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
        }
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => utils::collect_plan_metrics(writer),
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::empty::EmptyExec;

    /// Build a `DataSourceExec` over `n` file groups, one file each.
    fn scan_with_file_groups(n: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let source = Arc::new(ParquetSource::new(schema));
        let mut builder =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source);
        for i in 0..n {
            builder =
                builder.with_file_group(FileGroup::new(vec![PartitionedFile::new(
                    format!("file{i}.parquet"),
                    100,
                )]));
        }
        DataSourceExec::from_data_source(builder.build())
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
