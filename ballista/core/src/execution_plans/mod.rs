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

//! This module contains execution plans that are needed to distribute DataFusion's execution plans into
//! several Ballista executors.

mod distributed_explain_analyze;
mod distributed_query;
mod shuffle_manager;
pub(crate) mod shuffle_reader;
mod shuffle_writer;
mod shuffle_writer_trait;
pub mod sort_shuffle;
mod unresolved_shuffle;

#[cfg(feature = "vortex")]
pub mod vortex_shuffle;

pub use distributed_explain_analyze::DistributedExplainAnalyzeExec;
pub use distributed_query::DistributedQueryExec;
pub use shuffle_manager::{
    InMemoryShuffleManager, ShufflePartitionData, ShufflePartitionKey,
    global_shuffle_manager,
};
pub use shuffle_reader::ShuffleReaderExec;
pub use shuffle_reader::{stats_for_partition, stats_for_partitions};
pub use shuffle_writer::ShuffleWriterExec;
pub use shuffle_writer_trait::ShuffleWriter;
pub use sort_shuffle::SortShuffleWriterExec;
pub use unresolved_shuffle::UnresolvedShuffleExec;

#[cfg(feature = "vortex")]
pub use vortex_shuffle::{
    LocalVortexShuffleStream, VortexWriteTracker, vortex_file_extension,
    write_stream_to_disk_vortex,
};

use datafusion::common::tree_node::Transformed;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use std::sync::Arc;

/// Rebuild a `HashJoinExec` node via `try_new()` to strip any dynamic-filter
/// accumulator (e.g. `SharedBuildAccumulator`). The accumulator uses a
/// cross-partition `Barrier` that deadlocks in Ballista where each task runs a
/// single partition. `try_new()` never attaches an accumulator, so this is
/// always safe.
///
/// If `node` is not a `HashJoinExec`, returns `Transformed::no(node)`.
pub fn rebuild_hash_join_without_accumulator(
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
        let left = Arc::clone(hj.left());
        let left: Arc<dyn ExecutionPlan> = if *hj.partition_mode()
            == PartitionMode::CollectLeft
            && left.properties().output_partitioning().partition_count() > 1
        {
            Arc::new(CoalescePartitionsExec::new(left))
        } else {
            left
        };
        let rebuilt: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left,
            Arc::clone(hj.right()),
            hj.on().to_vec(),
            hj.filter().cloned(),
            hj.join_type(),
            hj.projection.clone(),
            *hj.partition_mode(),
            hj.null_equality(),
        )?);
        return Ok(Transformed::yes(rebuilt));
    }
    Ok(Transformed::no(node))
}
