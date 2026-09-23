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

mod buffer;
mod chaos_exec;
mod distributed_explain_analyze;
mod distributed_query;
mod ordered_range_repartition;
mod partitioned_bounded_window_agg;
mod per_partition_filter;
pub mod plan_algebra;
mod prefix_merge;
mod range_filter;
mod range_repartition_common;
pub mod range_shuffle;
mod range_shuffle_reader;
mod runtime_stats;
mod shuffle_manager;
pub(crate) mod shuffle_reader;
mod shuffle_writer;
mod shuffle_writer_trait;
pub mod sort_shuffle;
mod unordered_range_repartition;
mod unresolved_shuffle;
pub mod window_state;

#[cfg(feature = "vortex")]
pub mod vortex_shuffle;

pub use buffer::{BufferExec, BufferMode};
pub use chaos_exec::ChaosExec;
pub use distributed_explain_analyze::DistributedExplainAnalyzeExec;
pub use distributed_query::{DistributedQueryExec, execute_physical_plan};
pub use ordered_range_repartition::OrderedRangeRepartitionExec;
pub use partitioned_bounded_window_agg::PartitionedBoundedWindowAggExec;
pub use per_partition_filter::{PerPartitionFilterExec, range_partition_predicates};
pub use plan_algebra::{preserves_distribution, preserves_partitioning};
pub use prefix_merge::{FinalizedPartitionState, PrefixMergeExec, ScalarOp, WindowApply};
pub use range_filter::{InputOrder, RangeBound, RangeFilterExec, WidenedBound};
pub use range_shuffle::RangeShuffleWriterExec;
pub use range_shuffle_reader::RangeShuffleReaderExec;
pub use runtime_stats::{
    MergedRuntimeStats, RuntimeStatsExec, TaskRuntimeStats,
    collect_reports as collect_runtime_stats_reports, cut_partitions,
    log_merged_runtime_stats, merge_reports as merge_runtime_stats_reports,
    repartition_routing_expr,
};
pub use shuffle_manager::{
    InMemoryShuffleManager, ShufflePartitionData, ShufflePartitionKey,
    global_shuffle_manager,
};
pub use shuffle_reader::{CoalescePlan, PartitionGroup, ShuffleReaderExec};
pub use shuffle_reader::{
    connect_ballista_client, set_shuffle_transport_runtime, stats_for_partition,
    stats_for_partitions,
};
pub use shuffle_writer::DEFAULT_SHUFFLE_CHANNEL_CAPACITY;
pub use shuffle_writer::ShuffleWriterExec;
pub use shuffle_writer::compute_global_output_partition_ids;
pub use shuffle_writer_trait::ShuffleWriter;
pub use sort_shuffle::SortShuffleWriterExec;
pub use unordered_range_repartition::UnorderedRangeRepartitionExec;
pub use unresolved_shuffle::UnresolvedShuffleExec;
pub use window_state::{
    ObservedWindowState, TaskWindowState, WindowStateCollector,
    prefix_merge_window_state, window_state_from_proto, window_state_to_proto,
};

#[cfg(feature = "vortex")]
pub use vortex_shuffle::{
    LocalVortexShuffleStream, VortexWriteTracker, vortex_file_extension,
    write_stream_to_disk_vortex,
};
