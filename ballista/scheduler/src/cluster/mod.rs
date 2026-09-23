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

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::Stream;
use log::debug;

use ballista_core::config::BallistaConfig;
use ballista_core::consistent_hash::ConsistentHash;
use ballista_core::error::Result;
use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use ballista_core::serde::protobuf::{
    AvailableVcores, ExecutorHeartbeat, JobStatus, job_status,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata, TaskKey};
use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_core::{ConfigProducer, JobId, JobStatusSubscriber, consistent_hash};

use crate::cluster::memory::{InMemoryClusterState, InMemoryJobState};

use crate::config::{SchedulerConfig, TaskDistributionPolicy};
use crate::scheduler_server::SessionBuilder;
use crate::state::execution_graph::{
    ExecutionGraphBox, TaskDescription, create_task_info,
};
use crate::state::task_manager::JobInfoCache;

/// Event broadcasting and subscription for cluster state changes.
pub mod event;
/// In-memory cluster state implementation.
pub mod memory;

/// Test utilities for cluster state testing.
#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
pub mod test_util;

/// Enum to configure the cluster state storage backend.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum ClusterStorage {
    /// In-memory storage (non-persistent).
    Memory,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for ClusterStorage {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

/// Manages the distributed state of a Ballista cluster.
///
/// Combines cluster state (executor registration, heartbeats) with job state
/// (job submissions, execution graphs).
#[derive(Clone)]
pub struct BallistaCluster {
    /// State for tracking executors and their resources.
    cluster_state: Arc<dyn ClusterState>,
    /// State for tracking jobs and their execution progress.
    job_state: Arc<dyn JobState>,
}

impl BallistaCluster {
    /// Creates a new `BallistaCluster` with the given state backends.
    pub fn new(
        cluster_state: Arc<dyn ClusterState>,
        job_state: Arc<dyn JobState>,
    ) -> Self {
        Self {
            cluster_state,
            job_state,
        }
    }

    /// Creates a new `BallistaCluster` with in-memory state backends.
    pub fn new_memory(
        scheduler: impl Into<String>,
        session_builder: SessionBuilder,
        config_producer: ConfigProducer,
    ) -> Self {
        Self {
            cluster_state: Arc::new(InMemoryClusterState::default()),
            job_state: Arc::new(InMemoryJobState::new(
                scheduler,
                session_builder,
                config_producer,
            )),
        }
    }

    /// Creates a new `BallistaCluster` from scheduler configuration.
    pub async fn new_from_config(config: &SchedulerConfig) -> Result<Self> {
        let scheduler = config.scheduler_name();

        let session_builder = config
            .override_session_builder
            .clone()
            .unwrap_or_else(|| Arc::new(default_session_builder));

        let config_producer = config
            .override_config_producer
            .clone()
            .unwrap_or_else(|| Arc::new(default_config_producer));

        Ok(BallistaCluster::new_memory(
            scheduler,
            session_builder,
            config_producer,
        ))
    }

    /// Returns the cluster state backend.
    pub fn cluster_state(&self) -> Arc<dyn ClusterState> {
        self.cluster_state.clone()
    }

    /// Returns the job state backend.
    pub fn job_state(&self) -> Arc<dyn JobState> {
        self.job_state.clone()
    }
}

/// Stream of `ExecutorHeartbeat` messages.
///
/// This stream contains all heartbeats received by any schedulers sharing a `ClusterState`.
pub type ExecutorHeartbeatStream = Pin<Box<dyn Stream<Item = ExecutorHeartbeat> + Send>>;

/// A task bound to an executor for execution.
///
/// Tuple of (executor_id, task_description).
pub type BoundTask = (String, TaskDescription);

/// Shuffle affinity information for a bound task.
///
/// Tracks whether a task was scheduled on an executor that has local shuffle data
/// from the task's input stages.
#[derive(Debug, Clone)]
pub struct ShuffleAffinityInfo {
    /// Job ID for this task.
    pub job_id: JobId,
    /// Stage ID for this task.
    pub stage_id: usize,
    /// Executor ID where the task was scheduled.
    pub executor_id: String,
    /// True if the executor has local shuffle data for at least one input partition.
    pub has_local_data: bool,
}

/// Result of task binding including shuffle affinity metrics.
#[derive(Debug, Default)]
pub struct BindingResult {
    /// Tasks bound to executors.
    pub bound_tasks: Vec<BoundTask>,
    /// Shuffle affinity information for tasks that have shuffle inputs.
    /// Only populated for tasks whose stages read from shuffle (not leaf stages).
    pub shuffle_affinity: Vec<ShuffleAffinityInfo>,
}

impl BindingResult {
    /// Creates a new empty binding result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a binding result from bound tasks without affinity info.
    pub fn from_tasks(bound_tasks: Vec<BoundTask>) -> Self {
        Self {
            bound_tasks,
            shuffle_affinity: Vec::new(),
        }
    }

    /// Extends this result with another binding result.
    pub fn extend(&mut self, other: BindingResult) {
        self.bound_tasks.extend(other.bound_tasks);
        self.shuffle_affinity.extend(other.shuffle_affinity);
    }
}

/// An executor slot representing available task capacity.
///
/// Tuple of (executor_id, slot_count).
pub type ExecutorSlot = (String, u32);

/// Trait for maintaining a globally consistent view of cluster resources.
///
/// Implementations track executor registration, heartbeats, and available task slots.
#[tonic::async_trait]
pub trait ClusterState: Send + Sync + 'static {
    /// Initializes the cluster state backend.
    ///
    /// This is particularly important for backends with external storage.
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    /// Binds ready-to-run tasks from active jobs to executor vcores.
    ///
    /// If `executors` is provided, only bind slots from the specified executor IDs.
    /// Returns both the bound tasks and shuffle affinity information for metrics.
    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<JobId, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> Result<BindingResult>;

    /// Releases reserved vcores when tasks finish or fail.
    ///
    /// This operation is atomic: either all vcores are released or none are.
    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()>;

    /// Registers a new executor in the cluster.
    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
    ) -> Result<()>;

    /// Saves executor metadata, overwriting any existing metadata for the executor ID.
    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()>;

    /// Returns executor metadata for the given executor ID.
    ///
    /// Returns an error if the executor does not exist.
    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata>;

    /// Returns a list of all registered executor metadata.
    async fn registered_executor_metadata(&self) -> Vec<ExecutorMetadata>;

    /// Saves an executor heartbeat.
    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()>;

    /// Removes an executor from the cluster.
    async fn remove_executor(&self, executor_id: &str) -> Result<()>;

    /// Returns the last seen heartbeat for all active executors.
    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat>;

    /// Returns the executor heartbeat for the given executor ID, or None if not found.
    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat>;

    /// Returns a stream of cluster state events.
    ///
    /// Events are published whenever the cluster state changes (e.g., executor registration/removal).
    async fn cluster_state_events(&self) -> Result<ClusterStateEventStream>;
}

/// Events related to the state of jobs. Implementations may or may not support all event types.
#[derive(Debug, Clone, PartialEq)]
pub enum JobStateEvent {
    /// Event when a job status has been updated
    JobUpdated {
        /// Job ID of updated job
        job_id: JobId,
        /// New job status
        status: JobStatus,
    },
    /// Event when a scheduler acquires ownership of the job. This happens
    /// either when a scheduler submits a job (in which case ownership is implied)
    /// or when a scheduler acquires ownership of a running job release by a
    /// different scheduler
    JobAcquired {
        /// Job ID of the acquired job
        job_id: JobId,
        /// The scheduler which acquired ownership of the job
        owner: String,
    },
    /// Event when a scheduler releases ownership of a still active job
    JobReleased {
        /// Job ID of the released job
        job_id: JobId,
    },
    /// Event when a new session has been created.
    SessionAccessed {
        /// Session ID that was accessed.
        session_id: String,
    },
    /// Event when a session configuration has been removed.
    SessionRemoved {
        /// Session ID that was removed.
        session_id: String,
    },
}

/// Events related to the state of the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterStateEvent {
    /// An executor has been registered with the cluster.
    RegisteredExecutor {
        /// ID of the registered executor.
        executor_id: String,
    },
    /// An executor has been removed from the cluster.
    RemovedExecutor {
        /// ID of the removed executor.
        executor_id: String,
    },
}

/// Stream of cluster state events.
pub type ClusterStateEventStream = Pin<Box<dyn Stream<Item = ClusterStateEvent> + Send>>;

/// Stream of job state events.
///
/// This stream contains all events received by schedulers sharing a `ClusterState`.
pub type JobStateEventStream = Pin<Box<dyn Stream<Item = JobStateEvent> + Send>>;

/// Trait for persisting state related to executing jobs.
///
/// Implementations handle job lifecycle, execution graphs, and session management.
#[tonic::async_trait]
pub trait JobState: Send + Sync {
    /// Accepts a job into the scheduler's queue.
    ///
    /// Called when a job is received but before it is planned.
    fn accept_job(&self, job_id: &JobId, job_name: &str, queued_at: u64) -> Result<()>;

    /// Returns the number of queued jobs waiting to be scheduled.
    fn pending_job_number(&self) -> usize;

    /// Submits a new job to the job state.
    ///
    /// The submitter is assumed to own the job.
    async fn submit_job(
        &self,
        job_id: JobId,
        graph: &ExecutionGraphBox,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()>;

    /// Returns the set of all active job IDs.
    async fn get_jobs(&self) -> Result<HashSet<JobId>>;

    /// Returns the set of all job IDs including running, queued, and completed jobs.
    async fn get_all_jobs(&self) -> Result<HashSet<JobId>>;

    /// Returns the status of the specified job.
    async fn get_job_status(&self, job_id: &JobId) -> Result<Option<JobStatus>>;

    /// Returns the execution graph for a job.
    ///
    /// The job may not belong to the caller, and the graph may be updated
    /// by another scheduler after this call returns.
    async fn get_execution_graph(
        &self,
        job_id: &JobId,
    ) -> Result<Option<ExecutionGraphBox>>;

    /// Persists the current state of an owned job.
    ///
    /// Implementations are responsible for applying any backend-specific retry policy.
    /// Callers must not assume that this operation is safe to retry.
    ///
    /// Returns an error if the job is not owned by the caller.
    async fn save_job(&self, job_id: &JobId, graph: &ExecutionGraphBox) -> Result<()>;

    /// Marks an unscheduled job as failed.
    ///
    /// Called when a job fails during planning before an execution graph is created.
    async fn fail_unscheduled_job(&self, job_id: &JobId, reason: String) -> Result<()>;

    /// Deletes a job from the state.
    async fn remove_job(&self, job_id: &JobId) -> Result<()>;

    /// Attempts to acquire ownership of a job.
    ///
    /// Returns the execution graph if the job is still running and successfully acquired,
    /// otherwise returns None.
    async fn try_acquire_job(&self, job_id: &JobId) -> Result<Option<ExecutionGraphBox>>;

    /// Returns a stream of job state events.
    async fn job_state_events(&self) -> Result<JobStateEventStream>;

    /// Creates a new session or updates an existing one.
    async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>>;

    /// Removes a session from the state.
    async fn remove_session(&self, session_id: &str) -> Result<()>;

    /// Produces a session configuration for new sessions.
    fn produce_config(&self) -> SessionConfig;
}

use crate::state::execution_stage::RunningStage;

/// Collects the set of executor IDs that have local shuffle data for this stage.
///
/// Returns `None` if the stage has no shuffle inputs (leaf stage),
/// otherwise returns `Some(set)` where the set contains executor IDs with local data.
fn get_executors_with_local_shuffle_data(
    running_stage: &RunningStage,
) -> Option<HashSet<String>> {
    if running_stage.inputs.is_empty() {
        // Leaf stage with no shuffle inputs
        return None;
    }

    let mut executors_with_local_data = HashSet::new();
    for stage_output in running_stage.inputs.values() {
        for partition_locations in stage_output.partition_locations.values() {
            for location in partition_locations {
                executors_with_local_data.insert(location.executor_meta.id.clone());
            }
        }
    }

    Some(executors_with_local_data)
}

/// Whether this stage's plan contains a collapse — any operator whose
/// `output_partitioning().partition_count() == 1` (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`, `AggregateExec(Final, gby=[])`,
/// `SortExec(preserve_partitioning=false)`, …). Downstream of a collapse
/// expects the *combined* input; splitting across tasks would give each
/// task's local collapse only a slice, producing partial results.
///
/// Walks past *any* single-child operator: today's writers that bake
/// partitioning into the shuffle write itself (`SortShuffleWriterExec::Hash`),
/// and future partition operators that separate partitioning from writing
/// (e.g. `UnorderedRangeRepartitionExec`). Stops at leaves, multi-child
/// operators (fan-in / joins), and stage boundaries.
///
/// The stage-boundary stop enumerates the leaf readers that terminate a
/// resolved stage plan: `ShuffleReaderExec` (regular / broadcast / coalesced)
/// and `RangeShuffleReaderExec` (ordering-preserving). The *general* rule is
/// "stop at any stage boundary"; if new stage-boundary operators appear, add
/// them here (or, better, get `ExecutionPlan` upstream to expose an
/// `is_stage_boundary()` property so we don't keep enumerating).
fn stage_has_input_collapse(plan_root: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.downcast_ref::<ShuffleReaderExec>().is_some()
            || node.downcast_ref::<RangeShuffleReaderExec>().is_some()
        {
            return false;
        }
        if node.properties().output_partitioning().partition_count() == 1 {
            return true;
        }
        match node.children().as_slice() {
            [child] => walk(child),
            _ => false,
        }
    }
    let result = match plan_root.children().as_slice() {
        [child] => walk(child),
        _ => false,
    };
    debug!(
        "stage_has_input_collapse: root={} root_partitions={} → {result}",
        plan_root.name(),
        plan_root
            .properties()
            .output_partitioning()
            .partition_count(),
    );
    result
}

/// Binds one runnable slice of `running_stage`'s pending partitions to
/// `budget`'s executor, consuming as many vcores as the slice needs (see
/// [`crate::state::execution_stage::RunningStage::pending`]).  Returns `None`
/// once the stage has no more pending partitions to bind.
fn bind_one(
    running_stage: &mut crate::state::execution_stage::RunningStage,
    session_id: &str,
    job_id: &JobId,
    budget: &mut AvailableVcores,
) -> Option<BoundTask> {
    let is_collapse = stage_has_input_collapse(&running_stage.plan);
    // Cap non-collapse slices at the configured `max_partitions_per_task`.
    // Collapse stages must still pack their full pending queue into a single
    // task for correctness — a split collapse would produce partial results
    // downstream can't merge.
    let cap = running_stage
        .session_config
        .options()
        .extensions
        .get::<BallistaConfig>()
        .map(|bc| bc.max_partitions_per_task())
        .filter(|&n| n > 0)
        .unwrap_or(usize::MAX);
    let max_partitions = if is_collapse {
        usize::MAX
    } else {
        (budget.vcores as usize).min(cap)
    };
    let input_partition_ids = running_stage.pending.next_slice(max_partitions);
    if input_partition_ids.is_empty() {
        return None;
    }
    // Non-collapse: DataFusion's volcano/pull model drives one tokio task per
    // root output partition, so N input partitions bundled into a task need
    // N vcores of concurrent execution. Collapse: the plan's root has a
    // single output partition, so only 1 thread is ever active for the
    // whole pipeline no matter how many inputs the task packs — reserve
    // one vcore and leave the rest for other stages' tasks on this executor.
    let vcores_consumed = if is_collapse {
        1
    } else {
        input_partition_ids.len() as u32
    };
    debug!(
        "bind_one: job={} stage={} exec={} vcores={} max={} slice_len={} consumed={} partitions={:?} collapse={}",
        job_id,
        running_stage.stage_id,
        budget.executor_id,
        budget.vcores,
        if max_partitions == usize::MAX {
            -1i64
        } else {
            max_partitions as i64
        },
        input_partition_ids.len(),
        vcores_consumed,
        input_partition_ids,
        is_collapse,
    );
    let executor_id = budget.executor_id.clone();
    // task_id is the append-order slot in `task_infos` — since we're
    // about to push, that's `task_infos.len()`. `(job_id, stage_id,
    // task_id)` is globally unique.
    let task_id = running_stage.task_infos.len();
    let task_attempt = input_partition_ids
        .iter()
        .map(|pid| running_stage.task_failure_numbers[*pid])
        .max()
        .unwrap_or(0);
    let mut task_info = create_task_info(executor_id.clone(), task_id);
    task_info.global_input_partition_ids = input_partition_ids.clone();
    task_info.vcores_consumed = vcores_consumed;
    running_stage.task_infos.push(task_info);
    let key = TaskKey {
        job_id: job_id.clone(),
        stage_id: running_stage.stage_id,
        task_id,
    };
    let task_desc = TaskDescription {
        session_id: session_id.to_string(),
        key,
        stage_attempt_num: running_stage.stage_attempt_num,
        task_attempt,
        global_input_partition_ids: input_partition_ids,
        vcores_consumed,
        plan: running_stage.plan.clone(),
        session_config: running_stage.session_config.clone(),
        schedulable_time_millis: running_stage.stage_running_time,
    };
    budget.vcores -= vcores_consumed;
    Some((executor_id, task_desc))
}

pub(crate) async fn bind_task_bias(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> BindingResult {
    let mut result = BindingResult::new();

    if budgets.iter().all(|b| b.vcores == 0) {
        debug!("No executor vcores available for task binding");
        return result;
    }

    // Bias: give each stage the biggest exec available. Sort descending
    // so bind_one keeps packing tasks onto the largest executor until its
    // vcore budget is drained.
    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                debug!(
                    "Will skip stage {}/{} for bias task binding",
                    job_id, running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }

            // Get executors with local shuffle data before we borrow task_infos mutably
            let executors_with_local_data =
                get_executors_with_local_shuffle_data(running_stage);
            let stage_id = running_stage.stage_id;

            while idx < budgets.len() {
                while idx < budgets.len() && budgets[idx].vcores == 0 {
                    idx += 1;
                }
                if idx >= budgets.len() {
                    return result;
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some((executor_id, task_desc)) => {
                        // Record shuffle affinity for this task if it has shuffle inputs
                        if let Some(ref local_executors) = executors_with_local_data {
                            result.shuffle_affinity.push(ShuffleAffinityInfo {
                                job_id: job_id.clone(),
                                stage_id,
                                executor_id: executor_id.clone(),
                                has_local_data: local_executors.contains(&executor_id),
                            });
                        }
                        result.bound_tasks.push((executor_id, task_desc));
                    }
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    result
}

pub(crate) async fn bind_task_round_robin(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> BindingResult {
    let mut result = BindingResult::new();

    if budgets.iter().all(|b| b.vcores == 0) {
        debug!("No executor vcores available for task binding");
        return result;
    }

    // Round-robin across execs so multiple running stages each get one
    // exec per rotation. Order by vcores desc so the largest exec goes
    // first in the rotation.
    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                debug!(
                    "Will skip stage {}/{} for round robin task binding",
                    job_id, running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }

            // Get executors with local shuffle data before we borrow task_infos mutably
            let executors_with_local_data =
                get_executors_with_local_shuffle_data(running_stage);
            let stage_id = running_stage.stage_id;

            loop {
                let mut scanned = 0usize;
                while budgets[idx].vcores == 0 {
                    idx = (idx + 1) % budgets.len();
                    scanned += 1;
                    if scanned >= budgets.len() {
                        return result;
                    }
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some((executor_id, task_desc)) => {
                        // Record shuffle affinity for this task if it has shuffle inputs
                        if let Some(ref local_executors) = executors_with_local_data {
                            result.shuffle_affinity.push(ShuffleAffinityInfo {
                                job_id: job_id.clone(),
                                stage_id,
                                executor_id: executor_id.clone(),
                                has_local_data: local_executors.contains(&executor_id),
                            });
                        }
                        result.bound_tasks.push((executor_id, task_desc));
                        idx = (idx + 1) % budgets.len();
                    }
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    result
}

/// Maps execution plan to list of files it scans
type GetScanFilesFunc = fn(
    &JobId,
    Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Vec<Vec<Vec<PartitionedFile>>>>;

/// User provided task distribution policy
#[async_trait::async_trait]
pub trait DistributionPolicy: std::fmt::Debug + Send + Sync {
    // few open questions for later:
    //
    // - should scheduling policy type be a parameter
    //   as we see in the consistent hash, it does not work in
    //   pull based. Or we find another way to address this concern
    // - should we add `ClusterState` as method parameter
    //

    /// User provided custom task distribution policy
    ///
    /// # Parameters
    ///
    /// * `budgets` - per-executor free-vcore budgets (may be empty)
    /// * `running_jobs` - (JobId -> JobInfoCache) cache must contain only running jobs
    ///
    /// # Returns
    ///
    /// vector of task, executor bounding
    ///
    async fn bind_tasks(
        &self,
        mut budgets: Vec<&mut AvailableVcores>,
        running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>>;

    /// Name of [DistributionPolicy]
    fn name(&self) -> &str;
}

pub(crate) async fn bind_task_consistent_hash(
    topology_nodes: HashMap<String, TopologyNode>,
    num_replicas: usize,
    tolerance: usize,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    get_scan_files: GetScanFilesFunc,
) -> Result<(BindingResult, Option<ConsistentHash<TopologyNode>>)> {
    let mut total_slots = 0usize;
    for node in topology_nodes.values() {
        total_slots += node.available_slots as usize;
    }
    if total_slots == 0 {
        debug!(
            "Not enough available executor slots for binding tasks with consistent hashing policy!!!"
        );
        return Ok((BindingResult::new(), None));
    }
    debug!("Total slot number for consistent hash binding is {total_slots}");

    let node_replicas = topology_nodes
        .into_values()
        .map(|node| (node, num_replicas))
        .collect::<Vec<_>>();
    let mut ch_topology: ConsistentHash<TopologyNode> =
        ConsistentHash::new(node_replicas);

    let mut result = BindingResult::new();
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            let scan_files = get_scan_files(job_id, running_stage.plan.clone())?;
            if is_skip_consistent_hash(&scan_files) {
                debug!(
                    "Will skip stage {}/{} for consistent hashing task binding",
                    job_id, running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }
            let pre_total_slots = total_slots;
            let scan_files = &scan_files[0];
            let tolerance_list = vec![0, tolerance];
            // First round with 0 tolerance consistent hashing policy
            // Second round with [`tolerance`] tolerance consistent hashing policy
            for tolerance in tolerance_list {
                let runnable_partitions: Vec<usize> = running_stage
                    .pending
                    .pending_ids()
                    .into_iter()
                    .take(total_slots)
                    .collect();
                for partition_id in runnable_partitions {
                    let partition_files = &scan_files[partition_id];
                    assert!(!partition_files.is_empty());
                    // Currently we choose the first file for a task for consistent hash.
                    // Later when splitting files for tasks in datafusion, it's better to
                    // introduce this hash based policy besides the file number policy or file size policy.
                    let file_for_hash = &partition_files[0];
                    if let Some(node) = ch_topology.get_mut_with_tolerance(
                        file_for_hash.object_meta.location.as_ref().as_bytes(),
                        tolerance,
                    ) {
                        let executor_id = node.id.clone();
                        // task_id is the append-order slot in `task_infos` — since
                        // we're about to push, that's `task_infos.len()`.
                        let task_id = running_stage.task_infos.len();
                        running_stage.pending.take(partition_id);
                        let mut task_info = create_task_info(executor_id.clone(), task_id);
                        task_info.global_input_partition_ids = vec![partition_id];
                        task_info.vcores_consumed = 1;
                        running_stage.task_infos.push(task_info);

                        // Note: Consistent hash is used for scan (leaf) stages, not shuffle stages,
                        // so we don't track shuffle affinity here. The stage has scan files,
                        // meaning it's a leaf stage without shuffle inputs.

                        let key = TaskKey {
                            job_id: job_id.clone(),
                            stage_id: running_stage.stage_id,
                            task_id,
                        };
                        let task_desc = TaskDescription {
                            session_id: session_id.clone(),
                            key,
                            stage_attempt_num: running_stage.stage_attempt_num,
                            task_attempt: running_stage.task_failure_numbers
                                [partition_id],
                            global_input_partition_ids: vec![partition_id],
                            vcores_consumed: 1,
                            plan: running_stage.plan.clone(),
                            session_config: running_stage.session_config.clone(),
                            schedulable_time_millis: running_stage.stage_running_time,
                        };
                        result.bound_tasks.push((executor_id, task_desc));

                        node.available_slots -= 1;
                        total_slots -= 1;
                        if total_slots == 0 {
                            return Ok((result, Some(ch_topology)));
                        }
                    }
                }
            }
            // Since there's no more tasks from this stage which can be bound,
            // we should skip this stage at the next round.
            if pre_total_slots == total_slots {
                black_list.push(running_stage.stage_id);
            }
        }
    }

    Ok((result, Some(ch_topology)))
}

// If if there's no plan which needs to scan files, skip it.
// Or there are multiple plans which need to scan files for a stage, skip it.
pub(crate) fn is_skip_consistent_hash(scan_files: &[Vec<Vec<PartitionedFile>>]) -> bool {
    scan_files.is_empty() || scan_files.len() > 1
}

/// Callback adapter for [`GetScanFilesFunc`] that ignores job id.
pub(crate) fn scan_files_for_binding(
    _job_id: &JobId,
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Vec<Vec<Vec<PartitionedFile>>>> {
    get_scan_files(plan)
}

/// Get all of the [`PartitionedFile`] to be scanned for an [`ExecutionPlan`]
pub(crate) fn get_scan_files(
    plan: Arc<dyn ExecutionPlan>,
) -> std::result::Result<Vec<Vec<Vec<PartitionedFile>>>, DataFusionError> {
    let mut collector: Vec<Vec<Vec<PartitionedFile>>> = vec![];
    plan.apply(&mut |plan: &Arc<dyn ExecutionPlan>| {
        if let Some(config) = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|c| c.data_source().downcast_ref::<FileScanConfig>())
        {
            collector.push(
                config
                    .file_groups
                    .iter()
                    .map(|f| f.clone().into_inner())
                    .collect(),
            );
            Ok(TreeNodeRecursion::Jump)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(collector)
}

/// Represents a node in the cluster topology for consistent hashing.
#[derive(Clone)]
pub struct TopologyNode {
    /// Unique executor ID.
    pub id: String,
    /// Host:port name for the node.
    pub name: String,
    /// Timestamp of last heartbeat received.
    pub last_seen_ts: u64,
    /// Number of available task slots on this node.
    pub available_slots: u32,
}

impl TopologyNode {
    fn new(
        host: &str,
        port: u16,
        id: &str,
        last_seen_ts: u64,
        available_slots: u32,
    ) -> Self {
        Self {
            id: id.to_string(),
            name: format!("{host}:{port}"),
            last_seen_ts,
            available_slots,
        }
    }
}

impl consistent_hash::node::Node for TopologyNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_valid(&self) -> bool {
        self.available_slots > 0
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::datasource::listing::PartitionedFile;
    use object_store::ObjectMeta;
    use object_store::path::Path;

    use ballista_core::JobId;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::AvailableVcores;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    };

    use crate::cluster::{
        BoundTask, TopologyNode, bind_task_bias, bind_task_consistent_hash,
        bind_task_round_robin,
    };
    use crate::state::execution_graph::{ExecutionGraph, StaticExecutionGraph};
    use crate::state::task_manager::JobInfoCache;
    use crate::test_utils::{
        mock_completed_task, revive_graph_and_complete_next_stage,
        test_aggregation_plan_with_config,
    };
    use ballista_core::config::BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::prelude::SessionConfig;

    #[tokio::test]
    async fn test_bind_task_bias() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let mut available_slots = mock_budgets();
        let available_slots_ref: Vec<&mut AvailableVcores> =
            available_slots.iter_mut().collect();
        let binding_result =
            bind_task_bias(available_slots_ref, Arc::new(active_jobs), |_| false).await;
        assert_eq!(9, binding_result.bound_tasks.len());

        let result = get_result(binding_result.bound_tasks);

        // Multi-partition-task binding: each bind consumes slice.len() vcores,
        // so an executor with leftover vcores keeps taking work under the bias
        // policy (largest exec stays hot). Distribution depends on HashMap
        // iteration order across jobs.
        let mut expected = Vec::new();
        {
            let mut expected0 = HashMap::new();

            let mut entry_a = HashMap::new();
            entry_a.insert("executor_3".to_string(), 2);
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 5);
            entry_b.insert("executor_2".to_string(), 2);

            expected0.insert("job_a".to_string(), entry_a);
            expected0.insert("job_b".to_string(), entry_b);

            expected.push(expected0);
        }
        {
            let mut expected0 = HashMap::new();

            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 7);
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_2".to_string(), 2);

            expected0.insert("job_a".to_string(), entry_a);
            expected0.insert("job_b".to_string(), entry_b);

            expected.push(expected0);
        }

        assert!(
            expected.contains(&result),
            "The result {result:?} is not as expected {expected:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bind_task_round_robin() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let mut available_slots = mock_budgets();
        let available_slots_ref: Vec<&mut AvailableVcores> =
            available_slots.iter_mut().collect();
        let binding_result =
            bind_task_round_robin(available_slots_ref, Arc::new(active_jobs), |_| false)
                .await;
        assert_eq!(9, binding_result.bound_tasks.len());

        let result = get_result(binding_result.bound_tasks);

        let mut expected = Vec::new();
        {
            let mut expected0 = HashMap::new();

            let mut entry_a = HashMap::new();
            entry_a.insert("executor_3".to_string(), 2);
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_1".to_string(), 3);
            entry_b.insert("executor_3".to_string(), 2);
            entry_b.insert("executor_2".to_string(), 2);

            expected0.insert("job_a".to_string(), entry_a);
            expected0.insert("job_b".to_string(), entry_b);

            expected.push(expected0);
        }
        {
            let mut expected0 = HashMap::new();

            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 7);
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_2".to_string(), 1);
            entry_a.insert("executor_1".to_string(), 1);

            expected0.insert("job_a".to_string(), entry_a);
            expected0.insert("job_b".to_string(), entry_b);

            expected.push(expected0);
        }

        assert!(
            expected.contains(&result),
            "The result {result:?} is not as expected {expected:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bind_task_consistent_hash() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let active_jobs = Arc::new(active_jobs);
        let topology_nodes = mock_topology_nodes();
        let num_replicas = 31;
        let tolerance = 0;

        // Check none scan files case
        {
            let (binding_result, _) = bind_task_consistent_hash(
                topology_nodes.clone(),
                num_replicas,
                tolerance,
                active_jobs.clone(),
                |_, _| Ok(vec![]),
            )
            .await?;
            assert_eq!(0, binding_result.bound_tasks.len());
        }

        // Check job_b with scan files
        {
            let (binding_result, _) = bind_task_consistent_hash(
                topology_nodes,
                num_replicas,
                tolerance,
                active_jobs,
                |job_id, _| mock_get_scan_files(&JobId::new("job_b"), job_id, 8),
            )
            .await?;
            assert_eq!(6, binding_result.bound_tasks.len());

            let result = get_result(binding_result.bound_tasks);

            let mut expected = HashMap::new();
            {
                let mut entry_b = HashMap::new();
                entry_b.insert("executor_3".to_string(), 2);
                entry_b.insert("executor_2".to_string(), 3);
                entry_b.insert("executor_1".to_string(), 1);

                expected.insert("job_b".to_string(), entry_b);
            }
            assert!(
                expected.eq(&result),
                "The result {result:?} is not as expected {expected:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_bind_task_consistent_hash_with_tolerance() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let active_jobs = Arc::new(active_jobs);
        let topology_nodes = mock_topology_nodes();
        let num_replicas = 31;
        let tolerance = 1;

        {
            let (binding_result, _) = bind_task_consistent_hash(
                topology_nodes,
                num_replicas,
                tolerance,
                active_jobs,
                |job_id, _| mock_get_scan_files(&JobId::new("job_b"), job_id, 8),
            )
            .await?;
            assert_eq!(7, binding_result.bound_tasks.len());

            let result = get_result(binding_result.bound_tasks);

            let mut expected = HashMap::new();
            {
                let mut entry_b = HashMap::new();
                entry_b.insert("executor_3".to_string(), 3);
                entry_b.insert("executor_2".to_string(), 3);
                entry_b.insert("executor_1".to_string(), 1);

                expected.insert("job_b".to_string(), entry_b);
            }
            assert!(
                expected.eq(&result),
                "The result {result:?} is not as expected {expected:?}"
            );
        }

        Ok(())
    }

    fn get_result(
        bound_tasks: Vec<BoundTask>,
    ) -> HashMap<String, HashMap<String, usize>> {
        let mut result = HashMap::new();

        for bound_task in bound_tasks {
            let entry = result
                .entry(bound_task.1.key.job_id.to_string())
                .or_insert_with(HashMap::new);
            let n = entry.entry(bound_task.0).or_insert_with(|| 0);
            *n += bound_task.1.global_input_partition_ids.len();
        }

        result
    }

    /// Total partitions covered across every bound task — the multi-partition
    /// analogue of "how many tasks did we bind" for tests that used to assert
    /// on `bound_tasks.len()`.
    fn total_partitions_covered(bound_tasks: &[BoundTask]) -> usize {
        bound_tasks
            .iter()
            .map(|(_, task)| task.global_input_partition_ids.len())
            .sum()
    }

    async fn mock_active_jobs(
        num_partition: usize,
    ) -> Result<HashMap<JobId, JobInfoCache>> {
        let graph_a = mock_graph(&JobId::new("job_a"), num_partition, 2).await?;

        let graph_b = mock_graph(&JobId::new("job_b"), num_partition, 7).await?;

        let mut active_jobs = HashMap::new();
        active_jobs.insert(
            graph_a.job_id().clone(),
            JobInfoCache::new(Box::new(graph_a)),
        );
        active_jobs.insert(
            graph_b.job_id().clone(),
            JobInfoCache::new(Box::new(graph_b)),
        );

        Ok(active_jobs)
    }

    async fn mock_graph(
        job_id: &JobId,
        num_target_partitions: usize,
        num_pending_task: usize,
    ) -> Result<StaticExecutionGraph> {
        // These tests validate the *multi-partition* binding path: expected
        // task distributions include slice sizes up to 7. That requires an
        // unbounded `max_partitions_per_task`, which is also the default —
        // set explicitly here so the mock keeps exercising that path even if
        // the default changes again.
        let session_config = Arc::new(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "0"),
        );
        let mut graph = test_aggregation_plan_with_config(
            num_target_partitions,
            job_id,
            session_config,
        )
        .await;
        let executor = ExecutorMetadata {
            id: "executor_0".to_string(),
            host: "localhost".to_string(),
            port: 50051,
            grpc_port: 50052,
            specification: ExecutorSpecification { vcores: 32 },
            os_info: ExecutorOperatingSystemSpecification::default(),
        };

        // complete first stage
        revive_graph_and_complete_next_stage(&mut graph)?;

        for _ in 0..num_target_partitions - num_pending_task {
            if let Some(task) = graph.pop_next_task(&executor.id)? {
                let task_status = mock_completed_task(task, &executor.id);
                graph.update_task_status(&executor, vec![task_status], 1, 1)?;
            }
        }

        Ok(graph)
    }

    fn mock_budgets() -> Vec<AvailableVcores> {
        vec![
            AvailableVcores {
                executor_id: "executor_1".to_string(),
                vcores: 3,
            },
            AvailableVcores {
                executor_id: "executor_2".to_string(),
                vcores: 5,
            },
            AvailableVcores {
                executor_id: "executor_3".to_string(),
                vcores: 7,
            },
        ]
    }

    fn mock_topology_nodes() -> HashMap<String, TopologyNode> {
        let mut topology_nodes = HashMap::new();
        topology_nodes.insert(
            "executor_1".to_string(),
            TopologyNode::new("localhost", 8081, "executor_1", 0, 1),
        );
        topology_nodes.insert(
            "executor_2".to_string(),
            TopologyNode::new("localhost", 8082, "executor_2", 0, 3),
        );
        topology_nodes.insert(
            "executor_3".to_string(),
            TopologyNode::new("localhost", 8083, "executor_3", 0, 5),
        );
        topology_nodes
    }

    fn mock_get_scan_files(
        expected_job_id: &JobId,
        job_id: &JobId,
        num_partition: usize,
    ) -> datafusion::common::Result<Vec<Vec<Vec<PartitionedFile>>>> {
        Ok(if expected_job_id.eq(job_id) {
            mock_scan_files(num_partition)
        } else {
            vec![]
        })
    }

    fn mock_scan_files(num_partition: usize) -> Vec<Vec<Vec<PartitionedFile>>> {
        let mut scan_files = vec![];
        for i in 0..num_partition {
            scan_files.push(vec![PartitionedFile {
                object_meta: ObjectMeta {
                    location: Path::from(format!("file--{i}")),
                    last_modified: Default::default(),
                    size: 1,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![],
                range: None,
                extensions: Default::default(),
                statistics: None,
                ordering: None,
                metadata_size_hint: None,
                table_reference: None,
                arrow_schema: None,
            }]);
        }
        vec![scan_files]
    }
}
