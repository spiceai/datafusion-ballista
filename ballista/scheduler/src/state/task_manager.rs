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

use crate::cluster::{JobState, JobStateEventStream};
use crate::config::SchedulerConfig;
use crate::planner::DefaultDistributedPlanner;
use crate::state::task_builder::restrict_plan_to_partitions;

use crate::scheduler_server::event::SubmitPlan;
use crate::state::distributed_explain::handle_explain_plan;
use crate::state::execution_graph::{
    ExecutionGraphBox, RunningTaskInfo, StaticExecutionGraph, TaskDescription,
    TaskStatusUpdateResult,
};
use crate::state::executor_manager::ExecutorManager;

use ballista_core::JobId;
use ballista_core::JobStatusSubscriber;
use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::execution_plans::compute_global_output_partition_ids;
use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};

use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    JobStatus, MultiTaskDefinition, TaskDefinition, TaskId, TaskStatus, job_status,
};
use ballista_core::serde::scheduler::ExecutorMetadata;
use dashmap::DashMap;

use crate::state::aqe::AdaptiveExecutionGraph;
#[cfg(feature = "disable-stage-plan-cache")]
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "disable-stage-plan-cache")]
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

type ActiveJobCache = Arc<DashMap<JobId, JobInfoCache>>;

/// Trait for launching tasks on executors.
///
/// Implementations handle the communication with executors to start task execution.
#[async_trait::async_trait]
pub trait TaskLauncher: Send + Sync + 'static {
    /// Launches the given tasks on the specified executor.
    ///
    /// `Ok` means the RPC was dispatched; the returned set holds job IDs the
    /// executor rejected and failed individually. `Err` is only for a
    /// transport-level failure of the whole RPC.
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        executor_manager: &ExecutorManager,
    ) -> Result<HashSet<JobId>>;
}

struct DefaultTaskLauncher {
    scheduler_id: String,
}

impl DefaultTaskLauncher {
    pub fn new(scheduler_id: String) -> Self {
        Self { scheduler_id }
    }
}

#[async_trait::async_trait]
impl TaskLauncher for DefaultTaskLauncher {
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        executor_manager: &ExecutorManager,
    ) -> Result<HashSet<JobId>> {
        if log::max_level() >= log::Level::Info {
            let tasks_ids: Vec<String> = tasks
                .iter()
                .map(|task| {
                    let task_ids: Vec<u32> = task
                        .task_ids
                        .iter()
                        .map(|task_id| task_id.task_id)
                        .collect();
                    format!("{}/{}/{:?}", task.job_id, task.stage_id, task_ids)
                })
                .collect();
            info!(
                "Launching multi task on executor {:?} for {:?}",
                executor.id, tasks_ids
            );
        }
        let res = executor_manager
            .launch_multi_task(&executor.id, tasks, self.scheduler_id.clone())
            .await?;
        Ok(res)
    }
}

/// Upper bound on a single job-state persist (object-store meta CAS + graph
/// blob write). The scheduler event loop awaits these persists, so an
/// unbounded stall in a single object-store request would freeze the entire
/// scheduler: it stops scheduling and stops answering `poll_work`, executors
/// go idle, and the cluster wedges with no error logged.
const JOB_PERSIST_TIMEOUT: Duration = Duration::from_secs(10);

/// Manages task scheduling and execution for the Ballista scheduler.
///
/// The `TaskManager` is responsible for:
/// - Queuing and submitting jobs
/// - Tracking job and task status
/// - Launching tasks on executors
/// - Handling task failures and retries
/// - Managing the lifecycle of execution graphs
#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    /// Persistent job state storage.
    state: Arc<dyn JobState>,
    /// Codec for serializing/deserializing logical and physical plans.
    codec: BallistaCodec<T, U>,
    /// Unique identifier for this scheduler instance.
    scheduler_id: String,
    /// Cache for active jobs curated by this scheduler.
    active_job_cache: ActiveJobCache,
    /// Task launcher implementation.
    launcher: Arc<dyn TaskLauncher>,
    /// Maximum number of failure attempts for task-level retry before the task is considered failed
    task_max_failures: usize,
    /// Maximum number of failure attempts for stage-level retry before the stage is considered failed.
    stage_max_failures: usize,
}

/// Contains the execution graph and cached data to improve performance
/// when scheduling tasks for the job.
#[derive(Clone)]
pub struct JobInfoCache {
    /// The execution graph for this job, protected by a read-write lock.
    pub execution_graph: Arc<RwLock<ExecutionGraphBox>>,
    /// Cached job status for quick access.
    pub status: Option<job_status::Status>,
}

impl JobInfoCache {
    /// Creates a new `JobInfoCache` from an execution graph.
    pub fn new(graph: ExecutionGraphBox) -> Self {
        let status = graph.status().status.clone();

        Self {
            execution_graph: Arc::new(RwLock::new(graph)),
            status,
        }
    }
}

/// Tracks stage state changes during task status updates.
///
/// This struct is used internally to batch stage state transitions
/// after processing task status updates.
#[derive(Clone)]
pub struct UpdatedStages {
    /// Stage IDs that have been resolved and are ready to run.
    pub resolved_stages: HashSet<usize>,
    /// Stage IDs that have completed successfully.
    pub successful_stages: HashSet<usize>,
    /// Stage IDs that have failed, mapped to their error messages.
    pub failed_stages: HashMap<usize, String>,
    /// Running stages that need to be rolled back, mapped to failure reasons.
    pub rollback_running_stages: HashMap<usize, HashSet<String>>,
    /// Successful stages that need to be re-run due to lost outputs.
    pub resubmit_successful_stages: HashSet<usize>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    /// Creates a new `TaskManager` with the default task launcher.
    pub fn new(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            state,
            codec,
            scheduler_id: scheduler_id.clone(),
            active_job_cache: Arc::new(DashMap::new()),
            launcher: Arc::new(DefaultTaskLauncher::new(scheduler_id)),
            task_max_failures: config.task_max_failures,
            stage_max_failures: config.stage_max_failures,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_launcher(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
        launcher: Arc<dyn TaskLauncher>,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            state,
            codec,
            scheduler_id,
            active_job_cache: Arc::new(DashMap::new()),
            launcher,
            task_max_failures: config.task_max_failures,
            stage_max_failures: config.stage_max_failures,
        }
    }

    /// Enqueue a job for scheduling
    pub fn queue_job(
        &self,
        job_id: &JobId,
        job_name: &str,
        queued_at: u64,
    ) -> Result<()> {
        self.state.accept_job(job_id, job_name, queued_at)
    }

    /// Get the number of queued jobs. If it's big, then it means the scheduler is too busy.
    /// In normal case, it's better to be 0.
    pub fn pending_job_number(&self) -> usize {
        self.state.pending_job_number()
    }

    /// Get the number of running jobs.
    pub fn running_job_number(&self) -> usize {
        self.active_job_cache
            .iter()
            .filter(|job_info| {
                matches!(job_info.status, Some(job_status::Status::Running(_)))
            })
            .count()
    }

    /// Returns a stream of job state events from the configured state backend.
    pub async fn job_state_events(&self) -> Result<JobStateEventStream> {
        self.state.job_state_events().await
    }

    /// Get the total number of pending tasks across all active jobs.
    ///
    /// A pending task is a task that is available to schedule on an executor
    /// but cannot be scheduled because no resources are available.
    ///
    /// NOTE: This method iterates over all active jobs and acquires read locks
    /// on each execution graph. It should NOT be called frequently (e.g., in a
    /// hot loop or after every event) as it can cause lock contention with
    /// concurrent task binding operations.
    pub async fn total_pending_tasks(&self) -> usize {
        // Collect the graph handles first so no DashMap shard guard is held
        // across an await. A shard guard held while parked on a graph lock
        // makes every other shard access (task binding in poll_work, job
        // insert/remove on the event loop) block its OS worker thread; with
        // enough concurrent pollers that exhausts the runtime's workers, the
        // timer driver dies with them, and the process freezes permanently.
        let graphs: Vec<(JobId, Arc<RwLock<ExecutionGraphBox>>)> = self
            .active_job_cache
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().execution_graph.clone()))
            .collect();

        let mut total = 0;
        for (job_id, graph) in graphs {
            // Use a timeout to avoid blocking indefinitely if there's lock contention.
            // If we can't acquire the lock within the timeout, skip this job's count
            // rather than blocking the metrics collection.
            match tokio::time::timeout(Duration::from_millis(100), graph.read()).await {
                Ok(graph) => {
                    total += graph.available_tasks();
                }
                Err(_) => {
                    // Lock acquisition timed out, skip this job
                    trace!(
                        "Skipping pending task count for job {job_id} due to lock contention"
                    );
                }
            }
        }
        total
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_plan(
        &self,
        job_id: &JobId,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &SubmitPlan,
        queued_at: u64,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let mut planner = DefaultDistributedPlanner::new();
        let session_state = ctx.state();
        let session_config = session_state.config();

        let mut graph = match plan {
            SubmitPlan::Logical(logical_plan) => {
                if session_config.ballista_adaptive_query_planner_enabled() {
                    debug!("Using adaptive query planner (AQE) for job planning");
                    Box::new(
                        AdaptiveExecutionGraph::try_new(
                            &self.scheduler_id,
                            job_id,
                            job_name,
                            &ctx,
                            logical_plan,
                            queued_at,
                        )
                        .await?,
                    ) as ExecutionGraphBox
                } else {
                    debug!("Using static query planner for job planning");
                    let session_config = Arc::new(ctx.copied_config());

                    let physical_plan =
                        ctx.state().create_physical_plan(logical_plan).await?;
                    let physical_plan =
                        handle_explain_plan(job_id, &ctx, logical_plan, physical_plan)
                            .await?;

                    Box::new(StaticExecutionGraph::new(
                        &self.scheduler_id,
                        job_id,
                        job_name,
                        &ctx.session_id(),
                        physical_plan,
                        queued_at,
                        session_config,
                        &mut planner,
                        Some(logical_plan.display_indent().to_string()),
                    )?) as ExecutionGraphBox
                }
            }
            SubmitPlan::Physical(physical_plan) => {
                // AQE plans from the logical plan, so an already-built
                // physical plan always uses the static planner, including when
                // AQE is on (the default).
                debug!("Using static query planner for physical-plan job submission");
                let session_config = Arc::new(ctx.copied_config());

                Box::new(StaticExecutionGraph::new(
                    &self.scheduler_id,
                    job_id,
                    job_name,
                    &ctx.session_id(),
                    physical_plan.clone(),
                    queued_at,
                    session_config,
                    &mut planner,
                    None,
                )?) as ExecutionGraphBox
            }
        };
        let string_plan = DisplayableExecutionPlan::new(graph.physical_plan().as_ref())
            .indent(false)
            .to_string();

        info!("Submitting execution graph for job_id [{job_id}]:\n{string_plan}");

        self.state
            .submit_job(job_id.clone(), &graph, subscriber)
            .await?;
        graph.revive();
        self.active_job_cache
            .insert(job_id.clone(), JobInfoCache::new(graph));

        Ok(())
    }

    /// Submit a logical plan for distributed execution.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_job(
        &self,
        job_id: &JobId,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        queued_at: u64,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        self.submit_plan(
            job_id,
            job_name,
            ctx,
            &SubmitPlan::Logical(plan.clone()),
            queued_at,
            subscriber,
        )
        .await
    }

    /// Submit an already-built physical plan for distributed execution. The physical
    /// plan is planned with the static distributed planner; adaptive query planning
    /// (AQE) is not available for this path.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_physical_plan(
        &self,
        job_id: &JobId,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        self.submit_plan(
            job_id,
            job_name,
            ctx,
            &SubmitPlan::Physical(plan),
            queued_at,
            subscriber,
        )
        .await
    }

    /// Acquires ownership of a job from the persistent state and adds its
    /// execution graph back to the active cache so it can be driven locally.
    ///
    /// Returns `false` if the job could not be acquired (already owned by a
    /// live scheduler, terminal, or absent).
    pub(crate) async fn recover_job(&self, job_id: &JobId) -> Result<bool> {
        let Some(mut graph) = self.state.try_acquire_job(job_id).await? else {
            return Ok(false);
        };
        graph.revive();
        self.active_job_cache
            .insert(job_id.clone(), JobInfoCache::new(graph));
        Ok(true)
    }

    /// Returns a snapshot of currently running jobs from the cache.
    pub fn get_running_job_cache(&self) -> Arc<HashMap<JobId, JobInfoCache>> {
        let ret = self
            .active_job_cache
            .iter()
            .filter_map(|pair| {
                let (job_id, job_info) = pair.pair();
                if matches!(job_info.status, Some(job_status::Status::Running(_))) {
                    Some((job_id.clone(), job_info.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        Arc::new(ret)
    }

    /// Get all jobs optionally filtered by status.
    /// When `status` is None, returns all jobs regardless of status.
    pub async fn get_all_jobs(&self) -> Result<Vec<JobOverview>> {
        let job_ids = self.state.get_all_jobs().await?;

        let mut jobs = vec![];
        for job_id in &job_ids {
            if let Some(cached) = self.get_active_execution_graph(job_id) {
                let graph = cached.read().await;
                jobs.push(graph.deref().into());
            } else if let Some(graph) = self.state.get_execution_graph(job_id).await? {
                jobs.push((&graph).into());
            } else if let Some(job_status) = self.state.get_job_status(job_id).await? {
                let (start_time, end_time) = match &job_status.status {
                    Some(job_status::Status::Running(r)) => (r.started_at, 0),
                    Some(job_status::Status::Successful(s)) => (s.started_at, s.ended_at),
                    Some(job_status::Status::Failed(f)) => (f.started_at, f.ended_at),
                    // Queued jobs have no start or end time yet
                    _ => (0, 0),
                };
                jobs.push(JobOverview {
                    job_id: job_status.job_id.clone().into(),
                    job_name: job_status.job_name.clone(),
                    status: job_status,
                    start_time,
                    end_time,
                    num_stages: 0,
                    completed_stages: 0,
                });
            } else {
                warn!(
                    "Job {job_id} not found in active cache, execution graph, or job status"
                );
            }
        }
        Ok(jobs)
    }

    /// Get the session configuration for a job.
    pub async fn get_job_config(&self, job_id: &JobId) -> Result<Arc<SessionConfig>> {
        let graph = self
            .get_job_execution_graph(job_id)
            .await?
            .ok_or_else(|| BallistaError::General(format!("Job {job_id} not found")))?;

        Ok(graph.session_config())
    }

    /// Get the status of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &JobId) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let guard = graph.read().await;

            Ok(Some(guard.status().clone()))
        } else {
            self.state.get_job_status(job_id).await
        }
    }

    /// Get the execution graph of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs.
    ///
    /// Exposed as `pub` so embedded callers (e.g., Spice's distributed
    /// task_history writer) can walk per-stage and per-task state directly
    /// without going through a gRPC method.
    pub async fn get_job_execution_graph(
        &self,
        job_id: &JobId,
    ) -> Result<Option<ExecutionGraphBox>> {
        if let Some(cached) = self.get_active_execution_graph(job_id) {
            let guard = cached.read().await;

            Ok(Some(guard.deref().cloned()))
        } else {
            let graph = self.state.get_execution_graph(job_id).await?;

            Ok(graph)
        }
    }

    /// Sum the vcores consumed by the given task statuses at their original
    /// bind time. Used to refund the executor's vcore budget when tasks
    /// complete: refunding `statuses.len()` (one per task) would drop
    /// leftover vcores forever, because `bind_one` consumes `slice.len()`
    /// vcores per task under the multi-partition-task model.
    ///
    /// Statuses whose (job, stage, task_id) can no longer be resolved
    /// (e.g. the job's graph has been evicted) contribute 0.
    pub(crate) async fn sum_vcores_for_statuses(&self, statuses: &[TaskStatus]) -> u32 {
        let mut statuses_by_job: HashMap<String, Vec<&TaskStatus>> = HashMap::new();
        for status in statuses {
            statuses_by_job
                .entry(status.job_id.clone())
                .or_default()
                .push(status);
        }
        let mut total_vcores: u32 = 0;
        for (job_id, job_statuses) in statuses_by_job {
            let Some(graph_arc) = self.get_active_execution_graph(&job_id.into()) else {
                continue;
            };
            let graph = graph_arc.read().await;
            for status in job_statuses {
                if let Some(vcores) =
                    graph.task_vcores(status.stage_id as usize, status.task_id as usize)
                {
                    total_vcores += vcores;
                }
            }
        }
        total_vcores
    }

    /// Update given task statuses in the respective job and return a `TaskStatusUpdateResult`
    /// containing:
    /// 1. A list of `QueryStageSchedulerEvent` to publish.
    /// 2. Metrics information about stage/task lifecycle changes.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<TaskStatusUpdateResult> {
        let mut job_updates: HashMap<JobId, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            trace!("Task Update\n{status:?}");
            log_runtime_stats_arrival(executor, &status);
            let job_id: JobId = status.job_id.clone().into();
            let job_task_statuses = job_updates.entry(job_id).or_default();
            job_task_statuses.push(status);
        }

        let mut combined_result = TaskStatusUpdateResult::default();
        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!("Updating {num_tasks} tasks in job {job_id}");

            let events = if let Some(cached) = self.get_active_execution_graph(&job_id) {
                let mut graph = cached.write().await;
                graph.update_task_status(
                    executor,
                    statuses,
                    self.task_max_failures,
                    self.stage_max_failures,
                )?
            } else {
                // TODO Deal with curator changed case
                error!(
                    "Fail to find job {job_id} in the active cache and it may not be curated by this scheduler"
                );
                vec![]
            };

            // Combine events from all jobs
            combined_result.events.extend(events);
            // Note: metrics are not available through the trait interface.
            // Use update_task_status_with_metrics on StaticExecutionGraph for metrics tracking.
        }

        Ok(combined_result)
    }

    /// Persist the job state, bounded by [`JOB_PERSIST_TIMEOUT`] so a stalled
    /// object-store operation cannot hang the scheduler event loop (which
    /// awaits these persists). Failures and timeouts are logged and returned
    /// to the caller; the shared state is left to a later persist.
    async fn try_save_job(
        &self,
        job_id: &JobId,
        snapshot: &ExecutionGraphBox,
    ) -> Result<()> {
        match tokio::time::timeout(
            JOB_PERSIST_TIMEOUT,
            self.state.save_job(job_id, snapshot),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                warn!("save_job for {job_id} failed: {e}");
                Err(e)
            }
            Err(_) => {
                let msg = format!(
                    "save_job for {job_id} timed out after {}s",
                    JOB_PERSIST_TIMEOUT.as_secs()
                );
                warn!("{msg}");
                Err(BallistaError::General(msg))
            }
        }
    }

    /// Update the cached job status to its terminal value, persist the
    /// terminal snapshot, and evict the job from the active cache — even if
    /// the persist fails. The cached status is updated *before* the persist
    /// starts (not after) so a concurrent `running_job_number()`/
    /// `get_running_job_cache()` snapshot taken while the save is in flight
    /// doesn't still count the job as running. The persist is not retried on
    /// failure: the local cache entry is evicted regardless, and the failure
    /// is only logged, so a job whose terminal save fails doesn't leak in the
    /// active cache forever.
    async fn persist_terminal_and_evict(
        &self,
        job_id: &JobId,
        snapshot: &ExecutionGraphBox,
    ) -> Result<()> {
        if let Some(mut job_info) = self.active_job_cache.get_mut(job_id) {
            job_info.status = snapshot.status().status.clone();
        }
        let save_result = self.try_save_job(job_id, snapshot).await;
        if let Err(error) = &save_result {
            warn!(
                "Failed to persist terminal state for job {job_id}; evicting the local cache entry: {error}"
            );
        }
        self.remove_active_execution_graph(job_id);
        save_result
    }

    /// Mark a job to success. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    /// Move a job from Active to Success and return the ids of its intermediate
    /// (non-final) stages so their shuffle data can be reclaimed immediately.
    /// Returns an empty vec if the job is not found or not successful.
    pub(crate) async fn succeed_job(&self, job_id: &JobId) -> Result<Vec<u32>> {
        debug!("Moving job {job_id} from Active to Success");

        if let Some(graph) = self.get_active_execution_graph(job_id) {
            // Snapshot under the lock, persist outside it. Holding the graph lock
            // across object-store I/O blocks executor task-status updates (which
            // take the write lock inside the poll_work handlers), so a stalled
            // persist would wedge every executor's poll loop — and with it the
            // poll_work-carried heartbeats — taking down the whole cluster.
            let snapshot = {
                let graph = graph.read().await;
                if !graph.is_successful() {
                    error!("Job {job_id} has not finished and cannot be completed");
                    return Ok(vec![]);
                }
                graph.cloned()
            };
            self.persist_terminal_and_evict(job_id, &snapshot).await?;
            Ok(snapshot.intermediate_stage_ids())
        } else {
            warn!("Fail to find job {job_id} in the cache");
            Ok(vec![])
        }
    }

    /// Mark a job as failed, cancel its running tasks, and persist terminal state.
    ///
    /// Task cancellation is invoked before persistence so executor work is
    /// stopped even when all terminal-state save attempts fail.
    pub(crate) async fn abort_job<F, Fut>(
        &self,
        job_id: &JobId,
        failure_reason: String,
        cancel_tasks: F,
    ) -> Result<usize>
    where
        F: FnOnce(Vec<RunningTaskInfo>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let Some(graph) = self.get_active_execution_graph(job_id) else {
            // TODO listen the job state update event and fix task cancelling
            warn!(
                "Fail to find job {job_id} in the cache, unable to cancel tasks for job, fail the job state only."
            );
            return Ok(0);
        };

        let (running_tasks, pending_tasks, snapshot) = {
            let mut guard = graph.write().await;
            let pending_tasks = guard.available_tasks();
            let running_tasks = guard.abort_running(failure_reason);
            let snapshot = guard.cloned();
            if let Some(mut job_info) = self.active_job_cache.get_mut(job_id) {
                job_info.status = snapshot.status().status.clone();
            }
            (running_tasks, pending_tasks, snapshot)
        };

        info!(
            "Cancelling {} running tasks for job {}",
            running_tasks.len(),
            job_id
        );

        let cancel_result = cancel_tasks(running_tasks).await;
        let persist_result = self.persist_terminal_and_evict(job_id, &snapshot).await;
        match (cancel_result, persist_result) {
            (Ok(()), Ok(())) => Ok(pending_tasks),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(cancel_error), Err(persist_error)) => {
                Err(BallistaError::General(format!(
                    "Failed to cancel tasks for job {job_id}: {cancel_error}; failed to persist terminal state: {persist_error}"
                )))
            }
        }
    }

    /// Mark a unscheduled job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    pub async fn fail_unscheduled_job(
        &self,
        job_id: &JobId,
        failure_reason: String,
    ) -> Result<()> {
        self.state
            .fail_unscheduled_job(job_id, failure_reason)
            .await
    }

    /// Revive runnable stages and persist the job, returning the number of newly
    /// available tasks. Used on the event-driven update path, where the persisted
    /// status must track progress so clients can observe completion.
    pub async fn update_job(&self, job_id: &JobId) -> Result<usize> {
        self.revive_job_inner(job_id, true).await
    }

    /// Revive runnable stages WITHOUT persisting. Used by the reconciliation
    /// sweep, which runs in its own task concurrently with the event loop:
    /// persisting here would race the terminal save and could overwrite a
    /// finished job's graph blob with a stale "running" snapshot, leaving
    /// clients polling a completed job forever. Persistence is left to the
    /// serial event-driven update and terminal (succeed/fail) save paths.
    pub async fn revive_job(&self, job_id: &JobId) -> Result<usize> {
        self.revive_job_inner(job_id, false).await
    }

    async fn revive_job_inner(&self, job_id: &JobId, persist: bool) -> Result<usize> {
        debug!("Update active job {job_id}");
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            // Mutate and snapshot under the lock; the snapshot is consistent.
            let (new_tasks, snapshot) = {
                let mut graph = graph.write().await;
                let curr_available_tasks = graph.available_tasks();
                graph.revive();
                let new_tasks = graph.available_tasks() - curr_available_tasks;
                let snapshot = if persist { Some(graph.cloned()) } else { None };
                (new_tasks, snapshot)
            };

            // Persist the snapshot outside the graph lock so concurrent executor
            // task-status updates are not blocked by the object-store I/O. The
            // save is awaited rather than spawned so persisted job status only
            // ever advances within this serial path.
            if let Some(snapshot) = snapshot {
                info!("Saving job with status {:?}", snapshot.status());
                // Best-effort intermediate persist: on failure/timeout the next
                // event-driven update re-persists (the reconciliation sweep
                // deliberately revives WITHOUT persisting).
                let _ = self.try_save_job(job_id, &snapshot).await;
            }

            Ok(new_tasks)
        } else {
            warn!("Fail to find job {job_id} in the cache");

            Ok(0)
        }
    }

    /// Handles executor loss by resetting affected tasks and stages.
    ///
    /// Returns a list of running tasks that need to be cancelled.
    pub async fn executor_lost(&self, executor_id: &str) -> Result<Vec<RunningTaskInfo>> {
        // Collect all the running task need to cancel when there are running stages rolled back.
        let mut running_tasks_to_cancel: Vec<RunningTaskInfo> = vec![];

        // Collect the graph handles first: awaiting each graph's (unbounded)
        // write lock while holding the DashMap shard guard blocks every other
        // task touching that shard on its OS worker thread, which can exhaust
        // the runtime's workers and freeze the whole scheduler.
        let graphs: Vec<Arc<RwLock<ExecutionGraphBox>>> = self
            .active_job_cache
            .iter()
            .map(|entry| entry.value().execution_graph.clone())
            .collect();

        for graph in graphs {
            let mut graph = graph.write().await;
            let reset = graph.reset_stages_on_lost_executor(executor_id)?;
            if !reset.0.is_empty() {
                running_tasks_to_cancel.extend(reset.1);
            }
        }

        Ok(running_tasks_to_cancel)
    }

    /// Retrieves the number of available tasks for the given job.
    ///
    /// The value returned is a point-in-time snapshot and may change immediately.
    pub async fn get_available_task_count(&self, job_id: &JobId) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!("Fail to find job {job_id} in the cache");
            Ok(0)
        }
    }

    /// Prepares a task definition for a single task to be sent to an executor.
    ///
    /// The task's plan is rewritten to restrict leaves (Scan file_groups,
    /// ShuffleReader partitions) to this task's assigned slice, then encoded
    /// fresh. No shared-plan cache — each task gets its own bytes because
    /// each task's plan is different.
    #[allow(dead_code)]
    pub fn prepare_task_definition(
        &self,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        debug!("Preparing task definition for {task:?}");

        let job_id = task.key.job_id.clone();
        let stage_id = task.key.stage_id;

        if self.active_job_cache.get(&job_id).is_some() {
            let restricted = restrict_plan_to_partitions(
                task.plan.clone(),
                &task.global_input_partition_ids,
            )?;
            let mut plan_buf: Vec<u8> = vec![];
            let plan_proto = PhysicalPlanNode::try_from_physical_plan(
                restricted,
                self.codec.physical_extension_codec(),
            )?;
            plan_proto.try_encode(&mut plan_buf)?;

            let task_definition = TaskDefinition {
                task_id: task.key.task_id as u32,
                task_attempt_num: task.task_attempt as u32,
                job_id: job_id.into(),
                stage_id: stage_id as u32,
                stage_attempt_num: task.stage_attempt_num as u32,
                plan: plan_buf,
                session_id: task.session_id,
                launch_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                props: task.session_config.to_key_value_pairs(),
                global_output_partition_ids: compute_global_output_partition_ids(
                    &task.plan,
                    &task.global_input_partition_ids,
                )
                .into_iter()
                .map(|pid| pid as u32)
                .collect(),
                vcores_consumed: task.vcores_consumed,
            };
            Ok(task_definition)
        } else {
            Err(BallistaError::General(format!(
                "Cannot prepare task definition for job {job_id} which is not in active cache"
            )))
        }
    }

    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_multi_task(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<Vec<TaskDescription>>,
        executor_manager: &ExecutorManager,
    ) -> Result<HashSet<JobId>> {
        let mut multi_tasks = vec![];
        for stage_tasks in tasks {
            match self.prepare_multi_task_definition(stage_tasks) {
                Ok(stage_tasks) => multi_tasks.extend(stage_tasks),
                Err(e) => error!("Fail to prepare task definition: {e:?}"),
            }
        }

        if !multi_tasks.is_empty() {
            self.launcher
                .launch_tasks(executor, multi_tasks, executor_manager)
                .await
        } else {
            Ok(HashSet::new())
        }
    }

    #[allow(dead_code)]
    /// Emit one `MultiTaskDefinition` per task. Previously grouped multiple
    /// tasks under one shared plan; now each task's plan is uniquely restricted
    /// to its assigned partition slice so we can't share encoding.
    fn prepare_multi_task_definition(
        &self,
        tasks: Vec<TaskDescription>,
    ) -> Result<Vec<MultiTaskDefinition>> {
        let [first_task, ..] = tasks.as_slice() else {
            return Err(BallistaError::General(
                "Cannot prepare multi task definition for an empty vec".to_string(),
            ));
        };
        let session_id = first_task.session_id.clone();
        let job_id = first_task.key.job_id.clone();
        let stage_id = first_task.key.stage_id;
        let stage_attempt_num = first_task.stage_attempt_num;

        if log::max_level() >= log::Level::Debug {
            let task_ids: Vec<usize> =
                tasks.iter().map(|task| task.key.task_id).collect();
            debug!(
                "Preparing multi task definition for tasks {task_ids:?} belonging to job stage {job_id}/{stage_id}"
            );
            trace!("With task details {tasks:?}");
        }

        if self.active_job_cache.get(&job_id).is_none() {
            return Err(BallistaError::General(format!(
                "Cannot prepare multi task definition for job {job_id} which is not in active cache"
            )));
        }

        let launch_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let codec = self.codec.physical_extension_codec();

        let props = first_task.session_config.to_key_value_pairs();
        let mut multi_tasks = Vec::with_capacity(tasks.len());
        for task in tasks {
            let restricted = restrict_plan_to_partitions(
                task.plan.clone(),
                &task.global_input_partition_ids,
            )?;
            let mut plan_buf: Vec<u8> = vec![];
            let plan_proto = PhysicalPlanNode::try_from_physical_plan(restricted, codec)?;
            plan_proto.try_encode(&mut plan_buf)?;
            let task_ids = vec![TaskId {
                task_id: task.key.task_id as u32,
                task_attempt_num: task.task_attempt as u32,
                global_output_partition_ids: compute_global_output_partition_ids(
                    &task.plan,
                    &task.global_input_partition_ids,
                )
                .into_iter()
                .map(|p| p as u32)
                .collect(),
                vcores_consumed: task.vcores_consumed,
            }];
            multi_tasks.push(MultiTaskDefinition {
                task_ids,
                job_id: job_id.clone().into(),
                stage_id: stage_id as u32,
                stage_attempt_num: stage_attempt_num as u32,
                plan: plan_buf,
                session_id: session_id.clone(),
                launch_time,
                props: props.clone(),
            });
        }
        Ok(multi_tasks)
    }

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn get_active_execution_graph(
        &self,
        job_id: &JobId,
    ) -> Option<Arc<RwLock<ExecutionGraphBox>>> {
        self.active_job_cache
            .get(job_id)
            .as_deref()
            .map(|cached| cached.execution_graph.clone())
    }

    /// Remove the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn remove_active_execution_graph(
        &self,
        job_id: &JobId,
    ) -> Option<Arc<RwLock<ExecutionGraphBox>>> {
        self.active_job_cache
            .remove(job_id)
            .map(|value| value.1.execution_graph)
    }

    /// Clean up a failed job in FailedJobs Keyspace by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_delayed(&self, job_id: JobId, clean_up_interval: u64) {
        if clean_up_interval == 0 {
            info!(
                "The interval is 0 and the clean up for the failed job state {job_id} will not triggered"
            );
            return;
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            if let Err(err) = state.remove_job(&job_id).await {
                error!("Failed to delete job {job_id}: {err:?}");
            }
        });
    }
}

/// Summary information about a job for display purposes.
pub struct JobOverview {
    /// Unique identifier for this job.
    pub job_id: JobId,
    /// Human-readable name for this job.
    pub job_name: String,
    /// Current status of the job.
    pub status: JobStatus,
    /// Timestamp when the job started.
    pub start_time: u64,
    /// Timestamp when the job ended (0 if still running).
    pub end_time: u64,
    /// Total number of stages in the job.
    pub num_stages: usize,
    /// Number of stages that have completed successfully.
    pub completed_stages: usize,
}

impl From<&ExecutionGraphBox> for JobOverview {
    fn from(value: &ExecutionGraphBox) -> Self {
        let completed_stages = value.completed_stages();

        Self {
            job_id: value.job_id().clone(),
            job_name: value.job_name().to_string(),
            status: value.status().clone(),
            start_time: value.start_time(),
            end_time: value.end_time(),
            num_stages: value.stage_count(),
            completed_stages,
        }
    }
}

/// Log any `RuntimeStatsReport`s that arrived with this task status. For
/// now this is proof-of-life for the wire — the reports flow from
/// executor to scheduler and land here observably. The per-stage
/// accumulator + merged-quantile-cut logging comes in a follow-up commit;
/// this hook is what verifies the plumbing works against a live cluster.
fn log_runtime_stats_arrival(
    executor: &ExecutorMetadata,
    status: &ballista_core::serde::protobuf::TaskStatus,
) {
    use ballista_core::serde::protobuf::task_status::Status;
    let Some(Status::Successful(successful)) = status.status.as_ref() else {
        return;
    };
    if successful.runtime_stats.is_empty() {
        return;
    }
    for (report_idx, report) in successful.runtime_stats.iter().enumerate() {
        let non_empty_partitions =
            report.partitions.iter().filter(|p| p.row_count > 0).count();
        let total_rows: u64 = report.partitions.iter().map(|p| p.row_count).sum();
        let ranged_partitions = report
            .partitions
            .iter()
            .filter(|p| !p.key_min.is_empty() && !p.key_max.is_empty())
            .count();
        // Counted rather than assumed: an entry whose range never got filled
        // in would route no files, and nothing else here would say so.
        debug!(
            "RuntimeStats arrival: executor={} job={} stage={} task={} \
             report[{}] order_by_len={} partitions={} non_empty={} \
             total_rows={} key_ranges={}/{} sort_key_sketch={}",
            executor.id,
            status.job_id,
            status.stage_id,
            status.task_id,
            report_idx,
            report.order_by.len(),
            report.partitions.len(),
            non_empty_partitions,
            total_rows,
            ranged_partitions,
            report.partitions.len(),
            describe_sort_key_sketch(report),
        );
    }
}

/// Decode the report's merged [`SortKeySketch`] far enough to say what
/// arrived. Rebuilding it here is the point: a byte count proves the field
/// crossed, where a decoded count and range prove it survived.
///
/// Any failure is described rather than propagated — this is a log line, and
/// the query's data was already produced correctly.
///
/// [`SortKeySketch`]: ballista_core::sort_key::SortKeySketch
fn describe_sort_key_sketch(
    report: &ballista_core::serde::protobuf::RuntimeStatsReport,
) -> String {
    use ballista_core::sort_key::SortKeySketch;
    use datafusion::arrow::compute::SortOptions;

    let Some(state) = report.sketch.as_ref() else {
        return "none".to_string();
    };
    // The key's direction and NULL placement are not in the sketch — they
    // live once here, in the tag that says which expression it describes.
    let Some(first) = report.order_by.first() else {
        return "undescribable (sketch present with an empty order_by tag)".to_string();
    };
    let options = SortOptions {
        descending: !first.asc,
        nulls_first: first.nulls_first,
    };
    match SortKeySketch::try_from_proto(state, options) {
        Ok(sketch) => format!(
            "{{bytes={} k={} count={} nulls={} min={:?} max={:?}}}",
            state.levels.len(),
            state.k,
            sketch.count(),
            sketch.null_count(),
            sketch.value_min(),
            sketch.value_max(),
        ),
        Err(e) => format!("undecodable ({e})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::memory::InMemoryJobState;
    use crate::test_utils::{mock_completed_task, mock_executor, test_aggregation_plan};
    use ballista_core::serde::protobuf::job_status::Status;
    use ballista_core::utils::{default_config_producer, default_session_builder};
    use datafusion_proto::protobuf::LogicalPlanNode;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    struct BlockingJobState {
        inner: InMemoryJobState,
        save_started: Notify,
        allow_save: Notify,
        failures_remaining: AtomicUsize,
        save_attempts: AtomicUsize,
    }

    impl BlockingJobState {
        fn new() -> Self {
            Self {
                inner: InMemoryJobState::new(
                    "test-scheduler",
                    Arc::new(default_session_builder),
                    Arc::new(default_config_producer),
                ),
                save_started: Notify::new(),
                allow_save: Notify::new(),
                failures_remaining: AtomicUsize::new(0),
                save_attempts: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl JobState for BlockingJobState {
        fn accept_job(
            &self,
            job_id: &JobId,
            job_name: &str,
            queued_at: u64,
        ) -> Result<()> {
            self.inner.accept_job(job_id, job_name, queued_at)
        }

        fn pending_job_number(&self) -> usize {
            self.inner.pending_job_number()
        }

        async fn submit_job(
            &self,
            job_id: JobId,
            graph: &ExecutionGraphBox,
            subscriber: Option<JobStatusSubscriber>,
        ) -> Result<()> {
            self.inner.submit_job(job_id, graph, subscriber).await
        }

        async fn get_jobs(&self) -> Result<HashSet<JobId>> {
            self.inner.get_jobs().await
        }

        async fn get_all_jobs(&self) -> Result<HashSet<JobId>> {
            self.inner.get_all_jobs().await
        }

        async fn get_job_status(&self, job_id: &JobId) -> Result<Option<JobStatus>> {
            self.inner.get_job_status(job_id).await
        }

        async fn get_execution_graph(
            &self,
            job_id: &JobId,
        ) -> Result<Option<ExecutionGraphBox>> {
            self.inner.get_execution_graph(job_id).await
        }

        async fn save_job(
            &self,
            job_id: &JobId,
            graph: &ExecutionGraphBox,
        ) -> Result<()> {
            let attempt = self.save_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                self.save_started.notify_one();
                self.allow_save.notified().await;
            }
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(BallistaError::General("injected save failure".to_string()));
            }
            self.inner.save_job(job_id, graph).await
        }

        async fn fail_unscheduled_job(
            &self,
            job_id: &JobId,
            reason: String,
        ) -> Result<()> {
            self.inner.fail_unscheduled_job(job_id, reason).await
        }

        async fn remove_job(&self, job_id: &JobId) -> Result<()> {
            self.inner.remove_job(job_id).await
        }

        async fn try_acquire_job(
            &self,
            job_id: &JobId,
        ) -> Result<Option<ExecutionGraphBox>> {
            self.inner.try_acquire_job(job_id).await
        }

        async fn job_state_events(&self) -> Result<JobStateEventStream> {
            self.inner.job_state_events().await
        }

        async fn create_or_update_session(
            &self,
            session_id: &str,
            config: &SessionConfig,
        ) -> Result<Arc<SessionContext>> {
            self.inner
                .create_or_update_session(session_id, config)
                .await
        }

        async fn remove_session(&self, session_id: &str) -> Result<()> {
            self.inner.remove_session(session_id).await
        }

        fn produce_config(&self) -> SessionConfig {
            self.inner.produce_config()
        }
    }

    type TestTaskManager = TaskManager<LogicalPlanNode, PhysicalPlanNode>;

    async fn setup_job(
        successful: bool,
    ) -> Result<(
        TestTaskManager,
        Arc<BlockingJobState>,
        JobId,
        Arc<RwLock<ExecutionGraphBox>>,
    )> {
        let state = Arc::new(BlockingJobState::new());
        let job_state: Arc<dyn JobState> = state.clone();
        let manager = TaskManager::new(
            job_state,
            BallistaCodec::default(),
            "test-scheduler".to_string(),
            Arc::new(SchedulerConfig::default()),
        );

        let graph: ExecutionGraphBox = Box::new(test_aggregation_plan(2).await);
        let job_id = graph.job_id().to_owned();
        state.accept_job(&job_id, graph.job_name(), 0)?;
        state.submit_job(job_id.clone(), &graph, None).await?;

        manager
            .active_job_cache
            .insert(job_id.clone(), JobInfoCache::new(graph));
        let active_graph = manager
            .get_active_execution_graph(&job_id)
            .expect("job should be active");

        if successful {
            // Complete the graph after it is cached. `JobInfoCache::status` still
            // contains Running until terminal persistence updates it.
            let mut graph = active_graph.write().await;
            let executor = mock_executor("executor-1".to_string());
            while let Some(task) = graph.pop_next_task(&executor.id)? {
                graph.update_task_status(
                    &executor,
                    vec![mock_completed_task(task, &executor.id)],
                    1,
                    1,
                )?;
            }
            graph.succeed_job()?;
        }

        Ok((manager, state, job_id, active_graph))
    }

    #[tokio::test]
    async fn successful_job_stays_visible_and_unlocked_until_persisted() -> Result<()> {
        let (manager, state, job_id, active_graph) = setup_job(true).await?;
        let manager_for_save = manager.clone();
        let job_id_for_save = job_id.clone();
        let save =
            tokio::spawn(
                async move { manager_for_save.succeed_job(&job_id_for_save).await },
            );

        timeout(Duration::from_secs(1), state.save_started.notified())
            .await
            .expect("terminal save should start");

        let status = manager
            .get_job_status(&job_id)
            .await?
            .expect("successful job should remain visible during persistence");
        assert!(matches!(status.status, Some(Status::Successful(_))));
        assert!(!manager.get_running_job_cache().contains_key(&job_id));
        assert_eq!(manager.running_job_number(), 0);

        let graph_guard = timeout(Duration::from_secs(1), active_graph.write())
            .await
            .expect("graph lock must not be held across persistence");
        drop(graph_guard);

        state.allow_save.notify_one();
        save.await.expect("succeed_job task should not panic")?;

        assert!(manager.get_active_execution_graph(&job_id).is_none());
        let persisted = state
            .get_job_status(&job_id)
            .await?
            .expect("successful status should be persisted");
        assert!(matches!(persisted.status, Some(Status::Successful(_))));
        Ok(())
    }

    #[tokio::test]
    async fn failed_terminal_save_is_not_retried_and_evicts_local_cache() -> Result<()> {
        let (manager, state, job_id, active_graph) = setup_job(true).await?;
        state.failures_remaining.store(1, Ordering::SeqCst);
        let manager_for_save = manager.clone();
        let job_id_for_save = job_id.clone();
        let save =
            tokio::spawn(
                async move { manager_for_save.succeed_job(&job_id_for_save).await },
            );

        timeout(Duration::from_secs(1), state.save_started.notified())
            .await
            .expect("terminal save should start");
        assert!(manager.get_active_execution_graph(&job_id).is_some());
        assert_eq!(manager.running_job_number(), 0);
        state.allow_save.notify_one();
        assert!(
            save.await
                .expect("succeed_job task should not panic")
                .is_err()
        );

        assert_eq!(state.save_attempts.load(Ordering::SeqCst), 1);
        assert!(manager.get_active_execution_graph(&job_id).is_none());
        assert_eq!(manager.running_job_number(), 0);
        let authoritative_status = manager
            .get_job_status(&job_id)
            .await?
            .expect("shared status should remain available after local eviction");
        assert!(matches!(
            authoritative_status.status,
            Some(Status::Running(_))
        ));

        let graph_guard = timeout(Duration::from_secs(1), active_graph.write())
            .await
            .expect("graph lock must be released after a failed save");
        drop(graph_guard);
        Ok(())
    }

    #[tokio::test]
    async fn aborted_tasks_are_cancelled_before_terminal_persistence() -> Result<()> {
        let (manager, state, job_id, active_graph) = setup_job(false).await?;
        let executor = mock_executor("executor-1".to_string());
        active_graph
            .write()
            .await
            .pop_next_task(&executor.id)?
            .expect("job should have a task to assign");

        let cancelled_tasks = Arc::new(AtomicUsize::new(0));
        let cancelled_tasks_for_abort = cancelled_tasks.clone();
        let manager_for_abort = manager.clone();
        let job_id_for_abort = job_id.clone();
        let abort = tokio::spawn(async move {
            let manager_for_cancel = manager_for_abort.clone();
            let job_id_for_cancel = job_id_for_abort.clone();
            manager_for_abort
                .abort_job(
                    &job_id_for_abort,
                    "test failure".to_string(),
                    move |tasks| async move {
                        let status = manager_for_cancel
                            .get_job_status(&job_id_for_cancel)
                            .await?
                            .expect(
                                "aborted job should remain visible during cancellation",
                            );
                        assert!(
                            matches!(status.status, Some(Status::Failed(_))),
                            "job must be terminal before executor tasks are cancelled"
                        );
                        cancelled_tasks_for_abort.store(tasks.len(), Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
        });

        timeout(Duration::from_secs(1), state.save_started.notified())
            .await
            .expect("terminal save should start");
        assert_eq!(cancelled_tasks.load(Ordering::SeqCst), 1);
        assert_eq!(state.save_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(manager.running_job_number(), 0);

        let status = manager
            .get_job_status(&job_id)
            .await?
            .expect("failed job should remain visible during persistence");
        assert!(matches!(status.status, Some(Status::Failed(_))));
        let graph_guard = timeout(Duration::from_secs(1), active_graph.write())
            .await
            .expect("graph lock must not be held across persistence");
        drop(graph_guard);

        state.allow_save.notify_one();
        abort.await.expect("abort_job task should not panic")?;
        assert!(manager.get_active_execution_graph(&job_id).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn aborted_tasks_are_cancelled_when_terminal_save_fails() -> Result<()> {
        let (manager, state, job_id, active_graph) = setup_job(false).await?;
        let executor = mock_executor("executor-1".to_string());
        active_graph
            .write()
            .await
            .pop_next_task(&executor.id)?
            .expect("job should have a task to assign");
        state.failures_remaining.store(1, Ordering::SeqCst);

        let cancelled_tasks = Arc::new(AtomicUsize::new(0));
        let cancelled_tasks_for_abort = cancelled_tasks.clone();
        let manager_for_abort = manager.clone();
        let job_id_for_abort = job_id.clone();
        let abort = tokio::spawn(async move {
            manager_for_abort
                .abort_job(
                    &job_id_for_abort,
                    "test failure".to_string(),
                    move |tasks| async move {
                        cancelled_tasks_for_abort.store(tasks.len(), Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
        });
        timeout(Duration::from_secs(1), state.save_started.notified())
            .await
            .expect("terminal save should start");
        assert_eq!(cancelled_tasks.load(Ordering::SeqCst), 1);
        state.allow_save.notify_one();
        assert!(
            abort
                .await
                .expect("abort_job task should not panic")
                .is_err()
        );

        assert_eq!(cancelled_tasks.load(Ordering::SeqCst), 1);
        assert_eq!(state.save_attempts.load(Ordering::SeqCst), 1);
        assert!(manager.get_active_execution_graph(&job_id).is_none());
        assert_eq!(manager.running_job_number(), 0);
        Ok(())
    }
}
