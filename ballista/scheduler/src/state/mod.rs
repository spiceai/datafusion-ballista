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

use ballista_core::JobStatusSubscriber;
use ballista_core::JobId;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::listing::{ListingTable, ListingTableUrl};
use datafusion::datasource::source_as_provider;
use datafusion::error::DataFusionError;
use std::any::type_name;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SubmitPlan};

use crate::state::executor_manager::ExecutorManager;
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::{TaskLauncher, TaskManager};

use crate::cluster::{BallistaCluster, BoundTask, ExecutorSlot};
use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use crate::state::execution_graph::TaskDescription;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventSender;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::TaskStatus;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{debug, error, info, warn};
use prost::Message;

mod aqe;
mod distributed_explain;
/// Execution graph representation and management.
pub mod execution_graph;
/// DOT format export for execution graphs.
pub mod execution_graph_dot;
/// Execution stage tracking and status management.
pub mod execution_stage;
/// Executor registration and management.
pub mod executor_manager;
/// Session state management.
pub mod session_manager;
/// Task scheduling and lifecycle management.
pub mod task_manager;

/// Decodes a protobuf message from bytes.
pub fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

/// Decodes a protobuf message and converts it to another type.
pub fn decode_into<T: Message + Default + Into<U>, U>(bytes: &[u8]) -> Result<U> {
    T::decode(bytes)
        .map_err(|e| {
            BallistaError::Internal(format!(
                "Could not deserialize {}: {}",
                type_name::<T>(),
                e
            ))
        })
        .map(|t| t.into())
}

/// Encodes a protobuf message to bytes.
pub fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}

/// Shared state for the Ballista scheduler.
///
/// Contains managers for executors, tasks, and sessions.
#[derive(Clone)]
pub struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    /// Manager for executor registration and task slot allocation.
    pub executor_manager: ExecutorManager,
    /// Manager for job and task scheduling.
    pub task_manager: TaskManager<T, U>,
    /// Manager for DataFusion session contexts.
    pub session_manager: SessionManager,
    /// Codec for serializing logical and physical plans.
    pub codec: BallistaCodec<T, U>,
    /// Scheduler configuration.
    pub config: Arc<SchedulerConfig>,
    /// Metrics collector for recording scheduler metrics.
    pub metrics_collector: Arc<dyn SchedulerMetricsCollector>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    /// Creates a new `SchedulerState` with the given cluster and configuration.
    pub fn new(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::new(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
            metrics_collector,
        }
    }

    /// Creates a new `SchedulerState` with default scheduler name (for testing only).
    #[cfg(test)]
    pub fn new_with_default_scheduler_name(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    ) -> Self {
        let config = Arc::new(SchedulerConfig::default());
        SchedulerState::new(
            cluster,
            codec,
            "localhost:50050".to_owned(),
            config,
            metrics_collector,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_task_launcher(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        dispatcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::with_launcher(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
                dispatcher,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
            metrics_collector,
        }
    }

    /// Initializes the scheduler state.
    pub async fn init(&self) -> Result<()> {
        self.executor_manager.init().await
    }

    pub(crate) async fn revive_offers(
        &self,
        sender: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let binding_result = self
            .executor_manager
            .bind_schedulable_tasks(self.task_manager.get_running_job_cache())
            .await?;
        if binding_result.bound_tasks.is_empty() {
            debug!("No schedulable tasks found to be launched");
            return Ok(());
        }

        // Record shuffle affinity metrics
        for affinity in &binding_result.shuffle_affinity {
            if affinity.has_local_data {
                self.metrics_collector.record_task_shuffle_affinity_hit(
                    &affinity.job_id,
                    affinity.stage_id,
                    &affinity.executor_id,
                );
            } else {
                self.metrics_collector.record_task_shuffle_affinity_miss(
                    &affinity.job_id,
                    affinity.stage_id,
                    &affinity.executor_id,
                );
            }
        }

        let schedulable_tasks = binding_result.bound_tasks;
        let state = self.clone();
        tokio::spawn(async move {
            let mut if_revive = false;
            match state.launch_tasks(schedulable_tasks).await {
                Ok(unassigned_executor_slots) => {
                    if !unassigned_executor_slots.is_empty() {
                        if let Err(e) = state
                            .executor_manager
                            .unbind_tasks(unassigned_executor_slots)
                            .await
                        {
                            error!("Fail to unbind tasks: {e}");
                        }
                        if_revive = true;
                    }
                }
                Err(e) => {
                    error!("Fail to launch tasks: {e}");
                    if_revive = true;
                }
            }
            if if_revive
                && let Err(e) = sender
                    .post_event(QueryStageSchedulerEvent::ReviveOffers)
                    .await
            {
                error!("Fail to send revive offers event due to {e:?}");
            }
        });

        Ok(())
    }

    /// Remove an executor.
    /// 1. The executor related info will be removed from [`ExecutorManager`]
    /// 2. All of affected running execution graph will be rolled backed
    /// 3. All of the running tasks of the affected running stages will be cancelled
    pub(crate) async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
    ) {
        if let Err(e) = self
            .executor_manager
            .remove_executor(executor_id, reason)
            .await
        {
            warn!("Fail to remove executor {executor_id}: {e}");
        }

        match self.task_manager.executor_lost(executor_id).await {
            Ok(tasks) => {
                if !tasks.is_empty()
                    && let Err(e) =
                        self.executor_manager.cancel_running_tasks(tasks).await
                {
                    warn!("Fail to cancel running tasks due to {e:?}");
                }
            }
            Err(e) => {
                error!("TaskManager error to handle Executor {executor_id} lost: {e}");
            }
        }
    }

    /// Given a vector of bound tasks,
    /// 1. Firstly reorganize according to: executor -> job stage -> tasks;
    /// 2. Then launch the task set vector to each executor one by one.
    ///
    /// If it fails to launch a task set, the related [`ExecutorSlot`] will be returned.
    async fn launch_tasks(
        &self,
        bound_tasks: Vec<BoundTask>,
    ) -> Result<Vec<ExecutorSlot>> {
        // Get current time once for all latency calculations
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        // Record task scheduling metrics for each task
        for (executor_id, task) in &bound_tasks {
            // Calculate scheduling latency: time from when task became schedulable to now
            let latency_ms = now_millis.saturating_sub(task.schedulable_time_millis);
            self.metrics_collector.record_task_scheduled(
                &task.partition.job_id,
                task.partition.stage_id,
                executor_id,
                latency_ms as u64,
            );
        }

        // Put tasks to the same executor together
        // And put tasks belonging to the same stage together for creating MultiTaskDefinition
        let mut executor_stage_assignments: HashMap<
            String,
            HashMap<(JobId, usize), Vec<TaskDescription>>,
        > = HashMap::new();
        for (executor_id, task) in bound_tasks.into_iter() {
            let stage_key = (task.partition.job_id.clone(), task.partition.stage_id);
            if let Some(tasks) = executor_stage_assignments.get_mut(&executor_id) {
                if let Some(executor_stage_tasks) = tasks.get_mut(&stage_key) {
                    executor_stage_tasks.push(task);
                } else {
                    tasks.insert(stage_key, vec![task]);
                }
            } else {
                let mut executor_stage_tasks: HashMap<
                    (JobId, usize),
                    Vec<TaskDescription>,
                > = HashMap::new();
                executor_stage_tasks.insert(stage_key, vec![task]);
                executor_stage_assignments.insert(executor_id, executor_stage_tasks);
            }
        }

        let mut join_handles = vec![];
        for (executor_id, tasks) in executor_stage_assignments.into_iter() {
            let tasks: Vec<Vec<TaskDescription>> = tasks.into_values().collect();
            // Total number of tasks to be launched for one executor
            let n_tasks: usize = tasks.iter().map(|stage_tasks| stage_tasks.len()).sum();

            let state = self.clone();
            let join_handle = tokio::spawn(async move {
                let success = match state
                    .executor_manager
                    .get_executor_metadata(&executor_id)
                    .await
                {
                    Ok(executor) => {
                        if let Err(e) = state
                            .task_manager
                            .launch_multi_task(&executor, tasks, &state.executor_manager)
                            .await
                        {
                            let err_msg = format!("Failed to launch new task: {e}");
                            error!("{}", err_msg.clone());

                            // It's OK to remove executor aggressively,
                            // since if the executor is in healthy state, it will be registered again.
                            state.remove_executor(&executor_id, Some(err_msg)).await;

                            false
                        } else {
                            true
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to launch new task, could not get executor metadata: {e}"
                        );
                        false
                    }
                };
                if success {
                    vec![]
                } else {
                    vec![(executor_id.clone(), n_tasks as u32)]
                }
            });
            join_handles.push(join_handle);
        }

        let unassigned_executor_slots =
            futures::future::join_all(join_handles)
                .await
                .into_iter()
                .collect::<std::result::Result<
                    Vec<Vec<ExecutorSlot>>,
                    tokio::task::JoinError,
                >>()?;

        Ok(unassigned_executor_slots
            .into_iter()
            .flatten()
            .collect::<Vec<ExecutorSlot>>())
    }

    pub(crate) async fn update_task_statuses(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let executor = self
            .executor_manager
            .get_executor_metadata(executor_id)
            .await?;

        let result = self
            .task_manager
            .update_task_statuses(&executor, tasks_status)
            .await?;

        // Record stage lifecycle metrics
        for (job_id, stage_id, task_count, _started_at_ms) in
            &result.metrics_info.stages_started
        {
            self.metrics_collector
                .record_stage_started(job_id, *stage_id, *task_count);
        }
        for (job_id, stage_id, duration_ms) in &result.metrics_info.stages_completed {
            self.metrics_collector.record_stage_completed(
                job_id,
                *stage_id,
                *duration_ms,
            );
        }
        for (job_id, stage_id, error_type) in &result.metrics_info.stages_failed {
            self.metrics_collector
                .record_stage_failed(job_id, *stage_id, error_type);
        }
        for (job_id, stage_id) in &result.metrics_info.stages_retried {
            self.metrics_collector.record_stage_retry(job_id, *stage_id);
        }

        // Record task lifecycle metrics
        for (job_id, stage_id, executor_id) in &result.metrics_info.tasks_completed {
            self.metrics_collector
                .record_task_completed(job_id, *stage_id, executor_id);
        }
        for (job_id, stage_id, executor_id, error_type) in
            &result.metrics_info.tasks_failed
        {
            self.metrics_collector.record_task_failed(
                job_id,
                *stage_id,
                executor_id,
                error_type,
            );
        }
        for (job_id, stage_id) in &result.metrics_info.tasks_retried {
            self.metrics_collector.record_task_retry(job_id, *stage_id);
        }

        Ok(result.events)
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &JobId,
        job_name: &str,
        session_ctx: Arc<SessionContext>,
        plan: &SubmitPlan,
        queued_at: u64,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let start = Instant::now();

        if let SubmitPlan::Logical(logical_plan) = plan {
            validate_local_listing_table_accessibility(logical_plan)?;
        }

        self.task_manager
            .submit_plan(job_id, job_name, session_ctx, plan, queued_at, subscriber)
            .await?;

        let elapsed = start.elapsed();

        self.metrics_collector
            .record_planning_duration(job_id, elapsed.as_millis() as u64);

        info!("Planned job {job_id} in {elapsed:?}");

        Ok(())
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_successful_job(&self, job_id: JobId) {
        self.executor_manager.clean_up_job_data_delayed(
            job_id.clone(),
            self.config.finished_job_data_clean_up_interval_seconds,
        );
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_failed_job(&self, job_id: JobId) {
        self.executor_manager.clean_up_job_data(job_id.clone());
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }
}

/// Validates that local `file://` listing tables referenced in a logical plan are
/// accessible on the scheduler host.
fn validate_local_listing_table_accessibility(logical_plan: &LogicalPlan) -> Result<()> {
    logical_plan.apply(&mut |plan: &LogicalPlan| {
        if let LogicalPlan::TableScan(scan) = plan {
            let provider = source_as_provider(&scan.source)?;
            if let Some(table) = provider.downcast_ref::<ListingTable>() {
                let local_paths: Vec<&ListingTableUrl> = table
                    .table_paths()
                    .iter()
                    .filter(|url| url.as_str().starts_with("file:///"))
                    .collect();
                if !local_paths.is_empty() {
                    // These are local files rather than remote object stores, so we
                    // need to check that they are accessible on the scheduler (the client
                    // may not be on the same host, or the data path may not be correctly
                    // mounted in the container). There could be thousands of files so we
                    // just check the first one.
                    let url = &local_paths[0].as_str();
                    let stripped = url
                        .strip_prefix("file://")
                        .or_else(|| url.strip_prefix("file:///"))
                        .ok_or_else(|| {
                            DataFusionError::External(
                                format!(
                                    "logical plan refers to path on local file system \
                                    that is not accessible in the scheduler: {url}"
                                )
                                .into(),
                            )
                        })?;
                    ListingTableUrl::parse(stripped).map_err(|e| {
                        DataFusionError::External(
                            format!(
                                "logical plan refers to path on local file system \
                                that is not accessible in the scheduler: {url}: {e:?}"
                            )
                            .into(),
                        )
                    })?;
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
