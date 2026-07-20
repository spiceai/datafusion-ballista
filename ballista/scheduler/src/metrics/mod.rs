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

/// Prometheus metrics collector implementation.
#[cfg(feature = "prometheus")]
/// Module implementing prometheus metrics.
pub mod prometheus;

#[cfg(feature = "prometheus")]
use crate::metrics::prometheus::PrometheusMetricsCollector;
use ballista_core::{JobId, error::Result};
use std::sync::Arc;

/// Interface for recording metrics events in the scheduler.
///
/// An instance of `Arc<dyn SchedulerMetricsCollector>` will be passed when constructing
/// the `QueryStageScheduler` which is the core event loop of the scheduler.
/// The event loop will then record metric events through this trait.
///
/// This trait provides hooks for job lifecycle events, stage lifecycle events,
/// task scheduling events, and executor management events.
pub trait SchedulerMetricsCollector: Send + Sync {
    // =========================================================================
    // Job lifecycle events (existing)
    // =========================================================================

    /// Record that job with `job_id` was submitted. This will be invoked
    /// after the job's `ExecutionGraph` is created and it is ready to be scheduled
    /// on executors.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it was submitted
    fn record_submitted(&self, job_id: &JobId, queued_at: u64, submitted_at: u64);

    /// Record that job with `job_id` has completed successfully. This should only
    /// be invoked on successful job completion.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it was completed
    fn record_completed(&self, job_id: &JobId, queued_at: u64, completed_at: u64);

    /// Record that job with `job_id` has failed.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it failed.
    fn record_failed(&self, job_id: &JobId, queued_at: u64, failed_at: u64);

    /// Record that job with `job_id` was cancelled.
    fn record_cancelled(&self, job_id: &JobId);

    /// Set the current number of pending tasks in scheduler. A pending task is a task that is available
    /// to schedule on an executor but cannot be scheduled because no resources are available.
    fn set_pending_tasks_queue_size(&self, value: u64);

    /// Set the current number of pending jobs in scheduler. A pending job is a job that has been
    /// queued but not yet submitted for execution (i.e., not yet planned).
    fn set_pending_jobs_queue_size(&self, value: u64);

    /// Gather current metric set that should be returned when calling the scheduler's metrics API
    /// Should return a tuple containing the content of the metric set and the content type (e.g. `application/json`, `text/plain`, etc)
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>>;

    // =========================================================================
    // Stage lifecycle events (new)
    // =========================================================================

    /// Record that a stage has started execution.
    ///
    /// Called when a stage transitions to Running state. The `task_count` is the
    /// total number of partitions/tasks in this stage.
    fn record_stage_started(&self, job_id: &JobId, stage_id: usize, task_count: usize);

    /// Record that a stage has completed successfully.
    ///
    /// Called when all tasks in a stage complete successfully. The `duration_ms`
    /// is the wall-clock time from stage start to completion.
    fn record_stage_completed(&self, job_id: &JobId, stage_id: usize, duration_ms: u64);

    /// Record that a stage has failed.
    ///
    /// Called when a stage fails (e.g., due to task failures exceeding retry limit).
    /// The `error_type` is a categorized error string suitable for use as a metric label.
    fn record_stage_failed(&self, job_id: &JobId, stage_id: usize, error_type: &str);

    /// Record that a stage is being retried.
    ///
    /// Called when a stage is reset for retry after a failure.
    fn record_stage_retry(&self, job_id: &JobId, stage_id: usize);

    // =========================================================================
    // Task scheduling events (new)
    // =========================================================================

    /// Record that a task has been scheduled to an executor.
    ///
    /// Called when the scheduler assigns a task to an executor. The `latency_ms`
    /// is the time from when the task became schedulable to when it was assigned.
    fn record_task_scheduled(
        &self,
        job_id: &JobId,
        stage_id: usize,
        executor_id: &str,
        latency_ms: u64,
    );

    /// Record that a task has completed on an executor.
    ///
    /// Called when the scheduler receives notification of task completion.
    fn record_task_completed(&self, job_id: &JobId, stage_id: usize, executor_id: &str);

    /// Record that a task has failed on an executor.
    ///
    /// Called when the scheduler receives notification of task failure.
    fn record_task_failed(
        &self,
        job_id: &JobId,
        stage_id: usize,
        executor_id: &str,
        error_type: &str,
    );

    /// Record that a task is being retried.
    ///
    /// Called when a task is rescheduled after a failure. This is distinct from
    /// stage-level retries and tracks individual task retry attempts.
    fn record_task_retry(&self, job_id: &JobId, stage_id: usize);

    /// Record a shuffle affinity hit - task was assigned to an executor that has
    /// local shuffle data from a parent stage.
    ///
    /// Called when the scheduler assigns a task to an executor that already has
    /// the required shuffle partitions from upstream stages stored locally.
    /// This indicates the task can read shuffle data without network transfer.
    fn record_task_shuffle_affinity_hit(
        &self,
        job_id: &JobId,
        stage_id: usize,
        executor_id: &str,
    );

    /// Record a shuffle affinity miss - task was assigned to an executor that does
    /// NOT have local shuffle data from a parent stage.
    ///
    /// Called when the scheduler assigns a task to an executor that does not have
    /// the required shuffle partitions locally. This indicates the task will need
    /// to fetch shuffle data over the network from other executors.
    fn record_task_shuffle_affinity_miss(
        &self,
        job_id: &JobId,
        stage_id: usize,
        executor_id: &str,
    );

    // =========================================================================
    // Executor management events (new)
    // =========================================================================

    /// Set the current count of active executors.
    ///
    /// Called when the executor count changes (registration/deregistration).
    fn set_active_executor_count(&self, count: usize);

    /// Record that an executor has registered with the scheduler.
    fn record_executor_registered(&self, executor_id: &str);

    /// Record that an executor has been removed from the scheduler.
    ///
    /// This can happen due to explicit deregistration, heartbeat timeout, or
    /// executor failure.
    fn record_executor_deregistered(&self, executor_id: &str);

    // =========================================================================
    // Planning events (new)
    // =========================================================================

    /// Record the duration of query planning.
    ///
    /// Called after a query has been planned and the ExecutionGraph is created.
    /// The `duration_ms` is the time spent in the distributed planner.
    fn record_planning_duration(&self, job_id: &JobId, duration_ms: u64);
}

/// Implementation of `SchedulerMetricsCollector` that ignores all events. This can be used as
/// a default implementation when tracking scheduler metrics is not required (or performed through other means)
#[derive(Default)]
pub struct NoopMetricsCollector {}

impl SchedulerMetricsCollector for NoopMetricsCollector {
    // Job lifecycle
    fn record_submitted(&self, _job_id: &JobId, _queued_at: u64, _submitted_at: u64) {}
    fn record_completed(&self, _job_id: &JobId, _queued_at: u64, _completed_at: u64) {}
    fn record_failed(&self, _job_id: &JobId, _queued_at: u64, _failed_at: u64) {}
    fn record_cancelled(&self, _job_id: &JobId) {}
    fn set_pending_tasks_queue_size(&self, _value: u64) {}
    fn set_pending_jobs_queue_size(&self, _value: u64) {}
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }

    // Stage lifecycle
    fn record_stage_started(&self, _job_id: &JobId, _stage_id: usize, _task_count: usize) {}
    fn record_stage_completed(&self, _job_id: &JobId, _stage_id: usize, _duration_ms: u64) {
    }
    fn record_stage_failed(&self, _job_id: &JobId, _stage_id: usize, _error_type: &str) {}
    fn record_stage_retry(&self, _job_id: &JobId, _stage_id: usize) {}

    // Task scheduling
    fn record_task_scheduled(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
        _latency_ms: u64,
    ) {
    }
    fn record_task_completed(&self, _job_id: &JobId, _stage_id: usize, _executor_id: &str) {
    }
    fn record_task_failed(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
        _error_type: &str,
    ) {
    }
    fn record_task_retry(&self, _job_id: &JobId, _stage_id: usize) {}
    fn record_task_shuffle_affinity_hit(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
    ) {
    }
    fn record_task_shuffle_affinity_miss(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
    ) {
    }

    // Executor management
    fn set_active_executor_count(&self, _count: usize) {}
    fn record_executor_registered(&self, _executor_id: &str) {}
    fn record_executor_deregistered(&self, _executor_id: &str) {}

    // Planning
    fn record_planning_duration(&self, _job_id: &JobId, _duration_ms: u64) {}
}

/// Returns the default metrics collector for the system.
///
/// When the `prometheus` feature is enabled, returns a Prometheus-based collector.
/// Otherwise, returns a no-op collector.
#[cfg(feature = "prometheus")]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    PrometheusMetricsCollector::current()
}

/// Returns the default metrics collector for the system.
///
/// When the `prometheus` feature is enabled, returns a Prometheus-based collector.
/// Otherwise, returns a no-op collector.
#[cfg(not(feature = "prometheus"))]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    Ok(Arc::new(NoopMetricsCollector::default()))
}
