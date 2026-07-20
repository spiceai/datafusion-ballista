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

use crate::metrics::SchedulerMetricsCollector;
use ballista_core::error::{BallistaError, Result};
use ballista_core::JobId;

use once_cell::sync::OnceCell;
use prometheus::{
    Counter, Gauge, Histogram, Registry, register_counter_with_registry,
    register_gauge_with_registry, register_histogram_with_registry,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

static COLLECTOR: OnceCell<Arc<dyn SchedulerMetricsCollector>> = OnceCell::new();

/// SchedulerMetricsCollector implementation based on Prometheus.
///
/// # Job lifecycle metrics
/// - *job_exec_time_seconds* - Histogram of successful job execution time in seconds
/// - *planning_time_ms* - Histogram of job planning time in milliseconds
/// - *job_failed_total* - Counter of failed jobs
/// - *job_cancelled_total* - Counter of cancelled jobs
/// - *job_completed_total* - Counter of completed jobs
/// - *job_submitted_total* - Counter of submitted jobs
/// - *pending_task_queue_size* - Gauge of pending tasks
/// - *pending_jobs_queue_size* - Gauge of pending jobs
///
/// # Stage lifecycle metrics
/// - *stage_started_total* - Counter of stages started
/// - *stage_completed_total* - Counter of stages completed
/// - *stage_failed_total* - Counter of stages failed
/// - *stage_retry_total* - Counter of stage retries
/// - *stage_duration_ms* - Histogram of stage execution duration in milliseconds
///
/// # Task scheduling metrics
/// - *task_scheduled_total* - Counter of tasks scheduled
/// - *task_completed_total* - Counter of tasks completed
/// - *task_failed_total* - Counter of tasks failed
/// - *task_retry_total* - Counter of task retries
/// - *task_scheduling_latency_ms* - Histogram of task scheduling latency
/// - *task_shuffle_affinity_hit_total* - Counter of shuffle affinity hits
/// - *task_shuffle_affinity_miss_total* - Counter of shuffle affinity misses
///
/// # Executor management metrics
/// - *active_executor_count* - Gauge of active executors
/// - *executor_registered_total* - Counter of executor registrations
/// - *executor_deregistered_total* - Counter of executor deregistrations
///
/// # Planning metrics
/// - *distributed_planning_duration_ms* - Histogram of distributed planning duration
pub struct PrometheusMetricsCollector {
    // Job lifecycle
    execution_time: Histogram,
    planning_time: Histogram,
    failed: Counter,
    cancelled: Counter,
    completed: Counter,
    submitted: Counter,
    pending_queue_size: Gauge,
    pending_jobs_queue_size: Gauge,

    // Stage lifecycle
    stage_started: Counter,
    stage_completed: Counter,
    stage_failed: Counter,
    stage_retry: Counter,
    stage_duration: Histogram,

    // Task scheduling
    task_scheduled: Counter,
    task_completed: Counter,
    task_failed: Counter,
    task_retry: Counter,
    task_scheduling_latency: Histogram,
    task_shuffle_affinity_hit: Counter,
    task_shuffle_affinity_miss: Counter,

    // Executor management
    active_executor_count: Gauge,
    executor_registered: Counter,
    executor_deregistered: Counter,

    // Planning
    distributed_planning_duration: Histogram,
}

impl PrometheusMetricsCollector {
    /// Creates a new PrometheusMetricsCollector instance.
    pub fn new(registry: &Registry) -> Result<Self> {
        let execution_time = register_histogram_with_registry!(
            "job_exec_time_seconds",
            "Histogram of successful job execution time in seconds",
            vec![0.5_f64, 1_f64, 5_f64, 30_f64, 60_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let planning_time = register_histogram_with_registry!(
            "planning_time_ms",
            "Histogram of job planning time in milliseconds",
            vec![1.0_f64, 5.0_f64, 25.0_f64, 100.0_f64, 500.0_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let failed = register_counter_with_registry!(
            "job_failed_total",
            "Counter of failed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let cancelled = register_counter_with_registry!(
            "job_cancelled_total",
            "Counter of cancelled jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let completed = register_counter_with_registry!(
            "job_completed_total",
            "Counter of completed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let submitted = register_counter_with_registry!(
            "job_submitted_total",
            "Counter of submitted jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let pending_queue_size = register_gauge_with_registry!(
            "pending_task_queue_size",
            "Number of pending tasks",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let pending_jobs_queue_size = register_gauge_with_registry!(
            "pending_jobs_queue_size",
            "Number of pending jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        // Stage lifecycle metrics
        let stage_started = register_counter_with_registry!(
            "stage_started_total",
            "Counter of stages started",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let stage_completed = register_counter_with_registry!(
            "stage_completed_total",
            "Counter of stages completed",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let stage_failed = register_counter_with_registry!(
            "stage_failed_total",
            "Counter of stages failed",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let stage_retry = register_counter_with_registry!(
            "stage_retry_total",
            "Counter of stage retries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let stage_duration = register_histogram_with_registry!(
            "stage_duration_ms",
            "Histogram of stage execution duration in milliseconds",
            vec![100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 30_000.0, 60_000.0],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        // Task scheduling metrics
        let task_scheduled = register_counter_with_registry!(
            "task_scheduled_total",
            "Counter of tasks scheduled",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_completed = register_counter_with_registry!(
            "task_completed_total",
            "Counter of tasks completed",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_failed = register_counter_with_registry!(
            "task_failed_total",
            "Counter of tasks failed",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_retry = register_counter_with_registry!(
            "task_retry_total",
            "Counter of task retries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_scheduling_latency = register_histogram_with_registry!(
            "task_scheduling_latency_ms",
            "Histogram of task scheduling latency in milliseconds",
            vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1_000.0],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_shuffle_affinity_hit = register_counter_with_registry!(
            "task_shuffle_affinity_hit_total",
            "Counter of shuffle affinity hits",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let task_shuffle_affinity_miss = register_counter_with_registry!(
            "task_shuffle_affinity_miss_total",
            "Counter of shuffle affinity misses",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        // Executor management metrics
        let active_executor_count = register_gauge_with_registry!(
            "active_executor_count",
            "Number of active executors",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let executor_registered = register_counter_with_registry!(
            "executor_registered_total",
            "Counter of executor registrations",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let executor_deregistered = register_counter_with_registry!(
            "executor_deregistered_total",
            "Counter of executor deregistrations",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        // Planning metrics
        let distributed_planning_duration = register_histogram_with_registry!(
            "distributed_planning_duration_ms",
            "Histogram of distributed planning duration in milliseconds",
            vec![1.0, 5.0, 25.0, 100.0, 500.0, 1_000.0, 5_000.0],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        Ok(Self {
            execution_time,
            planning_time,
            failed,
            cancelled,
            completed,
            submitted,
            pending_queue_size,
            pending_jobs_queue_size,
            stage_started,
            stage_completed,
            stage_failed,
            stage_retry,
            stage_duration,
            task_scheduled,
            task_completed,
            task_failed,
            task_retry,
            task_scheduling_latency,
            task_shuffle_affinity_hit,
            task_shuffle_affinity_miss,
            active_executor_count,
            executor_registered,
            executor_deregistered,
            distributed_planning_duration,
        })
    }

    /// Returns the current global prometheus collector.
    pub fn current() -> Result<Arc<dyn SchedulerMetricsCollector>> {
        COLLECTOR
            .get_or_try_init(|| {
                let collector = Self::new(::prometheus::default_registry())?;

                Ok(Arc::new(collector) as Arc<dyn SchedulerMetricsCollector>)
            })
            .cloned()
    }
}

impl SchedulerMetricsCollector for PrometheusMetricsCollector {
    fn record_submitted(&self, _job_id: &JobId, queued_at: u64, submitted_at: u64) {
        self.submitted.inc();
        self.planning_time
            .observe((submitted_at - queued_at) as f64);
    }

    fn record_completed(&self, _job_id: &JobId, queued_at: u64, completed_at: u64) {
        self.completed.inc();
        self.execution_time
            .observe((completed_at - queued_at) as f64 / 1000_f64)
    }

    fn record_failed(&self, _job_id: &JobId, _queued_at: u64, _failed_at: u64) {
        self.failed.inc()
    }

    fn record_cancelled(&self, _job_id: &JobId) {
        self.cancelled.inc();
    }

    fn set_pending_tasks_queue_size(&self, value: u64) {
        self.pending_queue_size.set(value as f64);
    }

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).map_err(|e| {
            BallistaError::Internal(format!("Error encoding prometheus metrics: {e:?}"))
        })?;

        Ok(Some((buffer, encoder.format_type().to_owned())))
    }

    // Stage lifecycle
    fn set_pending_jobs_queue_size(&self, value: u64) {
        self.pending_jobs_queue_size.set(value as f64);
    }

    fn record_stage_started(&self, _job_id: &JobId, _stage_id: usize, _task_count: usize) {
        self.stage_started.inc();
    }

    fn record_stage_completed(&self, _job_id: &JobId, _stage_id: usize, duration_ms: u64) {
        self.stage_completed.inc();
        self.stage_duration.observe(duration_ms as f64);
    }

    fn record_stage_failed(&self, _job_id: &JobId, _stage_id: usize, _error_type: &str) {
        self.stage_failed.inc();
    }

    fn record_stage_retry(&self, _job_id: &JobId, _stage_id: usize) {
        self.stage_retry.inc();
    }

    // Task scheduling
    fn record_task_scheduled(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
        latency_ms: u64,
    ) {
        self.task_scheduled.inc();
        self.task_scheduling_latency.observe(latency_ms as f64);
    }

    fn record_task_completed(&self, _job_id: &JobId, _stage_id: usize, _executor_id: &str) {
        self.task_completed.inc();
    }

    fn record_task_failed(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
        _error_type: &str,
    ) {
        self.task_failed.inc();
    }

    fn record_task_retry(&self, _job_id: &JobId, _stage_id: usize) {
        self.task_retry.inc();
    }

    fn record_task_shuffle_affinity_hit(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        self.task_shuffle_affinity_hit.inc();
    }

    fn record_task_shuffle_affinity_miss(
        &self,
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        self.task_shuffle_affinity_miss.inc();
    }

    // Executor management
    fn set_active_executor_count(&self, count: usize) {
        self.active_executor_count.set(count as f64);
    }

    fn record_executor_registered(&self, _executor_id: &str) {
        self.executor_registered.inc();
    }

    fn record_executor_deregistered(&self, _executor_id: &str) {
        self.executor_deregistered.inc();
    }

    // Planning
    fn record_planning_duration(&self, _job_id: &JobId, duration_ms: u64) {
        self.distributed_planning_duration
            .observe(duration_ms as f64);
    }
}
