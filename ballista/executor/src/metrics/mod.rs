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

use crate::execution_engine::QueryStageExecutor;
use ballista_core::JobId;
use log::info;
use std::sync::Arc;

/// `ExecutorMetricsCollector` records metrics for task execution on an executor.
///
/// This trait provides hooks for recording metrics at various points during
/// task execution, including task start, completion, failure, and shuffle operations.
///
/// Implementations can use these hooks to integrate with metrics systems like
/// Prometheus, OpenTelemetry, or custom monitoring solutions.
pub trait ExecutorMetricsCollector: Send + Sync {
    /// Record that a task has started execution.
    ///
    /// Called when a task begins executing on this executor. Use this to track
    /// active task counts and task start times.
    fn record_task_started(&self, job_id: &JobId, stage_id: usize, partition: usize);

    /// Record metrics for a stage/task after successful execution.
    ///
    /// Called when a task completes successfully. The `plan` contains execution
    /// metrics from DataFusion, and `duration_ms` is the wall-clock execution time.
    fn record_stage(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        plan: Arc<dyn QueryStageExecutor>,
        duration_ms: u64,
    );

    /// Record that a task has failed.
    ///
    /// Called when a task fails with an error. The `error_type` is a categorized
    /// error string suitable for use as a metric label.
    fn record_task_failed(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        error_type: &str,
    );

    /// Record shuffle write metrics.
    ///
    /// Called after shuffle data is written. Tracks bytes, rows, and duration
    /// for shuffle write operations.
    fn record_shuffle_write(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record shuffle read metrics.
    ///
    /// Called after shuffle data is read. Tracks bytes, rows, and duration
    /// for shuffle read operations.
    fn record_shuffle_read(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record local shuffle read metrics (data read from local disk).
    ///
    /// Called when shuffle data is read from a local file. This means the partition
    /// was written by this same executor in a previous stage, avoiding network transfer.
    fn record_shuffle_read_local(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record remote shuffle read metrics (data fetched from another executor).
    ///
    /// Called when shuffle data must be fetched over the network from another
    /// executor that produced the partition. The `source_executor_id` identifies
    /// the executor that holds the shuffle data.
    #[allow(clippy::too_many_arguments)]
    fn record_shuffle_read_remote(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        source_executor_id: &str,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record executor memory availability.
    ///
    /// Called periodically (e.g., during heartbeat) to report the executor's
    /// available memory. This helps with capacity planning and load balancing.
    fn record_memory_available(&self, available_bytes: u64);
}

/// Configures which executor system/process metrics should be collected for heartbeats.
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum ExecutorMetricCollectionPolicy {
    /// Collect only system-wide metrics.
    #[cfg_attr(feature = "build-binary", clap(name = "sys"))]
    SystemOnly,
    /// Collect only current process metrics.
    #[cfg_attr(feature = "build-binary", clap(name = "proc"))]
    #[default]
    ProcessOnly,
    /// Collect both system-wide and process metrics.
    #[cfg_attr(feature = "build-binary", clap(name = "all"))]
    SystemAndProcess,
    /// No metrics collected.
    Off,
}

impl std::fmt::Display for ExecutorMetricCollectionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorMetricCollectionPolicy::SystemOnly => f.write_str("sys"),
            ExecutorMetricCollectionPolicy::ProcessOnly => f.write_str("proc"),
            ExecutorMetricCollectionPolicy::SystemAndProcess => f.write_str("all"),
            ExecutorMetricCollectionPolicy::Off => f.write_str("off"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for ExecutorMetricCollectionPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

/// Implementation of `ExecutorMetricsCollector` which logs the completed
/// plan to stdout. Useful for debugging and development.
#[derive(Default)]
pub struct LoggingMetricsCollector {}

impl ExecutorMetricsCollector for LoggingMetricsCollector {
    fn record_task_started(&self, job_id: &JobId, stage_id: usize, partition: usize) {
        info!("=== [{job_id}/{stage_id}/{partition}] Task started ===");
    }

    fn record_stage(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        plan: Arc<dyn QueryStageExecutor>,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Task completed in {duration_ms}ms ===\n{plan}\n"
        );
    }

    fn record_task_failed(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        error_type: &str,
    ) {
        info!("=== [{job_id}/{stage_id}/{partition}] Task failed: {error_type} ===");
    }

    fn record_shuffle_write(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Shuffle write: {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }

    fn record_shuffle_read(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Shuffle read: {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }

    fn record_shuffle_read_local(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Local shuffle read: {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }

    fn record_shuffle_read_remote(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition: usize,
        source_executor_id: &str,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Remote shuffle read from {source_executor_id}: {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }

    fn record_memory_available(&self, available_bytes: u64) {
        info!("=== Executor memory available: {available_bytes} bytes ===");
    }
}
