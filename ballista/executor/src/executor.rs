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

//! Ballista executor logic

use crate::execution_engine::DefaultExecutionEngine;
use crate::execution_engine::ExecutionEngine;
use crate::execution_engine::QueryStageExecutor;
use crate::execution_loop::any_to_string;
use crate::metrics::ExecutorMetricsCollector;
use crate::metrics::LoggingMetricsCollector;
use crate::runtime_cache::SessionRuntimeCache;
use ballista_core::ConfigProducer;
use ballista_core::JobId;
use ballista_core::RuntimeProducer;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf::{ExecutorRegistration, ShuffleWritePartition};
use ballista_core::serde::scheduler::TaskKey;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::FutureExt;
use futures::future::AbortHandle;
use futures::task::AtomicWaker;
use log::error;
use log::warn;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Categorize a BallistaError into a short string suitable for use as a metric label.
///
/// This function is provided for use by custom `ExecutorMetricsCollector` implementations
/// that may need to categorize errors differently than the default.
pub fn categorize_ballista_error(error: &BallistaError) -> String {
    match error {
        BallistaError::NotImplemented(_) => "not_implemented".to_string(),
        BallistaError::General(_) => "general".to_string(),
        BallistaError::Internal(_) => "internal".to_string(),
        BallistaError::Configuration(_) => "configuration".to_string(),
        BallistaError::ArrowError(_) => "arrow".to_string(),
        BallistaError::DataFusionError(_) => "datafusion".to_string(),
        BallistaError::SqlError(_) => "sql".to_string(),
        BallistaError::IoError(_) => "io".to_string(),
        BallistaError::TonicError(_) => "tonic".to_string(),
        BallistaError::GrpcError(_) => "grpc".to_string(),
        BallistaError::GrpcConnectionError(_) => "grpc_connection".to_string(),
        BallistaError::TokioError(_) => "tokio".to_string(),
        BallistaError::GrpcActionError(_) => "grpc_action".to_string(),
        BallistaError::FetchFailed(_, _, _, _) => "fetch_failed".to_string(),
        BallistaError::Cancelled => "cancelled".to_string(),
    }
}

/// Categorize a DataFusionError into a short string suitable for use as a metric label.
fn categorize_datafusion_error(error: &datafusion::error::DataFusionError) -> String {
    use datafusion::error::DataFusionError;
    match error {
        DataFusionError::ArrowError(_, _) => "arrow".to_string(),
        DataFusionError::IoError(_) => "io".to_string(),
        DataFusionError::SQL(_, _) => "sql".to_string(),
        DataFusionError::NotImplemented(_) => "not_implemented".to_string(),
        DataFusionError::Internal(_) => "internal".to_string(),
        DataFusionError::Plan(_) => "plan".to_string(),
        DataFusionError::Configuration(_) => "configuration".to_string(),
        DataFusionError::SchemaError(_, _) => "schema".to_string(),
        DataFusionError::Execution(_) => "execution".to_string(),
        DataFusionError::ResourcesExhausted(_) => "resources_exhausted".to_string(),
        DataFusionError::External(_) => "external".to_string(),
        DataFusionError::Context(_, _) => "context".to_string(),
        DataFusionError::Substrait(_) => "substrait".to_string(),
        DataFusionError::Diagnostic(_, _) => "diagnostic".to_string(),
        DataFusionError::Collection(_) => "collection".to_string(),
        DataFusionError::ParquetError(_) => "parquet".to_string(),
        DataFusionError::ObjectStore(_) => "object_store".to_string(),
        DataFusionError::ExecutionJoin(_) => "execution_join".to_string(),
        DataFusionError::Shared(_) => "shared".to_string(),
        // Catch-all for feature-gated variants (e.g., AvroError when avro feature is enabled)
        #[allow(unreachable_patterns)]
        _ => "other".to_string(),
    }
}

/// Extract shuffle write metrics from the query stage executor's plan metrics.
///
/// Returns (bytes_written, rows_written, write_time_ms) if metrics are available.
fn extract_shuffle_write_metrics(
    query_stage_exec: &Arc<dyn QueryStageExecutor>,
) -> Option<(u64, u64, u64)> {
    let metrics_sets = query_stage_exec.collect_plan_metrics();

    let total_bytes = 0u64;
    let mut total_rows = 0u64;
    let mut total_write_time_nanos = 0u64;

    for metrics_set in &metrics_sets {
        for metric in metrics_set.iter() {
            let name = metric.value().name();
            match name {
                "output_rows" => {
                    total_rows += metric.value().as_usize() as u64;
                }
                "write_time" => {
                    // write_time is recorded in nanoseconds
                    total_write_time_nanos += metric.value().as_usize() as u64;
                }
                _ => {}
            }
        }
    }

    // Note: bytes_written is not directly tracked in ShuffleWriteMetrics,
    // but we can estimate from the output file sizes or use output_rows as proxy.
    // For now, we'll return 0 for bytes and let the caller decide.
    // TODO: Add bytes tracking to ShuffleWriterExec if needed.

    if total_rows > 0 || total_write_time_nanos > 0 {
        let write_time_ms = total_write_time_nanos / 1_000_000;
        Some((total_bytes, total_rows, write_time_ms))
    } else {
        None
    }
}

/// Extract shuffle read metrics by walking the execution plan tree and summing
/// the partition statistics from all ShuffleReaderExec nodes.
///
/// Returns (total_bytes, total_rows) if any shuffle readers are found with stats.
/// Note: Duration is not available from stats; it would need to be tracked during execution.
fn extract_shuffle_read_metrics(plan: &dyn ExecutionPlan) -> Option<(u64, u64)> {
    let mut total_bytes = 0u64;
    let mut total_rows = 0u64;
    let mut found_any = false;

    // Recursively walk the plan tree
    extract_shuffle_read_metrics_recursive(
        plan,
        &mut total_bytes,
        &mut total_rows,
        &mut found_any,
    );

    if found_any {
        Some((total_bytes, total_rows))
    } else {
        None
    }
}

fn extract_shuffle_read_metrics_recursive(
    plan: &dyn ExecutionPlan,
    total_bytes: &mut u64,
    total_rows: &mut u64,
    found_any: &mut bool,
) {
    // Check if this node is a ShuffleReaderExec
    if let Some(shuffle_reader) = plan.downcast_ref::<ShuffleReaderExec>() {
        // Sum up partition stats from all partition locations
        for partition_locations in &shuffle_reader.partition {
            for location in partition_locations {
                if let Some(bytes) = location.partition_stats.num_bytes() {
                    *total_bytes += bytes;
                    *found_any = true;
                }
                if let Some(rows) = location.partition_stats.num_rows() {
                    *total_rows += rows;
                    *found_any = true;
                }
            }
        }
    }

    // Recurse into children
    for child in plan.children() {
        extract_shuffle_read_metrics_recursive(
            child.as_ref(),
            total_bytes,
            total_rows,
            found_any,
        );
    }
}

/// A future that resolves when all active tasks on an executor have completed.
///
/// This is used during graceful shutdown to wait for in-flight tasks to drain
/// before terminating the executor process.
pub struct TasksDrainedFuture(
    /// The executor instance to monitor for task completion.
    pub Arc<Executor>,
);

impl Future for TasksDrainedFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.tasks_drained_waker.register(cx.waker());
        if !self.0.abort_handles.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

type AbortHandles = Arc<DashMap<TaskKey, AbortHandle>>;

/// Ballista executor
#[derive(Clone)]
pub struct Executor {
    /// Metadata
    pub metadata: ExecutorRegistration,

    /// Directory for storing partial results
    pub work_dir: String,

    /// Function registry
    pub function_registry: Arc<BallistaFunctionRegistry>,

    /// Creates [RuntimeEnv] based on [SessionConfig]
    pub runtime_producer: RuntimeProducer,

    /// Creates default [SessionConfig]
    pub config_producer: ConfigProducer,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,

    /// Virtual cores assigned to this executor. See CLI docs on `--vcores`.
    pub vcores: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,

    tasks_drained_waker: Arc<AtomicWaker>,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,

    /// Optional session-keyed cache of shared base runtime envs. When set,
    /// `produce_runtime_for_session` reuses read-side state across a session's
    /// tasks; when `None`, each task builds a runtime from `runtime_producer`.
    session_runtime_cache: Option<Arc<dyn SessionRuntimeCache>>,

    /// Worker-thread override for the task-runner pool; `None` means `vcores`.
    task_runner_threads: Option<NonZeroUsize>,

    /// Slots that are always available; `None` means `vcores`.
    guaranteed_task_slots: Option<NonZeroUsize>,
}

impl Executor {
    /// Create a new executor instance with given [RuntimeEnv]
    /// It will use default scalar, aggregate and window functions
    pub fn new_basic(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        vcores: usize,
    ) -> Self {
        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            Arc::new(BallistaFunctionRegistry::default()),
            Arc::new(LoggingMetricsCollector::default()),
            vcores,
            None,
        )
    }

    /// Create a new executor instance with given [RuntimeEnv],
    /// [datafusion::logical_expr::ScalarUDF], [datafusion::logical_expr::AggregateUDF] and [datafusion::logical_expr::WindowUDF]
    ///
    /// `execution_engine` of `None` uses [`DefaultExecutionEngine`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        function_registry: Arc<BallistaFunctionRegistry>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        vcores: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            vcores,
            abort_handles: Default::default(),
            tasks_drained_waker: Default::default(),
            execution_engine: execution_engine
                .unwrap_or_else(|| Arc::new(DefaultExecutionEngine::new())),
            session_runtime_cache: None,
            task_runner_threads: None,
            guaranteed_task_slots: None,
        }
    }
    /// Creates new Executor with default `ExecutionEngine`.
    /// Default `ExecutionEngine` does not cache client connections.
    pub fn with_default_execution_engine(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        function_registry: Arc<BallistaFunctionRegistry>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        vcores: usize,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            vcores,
            abort_handles: Default::default(),
            tasks_drained_waker: Default::default(),
            execution_engine: Arc::new(DefaultExecutionEngine::new()),
            session_runtime_cache: None,
            task_runner_threads: None,
            guaranteed_task_slots: None,
        }
    }
}

impl Executor {
    fn wake_tasks_drained(&self) {
        if self.abort_handles.is_empty() {
            self.tasks_drained_waker.wake();
        }
    }

    /// Creates a [`RuntimeEnv`] using the configured runtime producer.
    pub fn produce_runtime(
        &self,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        (self.runtime_producer)(config)
    }

    /// Attaches (or clears) the session-keyed runtime cache. Cloning the
    /// `Executor` shares the same cache via `Arc`.
    pub fn with_session_runtime_cache(
        mut self,
        cache: Option<Arc<dyn SessionRuntimeCache>>,
    ) -> Self {
        self.session_runtime_cache = cache;
        self
    }

    /// Sets the number of worker threads in the task-runner pool, independently
    /// of `vcores`.
    ///
    /// `vcores` bounds how many tasks run concurrently (the slot count advertised
    /// to the scheduler); this bounds the OS threads that poll them. Tasks are
    /// async, so the two are independent: an embedder can advertise more slots
    /// than cores for I/O-bound work without oversubscribing the CPU. Defaults
    /// to `vcores` when unset.
    #[must_use]
    pub fn with_task_runner_threads(mut self, threads: NonZeroUsize) -> Self {
        self.task_runner_threads = Some(threads);
        self
    }

    /// Sets the number of task slots the embedder guarantees are always
    /// available, for executors whose slot count varies at runtime (the adaptive
    /// controller's `floor`). Defaults to `vcores`, which is correct for a fixed
    /// slot count.
    ///
    /// The pull loop charges a task one slot per bundled partition but never
    /// more than this, since waiting for more slots than the semaphore is
    /// guaranteed to hold could hang if it shrinks meanwhile. A task wider than
    /// this is under-charged rather than at risk of hanging.
    #[must_use]
    pub fn with_guaranteed_task_slots(mut self, slots: NonZeroUsize) -> Self {
        self.guaranteed_task_slots = Some(slots);
        self
    }

    /// The [`with_guaranteed_task_slots`](Self::with_guaranteed_task_slots)
    /// override, or `vcores` when none was set (at least 1).
    pub fn guaranteed_task_slots(&self) -> usize {
        self.guaranteed_task_slots
            .map_or(self.vcores.max(1), NonZeroUsize::get)
    }

    /// Worker threads for the task-runner pool: the
    /// [`with_task_runner_threads`](Self::with_task_runner_threads) override, or
    /// `vcores` when none was set (at least 1).
    pub fn task_runner_threads(&self) -> usize {
        self.task_runner_threads
            .map_or(self.vcores.max(1), NonZeroUsize::get)
    }

    /// Produces the runtime for a task, reusing the session's shared read-side
    /// state when a session cache is attached; otherwise builds a runtime per
    /// task via the `runtime_producer`. `vcores_consumed` is the number of
    /// vcores this task claimed at bind time; the memory pool policy uses it
    /// to size the per-task pool proportionally.
    pub fn produce_runtime_for_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        match &self.session_runtime_cache {
            Some(cache) => cache.produce_runtime(session_id, config, vcores_consumed),
            None => (self.runtime_producer)(config),
        }
    }

    /// Creates a default [`SessionConfig`] using the configured config producer.
    pub fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }

    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_query_stage(
        &self,
        key: TaskKey,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>, BallistaError> {
        let start_time = std::time::Instant::now();

        // Record task start for metrics tracking
        self.metrics_collector.record_task_started(
            &key.job_id,
            key.stage_id,
            key.task_id,
        );

        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(key.task_id, task_ctx),
        );

        self.abort_handles.insert(key.clone(), abort_handle);

        let result = std::panic::AssertUnwindSafe(task).catch_unwind().await;
        let duration_ms = start_time.elapsed().as_millis() as u64;

        // cancel_task only signals the abort; this task owns removal after unwinding.
        self.abort_handles.remove(&key);
        self.wake_tasks_drained();

        match result {
            Ok(Ok(Ok(partitions))) => {
                // Extract shuffle write metrics from the plan
                let shuffle_write_metrics =
                    extract_shuffle_write_metrics(&query_stage_exec);
                if let Some((bytes, rows, write_time_ms)) = shuffle_write_metrics {
                    self.metrics_collector.record_shuffle_write(
                        &key.job_id,
                        key.stage_id,
                        key.task_id,
                        bytes,
                        rows,
                        write_time_ms,
                    );
                }

                // Extract shuffle read metrics from ShuffleReaderExec nodes in the plan
                // Note: Duration is approximated as task duration minus write time since
                // we don't have fine-grained timing for the read phase
                let shuffle_read_metrics =
                    extract_shuffle_read_metrics(query_stage_exec.plan());
                if let Some((bytes, rows)) = shuffle_read_metrics {
                    // Approximate read duration: if we have write time, subtract it from total
                    // Otherwise, use 0 (the bytes/rows are still valuable)
                    let read_duration_ms = shuffle_write_metrics
                        .map(|(_, _, write_ms)| duration_ms.saturating_sub(write_ms))
                        .unwrap_or(0);
                    self.metrics_collector.record_shuffle_read(
                        &key.job_id,
                        key.stage_id,
                        key.task_id,
                        bytes,
                        rows,
                        read_duration_ms,
                    );
                }

                self.metrics_collector.record_stage(
                    &key.job_id,
                    key.stage_id,
                    key.task_id,
                    query_stage_exec,
                    duration_ms,
                );
                Ok(partitions)
            }
            Ok(Ok(Err(e))) => {
                self.metrics_collector.record_task_failed(
                    &key.job_id,
                    key.stage_id,
                    key.task_id,
                    &categorize_datafusion_error(&e),
                );
                Err(BallistaError::from(e))
            }
            Ok(Err(_aborted)) => {
                // Task was cancelled - don't record as failure, it was intentional
                warn!("Task has been aborted!");
                Err(BallistaError::Cancelled)
            }
            Err(p) => {
                let error_msg = format!("{:#?}", any_to_string(&p));
                error!("{error_msg}");
                let error = BallistaError::Internal(error_msg);
                self.metrics_collector.record_task_failed(
                    &key.job_id,
                    key.stage_id,
                    key.task_id,
                    &categorize_ballista_error(&error),
                );
                Err(error)
            }
        }
    }

    /// Cancels a running task by aborting its execution.
    ///
    /// Returns `Ok(true)` if the task was found and cancelled, `Ok(false)` if not found.
    pub async fn cancel_task(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
    ) -> Result<bool, BallistaError> {
        // execute_query_stage removes the handle after the aborted task unwinds.
        if let Some(handle) = self.abort_handles.get(&TaskKey {
            job_id,
            stage_id,
            task_id,
        }) {
            handle.abort();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns the working directory path for this executor.
    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }

    /// Returns the number of tasks currently executing on this executor.
    pub fn active_task_count(&self) -> usize {
        self.abort_handles.len()
    }
}

#[cfg(test)]
mod test {
    use crate::cpu_bound_executor::DedicatedExecutor;
    use crate::execution_engine::{DefaultQueryStageExec, ShuffleWriterVariant};
    use crate::executor::{Executor, TasksDrainedFuture};
    use crate::runtime_cache::{
        DefaultSessionRuntimeCache, MemoryPoolPolicy, SessionRuntimeCache,
    };
    use ballista_core::RuntimeProducer;
    use ballista_core::error::BallistaError;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::{ExecutorRegistration, ShuffleWritePartition};
    use ballista_core::serde::scheduler::TaskKey;
    use ballista_core::utils::default_config_producer;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::runtime_env::RuntimeEnv;

    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    };
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use futures::Stream;
    use futures::task::{ArcWake, waker_ref};
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tempfile::TempDir;

    /// A RecordBatchStream that will never terminate
    struct NeverendingRecordBatchStream;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl RecordBatchStream for NeverendingRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
    }

    impl Stream for NeverendingRecordBatchStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    /// An ExecutionPlan which will never terminate
    #[derive(Debug)]
    pub struct NeverendingOperator {
        properties: Arc<PlanProperties>,
    }

    impl NeverendingOperator {
        fn new() -> Self {
            NeverendingOperator {
                properties: Arc::new(PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(Arc::new(
                        Schema::empty(),
                    )),
                    Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                )),
            }
        }
    }

    impl DisplayAs for NeverendingOperator {
        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    write!(f, "NeverendingOperator")
                }
            }
        }
    }

    impl ExecutionPlan for NeverendingOperator {
        fn name(&self) -> &str {
            "NeverendingOperator"
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            Ok(Box::pin(NeverendingRecordBatchStream))
        }
    }

    /// The result `execute_query_stage` hands back once a spawned task unwinds.
    type TaskOutcome = Result<Vec<ShuffleWritePartition>, BallistaError>;

    /// Builds an executor over `work_dir`, along with the session context whose
    /// runtime its tasks run on.
    fn never_ending_executor(work_dir: &str) -> (Arc<Executor>, SessionContext) {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));

        let executor = Arc::new(Executor::new_basic(
            executor_registration,
            work_dir,
            runtime_producer,
            config_producer,
            2,
        ));

        (executor, ctx)
    }

    /// Spawns a task that never yields a batch on a separate fiber. The returned
    /// channel fires once `execute_query_stage` has unwound, which is after it has
    /// removed its own abort handle.
    fn spawn_never_ending_task(
        executor: &Arc<Executor>,
        ctx: &SessionContext,
        work_dir: &str,
        key: TaskKey,
    ) -> tokio::sync::oneshot::Receiver<TaskOutcome> {
        let shuffle_write = ShuffleWriterExec::try_new(
            key.job_id.clone(),
            key.stage_id,
            Arc::new(NeverendingOperator::new()),
            work_dir.to_string(),
        )
        .expect("creating shuffle writer");
        let query_stage_exec =
            DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(shuffle_write));

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let executor = executor.clone();
        let task_ctx = ctx.task_ctx();
        tokio::task::spawn(async move {
            let task_result = executor
                .execute_query_stage(key, Arc::new(query_stage_exec), task_ctx)
                .await;
            sender.send(task_result).expect("sending result");
        });

        receiver
    }

    /// A task is only registered once it starts executing, so poll until the
    /// executor reports the count the test is waiting on.
    async fn await_active_task_count(executor: &Executor, expected: usize) {
        for _ in 0..20 {
            if executor.active_task_count() == expected {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert_eq!(executor.active_task_count(), expected);
    }

    /// Awaits a cancelled task's unwind and asserts it reported failure.
    async fn await_cancelled_task(receiver: tokio::sync::oneshot::Receiver<TaskOutcome>) {
        tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .expect("task unwinding before the timeout")
            .expect("receiving task result")
            .expect_err("a cancelled task fails");
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();
        let (executor, ctx) = never_ending_executor(&work_dir);

        let receiver = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 0,
            },
        );
        await_active_task_count(&executor, 1).await;

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = waker_ref(&wake_counter);
        let mut context = Context::from_waker(&waker);
        let mut tasks_drained = Box::pin(TasksDrainedFuture(executor.clone()));
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);
        assert!(
            executor
                .cancel_task("job-id".into(), 1, 0)
                .await
                .expect("cancelling task")
        );
        assert_eq!(executor.active_task_count(), 1);

        await_cancelled_task(receiver).await;

        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Ready(()));
    }

    #[tokio::test]
    async fn test_tasks_drained_waits_for_last_task() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();
        let (executor, ctx) = never_ending_executor(&work_dir);

        let first = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 0,
            },
        );
        let second = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 1,
            },
        );
        await_active_task_count(&executor, 2).await;

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = waker_ref(&wake_counter);
        let mut context = Context::from_waker(&waker);
        let mut tasks_drained = Box::pin(TasksDrainedFuture(executor.clone()));
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);

        assert!(
            executor
                .cancel_task("job-id".into(), 1, 0)
                .await
                .expect("cancelling the first task")
        );
        await_cancelled_task(first).await;

        // Draining a task that is not the last one may wake the future, but it must
        // not resolve it, so re-poll rather than counting wakes.
        assert_eq!(executor.active_task_count(), 1);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);

        let wakes_before_last = wake_counter.0.load(Ordering::SeqCst);
        assert!(
            executor
                .cancel_task("job-id".into(), 1, 1)
                .await
                .expect("cancelling the second task")
        );
        await_cancelled_task(second).await;

        // Draining the last task has to wake the waker registered by the re-poll
        // above; without that, shutdown would never look at the map again.
        assert!(wake_counter.0.load(Ordering::SeqCst) > wakes_before_last);
        assert_eq!(executor.active_task_count(), 0);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Ready(()));
    }

    #[test]
    fn produce_runtime_for_session_shares_read_side_state() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);

        // A base producer that builds a fresh env each call, plus an identity
        // pool policy, so shared read-side state is observable via ptr equality.
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        let identity: MemoryPoolPolicy = Arc::new(|base, _, _| Ok(base));
        let cache: Arc<dyn SessionRuntimeCache> = Arc::new(
            DefaultSessionRuntimeCache::new(base_producer.clone(), identity, 4),
        );

        let executor = Executor::new_basic(
            executor_registration,
            "/tmp",
            base_producer,
            config_producer,
            2,
        )
        .with_session_runtime_cache(Some(cache));

        let cfg = SessionConfig::new();
        let e1 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e3 = executor.produce_runtime_for_session("s2", &cfg, 1).unwrap();

        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e3.cache_manager));
    }

    #[test]
    fn produce_runtime_for_session_falls_back_without_cache() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));

        // No cache attached: each call builds a fresh env.
        let executor = Executor::new_basic(
            executor_registration,
            "/tmp",
            base_producer,
            config_producer,
            2,
        );

        let cfg = SessionConfig::new();
        let e1 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn task_runner_threads_defaults_to_vcores() {
        let executor = test_executor(7);
        assert_eq!(executor.task_runner_threads(), 7);
    }

    #[test]
    fn task_runner_threads_override_wins_and_leaves_vcores() {
        let executor = test_executor(256)
            .with_task_runner_threads(NonZeroUsize::new(3).expect("3 is non-zero"));
        assert_eq!(executor.task_runner_threads(), 3);
        assert_eq!(executor.vcores, 256);
    }

    #[test]
    fn task_runner_pool_uses_configured_thread_count() {
        let executor = test_executor(256)
            .with_task_runner_threads(NonZeroUsize::new(2).expect("2 is non-zero"));
        let pool = DedicatedExecutor::new("task_runner", executor.task_runner_threads());
        assert!(
            format!("{pool:?}").contains("num_threads: 2"),
            "unexpected pool state: {pool:?}"
        );
        pool.join();
    }

    fn test_executor(vcores: usize) -> Executor {
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        Executor::new_basic(
            ExecutorRegistration::default(),
            "/tmp",
            base_producer,
            Arc::new(default_config_producer),
            vcores,
        )
    }
}
