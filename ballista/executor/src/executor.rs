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
use crate::metrics::ExecutorMetricsCollector;
use crate::metrics::LoggingMetricsCollector;
use crate::runtime_cache::SessionRuntimeCache;
use ballista_core::JobId;
use ballista_core::ConfigProducer;
use ballista_core::RuntimeProducer;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::ExecutorRegistration;
use ballista_core::serde::scheduler::PartitionId;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::future::AbortHandle;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Categorize a BallistaError into a short string suitable for use as a metric label.
///
/// This function is provided for use by custom `ExecutorMetricsCollector` implementations
/// that may need to categorize errors differently than the default.
#[allow(dead_code)]
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

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.0.abort_handles.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

type AbortHandles = Arc<DashMap<(usize, PartitionId), AbortHandle>>;

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

    /// Concurrent tasks can run in executor
    pub concurrent_tasks: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,

    /// Optional session-keyed cache of shared base runtime envs. When set,
    /// `produce_runtime_for_session` reuses read-side state across a session's
    /// tasks; when `None`, each task builds a runtime from `runtime_producer`.
    session_runtime_cache: Option<Arc<dyn SessionRuntimeCache>>,
}

impl Executor {
    /// Create a new executor instance with given [RuntimeEnv]
    /// It will use default scalar, aggregate and window functions
    pub fn new_basic(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        concurrent_tasks: usize,
    ) -> Self {
        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            Arc::new(BallistaFunctionRegistry::default()),
            Arc::new(LoggingMetricsCollector::default()),
            concurrent_tasks,
            None,
        )
    }

    /// Create a new executor instance with given [RuntimeEnv],
    /// [datafusion::logical_expr::ScalarUDF], [datafusion::logical_expr::AggregateUDF] and [datafusion::logical_expr::WindowUDF]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        function_registry: Arc<BallistaFunctionRegistry>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            concurrent_tasks,
            abort_handles: Default::default(),
            execution_engine: execution_engine
                .unwrap_or_else(|| Arc::new(DefaultExecutionEngine::new())),
            session_runtime_cache: None,
        }
    }
}

impl Executor {
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

    /// Produces the runtime for a task, reusing the session's shared read-side
    /// state when a session cache is attached; otherwise builds a runtime per
    /// task via the `runtime_producer`.
    pub fn produce_runtime_for_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        match &self.session_runtime_cache {
            Some(cache) => cache.produce_runtime(session_id, config),
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
        task_id: usize,
        partition: PartitionId,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let start_time = std::time::Instant::now();

        // Record task start for metrics tracking
        self.metrics_collector.record_task_started(
            &partition.job_id,
            partition.stage_id,
            partition.partition_id,
        );

        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(partition.partition_id, task_ctx),
        );

        self.abort_handles
            .insert((task_id, partition.clone()), abort_handle);

        let result = task.await;
        let duration_ms = start_time.elapsed().as_millis() as u64;

        self.abort_handles.remove(&(task_id, partition.clone()));

        match result {
            Ok(Ok(partitions)) => {
                // Extract shuffle write metrics from the plan
                let shuffle_write_metrics =
                    extract_shuffle_write_metrics(&query_stage_exec);
                if let Some((bytes, rows, write_time_ms)) = shuffle_write_metrics {
                    self.metrics_collector.record_shuffle_write(
                        &partition.job_id,
                        partition.stage_id,
                        partition.partition_id,
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
                        &partition.job_id,
                        partition.stage_id,
                        partition.partition_id,
                        bytes,
                        rows,
                        read_duration_ms,
                    );
                }

                self.metrics_collector.record_stage(
                    &partition.job_id,
                    partition.stage_id,
                    partition.partition_id,
                    query_stage_exec,
                    duration_ms,
                );
                Ok(partitions)
            }
            Ok(Err(e)) => {
                self.metrics_collector.record_task_failed(
                    &partition.job_id,
                    partition.stage_id,
                    partition.partition_id,
                    &categorize_datafusion_error(&e),
                );
                Err(BallistaError::from(e))
            }
            Err(_aborted) => {
                // Task was cancelled - don't record as failure, it was intentional
                Err(BallistaError::Cancelled)
            }
        }
    }

    /// Cancels a running task by aborting its execution.
    ///
    /// Returns `Ok(true)` if the task was found and cancelled, `Ok(false)` if not found.
    pub async fn cancel_task(
        &self,
        task_id: usize,
        job_id: JobId,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<bool, BallistaError> {
        if let Some((_, handle)) = self.abort_handles.remove(&(
            task_id,
            PartitionId {
                job_id,
                stage_id,
                partition_id,
            },
        )) {
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
    use crate::execution_engine::{DefaultQueryStageExec, ShuffleWriterVariant};
    use crate::executor::Executor;
    use crate::runtime_cache::{
        DefaultSessionRuntimeCache, MemoryPoolPolicy, SessionRuntimeCache,
    };
    use ballista_core::JobId;
    use ballista_core::RuntimeProducer;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::{
        ExecutorOperatingSystemSpecification, ExecutorRegistration,
    };
    use ballista_core::serde::scheduler::PartitionId;
    use ballista_core::utils::default_config_producer;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionConfig;

    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    };
    use datafusion::prelude::SessionContext;
    use futures::Stream;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tempfile::TempDir;

    /// A RecordBatchStream that will never terminate
    struct NeverendingRecordBatchStream;

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

        fn partition_statistics(
            &self,
            _partition: Option<usize>,
        ) -> Result<Arc<Statistics>> {
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        }
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();

        let job_id = JobId::new("job-id");
        let cancel_job_id = job_id.clone();
        let shuffle_write = ShuffleWriterExec::try_new(
            job_id.clone(),
            1,
            Arc::new(NeverendingOperator::new()),
            work_dir.clone(),
            None,
        )
        .expect("creating shuffle writer");

        let query_stage_exec =
            DefaultQueryStageExec::new(ShuffleWriterVariant::Hash(shuffle_write));

        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            port: 0,
            grpc_port: 0,
            specification: None,
            host: None,
            os_info: Some(ExecutorOperatingSystemSpecification::default()),
        };
        let config_producer = Arc::new(default_config_producer);
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));

        let executor = Executor::new_basic(
            executor_registration,
            &work_dir,
            runtime_producer,
            config_producer,
            2,
        );

        let (sender, receiver) = tokio::sync::oneshot::channel();

        // Spawn our non-terminating task on a separate fiber.
        let executor_clone = executor.clone();
        tokio::task::spawn(async move {
            let part = PartitionId {
                job_id: job_id.clone(),
                stage_id: 1,
                partition_id: 0,
            };
            let task_result = executor_clone
                .execute_query_stage(1, part, Arc::new(query_stage_exec), ctx.task_ctx())
                .await;
            sender.send(task_result).expect("sending result");
        });

        // Now cancel the task. We can only cancel once the task has been executed and has an `AbortHandle` registered, so
        // poll until that happens.
        for _ in 0..20 {
            if executor
                .cancel_task(1, cancel_job_id.clone(), 1, 0)
                .await
                .expect("cancelling task")
            {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // Wait for our task to complete
        let result = tokio::time::timeout(Duration::from_secs(5), receiver).await;

        // Make sure the task didn't timeout
        assert!(result.is_ok());

        // Make sure the actual task failed
        let inner_result = result.unwrap().unwrap();
        assert!(inner_result.is_err());
    }

    #[test]
    fn produce_runtime_for_session_shares_read_side_state() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            port: 0,
            grpc_port: 0,
            specification: None,
            host: None,
            os_info: Some(ExecutorOperatingSystemSpecification::default()),
        };
        let config_producer = Arc::new(default_config_producer);

        // A base producer that builds a fresh env each call, plus an identity
        // pool policy, so shared read-side state is observable via ptr equality.
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        let identity: MemoryPoolPolicy = Arc::new(|base, _| Ok(base));
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
        let e1 = executor.produce_runtime_for_session("s1", &cfg).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg).unwrap();
        let e3 = executor.produce_runtime_for_session("s2", &cfg).unwrap();

        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e3.cache_manager));
    }

    #[test]
    fn produce_runtime_for_session_falls_back_without_cache() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            port: 0,
            grpc_port: 0,
            specification: None,
            host: None,
            os_info: Some(ExecutorOperatingSystemSpecification::default()),
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
        let e1 = executor.produce_runtime_for_session("s1", &cfg).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }
}
