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

//! Pull-based task execution loop for the executor.
//!
//! This module implements the polling mechanism where executors actively
//! request work from the scheduler, as opposed to push-based scheduling
//! where the scheduler sends tasks to executors.

use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::executor_process::remove_job_dir;

use crate::{TaskExecutionTimes, as_task_status};

use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;

use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigHelperExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    PollWorkParams, PollWorkResult, TaskDefinition, TaskStatus,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::scheduler::{ExecutorSpecification, PartitionId};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::FutureExt;
use log::{debug, error, info, warn};
use std::any::Any;
use std::cell::LazyCell;
use std::convert::TryInto;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tonic::codegen::{Body, Bytes, StdError};

// Maximum time to wait for a free task slot before sending poll_work anyway.
// In pull-based scheduling, poll_work also serves as the heartbeat to the
// scheduler. If we block indefinitely waiting for a free slot, the scheduler
// will declare this executor dead after executor_timeout_seconds.
const SLOT_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

/// Main execution loop that polls the scheduler for available tasks.
///
/// This function runs indefinitely, periodically asking the scheduler for
/// work. When tasks are received, they are executed on a dedicated thread
/// pool and results are reported back to the scheduler.
///
/// The loop respects the executor's concurrent task limit via a semaphore,
/// ensuring no more than the configured number of tasks run simultaneously.
/// Number of consecutive failures before reducing log level from WARN to DEBUG.
const QUIET_AFTER_FAILURES: u32 = 5;

/// Main polling loop for executor task execution.
///
/// This function polls the scheduler for new tasks to execute and runs them,
/// ensuring no more than the configured number of tasks run simultaneously.
///
/// # Arguments
///
/// * `scheduler` - gRPC client for communicating with the scheduler
/// * `executor` - The executor instance that runs tasks
/// * `codec` - Codec for serializing/deserializing plans
/// * `readiness` - Optional channel to signal when the executor is ready
/// * `poll_now_notify` - Optional notify to wake the poll loop immediately when new work is available
/// * `available_task_slots` - Optional semaphore for controlling task concurrency. If None, creates one internally.
pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan, C>(
    scheduler: SchedulerGrpcClient<C>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    readiness: Option<OneShotSender<String>>,
    poll_now_notify: Option<Arc<Notify>>,
    available_task_slots: Option<Arc<Semaphore>>,
) -> Result<(), BallistaError>
where
    C: tonic::client::GrpcService<tonic::body::Body>,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    let executor_specification: ExecutorSpecification = executor
        .metadata
        .specification
        .as_ref()
        .unwrap()
        .clone()
        .into();
    let available_task_slots = available_task_slots.unwrap_or_else(|| {
        Arc::new(Semaphore::new(executor_specification.task_slots as usize))
    });

    poll_loop_with_slots(
        scheduler,
        executor,
        codec,
        available_task_slots,
        readiness,
        poll_now_notify,
    )
    .await
}

async fn poll_loop_with_slots<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
    C,
>(
    mut scheduler: SchedulerGrpcClient<C>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    available_task_slots: Arc<Semaphore>,
    readiness: Option<OneShotSender<String>>,
    poll_now_notify: Option<Arc<Notify>>,
) -> Result<(), BallistaError>
where
    C: tonic::client::GrpcService<tonic::body::Body>,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();
    info!("Starting poll work loop with scheduler");

    let dedicated_executor =
        DedicatedExecutor::new("task_runner", executor.concurrent_tasks);

    // Track consecutive scheduler connection failures for backoff and log suppression
    let mut consecutive_failures: u32 = 0;
    let mut backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(30),
        max_elapsed_time: None, // Never give up
        ..ExponentialBackoff::default()
    };

    let report_ready = LazyCell::new(|| {
        if let Some(chan) = readiness {
            chan.send(executor.metadata.id.clone())
                .expect("Must send readiness")
        }
    });

    loop {
        // Wait for task slots to be available, but don't block indefinitely.
        // We must call poll_work periodically even when all slots are busy to
        // maintain the heartbeat with the scheduler (poll_work updates executor
        // metadata/heartbeat on the scheduler side).
        match tokio::time::timeout(SLOT_WAIT_TIMEOUT, available_task_slots.acquire())
            .await
        {
            Ok(Ok(permit)) => drop(permit),
            Ok(Err(_)) => {
                // Semaphore closed - should not happen in normal operation
                warn!("Task slot semaphore closed unexpectedly");
                return Err(BallistaError::General(
                    "Task slot semaphore closed unexpectedly".to_string(),
                ));
            }
            Err(_) => {
                // Timeout: all task slots are busy. Continue to call poll_work
                // with 0 free slots so the scheduler updates our heartbeat.
                debug!("All task slots occupied, sending heartbeat-only poll_work");
            }
        }

        let task_status: Vec<TaskStatus> =
            sample_tasks_status(&mut task_status_receiver).await;

        let reported_task_statuses = task_status.len();
        let free_slots = available_task_slots.available_permits() as u32;
        let poll_started = Instant::now();
        let poll_work_result: Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor.metadata.clone()),
                    num_free_slots: free_slots,
                    task_status,
                })
                .await;

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let active_job;

        *report_ready;

        match poll_work_result {
            Ok(result) => {
                // Reset backoff state on successful connection
                if consecutive_failures > 0 {
                    info!(
                        "Scheduler connection restored after {consecutive_failures} failed attempts"
                    );
                }
                consecutive_failures = 0;
                backoff.reset();

                let PollWorkResult {
                    tasks,
                    jobs_to_clean,
                } = result.into_inner();
                active_job = !tasks.is_empty();

                if active_job || reported_task_statuses > 0 {
                    let task_summary = tasks
                        .iter()
                        .map(|task| {
                            format!(
                                "{}:{}.{}/{}.{},tid={}",
                                task.job_id,
                                task.stage_id,
                                task.stage_attempt_num,
                                task.partition_id,
                                task.task_attempt_num,
                                task.task_id
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    info!(
                        target: "ballista_debug",
                        "BALLISTA_DEBUG executor_poll_work executor_id={} poll_ms={} reported_task_statuses={} requested_free_slots={} assigned_tasks={} tasks={}",
                        executor.metadata.id,
                        poll_started.elapsed().as_millis(),
                        reported_task_statuses,
                        free_slots,
                        tasks.len(),
                        task_summary
                    );
                }

                // Clean up any state related to the listed jobs
                for cleanup in jobs_to_clean {
                    let job_id = cleanup.job_id.clone();
                    let work_dir = executor.work_dir.clone();

                    // In poll-based cleanup, removing job data is fire-and-forget.
                    // Failures here do not affect task execution and are only logged.
                    tokio::spawn(async move {
                        if let Err(e) = remove_job_dir(&work_dir, &job_id).await {
                            error!("failed to remove job dir {job_id}: {e}");
                        }
                    });
                }

                for task in tasks {
                    let task_status_sender = task_status_sender.clone();

                    // Acquire a permit/slot for the task
                    let permit =
                        available_task_slots.clone().acquire_owned().await.unwrap();

                    let start_exec_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    match run_received_task(
                        executor.clone(),
                        permit,
                        task_status_sender.clone(),
                        task.clone(),
                        &codec,
                        &dedicated_executor,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            //
                            // notifying scheduler about task failure
                            // as scheduler expects notification.
                            //

                            let partition_id = PartitionId {
                                job_id: task.job_id.clone(),
                                stage_id: task.stage_id as usize,
                                partition_id: task.partition_id as usize,
                            };

                            warn!(
                                "Executor failed to run task: {partition_id:?}, error: {e:?}"
                            );

                            let end_exec_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis()
                                as u64;

                            let task_execution_times = TaskExecutionTimes {
                                launch_time: task.launch_time,
                                start_exec_time,
                                end_exec_time,
                            };

                            // TODO: MM should we re-try message?
                            if let Err(error) = task_status_sender.send(as_task_status(
                                Err(e),
                                executor.metadata.id.clone(),
                                task.task_id as usize,
                                task.task_attempt_num as usize,
                                partition_id,
                                None,
                                task_execution_times,
                            )) {
                                warn!("failed to send task status: {error:?}");
                            };
                        }
                    }
                }
            }
            Err(error) => {
                warn!(
                    "Executor poll work loop failed. If this continues to happen the Scheduler might be marked as dead. Error: {error}"
                );

                consecutive_failures = consecutive_failures.saturating_add(1);

                // Log at WARN level for first few failures, then reduce to DEBUG to avoid log spam
                if consecutive_failures <= QUIET_AFTER_FAILURES {
                    warn!(
                        "Executor poll work loop failed (attempt {consecutive_failures}). If this continues, the scheduler might be unavailable. Error: {error}"
                    );
                } else {
                    debug!(
                        "Executor poll work loop failed (attempt {consecutive_failures}). Error: {error}"
                    );
                }

                // Apply exponential backoff before retrying
                if let Some(duration) = backoff.next_backoff() {
                    tokio::time::sleep(duration).await;
                }
                continue;
            }
        }

        if !active_job {
            // Wait for either the poll interval or a poll_now notification
            match &poll_now_notify {
                Some(notify) => {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        _ = notify.notified() => {
                            debug!("Received poll_now notification, polling immediately");
                        }
                    }
                }
                None => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

/// Tries to get meaningful description from panic-error.
pub(crate) fn any_to_string(any: &Box<dyn Any + Send>) -> String {
    if let Some(s) = any.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = any.downcast_ref::<String>() {
        s.clone()
    } else if let Some(error) = any.downcast_ref::<Box<dyn Error + Send>>() {
        error.to_string()
    } else {
        "Unknown error occurred".to_string()
    }
}

async fn run_received_task<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    executor: Arc<Executor>,
    permit: OwnedSemaphorePermit,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
    codec: &BallistaCodec<T, U>,
    dedicated_executor: &DedicatedExecutor,
) -> Result<(), BallistaError> {
    let task_id = task.task_id;
    let task_attempt_num = task.task_attempt_num;
    let job_id = task.job_id;
    let stage_id = task.stage_id;
    let stage_attempt_num = task.stage_attempt_num;
    let task_launch_time = task.launch_time;
    let partition_id = task.partition_id;
    let start_exec_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let task_identity = format!(
        "TID {task_id} {job_id}/{stage_id}.{stage_attempt_num}/{partition_id}.{task_attempt_num}"
    );
    info!(
        target: "ballista_debug",
        "BALLISTA_DEBUG executor_task_received executor_id={} job_id={} stage_id={} stage_attempt_num={} partition_id={} task_id={} task_attempt_num={}",
        executor.metadata.id,
        job_id,
        stage_id,
        stage_attempt_num,
        partition_id,
        task_id,
        task_attempt_num
    );

    log::trace!(
        "Received task: [{}], task_properties: {:?}",
        task_identity,
        task.props
    );
    let session_config = executor.produce_config();
    let session_config = session_config.update_from_key_value_pair(&task.props);

    let task_scalar_functions = executor.function_registry.scalar_functions.clone();
    let task_aggregate_functions = executor.function_registry.aggregate_functions.clone();
    let task_window_functions = executor.function_registry.window_functions.clone();

    let runtime = executor.produce_runtime(&session_config)?;

    let session_id = task.session_id.clone();
    let task_context = Arc::new(TaskContext::new(
        Some(task_identity.clone()),
        session_id,
        session_config,
        task_scalar_functions,
        task_aggregate_functions,
        task_window_functions,
        runtime.clone(),
    ));

    let decode_started = Instant::now();
    let plan: Arc<dyn ExecutionPlan> =
        U::try_decode(task.plan.as_slice()).and_then(|proto| {
            proto.try_into_physical_plan(&task_context, codec.physical_extension_codec())
        })?;
    let plan_name = plan.name().to_string();
    let plan_partitions = plan.properties().output_partitioning().partition_count();
    let decode_ms = decode_started.elapsed().as_millis();

    let create_stage_started = Instant::now();
    let query_stage_exec = executor.execution_engine.create_query_stage_exec(
        job_id.clone(),
        stage_id as usize,
        plan,
        &executor.work_dir,
        task_context.session_config().options(),
    )?;
    let create_stage_ms = create_stage_started.elapsed().as_millis();
    info!(
        target: "ballista_debug",
        "BALLISTA_DEBUG executor_task_plan_ready executor_id={} job_id={} stage_id={} stage_attempt_num={} partition_id={} task_id={} plan_name={} plan_partitions={} decode_ms={} create_stage_ms={}",
        executor.metadata.id,
        job_id,
        stage_id,
        stage_attempt_num,
        partition_id,
        task_id,
        plan_name,
        plan_partitions,
        decode_ms,
        create_stage_ms
    );
    dedicated_executor.spawn(async move {
        use std::panic::AssertUnwindSafe;
        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id: stage_id as usize,
            partition_id: partition_id as usize,
        };

        let execution_started = Instant::now();
        let execution_result = match AssertUnwindSafe(executor.execute_query_stage(
            task_id as usize,
            part.clone(),
            query_stage_exec.clone(),
            task_context,
        ))
        .catch_unwind()
        .await
        {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(r)) => Err(r),
            Err(r) => {
                error!("Error executing task: {:?}", any_to_string(&r));
                Err(BallistaError::Internal(format!("{:#?}", any_to_string(&r))))
            }
        };

        let execution_ms = execution_started.elapsed().as_millis();
        let execution_ok = execution_result.is_ok();
        info!(
            target: "ballista_debug",
            "BALLISTA_DEBUG executor_task_done executor_id={} job_id={} stage_id={} stage_attempt_num={} partition_id={} task_id={} task_attempt_num={} execution_ms={} execution_ok={}",
            executor.metadata.id,
            job_id,
            stage_id,
            stage_attempt_num,
            partition_id,
            task_id,
            task_attempt_num,
            execution_ms,
            execution_ok
        );
        debug!("Statistics: {execution_result:?}");

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
            .ok();

        let end_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let task_execution_times = TaskExecutionTimes {
            launch_time: task_launch_time,
            start_exec_time,
            end_exec_time,
        };

        let _ = task_status_sender.send(as_task_status(
            execution_result,
            executor.metadata.id.clone(),
            task_id as usize,
            stage_attempt_num as usize,
            part,
            operator_metrics,
            task_execution_times,
        ));

        // Release the permit after the work is done
        drop(permit);
    });

    Ok(())
}

async fn sample_tasks_status(
    task_status_receiver: &mut Receiver<TaskStatus>,
) -> Vec<TaskStatus> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        match task_status_receiver.try_recv() {
            Result::Ok(status) => {
                task_status.push(status);
            }
            Err(TryRecvError::Empty) => {
                break;
            }
            Err(TryRecvError::Disconnected) => {
                error!("Task statuses channel disconnected");
            }
        }
    }

    task_status
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::metrics::LoggingMetricsCollector;
    use ballista_core::RuntimeProducer;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::scheduler_grpc_server::{
        SchedulerGrpc, SchedulerGrpcServer,
    };
    use ballista_core::serde::protobuf::{
        CancelJobParams, CancelJobResult, CleanJobDataParams, CleanJobDataResult,
        CreateUpdateSessionParams, CreateUpdateSessionResult, ExecuteQueryParams,
        ExecuteQueryResult, ExecutorRegistration, ExecutorStoppedParams,
        ExecutorStoppedResult, GetCatalogParams, GetCatalogResult, GetJobMetricsParams,
        GetJobMetricsResult, GetJobStatusParams, GetJobStatusResult,
        GetRemoteFunctionsParams, GetRemoteFunctionsResult, HeartBeatParams,
        HeartBeatResult, RegisterExecutorParams, RegisterExecutorResult,
        RemoveSessionParams, RemoveSessionResult, UpdateTaskStatusParams,
        UpdateTaskStatusResult,
    };
    use ballista_core::utils::default_config_producer;
    use datafusion::execution::context::SessionContext;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tonic::{Request, Response, Status};

    /// A mock scheduler that counts poll_work calls and always returns no tasks.
    struct MockScheduler {
        poll_work_count: Arc<AtomicU32>,
    }

    #[tonic::async_trait]
    impl SchedulerGrpc for MockScheduler {
        async fn poll_work(
            &self,
            _request: Request<PollWorkParams>,
        ) -> Result<Response<PollWorkResult>, Status> {
            self.poll_work_count.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(PollWorkResult {
                tasks: vec![],
                jobs_to_clean: vec![],
            }))
        }

        async fn register_executor(
            &self,
            _request: Request<RegisterExecutorParams>,
        ) -> Result<Response<RegisterExecutorResult>, Status> {
            Ok(Response::new(RegisterExecutorResult { success: true }))
        }

        async fn heart_beat_from_executor(
            &self,
            _request: Request<HeartBeatParams>,
        ) -> Result<Response<HeartBeatResult>, Status> {
            Ok(Response::new(HeartBeatResult { reregister: false }))
        }

        async fn update_task_status(
            &self,
            _request: Request<UpdateTaskStatusParams>,
        ) -> Result<Response<UpdateTaskStatusResult>, Status> {
            Ok(Response::new(UpdateTaskStatusResult { success: true }))
        }

        async fn create_update_session(
            &self,
            _request: Request<CreateUpdateSessionParams>,
        ) -> Result<Response<CreateUpdateSessionResult>, Status> {
            Ok(Response::new(CreateUpdateSessionResult {
                session_id: String::new(),
            }))
        }

        async fn remove_session(
            &self,
            _request: Request<RemoveSessionParams>,
        ) -> Result<Response<RemoveSessionResult>, Status> {
            Ok(Response::new(RemoveSessionResult { success: true }))
        }

        async fn execute_query(
            &self,
            _request: Request<ExecuteQueryParams>,
        ) -> Result<Response<ExecuteQueryResult>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn get_job_status(
            &self,
            _request: Request<GetJobStatusParams>,
        ) -> Result<Response<GetJobStatusResult>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn get_job_metrics(
            &self,
            _request: Request<GetJobMetricsParams>,
        ) -> Result<Response<GetJobMetricsResult>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn executor_stopped(
            &self,
            _request: Request<ExecutorStoppedParams>,
        ) -> Result<Response<ExecutorStoppedResult>, Status> {
            Ok(Response::new(ExecutorStoppedResult {}))
        }

        async fn cancel_job(
            &self,
            _request: Request<CancelJobParams>,
        ) -> Result<Response<CancelJobResult>, Status> {
            Ok(Response::new(CancelJobResult { cancelled: true }))
        }

        async fn clean_job_data(
            &self,
            _request: Request<CleanJobDataParams>,
        ) -> Result<Response<CleanJobDataResult>, Status> {
            Ok(Response::new(CleanJobDataResult {}))
        }

        type ExecuteQueryPushStream =
            tokio_stream::wrappers::ReceiverStream<Result<GetJobStatusResult, Status>>;

        async fn execute_query_push(
            &self,
            _request: Request<ExecuteQueryParams>,
        ) -> Result<Response<Self::ExecuteQueryPushStream>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn get_catalog(
            &self,
            _request: Request<GetCatalogParams>,
        ) -> Result<Response<GetCatalogResult>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn get_remote_functions(
            &self,
            _request: Request<GetRemoteFunctionsParams>,
        ) -> Result<Response<GetRemoteFunctionsResult>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
    }

    /// Start a mock scheduler gRPC server on an ephemeral port and return
    /// the address and the poll_work call counter.
    async fn start_mock_scheduler() -> (SocketAddr, Arc<AtomicU32>) {
        let poll_work_count = Arc::new(AtomicU32::new(0));
        let svc = MockScheduler {
            poll_work_count: poll_work_count.clone(),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(SchedulerGrpcServer::new(svc))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, poll_work_count)
    }

    fn make_test_executor(task_slots: usize) -> Arc<Executor> {
        let work_dir = tempfile::TempDir::new()
            .unwrap()
            .keep()
            .to_str()
            .unwrap()
            .to_string();
        let metadata = ExecutorRegistration {
            id: "test-executor".to_string(),
            port: 0,
            grpc_port: 0,
            specification: Some(
                ballista_core::serde::protobuf::ExecutorSpecification {
                    resources: vec![
                        ballista_core::serde::protobuf::ExecutorResource {
                            resource: Some(
                                ballista_core::serde::protobuf::executor_resource::Resource::TaskSlots(task_slots as u32),
                            ),
                        },
                    ],
                },
            ),
            host: None,
        };
        let config_producer = Arc::new(default_config_producer);
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));
        Arc::new(Executor::new(
            metadata,
            &work_dir,
            runtime_producer,
            config_producer,
            Default::default(),
            Arc::new(LoggingMetricsCollector::default()),
            task_slots,
            None,
        ))
    }

    /// Regression test: In pull-based scheduling, poll_work is the heartbeat
    /// mechanism. When all task slots are occupied by running queries, the
    /// executor must still call poll_work periodically so the scheduler
    /// updates the heartbeat timestamp. Otherwise the scheduler declares
    /// the executor dead after executor_timeout_seconds.
    ///
    /// Before the fix, poll_loop blocked indefinitely at:
    ///   let permit = available_task_slots.acquire().await.unwrap();
    /// This starved all subsequent poll_work calls (heartbeats).
    #[tokio::test]
    async fn test_poll_loop_sends_heartbeat_when_all_slots_occupied() {
        let task_slots = 2usize;
        let (addr, poll_work_count) = start_mock_scheduler().await;

        let executor = make_test_executor(task_slots);
        let codec: BallistaCodec = BallistaCodec::default();
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let scheduler_client = SchedulerGrpcClient::new(channel);

        // Create the semaphore externally so we can hold all permits,
        // simulating all task slots being occupied by running queries.
        let available_task_slots = Arc::new(Semaphore::new(task_slots));
        let _held_permits: Vec<_> = {
            let mut permits = Vec::new();
            for _ in 0..task_slots {
                permits.push(available_task_slots.clone().acquire_owned().await.unwrap());
            }
            permits
        };
        assert_eq!(available_task_slots.available_permits(), 0);

        // Start poll_loop with all slots held — before the fix this would
        // block forever and poll_work (the heartbeat) would never be called.
        let handle = tokio::spawn(poll_loop_with_slots(
            scheduler_client,
            executor.clone(),
            codec,
            available_task_slots.clone(),
            None,
            None,
        ));

        // Wait long enough for the SLOT_WAIT_TIMEOUT (15s) to fire at least
        // once, plus margin. We use 20s to be safe. With the old code,
        // poll_work_count would remain 0 forever.
        tokio::time::sleep(Duration::from_secs(20)).await;
        let count = poll_work_count.load(Ordering::SeqCst);
        assert!(
            count > 0,
            "poll_work must be called even when all task slots are occupied \
             (heartbeat must not be starved). Got {count} calls in 20s."
        );

        handle.abort();
    }
}
