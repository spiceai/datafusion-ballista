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
use crate::executor_process::remove_job_data;
use crate::{TaskCompletionExtras, TaskExecutionTimes, as_task_status};
use ballista_core::JobId;

use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;

use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigHelperExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    PollWorkParams, PollWorkResult, TaskDefinition, TaskStatus,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::scheduler::{ExecutorSpecification, TaskKey};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::FutureExt;
use log::{debug, error, info, trace, warn};
use std::any::Any;
use std::cell::LazyCell;
use std::convert::TryInto;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tonic::codegen::{Body, Bytes, StdError};

/// Idle sleep between polls when polling is the only way to learn of new work.
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Idle sleep when a `poll_now_notify` wake-up is wired and the timer is only
/// a fallback. The Spice fork keeps this equal to `IDLE_POLL_INTERVAL` so
/// wiring `poll_now_notify` never slows the idle poll cadence.
const NOTIFIED_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Number of consecutive failures before reducing log level from WARN to DEBUG.
const QUIET_AFTER_FAILURES: u32 = 5;

/// Maximum time the poll loop waits for a free vcore before polling the
/// scheduler anyway. `poll_work` doubles as the executor's heartbeat under
/// pull-based scheduling, so a fully-busy executor must keep polling (reporting
/// zero free vcores) or the scheduler times it out and resets its tasks. Kept
/// well below the scheduler's executor timeout.
const HEARTBEAT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Main execution loop that polls the scheduler for available tasks.
///
/// Runs indefinitely, periodically asking the scheduler for work. When tasks
/// are received they are executed concurrently and results are reported back
/// to the scheduler.
///
/// Concurrency is bounded by a semaphore. Pass `free_vcores` to supply your
/// own semaphore — useful for sharing a single concurrency limit across
/// multiple poll loops or for observing executor load from outside.
/// Pass `None` to have the loop create a semaphore sized to the executor's
/// configured vcore count.
///
/// `readiness`, when provided, receives the executor id once the first
/// `poll_work` call to the scheduler has been attempted, so an embedder can
/// wait for the executor to be wired up before submitting work.
///
/// `poll_now_notify`, when provided, wakes an idle poll loop immediately
/// (typically wired to the scheduler's `on_work_available` callback) instead
/// of waiting out the idle interval. A notification sent mid-poll is not
/// lost: `Notify` stores the permit and the next `notified().await` returns
/// immediately.
///
/// When the scheduler is unreachable the loop retries with exponential
/// backoff (100ms up to 30s) and, after `QUIET_AFTER_FAILURES` consecutive
/// failures, lowers the per-attempt log line from WARN to DEBUG.
///
/// **Shared semaphores**: when one semaphore is shared across loops that
/// connect to different schedulers, each scheduler independently sees the
/// current free capacity and may dispatch up to that many tasks. The semaphore
/// still caps total concurrent execution — tasks that cannot run immediately
/// wait for capacity — but both schedulers may over-commit relative to what
/// the semaphore can actually admit at once. This is intentional: the
/// semaphore acts as an execution throttle, not a reservation system.
///
/// **Semaphore sizing**: if the provided semaphore allows more concurrent
/// tasks than the executor's thread pool has threads, excess admitted tasks
/// will queue behind running ones. The caller is responsible for sizing the
/// semaphore appropriately for their thread pool.
///
/// # Panics
///
/// Panics on startup if `free_vcores` is a semaphore with zero permits,
/// which would cause the loop to deadlock immediately.
pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan, C>(
    mut scheduler: SchedulerGrpcClient<C>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    readiness: Option<OneShotSender<String>>,
    poll_now_notify: Option<Arc<Notify>>,
    free_vcores: Option<Arc<Semaphore>>,
    health: crate::health::ExecutorHealth,
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
    let free_vcores = free_vcores.unwrap_or_else(|| {
        Arc::new(Semaphore::new(executor_specification.vcores as usize))
    });
    // A caller may pass a semaphore with no permits yet and add them later:
    // Spice registers an executor that way and opens its vcores only once
    // object stores are bound. That cannot stall the loop, because it waits
    // for a permit only up to HEARTBEAT_POLL_INTERVAL and then polls anyway,
    // reporting `num_free_vcores: 0`; a closed semaphore ends it with an error.

    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();
    info!("Starting poll work loop with scheduler");

    // poll_loop runs on the executor's I/O runtime; register it so pooled
    // shuffle-client transport tasks are polled there instead of on the
    // CPU-saturated dedicated pool the reducer tasks run on.
    ballista_core::execution_plans::set_shuffle_transport_runtime(
        tokio::runtime::Handle::current(),
    );

    let dedicated_executor =
        DedicatedExecutor::new("task_runner", executor.task_runner_threads());

    let report_ready = LazyCell::new(|| {
        if let Some(chan) = readiness {
            chan.send(executor.metadata.id.clone())
                .expect("Must send readiness")
        }
    });

    // Track consecutive scheduler connection failures for backoff and log suppression
    let mut consecutive_failures: u32 = 0;
    let mut backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(30),
        max_elapsed_time: None, // Never give up
        ..ExponentialBackoff::default()
    };

    // Task statuses are drained from the channel and handed to poll_work; if that
    // call fails the batch would otherwise be lost. A dropped completion leaves
    // its stage unfinished on the scheduler, its downstream stages never resolve,
    // and the job wedges. Carry undelivered statuses here and re-send them until a
    // poll_work succeeds. The scheduler tolerates a completion reported more than
    // once, so at-least-once delivery is safe.
    let mut pending_status: Vec<TaskStatus> = Vec::new();

    loop {
        // Wait for a vcore permit before asking for new work, but cap the wait
        // so a fully-busy executor still polls the scheduler periodically.
        // `poll_work` is the executor's ONLY heartbeat under pull-based
        // scheduling (the scheduler records a heartbeat on every poll). If every
        // vcore is held by a task running longer than the scheduler's executor
        // timeout, blocking here indefinitely stops heartbeats, so the scheduler
        // wrongly marks this healthy-but-busy executor dead and resets its
        // in-flight tasks. On timeout we poll anyway below, reporting
        // `num_free_vcores: 0`, so liveness no longer depends on vcore
        // availability.
        match tokio::time::timeout(HEARTBEAT_POLL_INTERVAL, free_vcores.acquire()).await {
            // A vcore is free; release it so the bind below can claim it.
            Ok(Ok(permit)) => drop(permit),
            // Semaphore closed (executor shutting down).
            Ok(Err(_)) => {
                return Err(BallistaError::Internal(
                    "vcore semaphore closed".to_string(),
                ));
            }
            // No free vcore within the interval; poll anyway to stay alive.
            Err(_) => {}
        }

        // The scheduler binds tasks against the free vcores reported below, so
        // reserve exactly those: a slot shrink queued before the tasks arrive
        // cannot take them, and admitting a task never waits.
        let mut reservation = reserve_free_vcores(&free_vcores)?;
        let reported_free_vcores = reservation.num_permits();

        let mut task_status: Vec<TaskStatus> = std::mem::take(&mut pending_status);
        task_status.extend(sample_tasks_status(&mut task_status_receiver).await);

        let poll_work_result: Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor.metadata.clone()),
                    num_free_vcores: reported_free_vcores as u32,
                    task_status: task_status.clone(),
                })
                .await;

        *report_ready;

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let active_job;

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
                health.mark_heartbeat_ok();

                let PollWorkResult {
                    tasks,
                    jobs_to_clean,
                } = result.into_inner();
                active_job = !tasks.is_empty();

                // Clean up any state related to the listed jobs
                for cleanup in jobs_to_clean {
                    let job_id: JobId = cleanup.job_id.clone().into();
                    let work_dir = executor.work_dir.clone();
                    let remove_stage_ids = cleanup.remove_stage_ids.clone();

                    // In poll-based cleanup, removing job data is fire-and-forget.
                    // Failures here do not affect task execution and are only logged.
                    tokio::spawn(async move {
                        if let Err(e) =
                            remove_job_data(&work_dir, &job_id, &remove_stage_ids).await
                        {
                            error!("failed to remove job data {job_id}: {e}");
                        }
                    });
                }

                for task in tasks {
                    let task_status_sender = task_status_sender.clone();

                    let permit = admit(
                        &mut reservation,
                        task.vcores_consumed,
                        executor.guaranteed_task_slots(),
                    )?;

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

                            let task_key = TaskKey {
                                job_id: task.job_id.clone().into(),
                                stage_id: task.stage_id as usize,
                                task_id: task.task_id as usize,
                            };

                            warn!(
                                "Executor failed to run task: {task_key:?}, error: {e:?}"
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
                                task.task_attempt_num as usize,
                                task_key,
                                task_execution_times,
                                TaskCompletionExtras::default(),
                            )) {
                                warn!("failed to send task status: {error:?}");
                            };
                        }
                    }
                }
                // Return what the scheduler did not use before idling.
                drop(reservation);
            }
            Err(error) => {
                drop(reservation);
                // Preserve this poll's statuses so the next attempt re-delivers
                // them rather than losing the completions.
                pending_status = task_status;
                health.mark_heartbeat_failed();

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
            match &poll_now_notify {
                Some(notify) => {
                    tokio::select! {
                        () = tokio::time::sleep(NOTIFIED_IDLE_POLL_INTERVAL) => {}
                        () = notify.notified() => {
                            debug!("Received poll_now notification, polling immediately");
                        }
                    }
                }
                None => {
                    tokio::time::sleep(IDLE_POLL_INTERVAL).await;
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

/// Permits to charge a task: the vcores the scheduler charged it at bind time
/// (at least 1), clamped to `guaranteed`, the slots that are always available.
/// Waiting for more than the semaphore is guaranteed to hold could hang if it
/// shrinks meanwhile, so a task wider than `guaranteed` is under-charged.
fn task_permits(vcores_consumed: u32, guaranteed: usize) -> u32 {
    let guaranteed = u32::try_from(guaranteed).unwrap_or(u32::MAX).max(1);
    vcores_consumed.clamp(1, guaranteed)
}

/// Takes every free permit up front, so the poll can report exactly that many
/// free vcores and a revocation queued by the adaptive slot controller
/// afterwards cannot take them. Returns an empty reservation when none are free.
fn reserve_free_vcores(
    free_vcores: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, BallistaError> {
    loop {
        let free = u32::try_from(free_vcores.available_permits()).unwrap_or(u32::MAX);
        match free_vcores.clone().try_acquire_many_owned(free) {
            Ok(reservation) => return Ok(reservation),
            // Another taker won the permits between the read and the acquire.
            Err(TryAcquireError::NoPermits) => {}
            Err(TryAcquireError::Closed) => {
                return Err(BallistaError::Internal(
                    "vcore semaphore closed".to_string(),
                ));
            }
        }
    }
}

/// Splits a task's permits out of `reservation`, held until the task
/// completes. Charges as [`task_permits`], keeping the executor's reported free
/// vcores in step with the scheduler's accounting.
///
/// Never waits: if the reservation is short of the charge, which the scheduler
/// binding against the reported free vcores should prevent, the task gets what
/// remains, since waiting would stop the loop heartbeating and get a busy
/// executor declared dead. The shortfall is logged once.
fn admit(
    reservation: &mut OwnedSemaphorePermit,
    vcores_consumed: u32,
    guaranteed: usize,
) -> Result<OwnedSemaphorePermit, BallistaError> {
    static SHORTFALL_LOGGED: std::sync::Once = std::sync::Once::new();
    let charge = task_permits(vcores_consumed, guaranteed) as usize;
    let take = charge.min(reservation.num_permits());
    if take < charge {
        SHORTFALL_LOGGED.call_once(|| {
            warn!(
                "scheduler assigned a task needing {charge} vcores but only {take} were reported free, so the task runs with fewer reserved slots"
            );
        });
    }
    reservation.split(take).ok_or_else(|| {
        BallistaError::Internal("vcore reservation split failed".to_string())
    })
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
    let job_id: JobId = task.job_id.into();
    let stage_id = task.stage_id;
    let stage_attempt_num = task.stage_attempt_num;
    let task_launch_time = task.launch_time;
    let start_exec_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let task_identity = format!(
        "TID {job_id}/{stage_id}.{stage_attempt_num}/{task_id}.{task_attempt_num}"
    );
    info!("Received task: [{task_identity}]");

    trace!(
        "Received task: [{}], task_properties: {:?}",
        task_identity, task.props
    );
    let session_config = executor.produce_config();
    let session_config = session_config.update_from_key_value_pair(&task.props);

    let task_scalar_functions = executor.function_registry.scalar_functions.clone();
    let task_aggregate_functions = executor.function_registry.aggregate_functions.clone();
    let task_window_functions = executor.function_registry.window_functions.clone();
    let task_higher_order_functions =
        executor.function_registry.higher_order_functions.clone();

    let runtime = executor.produce_runtime_for_session(
        &task.session_id,
        &session_config,
        task.vcores_consumed,
    )?;

    let session_id = task.session_id.clone();
    let task_context = Arc::new(TaskContext::new(
        Some(task_identity.clone()),
        session_id,
        session_config,
        task_scalar_functions,
        task_higher_order_functions,
        task_aggregate_functions,
        task_window_functions,
        runtime.clone(),
    ));

    let plan: Arc<dyn ExecutionPlan> =
        U::try_decode(task.plan.as_slice()).and_then(|proto| {
            proto.try_into_physical_plan(&task_context, codec.physical_extension_codec())
        })?;

    let global_output_partition_ids: Vec<usize> = task
        .global_output_partition_ids
        .iter()
        .map(|p| *p as usize)
        .collect();

    let query_stage_exec = executor.execution_engine.create_query_stage_exec(
        job_id.clone(),
        stage_id as usize,
        task_id as usize,
        global_output_partition_ids,
        plan,
        &executor.work_dir,
        task_context.session_config(),
    )?;
    dedicated_executor.spawn(async move {
        use std::panic::AssertUnwindSafe;
        let key = TaskKey {
            job_id: job_id.clone(),
            stage_id: stage_id as usize,
            task_id: task_id as usize,
        };

        let task_start = Instant::now();
        let execution_result = match AssertUnwindSafe(executor.execute_query_stage(
            key.clone(),
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

        info!(
            "Finished task : [{task_identity}] in {:?}",
            task_start.elapsed()
        );
        debug!("Task statistics: [{task_identity}] {execution_result:?}");

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
            .ok();
        let runtime_stats = query_stage_exec.collect_runtime_stats_reports();
        let column_stats = query_stage_exec.collect_column_stats();
        // Collect only when the task otherwise succeeded: a failed task's
        // partial state is meaningless, and its own error is the useful one.
        // A collection failure fails the task — these are load-bearing for the
        // downstream stage's prefix merge, so continuing without them would
        // ship a wrong answer that nothing later detects.
        let (execution_result, window_state) = match execution_result {
            Ok(partitions) => match query_stage_exec.collect_window_state_reports() {
                Ok(reports) => (Ok(partitions), reports),
                Err(e) => (Err(e.into()), Vec::new()),
            },
            Err(e) => (Err(e), Vec::new()),
        };

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
            stage_attempt_num as usize,
            key,
            task_execution_times,
            TaskCompletionExtras {
                operator_metrics,
                runtime_stats,
                window_state,
                column_stats,
            },
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
                // Without the break this arm spins forever: try_recv keeps
                // returning Disconnected once all senders are dropped.
                error!("Task statuses channel disconnected");
                break;
            }
        }
    }

    task_status
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn task_permits_charges_bundled_partitions_within_the_guarantee() {
        assert_eq!(task_permits(1, 8), 1);
        assert_eq!(task_permits(4, 8), 4);
        assert_eq!(task_permits(0, 8), 1);
        assert_eq!(task_permits(12, 8), 8);
        assert_eq!(task_permits(3, 0), 1);
    }

    #[tokio::test]
    async fn multi_partition_charge_comes_out_of_the_reservation() {
        let sem = Arc::new(Semaphore::new(8));
        let mut reservation = reserve_free_vcores(&sem).expect("open");
        assert_eq!(reservation.num_permits(), 8);
        assert_eq!(sem.available_permits(), 0);
        let task = admit(&mut reservation, 4, 8).expect("split");
        assert_eq!(task.num_permits(), 4);
        // What the tasks did not use returns to the semaphore.
        drop(reservation);
        assert_eq!(sem.available_permits(), 4);
        drop(task);
        assert_eq!(sem.available_permits(), 8);
    }

    #[tokio::test]
    async fn reservation_covers_only_the_free_permits() {
        let sem = Arc::new(Semaphore::new(4));
        let _running = sem.clone().try_acquire_many_owned(3).expect("running");
        assert_eq!(reserve_free_vcores(&sem).expect("open").num_permits(), 1);
        let none = Arc::new(Semaphore::new(0));
        assert_eq!(reserve_free_vcores(&none).expect("open").num_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn short_reservation_never_blocks() {
        let sem = Arc::new(Semaphore::new(4));
        let mut reservation = reserve_free_vcores(&sem).expect("open");
        let first = admit(&mut reservation, 3, 4).expect("split");
        // One permit remains for a task charged 3; it gets that one, instantly.
        let second = admit(&mut reservation, 3, 4).expect("split");
        let third = admit(&mut reservation, 1, 4).expect("split");
        assert_eq!(
            (
                first.num_permits(),
                second.num_permits(),
                third.num_permits()
            ),
            (3, 1, 0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn admission_does_not_wait_behind_a_queued_revocation() {
        let sem = Arc::new(Semaphore::new(4));
        let _running = sem
            .clone()
            .try_acquire_many_owned(2)
            .expect("running tasks");
        let mut reservation = reserve_free_vcores(&sem).expect("open");
        let reported = reservation.num_permits();
        assert_eq!(reported, 2);
        // The controller queues a shrink that needs more than is free.
        let revoker = sem.clone();
        tokio::spawn(async move { revoker.acquire_many_owned(3).await });
        tokio::task::yield_now().await;
        let admit_all = async {
            for _ in 0..reported {
                admit(&mut reservation, 1, 4).expect("split").forget();
            }
        };
        tokio::time::timeout(Duration::from_secs(5), admit_all)
            .await
            .expect("admission must not wait on running tasks");
    }

    #[tokio::test(start_paused = true)]
    async fn charge_wider_than_a_shrunken_semaphore_does_not_hang() {
        // Capacity shrank to the guaranteed 2 slots; a 6-partition task is
        // charged 2 and admitted from the reservation.
        let sem = Arc::new(Semaphore::new(2));
        let mut reservation = reserve_free_vcores(&sem).expect("open");
        let permit = admit(&mut reservation, 6, 2).expect("split");
        assert_eq!(permit.num_permits(), 2);
        drop(permit);
        drop(reservation);
        assert_eq!(sem.available_permits(), 2);
    }
}
