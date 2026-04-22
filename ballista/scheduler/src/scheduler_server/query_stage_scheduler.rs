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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ballista_core::serde::protobuf::{FailedJob, JobStatus};
use log::{error, info, trace, warn};
use tokio::sync::broadcast;

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};
use tokio::sync::mpsc::error::TrySendError;

use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::job_state_event::JobStateEvent;
use crate::scheduler_server::timestamp_millis;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    config: Arc<SchedulerConfig>,
    /// Broadcast sender for job state change notifications.
    job_state_sender: broadcast::Sender<JobStateEvent>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        config: Arc<SchedulerConfig>,
        job_state_sender: broadcast::Sender<JobStateEvent>,
    ) -> Self {
        Self {
            state,
            metrics_collector,
            config,
            job_state_sender,
        }
    }

    /// Broadcasts a job state event to all subscribers.
    fn broadcast_job_state(&self, event: JobStateEvent) {
        // Ignore send errors - no receivers is a valid state
        let _ = self.job_state_sender.send(event);
    }

    #[cfg(feature = "rest-api")]
    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.metrics_collector.as_ref()
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: &mpsc::Sender<QueryStageSchedulerEvent>,
        _rx_event: &mpsc::Receiver<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let mut time_recorder = None;
        if self.config.scheduler_event_expected_processing_duration > 0 {
            time_recorder = Some((Instant::now(), event.clone()));
        };
        let event_sender = EventSender::new(tx_event.clone());
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                job_name,
                session_ctx,
                plan,
                queued_at,
                subscriber,
            } => {
                info!("Job {job_id} queued with name {job_name:?}");

                // Broadcast job queued state
                self.broadcast_job_state(JobStateEvent::queued(&job_id));

                if let Err(e) = self
                    .state
                    .task_manager
                    .queue_job(&job_id, &job_name, queued_at)
                {
                    error!("Fail to queue job {job_id} due to {e:?}");
                    return Ok(());
                }

                let state = self.state.clone();

                // Clone the job state sender to move into the async task
                let job_state_sender = self.job_state_sender.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(
                            &job_id,
                            &job_name,
                            session_ctx,
                            &plan,
                            queued_at,
                            subscriber.clone(),
                        )
                        .await
                    {
                        let error = e.to_string();
                        let fail_message = format!("Error planning job {job_id}: {e:?}");

                        // this is a corner case, as most of job status changes are handled in
                        // job state, after job is submitted to job state
                        if let Some(subscriber) = subscriber {
                            let timestamp = timestamp_millis();
                            let job_status = JobStatus {
                                job_id: job_id.clone(),
                                job_name,
                                status: Some(ballista_core::serde::protobuf::job_status::Status::Failed(
                                    FailedJob { error, queued_at, started_at: timestamp, ended_at: timestamp }
                                ))
                            };

                            if matches!(
                                subscriber.try_send(job_status),
                                Err(TrySendError::Full(_))
                            ) {
                                error!(
                                    "jobs notification subscriber for job {} is blocked, can't deliver status update, job notification will be missed",
                                    job_id
                                )
                            }
                        }

                        error!("{}", &fail_message);
                        QueryStageSchedulerEvent::JobPlanningFailed {
                            job_id,
                            fail_message,
                            queued_at,
                            failed_at: timestamp_millis(),
                        }
                    } else {
                        // Broadcast job running state when successfully submitted
                        let _ = job_state_sender.send(JobStateEvent::running(&job_id));
                        QueryStageSchedulerEvent::JobSubmitted {
                            job_id,
                            queued_at,
                            submitted_at: timestamp_millis(),
                        }
                    };
                    if let Err(e) = event_sender.post_event(event).await {
                        error!("Fail to send event due to {e}");
                    }
                });
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
            } => {
                self.metrics_collector
                    .record_submitted(&job_id, queued_at, submitted_at);

                info!("Job {job_id} submitted");

                if self.state.config.is_push_staged_scheduling() {
                    event_sender
                        .post_event(QueryStageSchedulerEvent::ReviveOffers)
                        .await?;
                }

                // Notify external systems that new work is available
                if let Some(ref callback) = self.config.on_work_available {
                    callback(&format!("job_submitted:{job_id}"));
                }
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {job_id} failed: {fail_message}");

                // Persist terminal status before broadcasting so subscribers
                // can immediately read the Failed status on receipt of the event.
                match self
                    .state
                    .task_manager
                    .fail_unscheduled_job(&job_id, fail_message.clone())
                    .await
                {
                    Ok(()) => {
                        self.broadcast_job_state(JobStateEvent::failed(
                            &job_id,
                            &fail_message,
                        ));
                    }
                    Err(e) => {
                        error!(
                            "Fail to invoke fail_unscheduled_job for job {job_id} due to {e:?}"
                        );
                    }
                }
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);

                info!("Job {job_id} success");

                // Persist terminal status BEFORE broadcasting completion so that
                // subscribers (e.g. SpiceAI's QueryHandle) who receive the
                // Completed event can immediately read the Successful status
                // without hitting a retry/polling loop.
                match self.state.task_manager.succeed_job(&job_id).await {
                    Ok(()) => {
                        // Broadcast job completed state after status is persisted
                        self.broadcast_job_state(JobStateEvent::completed(&job_id));
                        self.state.clean_up_successful_job(job_id);
                    }
                    Err(e) => {
                        error!(
                            "Fail to invoke succeed_job for job {job_id} due to {e:?}"
                        );
                    }
                }
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {job_id} running failed");

                // Persist terminal status before broadcasting so subscribers
                // can immediately read the Failed status on receipt of the event.
                match self
                    .state
                    .task_manager
                    .abort_job(&job_id, fail_message.clone())
                    .await
                {
                    Ok((running_tasks, _pending_tasks)) => {
                        // Broadcast job failed state immediately after status is persisted
                        self.broadcast_job_state(JobStateEvent::failed(
                            &job_id,
                            &fail_message,
                        ));
                        if !running_tasks.is_empty()
                            && let Err(e) = event_sender
                                .post_event(QueryStageSchedulerEvent::CancelTasks(
                                    running_tasks,
                                ))
                                .await
                        {
                            error!(
                                "Fail to post CancelTasks for job {job_id} due to {e:?}"
                            );
                        }
                    }
                    Err(e) => {
                        error!("Fail to invoke abort_job for job {job_id} due to {e:?}");
                    }
                }

                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!("Job {job_id} Updated");
                if let Err(e) = self.state.task_manager.update_job(&job_id).await {
                    error!("Fail to invoke update_job for job {job_id} due to {e:?}");
                }

                // After update_job revives newly resolved stages (Resolved → Running),
                // trigger scheduling so tasks for those stages are actually bound and
                // launched. Without this, newly Running stages could sit idle if the
                // preceding ReviveOffers consumed all slots before these stages were
                // resolved.
                if self.state.config.is_push_staged_scheduling() {
                    event_sender
                        .post_event(QueryStageSchedulerEvent::ReviveOffers)
                        .await?;
                }
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.metrics_collector.record_cancelled(&job_id);

                info!("Job {job_id} Cancelled");

                // Persist terminal status before broadcasting so subscribers
                // can immediately read the terminal status on receipt of the event.
                // Note: cancel_job routes to abort_job, persisting a Failed status.
                match self.state.task_manager.cancel_job(&job_id).await {
                    Ok((running_tasks, _pending_tasks)) => {
                        // Broadcast cancelled state immediately after status is persisted
                        self.broadcast_job_state(JobStateEvent::cancelled(&job_id));
                        if let Err(e) = event_sender
                            .post_event(QueryStageSchedulerEvent::CancelTasks(
                                running_tasks,
                            ))
                            .await
                        {
                            error!(
                                "Fail to post CancelTasks for job {job_id} due to {e:?}"
                            );
                        }
                    }
                    Err(e) => {
                        error!("Fail to invoke cancel_job for job {job_id} due to {e:?}");
                    }
                }

                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                trace!(
                    "processing task status updates from {executor_id}: {tasks_status:?}"
                );

                let num_status = tasks_status.len();
                if self.state.config.is_push_staged_scheduling() {
                    self.state
                        .executor_manager
                        .unbind_tasks(vec![(executor_id.clone(), num_status as u32)])
                        .await?;
                }
                match self
                    .state
                    .update_task_statuses(&executor_id, tasks_status)
                    .await
                {
                    Ok(stage_events) => {
                        if !stage_events.is_empty() {
                            info!(
                                "TaskUpdating from executor {executor_id}: {num_status} tasks processed, \
                                 {} stage events emitted: {:?}",
                                stage_events.len(),
                                stage_events
                                    .iter()
                                    .map(|e| format!("{e:?}"))
                                    .collect::<Vec<_>>()
                            );
                        }

                        if self.state.config.is_push_staged_scheduling() {
                            event_sender
                                .post_event(QueryStageSchedulerEvent::ReviveOffers)
                                .await?;
                        }

                        // Notify external systems when new stages become runnable
                        if !stage_events.is_empty()
                            && let Some(ref callback) = self.config.on_work_available
                        {
                            callback("tasks_completed:new_stages_runnable");
                        }

                        for stage_event in stage_events {
                            event_sender.post_event(stage_event).await?;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to update {num_status} task statuses for Executor {executor_id}: {e:?}"
                        );
                        // TODO error handling
                    }
                }
            }
            QueryStageSchedulerEvent::ReviveOffers => {
                trace!("Processing ReviveOffers event");
                self.state.revive_offers(event_sender).await?;
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, _) => {
                match self.state.task_manager.executor_lost(&executor_id).await {
                    Ok(tasks) => {
                        if !tasks.is_empty()
                            && let Err(e) = self
                                .state
                                .executor_manager
                                .cancel_running_tasks(tasks)
                                .await
                        {
                            warn!("Fail to cancel running tasks due to {e:?}");
                        }
                    }
                    Err(e) => {
                        let msg = format!(
                            "TaskManager error to handle Executor {executor_id} lost: {e}"
                        );
                        error!("{msg}");
                    }
                }

                // After executor_lost resets tasks (task_info → None), trigger
                // scheduling so those tasks can be re-bound to surviving executors.
                if self.state.config.is_push_staged_scheduling() {
                    event_sender
                        .post_event(QueryStageSchedulerEvent::ReviveOffers)
                        .await?;
                }
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                if let Err(e) = self
                    .state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await
                {
                    warn!("Fail to cancel running tasks due to {e:?}");
                }
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                self.state.executor_manager.clean_up_job_data(job_id);
            }
        }
        if let Some((start, ec)) = time_recorder {
            let duration = start.elapsed();
            if duration.ge(&Duration::from_micros(
                self.config.scheduler_event_expected_processing_duration,
            )) {
                warn!(
                    "[METRICS] {:?} event cost {:?} us!",
                    ec,
                    duration.as_micros()
                );
            }
        }

        // Update pending jobs queue size metric (this is a cheap O(1) operation)
        let pending_jobs = self.state.task_manager.pending_job_number();
        self.metrics_collector
            .set_pending_jobs_queue_size(pending_jobs as u64);

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {error:?}");
    }
}

#[cfg(test)]
mod tests {
    use crate::config::SchedulerConfig;
    use crate::test_utils::{SchedulerTest, TestMetricsCollector, await_condition};
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::Result;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlan, col};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_pending_job_metric() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            1,
            1,
            None,
        )
        .await?;

        let job_id = test.submit("", &plan).await?;

        test.tick().await?;

        let pending_jobs = test.pending_job_number();
        let expected = 0usize;
        assert_eq!(
            expected, pending_jobs,
            "Expected {expected} pending jobs but found {pending_jobs}"
        );

        let running_jobs = test.running_job_number();
        let expected = 1usize;
        assert_eq!(
            expected, running_jobs,
            "Expected {expected} running jobs but found {running_jobs}"
        );

        test.cancel(&job_id).await?;

        let expected = 0usize;
        let success = await_condition(Duration::from_millis(10), 20, || {
            let running_jobs = test.running_job_number();

            futures::future::ready(Ok(running_jobs == expected))
        })
        .await
        .unwrap();
        assert!(
            success,
            "Expected {} running jobs but found {}",
            expected,
            test.running_job_number()
        );

        Ok(())
    }

    fn test_plan(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), partitions)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_join_plan_logical(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let left_plan =
            scan_empty_with_partitions(Some("left"), &schema, None, partitions).unwrap();
        let right_plan =
            scan_empty_with_partitions(Some("right"), &schema, None, partitions)
                .unwrap()
                .build()
                .unwrap();

        left_plan
            .join(
                right_plan,
                datafusion::prelude::JoinType::Inner,
                (vec!["id"], vec!["id"]),
                None,
            )
            .unwrap()
            .aggregate(vec![col("left.id")], vec![sum(col("left.gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    /// Regression test: a multi-stage job (join) should complete end-to-end
    /// through the scheduler's push-based scheduling pipeline.
    ///
    /// This tests for a bug where jobs with dependent stages would hang
    /// after the leaf stages completed because newly resolved stages were
    /// never picked up for scheduling.
    #[tokio::test]
    async fn test_multi_stage_job_completes_push_scheduling() -> Result<()> {
        let config = SchedulerConfig::default()
            .with_scheduler_policy(TaskSchedulingPolicy::PushStaged);
        let metrics = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(config, metrics.clone(), 2, 4, None).await?;

        // Join plan creates multiple stages with dependencies
        let plan = test_join_plan_logical(4);

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            test.run("multi_stage_join", &plan),
        )
        .await;

        match result {
            Ok(Ok((status, _job_id))) => {
                assert!(
                    matches!(status.status, Some(ballista_core::serde::protobuf::job_status::Status::Successful(_))),
                    "Expected job to succeed but got: {:?}",
                    status.status
                );
            }
            Ok(Err(e)) => panic!("Job execution error: {e}"),
            Err(_) => panic!(
                "Job timed out after 30s - suspected scheduling deadlock where \
                 dependent stages are never picked up after leaf stages complete"
            ),
        }

        Ok(())
    }

    /// Test that a simple aggregation also completes via push scheduling.
    #[tokio::test]
    async fn test_aggregation_completes_push_scheduling() -> Result<()> {
        let config = SchedulerConfig::default()
            .with_scheduler_policy(TaskSchedulingPolicy::PushStaged);
        let metrics = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(config, metrics, 2, 4, None).await?;

        let plan = test_plan(4);

        let result =
            tokio::time::timeout(Duration::from_secs(30), test.run("agg_test", &plan))
                .await;

        match result {
            Ok(Ok((status, _job_id))) => {
                assert!(
                    matches!(status.status, Some(ballista_core::serde::protobuf::job_status::Status::Successful(_))),
                    "Expected job to succeed but got: {:?}",
                    status.status
                );
            }
            Ok(Err(e)) => panic!("Job execution error: {e}"),
            Err(_) => panic!("Aggregation job timed out - scheduling deadlock"),
        }

        Ok(())
    }
}
