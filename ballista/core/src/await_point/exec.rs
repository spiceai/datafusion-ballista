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

use super::permit::{Resume, VcorePermit};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;
use tokio::sync::{AcquireError, OwnedSemaphorePermit};

/// An await point: a pass-through node whose input's `Pending` suspends the
/// task, so its vcore can be released while the input waits.
///
/// At execution time it looks for a [`VcorePermit`] in the task's
/// `SessionConfig`. Without one (plain `DataFusion`, push-based Ballista
/// executors) the input stream is returned unchanged. With one, every
/// `Pending` that is neither a self-wake nor a coop-budget yield suspends the
/// task, and after a suspension the stream acquires the vcore again, if it was
/// released, before polling its input.
#[derive(Debug)]
pub struct AwaitPointExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl AwaitPointExec {
    /// Wrap `input`.
    #[must_use]
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The wrapped input.
    #[must_use]
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for AwaitPointExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "AwaitPointExec")
    }
}

impl ExecutionPlan for AwaitPointExec {
    fn name(&self) -> &'static str {
        "AwaitPointExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let (Some(input), true) = (children.pop(), children.is_empty()) {
            Ok(Arc::new(Self::new(input)))
        } else {
            internal_err!("AwaitPointExec expects exactly one child")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let permit = VcorePermit::from_task_context(&context);
        let input = self.input.execute(partition, context)?;
        let Some(permit) = permit else {
            return Ok(input);
        };
        let metrics = AwaitMetrics::new(&self.metrics, partition);
        Ok(Box::pin(AwaitPointStream::new(input, permit, metrics)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct AwaitMetrics {
    /// Time spent inside the input's `poll_next`.
    poll_time: Time,
    /// Time between a suspension and the next poll.
    suspended_time: Time,
    /// Time waiting to get a released vcore back.
    resume_wait_time: Time,
    suspensions: Count,
    yields: Count,
}

impl AwaitMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            poll_time: MetricBuilder::new(set).subset_time("input_poll_time", partition),
            suspended_time: MetricBuilder::new(set)
                .subset_time("suspended_time", partition),
            resume_wait_time: MetricBuilder::new(set)
                .subset_time("resume_wait_time", partition),
            suspensions: MetricBuilder::new(set).counter("suspensions", partition),
            yields: MetricBuilder::new(set).counter("yields", partition),
        }
    }
}

/// Forwards wake-ups to the task's waker and records that one happened during
/// the input's poll, so a `Pending` that already scheduled its own re-poll is
/// not mistaken for waiting on I/O. Tokio defers the wake-up of coop-budget
/// yields until the task's poll returns, so those are detected separately via
/// `has_budget_remaining`.
struct FlagWaker {
    woken: AtomicBool,
    inner: Mutex<Waker>,
}

impl FlagWaker {
    fn inner(&self) -> std::sync::MutexGuard<'_, Waker> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.inner().wake_by_ref();
    }
}

enum Phase {
    /// Not polled yet.
    New,
    /// Runnable: the input is being polled or returned a batch.
    Running,
    /// The input returned `Pending` without waking itself.
    Suspended(Instant),
    /// Waiting for a vcore before polling the input again.
    Acquiring(
        BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>,
        Instant,
    ),
    Done,
}

struct AwaitPointStream {
    input: SendableRecordBatchStream,
    permit: Arc<VcorePermit>,
    phase: Phase,
    flag: Arc<FlagWaker>,
    waker: Waker,
    metrics: AwaitMetrics,
}

impl AwaitPointStream {
    fn new(
        input: SendableRecordBatchStream,
        permit: Arc<VcorePermit>,
        metrics: AwaitMetrics,
    ) -> Self {
        let flag = Arc::new(FlagWaker {
            woken: AtomicBool::new(false),
            inner: Mutex::new(Waker::noop().clone()),
        });
        let waker = Waker::from(Arc::clone(&flag));
        Self {
            input,
            permit,
            phase: Phase::New,
            flag,
            waker,
            metrics,
        }
    }

    /// Move to `Running`, or to `Reacquiring` if the vcore was lent.
    fn resume(&mut self) {
        let resume = match std::mem::replace(&mut self.phase, Phase::Running) {
            Phase::New => self.permit.start(),
            Phase::Suspended(since) => {
                self.metrics.suspended_time.add_duration(since.elapsed());
                self.permit.resume()
            }
            other => {
                self.phase = other;
                return;
            }
        };
        if resume == Resume::Acquire {
            let acquire = self.permit.semaphore().acquire_owned().boxed();
            self.phase = Phase::Acquiring(acquire, Instant::now());
        }
    }

    fn poll_acquire(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Phase::Acquiring(acquire, since) = &mut self.phase else {
            return Poll::Ready(Ok(()));
        };
        let permit = match acquire.poll_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(permit)) => permit,
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Err(DataFusionError::External(Box::new(e))));
            }
        };
        let waited = since.elapsed();
        self.metrics.resume_wait_time.add_duration(waited);
        drop(self.permit.install(permit, waited));
        self.phase = Phase::Running;
        Poll::Ready(Ok(()))
    }

    fn poll_input(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        {
            let mut inner = self.flag.inner();
            if !inner.will_wake(cx.waker()) {
                inner.clone_from(cx.waker());
            }
        }
        self.flag.woken.store(false, Ordering::Release);
        let start = Instant::now();
        let poll = self
            .input
            .as_mut()
            .poll_next(&mut Context::from_waker(&self.waker));
        self.metrics.poll_time.add_duration(start.elapsed());
        match &poll {
            Poll::Pending
                if self.flag.woken.load(Ordering::Acquire)
                    || !tokio::task::coop::has_budget_remaining() =>
            {
                self.metrics.yields.add(1);
                self.permit.record_yield();
            }
            Poll::Pending => {
                self.metrics.suspensions.add(1);
                self.phase = Phase::Suspended(Instant::now());
                self.permit.suspend();
            }
            Poll::Ready(None) => {
                self.phase = Phase::Done;
                self.permit.finish();
            }
            Poll::Ready(Some(_)) => {}
        }
        poll
    }
}

impl Stream for AwaitPointStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if matches!(this.phase, Phase::Done) {
            return Poll::Ready(None);
        }
        this.resume();
        match this.poll_acquire(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(())) => {}
        }
        this.poll_input(cx)
    }
}

impl RecordBatchStream for AwaitPointStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Drop for AwaitPointStream {
    fn drop(&mut self) {
        match self.phase {
            Phase::Running | Phase::Acquiring(..) => self.permit.finish(),
            Phase::Suspended(_) => self.permit.drop_suspended(),
            Phase::New | Phase::Done => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionConfig;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    fn ctx(permit: &Arc<VcorePermit>) -> Arc<TaskContext> {
        Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new().with_extension(Arc::clone(permit)),
            ),
        )
    }

    fn wrap(
        stream: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
        permit: &Arc<VcorePermit>,
    ) -> AwaitPointStream {
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        ));
        let set = ExecutionPlanMetricsSet::new();
        AwaitPointStream::new(input, Arc::clone(permit), AwaitMetrics::new(&set, 0))
    }

    async fn new_permit(threshold_ms: u64) -> (Arc<Semaphore>, Arc<VcorePermit>) {
        let sem = Arc::new(Semaphore::new(1));
        let permit =
            VcorePermit::new(Arc::clone(&sem), Duration::from_millis(threshold_ms));
        permit
            .acquire()
            .await
            .expect("fresh semaphore has a permit");
        (sem, permit)
    }

    #[tokio::test(start_paused = true)]
    async fn slow_input_releases_vcore_and_acquires_before_resuming() {
        let (sem, permit) = new_permit(10).await;
        let slow = futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(slow, &permit);
        let next = tokio::spawn(async move { stream.next().await.map(|r| r.is_ok()) });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !permit.is_held(),
            "vcore not released while input was suspended"
        );
        let borrowed = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("lent vcore is available to other tasks");

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!next.is_finished(), "stream resumed without a vcore");

        drop(borrowed);
        assert_eq!(next.await.expect("stream task"), Some(true));
        assert!(permit.is_held(), "vcore not acquired again");
    }

    #[tokio::test(start_paused = true)]
    async fn coop_budget_yields_do_not_suspend() {
        let (_sem, permit) = new_permit(0).await;
        // Consumes tokio coop budget per batch, like DataFusion's
        // `CooperativeStream`, so the task is forced to yield periodically.
        let yielding = futures::stream::iter(0..1000).then(|_| async {
            tokio::task::consume_budget().await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(yielding, &permit);
        let task = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                batch.expect("batch");
            }
            stream
        });
        let stream = task.await.expect("stream task");
        assert!(stream.metrics.yields.value() > 0, "no coop yields observed");
        assert_eq!(stream.metrics.suspensions.value(), 0);
        assert!(permit.is_held());
    }

    #[tokio::test(start_paused = true)]
    async fn yield_now_suspends_briefly_but_keeps_vcore() {
        let (_sem, permit) = new_permit(10).await;
        // `yield_now` wakes only after the task's poll returns and uses no
        // budget, so it looks like a suspension; `suspend_after` absorbs it.
        let yielding = futures::stream::iter(0..100).then(|_| async {
            tokio::task::yield_now().await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(yielding, &permit);
        while let Some(batch) = stream.next().await {
            batch.expect("batch");
        }
        assert_eq!(stream.metrics.suspensions.value(), 100);
        assert!(permit.is_held());
        assert_eq!(permit.stats().releases.load(Ordering::Relaxed), 0);
    }

    fn busy_for(d: Duration) {
        let start = std::time::Instant::now();
        while start.elapsed() < d {
            std::hint::black_box(0u64);
        }
    }

    /// A source that hands CPU work to another thread and awaits the result.
    fn offloaded_cpu_source() -> impl Stream<Item = Result<RecordBatch>> + Send + 'static
    {
        futures::stream::once(async {
            tokio::task::spawn_blocking(|| busy_for(Duration::from_millis(200)))
                .await
                .expect("blocking task");
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiting_on_offloaded_cpu_work_is_mistaken_for_io() {
        let (_sem, permit) = new_permit(10).await;
        let mut stream = wrap(offloaded_cpu_source(), &permit);
        stream.next().await.expect("batch").expect("ok");
        assert_eq!(
            permit.stats().releases.load(Ordering::Relaxed),
            1,
            "suspension cannot tell CPU work on another thread from I/O"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_io_inside_poll_is_invisible() {
        let (_sem, permit) = new_permit(10).await;
        // Blocking inside `poll_next` (a sync client, `std::fs`) never returns
        // `Pending`, so it looks like CPU work and the vcore stays held.
        let source = futures::stream::poll_fn(|_| {
            std::thread::sleep(Duration::from_millis(200));
            Poll::Ready(None::<Result<RecordBatch>>)
        });
        let mut stream = wrap(source, &permit);
        assert!(stream.next().await.is_none());
        assert_eq!(stream.metrics.suspensions.value(), 0);
        assert!(stream.metrics.poll_time.value() >= 200_000_000);
        assert_eq!(permit.stats().releases.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn without_permit_execute_returns_input_unchanged() {
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            Schema::empty(),
        )));
        let exec = AwaitPointExec::new(input);
        let stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        assert!(exec.metrics().expect("metrics").iter().next().is_none());
        drop(stream);
    }

    #[tokio::test(start_paused = true)]
    async fn execute_with_permit_registers_metrics() {
        let (_sem, permit) = new_permit(10).await;
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            Schema::empty(),
        )));
        let exec = AwaitPointExec::new(input);
        let mut stream = exec.execute(0, ctx(&permit)).expect("execute");
        while stream.next().await.is_some() {}
        assert!(exec.metrics().expect("metrics").iter().next().is_some());
    }
}
