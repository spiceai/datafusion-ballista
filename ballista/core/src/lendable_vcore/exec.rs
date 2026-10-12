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

use super::vcore::{LendableVcore, Resume};
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

/// Pass-through node that lets its input's task lend out its vcore while the
/// input is parked on I/O.
///
/// At execution time it looks for a [`LendableVcore`] in the task's
/// `SessionConfig`. Without one (plain `DataFusion`, push-based Ballista
/// executors) the input stream is returned unchanged. With one, the stream
/// reports every `Pending` that did not wake itself as "parked", and before
/// polling the input after a park it re-acquires the vcore if it was lent.
#[derive(Debug)]
pub struct LendableVcoreExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl LendableVcoreExec {
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

impl DisplayAs for LendableVcoreExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "LendableVcoreExec")
    }
}

impl ExecutionPlan for LendableVcoreExec {
    fn name(&self) -> &'static str {
        "LendableVcoreExec"
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
            internal_err!("LendableVcoreExec expects exactly one child")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let lease = LendableVcore::from_task_context(&context);
        let input = self.input.execute(partition, context)?;
        let Some(lease) = lease else {
            return Ok(input);
        };
        let metrics = LendMetrics::new(&self.metrics, partition);
        Ok(Box::pin(LendableVcoreStream::new(input, lease, metrics)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct LendMetrics {
    /// Time spent inside the input's `poll_next`.
    poll_time: Time,
    /// Time between a park and the next poll.
    parked_time: Time,
    /// Time waiting to get a lent vcore back.
    reacquire_time: Time,
    parks: Count,
    self_wakes: Count,
}

impl LendMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            poll_time: MetricBuilder::new(set).subset_time("input_poll_time", partition),
            parked_time: MetricBuilder::new(set).subset_time("pending_time", partition),
            reacquire_time: MetricBuilder::new(set)
                .subset_time("vcore_reacquire_time", partition),
            parks: MetricBuilder::new(set).counter("pending_parks", partition),
            self_wakes: MetricBuilder::new(set).counter("yields", partition),
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
    Parked(Instant),
    /// Waiting for a vcore before polling the input again.
    Reacquiring(
        BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>,
        Instant,
    ),
    Done,
}

struct LendableVcoreStream {
    input: SendableRecordBatchStream,
    lease: Arc<LendableVcore>,
    phase: Phase,
    flag: Arc<FlagWaker>,
    waker: Waker,
    metrics: LendMetrics,
}

impl LendableVcoreStream {
    fn new(
        input: SendableRecordBatchStream,
        lease: Arc<LendableVcore>,
        metrics: LendMetrics,
    ) -> Self {
        let flag = Arc::new(FlagWaker {
            woken: AtomicBool::new(false),
            inner: Mutex::new(Waker::noop().clone()),
        });
        let waker = Waker::from(Arc::clone(&flag));
        Self {
            input,
            lease,
            phase: Phase::New,
            flag,
            waker,
            metrics,
        }
    }

    /// Move to `Running`, or to `Reacquiring` if the vcore was lent.
    fn resume(&mut self) {
        let resume = match std::mem::replace(&mut self.phase, Phase::Running) {
            Phase::New => self.lease.stream_started(),
            Phase::Parked(since) => {
                self.metrics.parked_time.add_duration(since.elapsed());
                self.lease.stream_resumed()
            }
            other => {
                self.phase = other;
                return;
            }
        };
        if resume == Resume::Reacquire {
            let acquire = self.lease.semaphore().acquire_owned().boxed();
            self.phase = Phase::Reacquiring(acquire, Instant::now());
        }
    }

    fn poll_reacquire(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Phase::Reacquiring(acquire, since) = &mut self.phase else {
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
        self.metrics.reacquire_time.add_duration(waited);
        drop(self.lease.install(permit, waited));
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
                self.metrics.self_wakes.add(1);
                self.lease.record_self_wake();
            }
            Poll::Pending => {
                self.metrics.parks.add(1);
                self.phase = Phase::Parked(Instant::now());
                self.lease.stream_parked();
            }
            Poll::Ready(None) => {
                self.phase = Phase::Done;
                self.lease.stream_finished();
            }
            Poll::Ready(Some(_)) => {}
        }
        poll
    }
}

impl Stream for LendableVcoreStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if matches!(this.phase, Phase::Done) {
            return Poll::Ready(None);
        }
        this.resume();
        match this.poll_reacquire(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(())) => {}
        }
        this.poll_input(cx)
    }
}

impl RecordBatchStream for LendableVcoreStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Drop for LendableVcoreStream {
    fn drop(&mut self) {
        match self.phase {
            Phase::Running | Phase::Reacquiring(..) => self.lease.stream_finished(),
            Phase::Parked(_) => self.lease.parked_stream_dropped(),
            Phase::New | Phase::Done => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lendable_vcore::cpu_wait;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionConfig;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    fn ctx(lease: &Arc<LendableVcore>) -> Arc<TaskContext> {
        Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new().with_extension(Arc::clone(lease)),
            ),
        )
    }

    fn wrap(
        stream: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
        lease: &Arc<LendableVcore>,
    ) -> LendableVcoreStream {
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        ));
        let set = ExecutionPlanMetricsSet::new();
        LendableVcoreStream::new(input, Arc::clone(lease), LendMetrics::new(&set, 0))
    }

    async fn new_lease(threshold_ms: u64) -> (Arc<Semaphore>, Arc<LendableVcore>) {
        let sem = Arc::new(Semaphore::new(1));
        let lease =
            LendableVcore::new(Arc::clone(&sem), Duration::from_millis(threshold_ms));
        lease.acquire().await.expect("fresh semaphore has a permit");
        (sem, lease)
    }

    #[tokio::test(start_paused = true)]
    async fn slow_input_lends_vcore_and_reacquires_before_resuming() {
        let (sem, lease) = new_lease(10).await;
        let slow = futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(slow, &lease);
        let next = tokio::spawn(async move { stream.next().await.map(|r| r.is_ok()) });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!lease.is_held(), "vcore not lent while input was parked");
        let borrowed = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("lent vcore is available to other tasks");

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!next.is_finished(), "stream resumed without a vcore");

        drop(borrowed);
        assert_eq!(next.await.expect("stream task"), Some(true));
        assert!(lease.is_held(), "vcore not re-acquired");
    }

    #[tokio::test(start_paused = true)]
    async fn coop_budget_yields_do_not_park() {
        let (_sem, lease) = new_lease(0).await;
        // Consumes tokio coop budget per batch, like DataFusion's
        // `CooperativeStream`, so the task is forced to yield periodically.
        let yielding = futures::stream::iter(0..1000).then(|_| async {
            tokio::task::consume_budget().await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(yielding, &lease);
        let task = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                batch.expect("batch");
            }
            stream
        });
        let stream = task.await.expect("stream task");
        assert!(
            stream.metrics.self_wakes.value() > 0,
            "no coop yields observed"
        );
        assert_eq!(stream.metrics.parks.value(), 0);
        assert!(lease.is_held());
    }

    #[tokio::test(start_paused = true)]
    async fn yield_now_parks_briefly_but_keeps_vcore() {
        let (_sem, lease) = new_lease(10).await;
        // `yield_now` wakes only after the task's poll returns and uses no
        // budget, so it looks like a park; the release threshold absorbs it.
        let yielding = futures::stream::iter(0..100).then(|_| async {
            tokio::task::yield_now().await;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(yielding, &lease);
        while let Some(batch) = stream.next().await {
            batch.expect("batch");
        }
        assert_eq!(stream.metrics.parks.value(), 100);
        assert!(lease.is_held());
        assert_eq!(lease.stats().releases.load(Ordering::Relaxed), 0);
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
    async fn pending_on_offloaded_cpu_work_is_mistaken_for_io() {
        let (_sem, lease) = new_lease(10).await;
        let mut stream = wrap(offloaded_cpu_source(), &lease);
        stream.next().await.expect("batch").expect("ok");
        assert_eq!(
            lease.stats().releases.load(Ordering::Relaxed),
            1,
            "lending cannot tell CPU work on another thread from I/O"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_wait_keeps_vcore_for_offloaded_cpu_work() {
        let (_sem, lease) = new_lease(10).await;
        let task_ctx = ctx(&lease);
        let source = futures::stream::once(async move {
            cpu_wait(
                &task_ctx,
                tokio::task::spawn_blocking(|| busy_for(Duration::from_millis(200))),
            )
            .await
            .expect("blocking task");
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        let mut stream = wrap(source, &lease);
        stream.next().await.expect("batch").expect("ok");
        assert_eq!(lease.stats().releases.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_io_inside_poll_is_invisible() {
        let (_sem, lease) = new_lease(10).await;
        // Blocking inside `poll_next` (a sync client, `std::fs`) never returns
        // `Pending`, so it looks like CPU work and the vcore stays held.
        let source = futures::stream::poll_fn(|_| {
            std::thread::sleep(Duration::from_millis(200));
            Poll::Ready(None::<Result<RecordBatch>>)
        });
        let mut stream = wrap(source, &lease);
        assert!(stream.next().await.is_none());
        assert_eq!(stream.metrics.parks.value(), 0);
        assert!(stream.metrics.poll_time.value() >= 200_000_000);
        assert_eq!(lease.stats().releases.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn without_lease_execute_returns_input_unchanged() {
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            Schema::empty(),
        )));
        let exec = LendableVcoreExec::new(input);
        let stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        assert!(exec.metrics().expect("metrics").iter().next().is_none());
        drop(stream);
    }

    #[tokio::test(start_paused = true)]
    async fn execute_with_lease_registers_metrics() {
        let (_sem, lease) = new_lease(10).await;
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            Schema::empty(),
        )));
        let exec = LendableVcoreExec::new(input);
        let mut stream = exec.execute(0, ctx(&lease)).expect("execute");
        while stream.next().await.is_some() {}
        assert!(exec.metrics().expect("metrics").iter().next().is_some());
    }
}
