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

//! Lending a task's vcore while all of the task is waiting on remote I/O.
//!
//! Code that awaits a remote system marks the wait with DataFusion's
//! [`IoWaitExt::io_wait`](datafusion::common::runtime::IoWaitExt::io_wait).
//! When an application sets [`VcoreHook`] as DataFusion's
//! [`IoWaitHook`](datafusion::common::runtime::IoWaitHook), every task the
//! executor runs counts those waits on its [`VcorePermit`]. Once a wait has
//! been in flight for [`LEND_AFTER`] with no part of the task being polled,
//! the task's vcore goes back to the executor, which can start another task
//! on it. Before any part of the task runs again it takes a vcore back,
//! waiting for one if none is free.
//!
//! Without the hook nothing is counted, and a task keeps its vcore for its
//! whole life.

use datafusion::common::runtime::IoWaitHook;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
use datafusion::physical_plan::metrics::{Count, Metric, MetricValue, MetricsSet, Time};
use futures::FutureExt;
use futures::future::BoxFuture;
use pin_project_lite::pin_project;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// How long every part of a task must be idle, with a wait in flight, before
/// its vcore is lent.
pub const LEND_AFTER: Duration = Duration::from_millis(50);

tokio::task_local! {
    /// The permit of the task being polled. Set by [`VcorePermit::run`] and
    /// by [`VcoreHook::carry_future`]; read only by [`VcoreHook`].
    static VCORE: Arc<VcorePermit>;
}

/// DataFusion [`IoWaitHook`] that lends a task's vcore while all of the task
/// is waiting on remote I/O.
///
/// The application sets it once at startup, or wraps it in its own hook:
///
/// ```no_run
/// datafusion::common::runtime::set_io_wait_hook(&ballista_executor::VcoreHook)
///     .expect("hook set once");
/// ```
///
/// Outside a task the executor runs, every call does nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct VcoreHook;

impl IoWaitHook for VcoreHook {
    fn before_wait(&self) {
        let _ = VCORE.try_with(|p| p.wait_started());
    }

    fn after_wait(&self) {
        let _ = VCORE.try_with(|p| p.wait_ended());
    }

    fn carry_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        match VCORE.try_with(Arc::clone) {
            Ok(permit) => permit.run(fut).boxed(),
            Err(_) => fut,
        }
    }

    fn carry_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        match VCORE.try_with(Arc::clone) {
            Ok(permit) => Box::new(move || {
                // A blocking closure is the task doing work, so it keeps the
                // vcore while it runs.
                let _running = Running::start(&permit);
                VCORE.sync_scope(Arc::clone(&permit), f)
            }),
            Err(_) => f,
        }
    }
}

/// Counters for one task's permit.
#[derive(Debug, Default)]
pub struct VcoreStats {
    /// Waits on remote I/O that started.
    pub suspensions: AtomicU64,
    /// Times the vcore was lent.
    pub releases: AtomicU64,
    /// Nanoseconds the vcore was lent.
    pub suspended_nanos: AtomicU64,
    /// Times the task waited to take a vcore back.
    pub resume_waits: AtomicU64,
    /// Nanoseconds the task waited to take a vcore back.
    pub resume_wait_nanos: AtomicU64,
}

/// Lends a vcore only while the executor's memory pool can give another task
/// its share.
#[derive(Debug, Clone)]
pub struct MemoryHeadroom {
    pool: Arc<dyn MemoryPool>,
    vcores: usize,
}

impl MemoryHeadroom {
    /// A task's share is the pool's limit divided by the executor's vcores.
    pub fn new(pool: Arc<dyn MemoryPool>, vcores: usize) -> Self {
        Self {
            pool,
            vcores: vcores.max(1),
        }
    }

    fn has_room_for_a_task(&self) -> bool {
        match self.pool.memory_limit() {
            MemoryLimit::Finite(limit) => {
                limit.saturating_sub(self.pool.reserved()) >= limit / self.vcores
            }
            MemoryLimit::Infinite | MemoryLimit::Unknown => true,
        }
    }
}

#[derive(Debug, Default)]
struct Held {
    vcore: Option<OwnedSemaphorePermit>,
    lent_at: Option<Instant>,
    closed: bool,
}

/// A task's vcore, lent while all of the task is waiting on remote I/O.
///
/// The executor creates one per task, [`acquire`](Self::acquire)s it before
/// the task runs, runs the task through [`run`](Self::run), and
/// [`close`](Self::close)s it when the task ends.
#[derive(Debug)]
pub struct VcorePermit {
    vcores: Arc<Semaphore>,
    lend_after: Duration,
    memory: Option<MemoryHeadroom>,
    held: Mutex<Held>,
    /// Mirrors `held.vcore.is_none()` for the poll path; written under `held`.
    lent: AtomicBool,
    /// Parts of the task being polled right now.
    polling: AtomicUsize,
    /// Waits on remote I/O in flight.
    waits: AtomicUsize,
    /// Changes whenever any part of the task is polled or a wait starts or
    /// ends, so a lend timer can tell the task stayed idle.
    epoch: AtomicU64,
    timer_armed: AtomicBool,
    stats: VcoreStats,
}

impl VcorePermit {
    /// A permit drawing from `vcores`. It holds no vcore until
    /// [`acquire`](Self::acquire) succeeds.
    pub fn new(
        vcores: Arc<Semaphore>,
        lend_after: Duration,
        memory: Option<MemoryHeadroom>,
    ) -> Arc<Self> {
        Arc::new(Self {
            vcores,
            lend_after,
            memory,
            held: Mutex::new(Held::default()),
            lent: AtomicBool::new(true),
            polling: AtomicUsize::new(0),
            waits: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
            timer_armed: AtomicBool::new(false),
            stats: VcoreStats::default(),
        })
    }

    /// Wait for the task's vcore. The executor calls this before the task
    /// runs.
    ///
    /// # Errors
    ///
    /// Fails if the executor's vcore semaphore was closed (shutdown).
    pub async fn acquire(&self) -> Result<(), AcquireError> {
        let vcore = Arc::clone(&self.vcores).acquire_owned().await?;
        let mut held = self.lock();
        if !held.closed && held.vcore.is_none() {
            held.vcore = Some(vcore);
            self.lent.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Return the vcore for good. Parts of the task that outlive it keep
    /// running but never take a vcore again.
    pub fn close(&self) {
        let mut held = self.lock();
        held.closed = true;
        self.end_lend(&mut held);
        held.vcore = None;
    }

    /// Whether the task holds its vcore.
    pub fn is_held(&self) -> bool {
        self.lock().vcore.is_some()
    }

    /// This task's counters.
    pub fn stats(&self) -> &VcoreStats {
        &self.stats
    }

    /// Runs `fut` as part of this task: with the permit in scope, so
    /// [`VcoreHook`] can see it, and through a gate that takes a vcore back
    /// before `fut` is polled if the vcore was lent.
    pub fn run<F: Future>(
        self: &Arc<Self>,
        fut: F,
    ) -> impl Future<Output = F::Output> + use<F> {
        VCORE.scope(
            Arc::clone(self),
            Gate {
                inner: fut,
                permit: Arc::clone(self),
                acquiring: None,
            },
        )
    }

    /// Appends this task's counters to `metrics` as `suspensions`,
    /// `suspended_time` and `resume_wait_time`.
    pub fn add_metrics(&self, metrics: &mut MetricsSet, partition: usize) {
        let suspensions = Count::new();
        suspensions.add(self.stats.suspensions.load(Ordering::Relaxed) as usize);
        let suspended = Time::new();
        suspended.add_duration(Duration::from_nanos(
            self.stats.suspended_nanos.load(Ordering::Relaxed),
        ));
        let resume_wait = Time::new();
        resume_wait.add_duration(Duration::from_nanos(
            self.stats.resume_wait_nanos.load(Ordering::Relaxed),
        ));
        for value in [
            MetricValue::Count {
                name: "suspensions".into(),
                count: suspensions,
            },
            MetricValue::Time {
                name: "suspended_time".into(),
                time: suspended,
            },
            MetricValue::Time {
                name: "resume_wait_time".into(),
                time: resume_wait,
            },
        ] {
            metrics.push(Arc::new(Metric::new(value, Some(partition))));
        }
    }

    fn lock(&self) -> MutexGuard<'_, Held> {
        // Poisoning only means a panic mid-update; the state is still usable
        // for returning the vcore.
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_started(&self) {
        self.stats.suspensions.fetch_add(1, Ordering::Relaxed);
        self.waits.fetch_add(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn wait_ended(&self) {
        // Saturating: a wait whose start happened outside this task's scope
        // has no start to undo.
        let mut current = self.waits.load(Ordering::SeqCst);
        while current > 0 {
            match self.waits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// A part of the task stopped being polled. If nothing else is running
    /// and a wait is in flight, start counting down to lending the vcore.
    fn went_idle(self: &Arc<Self>) {
        if self.polling.load(Ordering::SeqCst) != 0
            || self.waits.load(Ordering::SeqCst) == 0
            || self.lent.load(Ordering::SeqCst)
            || self.timer_armed.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let permit = Arc::downgrade(self);
        let lend_after = self.lend_after;
        tokio::spawn(lend_when_idle(permit, lend_after));
    }

    /// Lend the vcore if the task stayed idle with a wait in flight since
    /// `epoch`.
    fn try_lend(&self, epoch: u64) -> LendAttempt {
        let mut held = self.lock();
        if held.closed || held.vcore.is_none() {
            return LendAttempt::Done;
        }
        if self.polling.load(Ordering::SeqCst) != 0
            || self.waits.load(Ordering::SeqCst) == 0
        {
            // A running part re-arms the timer when it goes idle again.
            return LendAttempt::Done;
        }
        if self.epoch.load(Ordering::SeqCst) != epoch {
            return LendAttempt::Retry;
        }
        if self
            .memory
            .as_ref()
            .is_some_and(|m| !m.has_room_for_a_task())
        {
            return LendAttempt::Retry;
        }
        // Publish the lend before re-checking that nothing started polling:
        // a gate increments `polling` before it reads `lent`, so either the
        // gate sees the lend or this sees the gate.
        self.lent.store(true, Ordering::SeqCst);
        if self.polling.load(Ordering::SeqCst) != 0
            || self.epoch.load(Ordering::SeqCst) != epoch
        {
            self.lent.store(false, Ordering::SeqCst);
            return LendAttempt::Retry;
        }
        held.vcore = None;
        held.lent_at = Some(Instant::now());
        self.stats.releases.fetch_add(1, Ordering::Relaxed);
        LendAttempt::Done
    }

    /// Store a vcore taken back by a gate. Returns it if the task already has
    /// one or has ended, so the caller drops it.
    fn install(
        &self,
        vcore: OwnedSemaphorePermit,
        waited: Duration,
    ) -> Option<OwnedSemaphorePermit> {
        self.stats.resume_waits.fetch_add(1, Ordering::Relaxed);
        self.stats
            .resume_wait_nanos
            .fetch_add(duration_nanos(waited), Ordering::Relaxed);
        let mut held = self.lock();
        if held.closed || held.vcore.is_some() {
            return Some(vcore);
        }
        self.end_lend(&mut held);
        held.vcore = Some(vcore);
        self.lent.store(false, Ordering::SeqCst);
        None
    }

    /// Whether a gate must take a vcore back before polling. Decided under
    /// the lock, so a lend that a concurrent poll reverted is not mistaken for
    /// a real one.
    fn needs_vcore(&self) -> bool {
        if !self.lent.load(Ordering::SeqCst) {
            return false;
        }
        let held = self.lock();
        !held.closed && held.vcore.is_none()
    }

    fn end_lend(&self, held: &mut Held) {
        if let Some(since) = held.lent_at.take() {
            self.stats
                .suspended_nanos
                .fetch_add(duration_nanos(since.elapsed()), Ordering::Relaxed);
        }
    }
}

enum LendAttempt {
    Done,
    Retry,
}

async fn lend_when_idle(permit: Weak<VcorePermit>, lend_after: Duration) {
    loop {
        let Some(epoch) = permit.upgrade().map(|p| p.epoch.load(Ordering::SeqCst)) else {
            return;
        };
        tokio::time::sleep(lend_after).await;
        let Some(p) = permit.upgrade() else {
            return;
        };
        if let LendAttempt::Retry = p.try_lend(epoch) {
            continue;
        }
        p.timer_armed.store(false, Ordering::SeqCst);
        // The task may have gone idle again between the attempt and clearing
        // the flag, when `went_idle` saw the timer still armed.
        if p.polling.load(Ordering::SeqCst) == 0
            && p.waits.load(Ordering::SeqCst) != 0
            && !p.lent.load(Ordering::SeqCst)
            && !p.timer_armed.swap(true, Ordering::SeqCst)
        {
            continue;
        }
        return;
    }
}

fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Counts a part of the task as running while alive.
struct Running<'a> {
    permit: &'a Arc<VcorePermit>,
}

impl<'a> Running<'a> {
    fn start(permit: &'a Arc<VcorePermit>) -> Self {
        permit.polling.fetch_add(1, Ordering::SeqCst);
        permit.epoch.fetch_add(1, Ordering::SeqCst);
        Self { permit }
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.permit.polling.fetch_sub(1, Ordering::SeqCst);
        self.permit.went_idle();
    }
}

pin_project! {
    /// Takes a vcore back before polling its future if the task's vcore was
    /// lent, and counts the task as running while it polls.
    struct Gate<F> {
        #[pin]
        inner: F,
        permit: Arc<VcorePermit>,
        acquiring: Option<(BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>, Instant)>,
    }
}

impl<F: Future> Future for Gate<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let permit: &Arc<VcorePermit> = this.permit;
        loop {
            if let Some((acquire, started)) = this.acquiring.as_mut() {
                let Poll::Ready(vcore) = acquire.as_mut().poll(cx) else {
                    return Poll::Pending;
                };
                let waited = started.elapsed();
                *this.acquiring = None;
                // A closed semaphore means shutdown: run without a vcore.
                if let Ok(vcore) = vcore {
                    drop(permit.install(vcore, waited));
                }
            }
            let running = Running::start(permit);
            if permit.needs_vcore() {
                drop(running);
                *this.acquiring = Some((
                    Arc::clone(&permit.vcores).acquire_owned().boxed(),
                    Instant::now(),
                ));
                continue;
            }
            let poll = this.inner.poll(cx);
            drop(running);
            return poll;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::runtime::{IoWaitExt, SpawnedTask, set_io_wait_hook};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
    use std::sync::LazyLock;

    static INSTALL: LazyLock<()> = LazyLock::new(|| {
        set_io_wait_hook(&VcoreHook).expect("only these tests set the hook");
    });

    const LEND: Duration = Duration::from_millis(20);

    async fn remote_call(ms: u64) -> u32 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        7
    }

    /// A one-vcore executor and a permit for a task that holds the vcore.
    async fn one_vcore_task(
        memory: Option<MemoryHeadroom>,
    ) -> (Arc<Semaphore>, Arc<VcorePermit>) {
        LazyLock::force(&INSTALL);
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(Arc::clone(&vcores), LEND, memory);
        permit.acquire().await.expect("vcore");
        (vcores, permit)
    }

    /// Polls `vcores` until a vcore is free or `within` passes; returns
    /// whether one was.
    async fn vcore_frees_within(vcores: &Semaphore, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if vcores.available_permits() == 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        vcores.available_permits() == 1
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_io_wait_lends_the_vcore_after_lend_after_not_before() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(remote_call(200).io_wait()));
        assert!(
            !vcore_frees_within(&vcores, LEND / 2).await,
            "lent before {LEND:?}"
        );
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(150)).await,
            "never lent while the remote call was pending"
        );
        assert_eq!(task.await.expect("task"), 7);
        assert!(permit.is_held(), "took the vcore back before finishing");
        assert_eq!(permit.stats().releases.load(Ordering::SeqCst), 1);
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmarked_wait_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(remote_call(100)));
        assert!(!vcore_frees_within(&vcores, Duration::from_millis(80)).await);
        assert_eq!(task.await.expect("task"), 7);
        assert_eq!(permit.stats().releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn io_wait_in_a_spawned_task_lends_the_vcore() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(async {
            SpawnedTask::spawn(remote_call(200).io_wait())
                .join()
                .await
                .expect("spawned")
        }));
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(150)).await,
            "a wait inside a DataFusion spawn should lend the vcore"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn computing_sibling_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(async {
            let waiting = SpawnedTask::spawn(remote_call(150).io_wait());
            let computing = SpawnedTask::spawn(async {
                let until = Instant::now() + Duration::from_millis(150);
                while Instant::now() < until {
                    std::hint::black_box((0..1_000u64).sum::<u64>());
                    tokio::task::yield_now().await;
                }
            });
            computing.join().await.expect("computing");
            waiting.join().await.expect("waiting")
        }));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(120)).await,
            "lent while a sibling was computing"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_polled_within_every_lend_window_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(async {
            // Wakes every 5 ms, well inside `LEND`, so the task is never idle
            // for a whole window even though it mostly waits.
            let ticking = async {
                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            let (v, ()) = futures::join!(remote_call(150).io_wait(), ticking);
            v
        }));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(120)).await,
            "lent although part of the task ran within every window"
        );
        assert_eq!(task.await.expect("task"), 7);
        // A lend taken straight back is churn the 1 ms probe above can miss.
        assert_eq!(permit.stats().releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_sibling_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(async {
            let waiting = SpawnedTask::spawn(remote_call(150).io_wait());
            let computing = SpawnedTask::spawn_blocking(|| {
                let until = Instant::now() + Duration::from_millis(150);
                while Instant::now() < until {
                    std::hint::black_box((0..1_000u64).sum::<u64>());
                }
            });
            computing.join().await.expect("computing");
            waiting.join().await.expect("waiting")
        }));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(120)).await,
            "lent while a blocking sibling was computing"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resuming_task_waits_for_a_vcore_before_it_runs() {
        let (vcores, permit) = one_vcore_task(None).await;
        let ran_at = Arc::new(Mutex::new(None));
        let ran_at_in_task = Arc::clone(&ran_at);
        let task = tokio::spawn(permit.run(async move {
            let v = remote_call(60).io_wait().await;
            *ran_at_in_task.lock().expect("lock") = Some(Instant::now());
            v
        }));
        assert!(vcore_frees_within(&vcores, Duration::from_millis(50)).await);
        // Another task takes the lent vcore and holds it past the response.
        let other = Arc::clone(&vcores).acquire_owned().await.expect("vcore");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            ran_at.lock().expect("lock").is_none(),
            "resumed without a vcore"
        );
        let released_at = Instant::now();
        drop(other);
        assert_eq!(task.await.expect("task"), 7);
        let ran_at = ran_at.lock().expect("lock").expect("task ran");
        assert!(ran_at >= released_at, "ran before the vcore was free");
        assert!(permit.stats().resume_waits.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_pending_wait_ends_the_wait() {
        let (vcores, permit) = one_vcore_task(None).await;
        let task = tokio::spawn(permit.run(async {
            let timed_out = tokio::time::timeout(
                Duration::from_millis(5),
                remote_call(500).io_wait(),
            )
            .await;
            assert!(timed_out.is_err(), "the call should time out");
            // Idle again, but with no wait in flight.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }));
        task.await.expect("task");
        assert_eq!(permit.waits.load(Ordering::SeqCst), 0);
        assert_eq!(permit.stats().releases.load(Ordering::SeqCst), 0);
        assert_eq!(vcores.available_permits(), 0, "the task kept its vcore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_lend_without_memory_for_another_task() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1_000));
        // Two vcores, so a task's share is 500 bytes; leave 400 free.
        let reservation = MemoryConsumer::new("held by a suspended task").register(&pool);
        reservation.grow(600);
        let (vcores, permit) =
            one_vcore_task(Some(MemoryHeadroom::new(Arc::clone(&pool), 2))).await;
        let task = tokio::spawn(permit.run(remote_call(150).io_wait()));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(100)).await,
            "lent without memory for another task"
        );
        assert_eq!(task.await.expect("task"), 7);

        reservation.shrink(200);
        let task = tokio::spawn(permit.run(remote_call(150).io_wait()));
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(100)).await,
            "never lent with room for another task"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_permit_returns_the_vcore_and_never_takes_one_back() {
        let (vcores, permit) = one_vcore_task(None).await;
        permit.close();
        assert_eq!(vcores.available_permits(), 1);
        assert_eq!(permit.run(remote_call(30).io_wait()).await, 7);
        assert!(!permit.is_held());
        assert_eq!(vcores.available_permits(), 1);
    }

    #[test]
    fn task_counters_become_metrics() {
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(vcores, LEND, None);
        permit.stats.suspensions.store(3, Ordering::SeqCst);
        permit
            .stats
            .suspended_nanos
            .store(2_000_000, Ordering::SeqCst);
        permit
            .stats
            .resume_wait_nanos
            .store(5_000, Ordering::SeqCst);
        let mut metrics = MetricsSet::new();
        permit.add_metrics(&mut metrics, 4);
        let rendered: Vec<String> = metrics
            .iter()
            .map(|m| {
                format!(
                    "{}={} (partition {:?})",
                    m.value().name(),
                    m.value().as_usize(),
                    m.partition()
                )
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "suspensions=3 (partition Some(4))",
                "suspended_time=2000000 (partition Some(4))",
                "resume_wait_time=5000 (partition Some(4))",
            ]
        );
    }
}
