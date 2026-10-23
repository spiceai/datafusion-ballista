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
//! When an application sets [`VcoreHook`](crate::vcore_permit::VcoreHook) as
//! DataFusion's [`IoWaitHook`](datafusion::common::runtime::IoWaitHook), every
//! task the executor runs counts those waits on its
//! [`VcorePermit`](crate::vcore_permit::VcorePermit). Once a wait has been in
//! flight for [`LEND_AFTER`](crate::vcore_permit::LEND_AFTER) with no part of
//! the task being polled, the task's vcore goes back to the executor, which
//! can start another task on it. Before any part of the task runs again it
//! takes a vcore back, waiting for one if none is free.
//!
//! Blocking work a task spawns counts as the task running from when it is
//! spawned until it ends, so the vcore is never lent while it is queued or
//! running, and a blocking thread never waits for a vcore.
//!
//! Without the hook nothing is counted, and a task keeps its vcore for its
//! whole life.

use datafusion::common::runtime::IoWaitHook;
use datafusion::physical_plan::metrics::{Count, Metric, MetricValue, MetricsSet, Time};
use futures::FutureExt;
use futures::future::{AbortHandle, Aborted, BoxFuture, Either, LocalBoxFuture, Shared};
use pin_project_lite::pin_project;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// A task taking its lent vcore back, shared by every part of the task that
/// needs it.
type Resume = Shared<BoxFuture<'static, ()>>;

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
/// datafusion::common::runtime::set_io_wait_hook(&ballista_core::vcore_permit::VcoreHook)
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

    fn carry_local_future(
        &self,
        fut: LocalBoxFuture<'static, Box<dyn Any>>,
    ) -> LocalBoxFuture<'static, Box<dyn Any>> {
        match VCORE.try_with(Arc::clone) {
            Ok(permit) => permit.run(fut).boxed_local(),
            Err(_) => fut,
        }
    }

    fn carry_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        match Carried::start() {
            Some(carried) => Box::new(move || carried.run(f)),
            None => f,
        }
    }
}

/// A blocking closure spawned by a part of the task, counted as the task
/// running from when it is spawned until it ends or is dropped unrun.
///
/// The part spawning it holds the task's vcore, so counting from the spawn
/// keeps the vcore with the task until the closure is done: the closure never
/// has to take a vcore back, and so never parks a blocking thread waiting for
/// one, which a task holding a vcore may need for its own blocking work.
struct Carried {
    permit: Arc<VcorePermit>,
}

impl Carried {
    /// Counts a closure spawned now as part of the task being polled, if
    /// there is one.
    fn start() -> Option<Self> {
        let permit = VCORE.try_with(Arc::clone).ok()?;
        permit.enter();
        Some(Self { permit })
    }

    fn run<R>(self, f: impl FnOnce() -> R) -> R {
        VCORE.sync_scope(Arc::clone(&self.permit), f)
    }
}

impl Drop for Carried {
    fn drop(&mut self) {
        self.permit.leave();
    }
}

/// Runs `fut` as part of the task being polled, if there is one.
///
/// DataFusion's spawns reach [`VcoreHook::carry_future`] on their own. Ballista's
/// own spawns on a task's execution path (the shuffle writers) wrap their
/// futures with this instead, so a wait inside them is counted on the task's
/// permit and the spawned work takes a vcore back before it runs.
pub fn carry<F: Future>(fut: F) -> impl Future<Output = F::Output> + use<F> {
    match VCORE.try_with(Arc::clone) {
        Ok(permit) => Either::Left(permit.run(fut)),
        Err(_) => Either::Right(fut),
    }
}

/// Counts a blocking closure spawned on a task's execution path as the task
/// running, from now until the closure ends, as [`carry`] does for futures.
pub fn carry_blocking<F, R>(f: F) -> impl FnOnce() -> R + Send + use<F, R>
where
    F: FnOnce() -> R + Send,
{
    let carried = Carried::start();
    move || match carried {
        Some(carried) => carried.run(f),
        None => f(),
    }
}

/// Counters for one task's permit.
#[derive(Debug, Default)]
pub struct VcoreStats {
    /// Times the vcore was lent.
    pub suspensions: AtomicU64,
    /// Nanoseconds the vcore was lent.
    pub suspended_nanos: AtomicU64,
    /// Times the task waited to take a vcore back.
    pub resume_waits: AtomicU64,
    /// Nanoseconds the task waited to take a vcore back.
    pub resume_wait_nanos: AtomicU64,
}

#[derive(Debug, Default)]
struct Held {
    vcore: Option<OwnedSemaphorePermit>,
    lent_at: Option<Instant>,
    /// The task taking its vcore back, while one is on the way.
    resume: Option<Resume>,
    /// Ends `resume` when the task ends, so parts waiting on it run without
    /// a vcore rather than wait for executor capacity.
    resume_abort: Option<AbortHandle>,
    closed: bool,
}

/// A task's vcore, lent while all of the task is waiting on remote I/O.
///
/// The executor creates one per task, [`acquire`](Self::acquire)s it before
/// the task runs, runs the task through [`run`](Self::run), and
/// [`close`](Self::close)s it when the task ends.
///
/// Lending controls CPU concurrency only. Tasks retain their actual memory
/// reservations while suspended and allocate through their runtime's memory
/// pool; lending does not reserve memory for hypothetical replacement tasks.
#[derive(Debug)]
pub struct VcorePermit {
    vcores: Arc<Semaphore>,
    lend_after: Duration,
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
    pub fn new(vcores: Arc<Semaphore>, lend_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            vcores,
            lend_after,
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
        held.resume = None;
        if let Some(abort) = held.resume_abort.take() {
            abort.abort();
        }
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
        // A blocking closure dropped while the runtime shuts down has no
        // runtime to lend on, and nothing left to lend to.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.timer_armed.store(false, Ordering::SeqCst);
            return;
        };
        let permit = Arc::downgrade(self);
        let lend_after = self.lend_after;
        runtime.spawn(lend_when_idle(permit, lend_after));
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
        self.stats.suspensions.fetch_add(1, Ordering::Relaxed);
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
        held.resume = None;
        held.resume_abort = None;
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

    /// A part of the task started running.
    fn enter(&self) {
        self.polling.fetch_add(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// A part of the task stopped running.
    fn leave(self: &Arc<Self>) {
        self.polling.fetch_sub(1, Ordering::SeqCst);
        self.went_idle();
    }

    /// Takes the lent vcore back for every part of the task that needs it:
    /// parts resuming together wait in the executor's queue once, and all
    /// run once the vcore is back. A part waiting on its own acquire could
    /// otherwise wait forever for a vcore its task already holds.
    fn resume(self: &Arc<Self>) -> Resume {
        let mut held = self.lock();
        // A sibling may have taken the vcore back, or the task ended, since
        // this part saw it lent.
        if held.closed || held.vcore.is_some() {
            return futures::future::ready(()).boxed().shared();
        }
        if let Some(resume) = &held.resume
            && resume.peek().is_none()
        {
            return resume.clone();
        }
        let permit = Arc::downgrade(self);
        let (vcore, abort) =
            futures::future::abortable(Arc::clone(&self.vcores).acquire_owned());
        let resume = async move {
            let started = Instant::now();
            let vcore = vcore.await;
            let Some(permit) = permit.upgrade() else {
                return;
            };
            match vcore {
                Ok(Ok(vcore)) => drop(permit.install(vcore, started.elapsed())),
                // A closed semaphore means shutdown: run without a vcore from
                // now on, rather than asking again.
                Ok(Err(_)) => permit.close(),
                // The task ended.
                Err(Aborted) => {}
            }
        }
        .boxed()
        .shared();
        held.resume = Some(resume.clone());
        held.resume_abort = Some(abort);
        resume
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
        permit.enter();
        Self { permit }
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.permit.leave();
    }
}

pin_project! {
    /// Takes a vcore back before polling its future if the task's vcore was
    /// lent, and counts the task as running while it polls.
    struct Gate<F> {
        #[pin]
        inner: F,
        permit: Arc<VcorePermit>,
        acquiring: Option<Resume>,
    }
}

impl<F: Future> Future for Gate<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let permit: &Arc<VcorePermit> = this.permit;
        loop {
            if let Some(resume) = this.acquiring.as_mut() {
                if resume.poll_unpin(cx).is_pending() {
                    return Poll::Pending;
                }
                *this.acquiring = None;
            }
            let running = Running::start(permit);
            if permit.needs_vcore() {
                drop(running);
                *this.acquiring = Some(permit.resume());
                continue;
            }
            let poll = this.inner.poll(cx);
            drop(running);
            return poll;
        }
    }
}

/// Sets [`VcoreHook`] for this crate's tests, whichever module gets there
/// first.
#[cfg(test)]
pub(crate) fn install_hook_for_tests() {
    use datafusion::common::runtime::{IoWaitHookError, set_io_wait_hook};
    match set_io_wait_hook(&VcoreHook) {
        Ok(()) | Err(IoWaitHookError::AlreadySet) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::runtime::{IoWaitExt, SpawnedTask};
    use std::sync::LazyLock;

    static INSTALL: LazyLock<()> = LazyLock::new(install_hook_for_tests);

    const LEND: Duration = Duration::from_millis(20);

    async fn remote_call(ms: u64) -> u32 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        7
    }

    /// A one-vcore executor and a permit for a task that holds the vcore.
    async fn one_vcore_task() -> (Arc<Semaphore>, Arc<VcorePermit>) {
        LazyLock::force(&INSTALL);
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(Arc::clone(&vcores), LEND);
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
        let (vcores, permit) = one_vcore_task().await;
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
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmarked_wait_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(remote_call(100)));
        assert!(!vcore_frees_within(&vcores, Duration::from_millis(80)).await);
        assert_eq!(task.await.expect("task"), 7);
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn io_wait_in_a_spawned_task_lends_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
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
    async fn io_wait_in_a_carried_tokio_spawn_lends_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(async {
            // How the shuffle writers spawn a stage's partitions.
            tokio::spawn(carry(remote_call(200).io_wait()))
                .await
                .expect("spawned")
        }));
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(150)).await,
            "a wait inside a carried tokio::spawn should lend the vcore"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn carried_blocking_closure_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(async {
            let waiting = SpawnedTask::spawn(remote_call(150).io_wait());
            let writing = tokio::task::spawn_blocking(carry_blocking(|| {
                let until = Instant::now() + Duration::from_millis(150);
                while Instant::now() < until {
                    std::hint::black_box((0..1_000u64).sum::<u64>());
                }
            }));
            writing.await.expect("writing");
            waiting.join().await.expect("waiting")
        }));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(120)).await,
            "lent while a carried blocking closure was running"
        );
        assert_eq!(task.await.expect("task"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn computing_sibling_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
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
        let (vcores, permit) = one_vcore_task().await;
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
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_sibling_keeps_the_vcore() {
        let (vcores, permit) = one_vcore_task().await;
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
        let (vcores, permit) = one_vcore_task().await;
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
        let (vcores, permit) = one_vcore_task().await;
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
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 0);
        assert_eq!(vcores.available_permits(), 0, "the task kept its vcore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_permit_returns_the_vcore_and_never_takes_one_back() {
        let (vcores, permit) = one_vcore_task().await;
        permit.close();
        assert_eq!(vcores.available_permits(), 1);
        assert_eq!(permit.run(remote_call(30).io_wait()).await, 7);
        assert!(!permit.is_held());
        assert_eq!(vcores.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_shorter_than_lend_after_is_no_suspension() {
        let (vcores, permit) = one_vcore_task().await;
        assert_eq!(permit.run(remote_call(2).io_wait()).await, 7);
        assert_eq!(permit.stats().suspensions.load(Ordering::SeqCst), 0);
        assert_eq!(vcores.available_permits(), 0, "the task kept its vcore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_blocking_closure_keeps_the_vcore_until_it_ends() {
        let (vcores, permit) = one_vcore_task().await;
        let ran = Arc::new(Mutex::new(None));
        let ran_in_closure = Arc::clone(&ran);
        let permit_in_closure = Arc::clone(&permit);
        // Carried while the task holds its vcore and not yet started, like a
        // closure queued behind a busy blocking pool.
        let closure = permit
            .run(async move {
                carry_blocking(move || {
                    *ran_in_closure.lock().expect("lock") =
                        Some(permit_in_closure.is_held());
                })
            })
            .await;
        let waiting = tokio::spawn(permit.run(remote_call(300).io_wait()));
        assert!(
            !vcore_frees_within(&vcores, Duration::from_millis(100)).await,
            "lent while a carried blocking closure was queued"
        );
        tokio::task::spawn_blocking(closure)
            .await
            .expect("blocking");
        assert_eq!(
            *ran.lock().expect("lock"),
            Some(true),
            "ran without the vcore"
        );
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(150)).await,
            "never lent once the blocking closure ended"
        );
        assert_eq!(waiting.await.expect("waiting"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_unrun_blocking_closure_stops_counting() {
        let (vcores, permit) = one_vcore_task().await;
        let closure = permit.run(async { carry_blocking(|| ()) }).await;
        let waiting = tokio::spawn(permit.run(remote_call(300).io_wait()));
        assert!(!vcore_frees_within(&vcores, Duration::from_millis(60)).await);
        drop(closure);
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(150)).await,
            "a dropped closure kept the vcore"
        );
        assert_eq!(waiting.await.expect("waiting"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lent_task_finishes_after_the_executor_closes_its_vcores() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(remote_call(100).io_wait()));
        assert!(vcore_frees_within(&vcores, Duration::from_millis(80)).await);
        // Shutdown: the task can never take a vcore back.
        vcores.close();
        let v = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the task should finish without a vcore after shutdown")
            .expect("task");
        assert_eq!(v, 7);
        assert!(!permit.is_held());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parts_resuming_together_take_one_vcore_back() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(async {
            // `a` waits on a remote call; `b` on a timer, which keeps no wait
            // in flight once `a` is back, so nothing lends the vcore to it.
            let a = SpawnedTask::spawn(remote_call(100).io_wait());
            let b = SpawnedTask::spawn(remote_call(120));
            a.join().await.expect("a") + b.join().await.expect("b")
        }));
        assert!(vcore_frees_within(&vcores, Duration::from_millis(80)).await);
        // Another task holds the lent vcore until both parts have woken, so
        // each wakes needing a vcore, `a` first.
        let other = Arc::clone(&vcores).acquire_owned().await.expect("vcore");
        tokio::time::sleep(Duration::from_millis(170)).await;
        drop(other);
        let v = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("parts of the task waited for a vcore the task already held")
            .expect("task");
        assert_eq!(v, 14);
        assert!(permit.is_held());
        assert_eq!(permit.stats().resume_waits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn part_resuming_after_a_sibling_took_the_vcore_back_runs_on_it() {
        let (vcores, permit) = one_vcore_task().await;
        let task = tokio::spawn(permit.run(remote_call(60_000).io_wait()));
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(500)).await,
            "never lent"
        );
        // Two parts both see the vcore lent, then one takes it back before
        // the other asks for it.
        assert!(permit.needs_vcore());
        assert!(permit.needs_vcore());
        permit.resume().await;
        assert!(permit.is_held());
        tokio::time::timeout(Duration::from_secs(2), permit.resume())
            .await
            .expect("a part waited for a second vcore while its task held one");
        assert!(permit.is_held());
        assert_eq!(vcores.available_permits(), 0);
        task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_a_lent_permit_lets_parts_waiting_for_a_vcore_finish() {
        let (vcores, permit) = one_vcore_task().await;
        let waiting = tokio::spawn(permit.run(remote_call(60_000).io_wait()));
        assert!(
            vcore_frees_within(&vcores, Duration::from_millis(500)).await,
            "never lent"
        );
        // Another task takes the lent vcore, so a part resuming must wait.
        let other = Arc::clone(&vcores).acquire_owned().await.expect("vcore");
        let part = tokio::spawn(permit.run(async { 7 }));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!part.is_finished(), "ran without a vcore");

        // The task ends: its parts run without a vcore from now on.
        permit.close();
        let v = tokio::time::timeout(Duration::from_secs(2), part)
            .await
            .expect("a part of a closed task kept waiting for a vcore")
            .expect("part");
        assert_eq!(v, 7);
        assert!(!permit.is_held());
        assert_eq!(vcores.available_permits(), 0);
        drop(other);
        assert_eq!(vcores.available_permits(), 1);
        waiting.abort();
    }

    #[test]
    fn task_counters_become_metrics() {
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(vcores, LEND);
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
