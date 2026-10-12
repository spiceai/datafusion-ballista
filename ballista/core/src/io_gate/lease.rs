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

use datafusion::execution::TaskContext;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

/// What the background releaser is told about the lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    /// At least one gated stream is runnable; keep the vcore.
    Busy,
    /// Every gated stream is parked. `epoch` identifies this parked period and
    /// `hinted` is true when an [`io_wait`] section is open, which skips the
    /// release threshold.
    Parked { epoch: u64, hinted: bool },
    /// The task finished; the releaser exits.
    Closed,
}

#[derive(Debug)]
struct State {
    permit: Option<OwnedSemaphorePermit>,
    /// Gated streams that are being polled or will be polled without a wake-up.
    running: usize,
    /// Gated streams that started and have not finished.
    registered: usize,
    /// Open [`io_wait`] sections.
    io_hints: usize,
    /// Open [`cpu_wait`] sections; while non-zero the vcore is never lent.
    cpu_holds: usize,
    epoch: u64,
    lent_since: Option<Instant>,
    closed: bool,
}

/// Whether a stream may run after re-registering with the lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resume {
    /// The task still holds its vcore.
    Held,
    /// The vcore was lent out; acquire one before polling the input.
    Reacquire,
    /// The task has finished; run without a vcore.
    Closed,
}

/// Counters for one lease, also summed into process-wide totals by
/// [`global_stats`].
#[derive(Debug, Default)]
pub struct LeaseStats {
    /// Times the vcore was lent back to the executor.
    pub releases: AtomicU64,
    /// Times a stream had to wait for a vcore before resuming.
    pub reacquire_waits: AtomicU64,
    /// Total nanoseconds spent waiting to get a vcore back.
    pub reacquire_wait_nanos: AtomicU64,
    /// Total nanoseconds the vcore was lent out.
    pub lent_nanos: AtomicU64,
    /// `Pending` results that woke the task before returning (cooperative
    /// yields); these never count as parking.
    pub self_wakes: AtomicU64,
    /// `Pending` results that parked a stream.
    pub parks: AtomicU64,
}

impl LeaseStats {
    fn add(&self, field: impl Fn(&Self) -> &AtomicU64, n: u64) {
        field(self).fetch_add(n, Ordering::Relaxed);
        field(&GLOBAL_STATS).fetch_add(n, Ordering::Relaxed);
    }
}

static GLOBAL_STATS: LeaseStats = LeaseStats {
    releases: AtomicU64::new(0),
    reacquire_waits: AtomicU64::new(0),
    reacquire_wait_nanos: AtomicU64::new(0),
    lent_nanos: AtomicU64::new(0),
    self_wakes: AtomicU64::new(0),
    parks: AtomicU64::new(0),
};

/// Process-wide totals across every lease, for experiments and diagnostics.
#[must_use]
pub fn global_stats() -> &'static LeaseStats {
    &GLOBAL_STATS
}

/// A task's claim on its executor vcore that can be lent back to the executor
/// while every gated stream of the task is parked on I/O.
///
/// The executor creates one lease per task, puts it into the task's
/// `SessionConfig` as an extension, and closes it when the task ends. Gated
/// streams ([`IoGateExec`](super::IoGateExec)) report when they park and
/// resume; once all of them have been parked for `release_threshold` the
/// permit is dropped, which frees the vcore for the poll loop to advertise.
/// The next stream to resume must acquire a vcore before it polls its input.
#[derive(Debug)]
pub struct VcoreLease {
    semaphore: Arc<Semaphore>,
    release_threshold: Duration,
    state: Mutex<State>,
    signal: watch::Sender<Signal>,
    stats: LeaseStats,
}

impl VcoreLease {
    /// Wrap `permit`, which must come from `semaphore`, in a lease and start
    /// its background releaser on the current tokio runtime.
    ///
    /// With `permit` set to `None` the lease starts lent: the first gated
    /// stream acquires a vcore before polling its input. The poll loop uses
    /// this so it never blocks on a vcore that a resuming task took back
    /// between advertising it and binding the new task.
    pub fn new(
        semaphore: Arc<Semaphore>,
        permit: Option<OwnedSemaphorePermit>,
        release_threshold: Duration,
    ) -> Arc<Self> {
        let lent_since = permit.is_none().then(Instant::now);
        let (signal, rx) = watch::channel(Signal::Busy);
        let lease = Arc::new(Self {
            semaphore,
            release_threshold,
            state: Mutex::new(State {
                permit,
                running: 0,
                registered: 0,
                io_hints: 0,
                cpu_holds: 0,
                epoch: 0,
                lent_since,
                closed: false,
            }),
            signal,
            stats: LeaseStats::default(),
        });
        tokio::spawn(run_releaser(Arc::downgrade(&lease), rx));
        lease
    }

    /// The lease attached to `ctx` by the executor, if any.
    #[must_use]
    pub fn from_task_context(ctx: &TaskContext) -> Option<Arc<Self>> {
        ctx.session_config().get_extension::<Self>()
    }

    /// Counters for this lease.
    pub fn stats(&self) -> &LeaseStats {
        &self.stats
    }

    /// Whether the task currently holds its vcore.
    pub fn is_held(&self) -> bool {
        self.lock().permit.is_some()
    }

    /// Return the vcore for good and stop the releaser. Streams that outlive
    /// the task keep running but never acquire a vcore again.
    pub fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        self.end_lent_period(&mut s);
        s.permit = None;
        self.signal.send_replace(Signal::Closed);
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned lock only means another stream panicked mid-update; the
        // counters are still usable for releasing the vcore.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.semaphore)
    }

    pub(crate) fn record_self_wake(&self) {
        self.stats.add(|s| &s.self_wakes, 1);
    }

    /// A gated stream is about to poll its input for the first time.
    pub(crate) fn stream_started(&self) -> Resume {
        let mut s = self.lock();
        s.registered += 1;
        self.resume_locked(&mut s)
    }

    /// A gated stream finished or was dropped while runnable.
    pub(crate) fn stream_finished(&self) {
        let mut s = self.lock();
        s.running = s.running.saturating_sub(1);
        s.registered = s.registered.saturating_sub(1);
    }

    /// A gated stream was dropped while parked.
    pub(crate) fn parked_stream_dropped(&self) {
        let mut s = self.lock();
        s.registered = s.registered.saturating_sub(1);
    }

    /// A gated stream returned `Pending` without waking itself.
    pub(crate) fn stream_parked(&self) {
        self.stats.add(|s| &s.parks, 1);
        let mut s = self.lock();
        s.running = s.running.saturating_sub(1);
        s.epoch += 1;
        self.publish(&s);
    }

    /// A parked stream was woken and polled again.
    pub(crate) fn stream_resumed(&self) -> Resume {
        let mut s = self.lock();
        self.resume_locked(&mut s)
    }

    fn resume_locked(&self, s: &mut State) -> Resume {
        s.running += 1;
        s.epoch += 1;
        self.publish(s);
        if s.closed {
            Resume::Closed
        } else if s.permit.is_some() {
            Resume::Held
        } else {
            Resume::Reacquire
        }
    }

    /// Store a permit acquired by a resuming stream. Returns the permit if
    /// another stream already re-acquired, so the caller drops the extra.
    pub(crate) fn install(
        &self,
        permit: OwnedSemaphorePermit,
        waited: Duration,
    ) -> Option<OwnedSemaphorePermit> {
        self.stats.add(|s| &s.reacquire_waits, 1);
        self.stats
            .add(|s| &s.reacquire_wait_nanos, duration_nanos(waited));
        let mut s = self.lock();
        if s.closed || s.permit.is_some() {
            return Some(permit);
        }
        self.end_lent_period(&mut s);
        s.permit = Some(permit);
        None
    }

    fn publish(&self, s: &State) {
        let signal = if s.closed {
            Signal::Closed
        } else if s.running == 0 && s.registered > 0 && s.cpu_holds == 0 {
            Signal::Parked {
                epoch: s.epoch,
                hinted: s.io_hints > 0,
            }
        } else {
            Signal::Busy
        };
        self.signal.send_if_modified(|current| {
            let changed = *current != signal;
            *current = signal;
            changed
        });
    }

    fn release_if_still_parked(&self, epoch: u64) {
        let mut s = self.lock();
        if s.closed || s.running != 0 || s.cpu_holds != 0 || s.epoch != epoch {
            return;
        }
        if s.permit.take().is_some() {
            s.lent_since = Some(Instant::now());
            self.stats.add(|st| &st.releases, 1);
        }
    }

    fn end_lent_period(&self, s: &mut State) {
        if let Some(since) = s.lent_since.take() {
            self.stats
                .add(|st| &st.lent_nanos, duration_nanos(since.elapsed()));
        }
    }

    fn adjust(&self, f: impl FnOnce(&mut State)) {
        let mut s = self.lock();
        f(&mut s);
        s.epoch += 1;
        self.publish(&s);
    }
}

fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

async fn run_releaser(lease: Weak<VcoreLease>, mut rx: watch::Receiver<Signal>) {
    loop {
        let signal = *rx.borrow_and_update();
        match signal {
            Signal::Closed => return,
            Signal::Busy => {
                if rx.changed().await.is_err() {
                    return;
                }
            }
            Signal::Parked { epoch, hinted } => {
                let Some(threshold) = lease.upgrade().map(|l| l.release_threshold) else {
                    return;
                };
                if !hinted {
                    tokio::select! {
                        () = tokio::time::sleep(threshold) => {}
                        changed = rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            continue;
                        }
                    }
                }
                let Some(lease) = lease.upgrade() else {
                    return;
                };
                lease.release_if_still_parked(epoch);
                drop(lease);
                if rx.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Await `fut`, telling the task's [`VcoreLease`] (if any) that the wait is
/// on external I/O.
///
/// Inside a gated stream this lends the vcore as soon as every gated stream
/// of the task is parked, without waiting for the release threshold. Use it
/// around requests to slow remote systems (HTTP origins, databases). Without
/// a lease, it simply awaits `fut`.
pub async fn io_wait<F: Future>(ctx: &TaskContext, fut: F) -> F::Output {
    let Some(lease) = VcoreLease::from_task_context(ctx) else {
        return fut.await;
    };
    lease.adjust(|s| s.io_hints += 1);
    let _guard = Guard::new(lease, |s| s.io_hints -= 1);
    fut.await
}

/// Await `fut`, keeping the task's vcore even if every gated stream parks.
///
/// Use it when a source returns `Pending` while CPU work it depends on runs
/// elsewhere (a spawned decode task, a blocking pool), so the lease does not
/// mistake that wait for I/O. Without a lease, it simply awaits `fut`.
pub async fn cpu_wait<F: Future>(ctx: &TaskContext, fut: F) -> F::Output {
    let Some(lease) = VcoreLease::from_task_context(ctx) else {
        return fut.await;
    };
    lease.adjust(|s| s.cpu_holds += 1);
    let _guard = Guard::new(lease, |s| s.cpu_holds -= 1);
    fut.await
}

/// Undoes an `adjust` when an [`io_wait`] or [`cpu_wait`] section ends or is
/// cancelled.
struct Guard {
    lease: Arc<VcoreLease>,
    undo: fn(&mut State),
}

impl Guard {
    fn new(lease: Arc<VcoreLease>, undo: fn(&mut State)) -> Self {
        Self { lease, undo }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.lease.adjust(self.undo);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn lease(threshold_ms: u64) -> (Arc<Semaphore>, Arc<VcoreLease>) {
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&sem)
            .acquire_owned()
            .await
            .expect("fresh semaphore has a permit");
        let lease = VcoreLease::new(
            Arc::clone(&sem),
            Some(permit),
            Duration::from_millis(threshold_ms),
        );
        (sem, lease)
    }

    #[tokio::test(start_paused = true)]
    async fn parked_stream_lends_vcore_after_threshold() {
        let (sem, lease) = lease(50).await;
        let _ = lease.stream_started();
        lease.stream_parked();
        tokio::time::sleep(Duration::from_millis(49)).await;
        assert!(lease.is_held(), "released before the threshold");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(!lease.is_held(), "not released after the threshold");
        assert_eq!(sem.available_permits(), 1);
        assert_eq!(lease.stats().releases.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_before_threshold_keeps_vcore() {
        let (sem, lease) = lease(50).await;
        let _ = lease.stream_started();
        lease.stream_parked();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(lease.stream_resumed(), Resume::Held);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(lease.is_held());
        assert_eq!(sem.available_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn one_runnable_stream_keeps_vcore() {
        let (_sem, lease) = lease(10).await;
        let _ = lease.stream_started();
        let _ = lease.stream_started();
        lease.stream_parked();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            lease.is_held(),
            "released while a sibling stream was runnable"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resume_after_release_requires_reacquire() {
        let (sem, lease) = lease(10).await;
        let _ = lease.stream_started();
        lease.stream_parked();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let borrowed = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("lent vcore is available");
        assert_eq!(lease.stream_resumed(), Resume::Reacquire);
        drop(borrowed);
        let permit = sem.acquire_owned().await.expect("permit returned");
        assert!(lease.install(permit, Duration::ZERO).is_none());
        assert!(lease.is_held());
    }

    #[tokio::test(start_paused = true)]
    async fn cpu_wait_blocks_release() {
        let (_sem, lease) = lease(10).await;
        let ctx = TaskContext::default().with_session_config(
            datafusion::prelude::SessionConfig::new().with_extension(Arc::clone(&lease)),
        );
        let _ = lease.stream_started();
        lease.stream_parked();
        cpu_wait(&ctx, tokio::time::sleep(Duration::from_millis(100))).await;
        assert!(lease.is_held(), "released during cpu_wait");
    }

    #[tokio::test(start_paused = true)]
    async fn io_wait_skips_threshold() {
        let (_sem, lease) = lease(10_000).await;
        let ctx = TaskContext::default().with_session_config(
            datafusion::prelude::SessionConfig::new().with_extension(Arc::clone(&lease)),
        );
        let _ = lease.stream_started();
        let observer = Arc::clone(&lease);
        io_wait(&ctx, async move {
            observer.stream_parked();
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert!(!observer.is_held(), "io_wait did not release immediately");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn lease_started_lent_requires_acquire_on_first_poll() {
        let sem = Arc::new(Semaphore::new(1));
        let lease = VcoreLease::new(Arc::clone(&sem), None, Duration::from_millis(10));
        assert!(!lease.is_held());
        assert_eq!(lease.stream_started(), Resume::Reacquire);
    }

    #[tokio::test(start_paused = true)]
    async fn close_returns_vcore_and_stops_reacquire() {
        let (sem, lease) = lease(10).await;
        let _ = lease.stream_started();
        lease.close();
        assert_eq!(sem.available_permits(), 1);
        lease.stream_parked();
        assert_eq!(lease.stream_resumed(), Resume::Closed);
    }
}
