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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, watch};

/// What the background releaser is told about the permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    /// At least one await point is running; keep the vcore.
    Running,
    /// Every await point is suspended. `epoch` identifies this suspension.
    Suspended { epoch: u64 },
    /// The task finished; the releaser exits.
    Closed,
}

#[derive(Debug)]
struct State {
    permit: Option<OwnedSemaphorePermit>,
    /// Await points that are being polled or will be polled without a wake-up.
    running: usize,
    /// Await points that started and have not finished.
    registered: usize,
    epoch: u64,
    released_at: Option<Instant>,
    closed: bool,
}

/// What an await point must do before it polls its input again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resume {
    /// The task still holds its vcore.
    Held,
    /// The vcore was released; acquire one before polling the input.
    Acquire,
    /// The task has finished; run without a vcore.
    Closed,
}

/// Counters for one permit, also summed into process-wide totals by
/// [`global_stats`].
#[derive(Debug, Default)]
pub struct SuspendStats {
    /// Times an await point suspended (`Pending` that was not a yield).
    pub suspensions: AtomicU64,
    /// `Pending` results that were cooperative yields, not suspensions.
    pub yields: AtomicU64,
    /// Times the vcore was released while the task was suspended.
    pub releases: AtomicU64,
    /// Times a resuming task waited to get a vcore back.
    pub resume_waits: AtomicU64,
    /// Total nanoseconds resuming tasks waited for a vcore.
    pub resume_wait_nanos: AtomicU64,
    /// Total nanoseconds vcores were released.
    pub released_nanos: AtomicU64,
}

impl SuspendStats {
    fn add(&self, field: impl Fn(&Self) -> &AtomicU64, n: u64) {
        field(self).fetch_add(n, Ordering::Relaxed);
        field(&GLOBAL_STATS).fetch_add(n, Ordering::Relaxed);
    }
}

static GLOBAL_STATS: SuspendStats = SuspendStats {
    suspensions: AtomicU64::new(0),
    yields: AtomicU64::new(0),
    releases: AtomicU64::new(0),
    resume_waits: AtomicU64::new(0),
    resume_wait_nanos: AtomicU64::new(0),
    released_nanos: AtomicU64::new(0),
};

/// Process-wide totals across every permit, for experiments and diagnostics.
#[must_use]
pub fn global_stats() -> &'static SuspendStats {
    &GLOBAL_STATS
}

/// A task's executor vcore, released while the task is suspended at its await
/// points and acquired again before it resumes.
///
/// The executor creates one per task, acquires it before the task runs, puts it
/// into the task's `SessionConfig` as an extension, and closes it when the task
/// ends. [`AwaitPointExec`](super::AwaitPointExec) streams call
/// [`suspend`](Self::suspend) when they return `Pending` and
/// [`resume`](Self::resume) when they are polled again. Once every await point
/// of the task has been suspended for `suspend_after`, the vcore is released,
/// so the executor can run another task on it.
#[derive(Debug)]
pub struct VcorePermit {
    semaphore: Arc<Semaphore>,
    suspend_after: Duration,
    state: Mutex<State>,
    signal: watch::Sender<Signal>,
    stats: SuspendStats,
}

impl VcorePermit {
    /// Create a permit drawing from `semaphore` and start its background
    /// releaser on the current tokio runtime. It holds no vcore until
    /// [`acquire`](Self::acquire) succeeds.
    pub fn new(semaphore: Arc<Semaphore>, suspend_after: Duration) -> Arc<Self> {
        let (signal, rx) = watch::channel(Signal::Running);
        let permit = Arc::new(Self {
            semaphore,
            suspend_after,
            state: Mutex::new(State {
                permit: None,
                running: 0,
                registered: 0,
                epoch: 0,
                released_at: None,
                closed: false,
            }),
            signal,
            stats: SuspendStats::default(),
        });
        tokio::spawn(run_releaser(Arc::downgrade(&permit), rx));
        permit
    }

    /// The permit the executor attached to `ctx`, if any.
    #[must_use]
    pub fn from_task_context(ctx: &TaskContext) -> Option<Arc<Self>> {
        ctx.session_config().get_extension::<Self>()
    }

    /// Counters for this permit.
    pub fn stats(&self) -> &SuspendStats {
        &self.stats
    }

    /// Whether the task currently holds its vcore.
    pub fn is_held(&self) -> bool {
        self.lock().permit.is_some()
    }

    /// Wait for the task's vcore. The executor calls this before the task runs.
    ///
    /// # Errors
    ///
    /// Fails if the executor's vcore semaphore was closed (shutdown).
    pub async fn acquire(&self) -> Result<(), AcquireError> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
        let mut s = self.lock();
        if !s.closed && s.permit.is_none() {
            s.permit = Some(permit);
        }
        Ok(())
    }

    /// Return the vcore for good and stop the releaser. Await points that
    /// outlive the task keep running but never acquire a vcore again.
    pub fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        self.end_release(&mut s);
        s.permit = None;
        self.signal.send_replace(Signal::Closed);
    }

    /// An await point returned `Pending` without having scheduled its own
    /// wake-up: it is waiting on something outside the task.
    pub(crate) fn suspend(&self) {
        self.stats.add(|s| &s.suspensions, 1);
        let mut s = self.lock();
        s.running = s.running.saturating_sub(1);
        s.epoch += 1;
        self.publish(&s);
    }

    /// A suspended await point was woken and is about to poll its input.
    pub(crate) fn resume(&self) -> Resume {
        let mut s = self.lock();
        self.mark_running(&mut s)
    }

    /// An await point is about to poll its input for the first time.
    pub(crate) fn start(&self) -> Resume {
        let mut s = self.lock();
        s.registered += 1;
        self.mark_running(&mut s)
    }

    /// An await point finished or was dropped while running.
    pub(crate) fn finish(&self) {
        let mut s = self.lock();
        s.running = s.running.saturating_sub(1);
        s.registered = s.registered.saturating_sub(1);
    }

    /// An await point was dropped while suspended.
    pub(crate) fn drop_suspended(&self) {
        let mut s = self.lock();
        s.registered = s.registered.saturating_sub(1);
    }

    pub(crate) fn record_yield(&self) {
        self.stats.add(|s| &s.yields, 1);
    }

    pub(crate) fn semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.semaphore)
    }

    /// Store a vcore acquired by a resuming await point. Returns it if another
    /// await point already got one back, so the caller drops the extra.
    pub(crate) fn install(
        &self,
        permit: OwnedSemaphorePermit,
        waited: Duration,
    ) -> Option<OwnedSemaphorePermit> {
        self.stats.add(|s| &s.resume_waits, 1);
        self.stats
            .add(|s| &s.resume_wait_nanos, duration_nanos(waited));
        let mut s = self.lock();
        if s.closed || s.permit.is_some() {
            return Some(permit);
        }
        self.end_release(&mut s);
        s.permit = Some(permit);
        None
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned lock only means another await point panicked mid-update;
        // the counters are still usable for releasing the vcore.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn mark_running(&self, s: &mut State) -> Resume {
        s.running += 1;
        s.epoch += 1;
        self.publish(s);
        if s.closed {
            Resume::Closed
        } else if s.permit.is_some() {
            Resume::Held
        } else {
            Resume::Acquire
        }
    }

    fn publish(&self, s: &State) {
        let signal = if s.closed {
            Signal::Closed
        } else if s.running == 0 && s.registered > 0 {
            Signal::Suspended { epoch: s.epoch }
        } else {
            Signal::Running
        };
        self.signal.send_if_modified(|current| {
            let changed = *current != signal;
            *current = signal;
            changed
        });
    }

    fn release_if_still_suspended(&self, epoch: u64) {
        let mut s = self.lock();
        if s.closed || s.running != 0 || s.epoch != epoch {
            return;
        }
        if s.permit.take().is_some() {
            s.released_at = Some(Instant::now());
            self.stats.add(|st| &st.releases, 1);
        }
    }

    fn end_release(&self, s: &mut State) {
        if let Some(since) = s.released_at.take() {
            self.stats
                .add(|st| &st.released_nanos, duration_nanos(since.elapsed()));
        }
    }
}

fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

async fn run_releaser(permit: Weak<VcorePermit>, mut rx: watch::Receiver<Signal>) {
    loop {
        let signal = *rx.borrow_and_update();
        match signal {
            Signal::Closed => return,
            Signal::Running => {
                if rx.changed().await.is_err() {
                    return;
                }
            }
            Signal::Suspended { epoch } => {
                let Some(suspend_after) = permit.upgrade().map(|p| p.suspend_after)
                else {
                    return;
                };
                tokio::select! {
                    () = tokio::time::sleep(suspend_after) => {}
                    changed = rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue;
                    }
                }
                let Some(p) = permit.upgrade() else {
                    return;
                };
                p.release_if_still_suspended(epoch);
                drop(p);
                if rx.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn permit(suspend_after_ms: u64) -> (Arc<Semaphore>, Arc<VcorePermit>) {
        let sem = Arc::new(Semaphore::new(1));
        let permit =
            VcorePermit::new(Arc::clone(&sem), Duration::from_millis(suspend_after_ms));
        permit
            .acquire()
            .await
            .expect("fresh semaphore has a permit");
        (sem, permit)
    }

    #[tokio::test(start_paused = true)]
    async fn holds_no_vcore_until_acquired() {
        let sem = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(Arc::clone(&sem), Duration::from_millis(10));
        assert!(!permit.is_held());
        assert_eq!(sem.available_permits(), 1);
        permit.acquire().await.expect("permit");
        assert!(permit.is_held());
        assert_eq!(sem.available_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn suspended_task_releases_vcore_after_suspend_after() {
        let (sem, permit) = permit(50).await;
        let _ = permit.start();
        permit.suspend();
        tokio::time::sleep(Duration::from_millis(49)).await;
        assert!(permit.is_held(), "released before suspend_after");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(!permit.is_held(), "not released after suspend_after");
        assert_eq!(sem.available_permits(), 1);
        assert_eq!(permit.stats().releases.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_before_suspend_after_keeps_vcore() {
        let (sem, permit) = permit(50).await;
        let _ = permit.start();
        permit.suspend();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(permit.resume(), Resume::Held);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(permit.is_held());
        assert_eq!(sem.available_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn one_running_await_point_keeps_vcore() {
        let (_sem, permit) = permit(10).await;
        let _ = permit.start();
        let _ = permit.start();
        permit.suspend();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(permit.is_held(), "released while a sibling was running");
    }

    #[tokio::test(start_paused = true)]
    async fn resume_after_release_must_acquire() {
        let (sem, permit) = permit(10).await;
        let _ = permit.start();
        permit.suspend();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let borrowed = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("released vcore is available");
        assert_eq!(permit.resume(), Resume::Acquire);
        drop(borrowed);
        let vcore = sem.acquire_owned().await.expect("vcore returned");
        assert!(permit.install(vcore, Duration::ZERO).is_none());
        assert!(permit.is_held());
    }

    #[tokio::test(start_paused = true)]
    async fn close_returns_vcore_and_stops_acquiring() {
        let (sem, permit) = permit(10).await;
        let _ = permit.start();
        permit.close();
        assert_eq!(sem.available_permits(), 1);
        permit.suspend();
        assert_eq!(permit.resume(), Resume::Closed);
    }
}
