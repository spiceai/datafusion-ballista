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

//! Adaptive task-slot controller for pull-mode executors.
//!
//! A pull-mode executor admits at most as many concurrent tasks as its slot
//! semaphore has permits (see [`crate::execution_loop::poll_loop`]). A fixed
//! count of one slot per core leaves cores idle when tasks wait on I/O, and no
//! fixed count suits a workload that moves between I/O-bound and CPU-bound.
//! [`AdaptiveSlotsConfig::start`] drives that semaphore at runtime so the
//! executor holds its CPU utilization near a setpoint (80% by default)
//! whenever there is work to run. It applies to pull mode only; push mode
//! (`executor_server`) has no slot semaphore.
//!
//! # Control law
//!
//! The control basis is Gandhi, Tilbury, Diao, Hellerstein and Parekh,
//! [*MIMO Control of an Apache Web Server: Modeling and Controller
//! Design*](https://cs.uwaterloo.ca/~brecht/servers/readings-new/acc02final2.pdf),
//! ACC 2002, which regulates CPU utilization by adjusting a worker-pool
//! concurrency limit (`MaxClients`). It models limit to utilization as first
//! order, uses integral action, and finds sample intervals of about 5 s or more
//! average out CPU noise.
//!
//! Utilization is `u ~= g * slots / cores`, where `g` is the CPU a slot uses
//! (a task that waits a fraction `w` of the time uses `1 - w` of a core). `g`
//! varies with the workload, so each interval it is estimated from the
//! measurement (`g = u * cores / slots`) and the controller steps toward the
//! setpoint `u*`:
//!
//! ```text
//! target = slots * (1 + lambda * (u* / u - 1))
//! ```
//!
//! This is integral control with gain `K = lambda / g`. With `lambda` in
//! `(0, 1]` the closed-loop pole is `1 - lambda`, in `[0, 1)`, so the loop
//! converges without overshoot or oscillation.
//!
//! * Anti-windup: slots only grow when they were saturated (no free permit)
//!   for most of the interval, so an idle executor never grows.
//! * Memory guard: slots do not grow while the executor reports memory
//!   pressure (spilling). Shrinking is still allowed.
//! * Growth per interval is capped (at 2x) so a near-zero `u` cannot explode.
//! * A deadband around `u*` holds the count, so noise does not make it chatter.
//! * Overload: if `u` stays at or above the overload threshold for several
//!   consecutive intervals, slots are cut multiplicatively, following the
//!   multiplicative decrease of Chiu and Jain, [*Analysis of the Increase and
//!   Decrease Algorithms for Congestion Avoidance in Computer
//!   Networks*](https://doi.org/10.1016/0169-7552(89)90019-6), 1989. This is
//!   needed because measured utilization saturates at 1 and says nothing about
//!   how far over the setpoint the load is.
//! * Slots stay within `[floor, ceiling]`, and are integers: growth rounds up
//!   and shrinking rounds down, each by at least one slot once outside the
//!   deadband.
//!
//! # Actuator
//!
//! Growing adds permits. Shrinking takes permits out of circulation as they
//! become free, with a non-blocking `try_acquire` followed by `forget`, and
//! carries any unmet deficit to the next sample. Running tasks are never
//! cancelled or waited on, so the semaphore's `available_permits` already
//! reflects the effective slot count that the poll loop reports to the
//! scheduler.
//!
//! The controller is created closed: nothing touches the semaphore until
//! [`AdaptiveSlotsConfig::start`], which grants the `floor` permits and spawns
//! the control task. An embedder that creates its semaphore with zero permits
//! and opens it once its object stores are bound calls `start` at that point.
//! The task-runner thread pool stays sized by the CPU budget (see
//! `with_task_runner_threads`), so growing slots never adds threads.

use ballista_core::error::{BallistaError, Result};
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::{CancellationToken, DropGuard};

/// A source of CPU utilization as a fraction of the executor's CPU budget.
///
/// Implement this to supply a cgroup-aware measurement; [`ProcessCpu`] is the
/// default.
pub trait CpuUtilization: Send + 'static {
    /// Returns utilization (`1.0` = the whole budget busy) averaged since the
    /// previous call, or `None` if it cannot be measured, in which case the
    /// controller holds its current slot count.
    fn sample(&mut self) -> Option<f64>;
}

/// Returns `true` while the executor is under memory pressure (spilling).
pub type MemoryPressure = Arc<dyn Fn() -> bool + Send + Sync>;

/// [`CpuUtilization`] of this process: CPU time consumed (user + system, all
/// threads) divided by wall time and the CPU budget.
///
/// Measurable on unix only; elsewhere [`CpuUtilization::sample`] returns `None`.
#[derive(Debug)]
pub struct ProcessCpu {
    budget_cores: f64,
    last: Option<(std::time::Instant, f64)>,
}

impl ProcessCpu {
    /// Creates a sampler whose utilization is relative to `budget_cores` CPUs.
    #[must_use]
    pub fn new(budget_cores: f64) -> Self {
        Self {
            budget_cores,
            last: process_cpu_seconds().map(|c| (std::time::Instant::now(), c)),
        }
    }
}

impl CpuUtilization for ProcessCpu {
    fn sample(&mut self) -> Option<f64> {
        let cpu = process_cpu_seconds()?;
        let now = std::time::Instant::now();
        let (then, cpu_then) = self.last.replace((now, cpu))?;
        let wall = now.duration_since(then).as_secs_f64();
        (wall > 0.0 && self.budget_cores > 0.0)
            .then(|| ((cpu - cpu_then) / (wall * self.budget_cores)).max(0.0))
    }
}

#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fully initializes `usage` when it returns 0.
    let usage = unsafe {
        (libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) == 0)
            .then(|| usage.assume_init())?
    };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    Some(secs(usage.ru_utime) + secs(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

/// Configuration of the adaptive slot controller.
#[derive(Debug, Clone)]
pub struct AdaptiveSlotsConfig {
    /// Fewest slots, granted when the controller starts. At least 1.
    pub floor: usize,
    /// Most slots.
    pub ceiling: usize,
    /// CPU utilization the controller holds (`u*`).
    pub setpoint: f64,
    /// Time between slot adjustments. The paper finds about 5 s or more
    /// averages out CPU noise.
    pub interval: Duration,
    /// Time between saturation samples and shrink reconciliation.
    pub sample_period: Duration,
    /// Damping `lambda` of the integral step, in `(0, 1]`.
    pub damping: f64,
    /// Utilization band `u* +/- deadband` in which the slot count is held.
    pub deadband: f64,
    /// Utilization at or above which an interval counts as overloaded.
    pub overload_threshold: f64,
    /// Consecutive overloaded intervals that trigger the multiplicative cut.
    pub overload_intervals: u32,
    /// Factor `beta` slots are multiplied by on overload, in `(0, 1)`.
    pub overload_backoff: f64,
    /// Fraction of an interval's samples that must find every slot busy
    /// before slots may grow.
    pub saturation_threshold: f64,
    /// Largest factor slots may grow by in one interval.
    pub max_growth: f64,
}

impl AdaptiveSlotsConfig {
    /// Defaults with the given bounds: setpoint 0.8, 5 s interval, 250 ms
    /// samples, damping 0.5, deadband 0.05, overload at 0.95 for 2 intervals
    /// with a 0.5 back-off, growth only when saturated for 75% of samples,
    /// at most 2x growth per interval.
    #[must_use]
    pub fn new(floor: usize, ceiling: usize) -> Self {
        Self {
            floor,
            ceiling,
            setpoint: 0.8,
            interval: Duration::from_secs(5),
            sample_period: Duration::from_millis(250),
            damping: 0.5,
            deadband: 0.05,
            overload_threshold: 0.95,
            overload_intervals: 2,
            overload_backoff: 0.5,
            saturation_threshold: 0.75,
            max_growth: 2.0,
        }
    }

    /// Checks the configuration is consistent.
    ///
    /// # Errors
    ///
    /// Returns an error if `floor` is 0 or above `ceiling`, a period is zero or
    /// the interval is shorter than the sample period, or a ratio is out of
    /// range.
    pub fn validate(&self) -> Result<()> {
        let check = |ok: bool, what: &str| {
            ok.then_some(()).ok_or_else(|| {
                BallistaError::General(format!("invalid adaptive slots config: {what}"))
            })
        };
        check(self.floor >= 1, "floor must be at least 1")?;
        check(self.floor <= self.ceiling, "floor must not exceed ceiling")?;
        check(
            !self.sample_period.is_zero(),
            "sample_period must be non-zero",
        )?;
        check(
            self.interval >= self.sample_period,
            "interval must be at least sample_period",
        )?;
        check(
            self.setpoint > 0.0 && self.setpoint < 1.0,
            "setpoint must be in (0, 1)",
        )?;
        check(
            self.damping > 0.0 && self.damping <= 1.0,
            "damping must be in (0, 1]",
        )?;
        check(
            self.deadband >= 0.0
                && self.setpoint - self.deadband > 0.0
                && self.setpoint + self.deadband < self.overload_threshold,
            "deadband must keep setpoint +/- deadband inside (0, overload_threshold)",
        )?;
        check(
            self.overload_intervals >= 1,
            "overload_intervals must be at least 1",
        )?;
        check(
            self.overload_backoff > 0.0 && self.overload_backoff < 1.0,
            "overload_backoff must be in (0, 1)",
        )?;
        check(
            self.saturation_threshold > 0.0 && self.saturation_threshold <= 1.0,
            "saturation_threshold must be in (0, 1]",
        )?;
        check(self.max_growth > 1.0, "max_growth must exceed 1")
    }

    /// One control step: the new slot count for `measurement`. Pure and
    /// deterministic; updates `state` in place.
    pub fn step(&self, state: &mut ControllerState, m: &Measurement) -> usize {
        let slots = state.slots;
        let u = m.cpu;
        if !u.is_finite() || u < 0.0 {
            return slots;
        }

        if u >= self.overload_threshold {
            state.overload_streak += 1;
            if state.overload_streak >= self.overload_intervals {
                state.overload_streak = 0;
                let cut = (slots as f64 * self.overload_backoff).floor() as usize;
                state.slots = cut.clamp(self.floor, self.ceiling);
                return state.slots;
            }
        } else {
            state.overload_streak = 0;
        }

        if (u - self.setpoint).abs() <= self.deadband {
            return slots;
        }

        // Floor `u` so a near-idle executor yields a large, but finite, ratio.
        let target =
            slots as f64 * (1.0 + self.damping * (self.setpoint / u.max(1e-3) - 1.0));
        let next = if u < self.setpoint {
            if m.saturation < self.saturation_threshold || m.memory_pressure {
                return slots;
            }
            let capped = target.min(slots as f64 * self.max_growth);
            (capped.ceil() as usize).max(slots + 1)
        } else {
            (target.floor() as usize).min(slots.saturating_sub(1))
        };
        state.slots = next.clamp(self.floor, self.ceiling);
        state.slots
    }

    /// Grants `floor` permits on `semaphore` and spawns the control task on
    /// `runtime`. The task stops when the returned handle is dropped.
    ///
    /// `semaphore` is the one passed to `poll_loop` as `free_vcores`; it
    /// should hold no permits when this is called, as the controller owns its
    /// permit count from here on.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn start(
        self,
        semaphore: Arc<Semaphore>,
        cpu: impl CpuUtilization,
        memory_pressure: Option<MemoryPressure>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<AdaptiveSlots> {
        self.validate()?;
        let stats = Arc::new(Stats {
            slots: AtomicUsize::new(self.floor),
            floor: self.floor,
            ceiling: self.ceiling,
        });
        semaphore.add_permits(self.floor);

        let token = CancellationToken::new();
        let controller = Controller {
            state: ControllerState::new(self.floor),
            config: self,
            semaphore,
            stats: stats.clone(),
            cpu: Box::new(cpu),
            memory_pressure,
            revoke_pending: 0,
        };
        runtime.spawn(controller.run(token.clone()));
        Ok(AdaptiveSlots {
            stats,
            _guard: token.drop_guard(),
        })
    }
}

/// Mutable state of the control law.
#[derive(Debug, Clone)]
pub struct ControllerState {
    /// Current target slot count.
    pub slots: usize,
    /// Consecutive overloaded intervals so far.
    pub overload_streak: u32,
}

impl ControllerState {
    /// State at `slots` with no overload history.
    #[must_use]
    pub fn new(slots: usize) -> Self {
        Self {
            slots,
            overload_streak: 0,
        }
    }
}

/// What the controller observed over one interval.
#[derive(Debug, Clone, Copy)]
pub struct Measurement {
    /// CPU utilization as a fraction of the CPU budget.
    pub cpu: f64,
    /// Fraction of samples that found every slot busy.
    pub saturation: f64,
    /// Whether the executor was under memory pressure at any sample.
    pub memory_pressure: bool,
}

#[derive(Debug)]
struct Stats {
    slots: AtomicUsize,
    floor: usize,
    ceiling: usize,
}

/// Handle to a running controller; dropping it stops the control task. Slots
/// already granted stay in place.
#[derive(Debug)]
pub struct AdaptiveSlots {
    stats: Arc<Stats>,
    _guard: DropGuard,
}

impl AdaptiveSlots {
    /// Current target slot count, which `executor_task_slots` can report.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.stats.slots.load(Ordering::Relaxed)
    }

    /// Fewest slots.
    #[must_use]
    pub fn floor(&self) -> usize {
        self.stats.floor
    }

    /// Most slots.
    #[must_use]
    pub fn ceiling(&self) -> usize {
        self.stats.ceiling
    }
}

struct Controller {
    config: AdaptiveSlotsConfig,
    state: ControllerState,
    semaphore: Arc<Semaphore>,
    stats: Arc<Stats>,
    cpu: Box<dyn CpuUtilization>,
    memory_pressure: Option<MemoryPressure>,
    /// Permits still to be taken out of circulation after a shrink.
    revoke_pending: usize,
}

impl Controller {
    async fn run(mut self, token: CancellationToken) {
        let samples_per_interval = (self.config.interval.as_nanos()
            / self.config.sample_period.as_nanos())
        .max(1) as u32;
        let mut ticker = interval(self.config.sample_period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes immediately.
        ticker.tick().await;
        // Discard utilization accrued before the controller was running.
        let _ = self.cpu.sample();

        let (mut samples, mut saturated, mut pressured) = (0u32, 0u32, false);
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                _ = ticker.tick() => {}
            }
            self.reconcile_revoked();
            samples += 1;
            saturated += u32::from(self.semaphore.available_permits() == 0);
            pressured |= self.memory_pressure.as_ref().is_some_and(|f| f());
            if samples < samples_per_interval {
                continue;
            }
            let measurement = self.cpu.sample().map(|cpu| Measurement {
                cpu,
                saturation: f64::from(saturated) / f64::from(samples),
                memory_pressure: pressured,
            });
            (samples, saturated, pressured) = (0, 0, false);
            if let Some(m) = measurement {
                let from = self.state.slots;
                let to = self.config.step(&mut self.state, &m);
                if to != from {
                    debug!(
                        "adaptive task slots {from} -> {to} (cpu {:.2}, saturation {:.2}, memory pressure {})",
                        m.cpu, m.saturation, m.memory_pressure
                    );
                    self.apply(from, to);
                }
            }
        }
    }

    fn apply(&mut self, from: usize, to: usize) {
        if to > from {
            let grow = to - from;
            // Growth first cancels any shrink that has not completed.
            let cancelled = grow.min(self.revoke_pending);
            self.revoke_pending -= cancelled;
            self.semaphore.add_permits(grow - cancelled);
        } else {
            self.revoke_pending += from - to;
            self.reconcile_revoked();
        }
        self.stats.slots.store(to, Ordering::Relaxed);
    }

    /// Takes as many pending permits out of circulation as are free now;
    /// permits held by running tasks are taken as they are released.
    fn reconcile_revoked(&mut self) {
        let take = self.revoke_pending.min(self.semaphore.available_permits());
        if take == 0 {
            return;
        }
        let Ok(take_u32) = u32::try_from(take) else {
            return;
        };
        if let Ok(permits) = self.semaphore.try_acquire_many(take_u32) {
            permits.forget();
            self.revoke_pending -= take;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn cfg() -> AdaptiveSlotsConfig {
        AdaptiveSlotsConfig::new(8, 64)
    }

    fn m(cpu: f64, saturation: f64) -> Measurement {
        Measurement {
            cpu,
            saturation,
            memory_pressure: false,
        }
    }

    fn step_from(slots: usize, m: &Measurement) -> usize {
        cfg().step(&mut ControllerState::new(slots), m)
    }

    #[test]
    fn validate_rejects_bad_bounds() {
        assert!(AdaptiveSlotsConfig::new(0, 8).validate().is_err());
        assert!(AdaptiveSlotsConfig::new(9, 8).validate().is_err());
        assert!(AdaptiveSlotsConfig::new(8, 8).validate().is_ok());
    }

    #[test]
    fn deadband_holds() {
        for u in [0.76, 0.8, 0.84] {
            assert_eq!(step_from(20, &m(u, 1.0)), 20);
        }
    }

    #[test]
    fn grows_when_saturated_and_below_setpoint() {
        // 20 * (1 + 0.5 * (0.8/0.7 - 1)) = 21.43 -> rounds up
        assert_eq!(step_from(20, &m(0.7, 1.0)), 22);
    }

    #[test]
    fn does_not_grow_when_not_saturated() {
        assert_eq!(step_from(20, &m(0.2, 0.5)), 20);
    }

    #[test]
    fn does_not_grow_under_memory_pressure_but_shrinks() {
        let mut p = m(0.2, 1.0);
        p.memory_pressure = true;
        assert_eq!(step_from(20, &p), 20);
        p.cpu = 0.9;
        assert!(step_from(20, &p) < 20);
    }

    #[test]
    fn growth_is_capped_per_interval() {
        // An unbounded step would be 10 * (1 + 0.5 * (800 - 1)).
        assert_eq!(step_from(10, &m(0.001, 1.0)), 20);
        assert_eq!(step_from(10, &m(0.0, 1.0)), 20);
    }

    #[test]
    fn shrink_rounds_down_by_at_least_one() {
        // 20 * (1 + 0.5 * (0.8/0.9 - 1)) = 18.9 -> 18
        assert_eq!(step_from(20, &m(0.9, 0.0)), 18);
        // 9 * (1 + 0.5 * (0.8/0.86 - 1)) = 8.68 -> 8
        assert_eq!(step_from(9, &m(0.86, 0.0)), 8);
    }

    #[test]
    fn clamps_to_floor_and_ceiling() {
        assert_eq!(step_from(8, &m(0.9, 0.0)), 8);
        assert_eq!(step_from(60, &m(0.1, 1.0)), 64);
        assert_eq!(step_from(64, &m(0.1, 1.0)), 64);
    }

    #[test]
    fn overload_cuts_multiplicatively_after_consecutive_intervals() {
        let c = cfg();
        let mut s = ControllerState::new(40);
        // First overloaded interval takes the integral step (cpu saturates at 1).
        assert_eq!(c.step(&mut s, &m(1.0, 1.0)), 36);
        assert_eq!(s.overload_streak, 1);
        assert_eq!(c.step(&mut s, &m(1.0, 1.0)), 18);
        assert_eq!(s.overload_streak, 0);
        // The cut never goes below the floor.
        let mut s = ControllerState::new(10);
        c.step(&mut s, &m(1.0, 1.0));
        assert_eq!(c.step(&mut s, &m(1.0, 1.0)), 8);
    }

    #[test]
    fn overload_streak_resets_below_threshold() {
        let c = cfg();
        let mut s = ControllerState::new(40);
        c.step(&mut s, &m(1.0, 1.0));
        c.step(&mut s, &m(0.8, 1.0));
        assert_eq!(s.overload_streak, 0);
    }

    #[test]
    fn non_finite_cpu_holds() {
        assert_eq!(step_from(20, &m(f64::NAN, 1.0)), 20);
    }

    const CORES: f64 = 8.0;

    /// Deterministic noise in `[-0.01, 0.01]`.
    fn noise(i: usize) -> f64 {
        ((i * 7919) % 21) as f64 / 1000.0 - 0.01
    }

    /// Runs the controller against `u = min(1, g * slots / cores) + noise`.
    /// `phases` is `(intervals, g, saturated)`. Returns `(slots, cpu)` per interval.
    fn simulate(phases: &[(usize, f64, bool)]) -> Vec<(usize, f64)> {
        let c = cfg();
        let mut state = ControllerState::new(c.floor);
        let mut trace = Vec::new();
        for &(n, g, saturated) in phases {
            for _ in 0..n {
                let i = trace.len();
                let cpu = (g * state.slots as f64 / CORES + noise(i)).clamp(0.0, 1.0);
                let measurement = m(cpu, if saturated { 1.0 } else { 0.0 });
                c.step(&mut state, &measurement);
                assert!((c.floor..=c.ceiling).contains(&state.slots));
                trace.push((state.slots, cpu));
            }
        }
        trace
    }

    fn direction_changes(trace: &[(usize, f64)]) -> usize {
        let mut last = 0i64;
        let mut changes = 0;
        for w in trace.windows(2) {
            let d = (w[1].0 as i64 - w[0].0 as i64).signum();
            if d != 0 {
                changes += usize::from(last != 0 && d != last);
                last = d;
            }
        }
        changes
    }

    #[test]
    fn sim_cpu_bound_stays_at_floor() {
        let trace = simulate(&[(40, 1.0, true)]);
        assert!(trace.iter().all(|&(s, _)| s == 8));
    }

    #[test]
    fn sim_io_bound_converges_to_setpoint_without_oscillating() {
        let trace = simulate(&[(40, 0.2, true)]);
        // 0.8 / 0.2 * 8 = 32 slots; the deadband accepts 30..=34.
        let (slots, _) = trace[39];
        assert!((30..=34).contains(&slots), "slots {slots}");
        let settled = trace.iter().position(|&(s, _)| s >= 30).expect("converges");
        assert!(settled <= 8, "settled after {settled} intervals");
        assert!(
            trace[settled + 1..]
                .iter()
                .all(|&(_, u)| (u - 0.8).abs() <= 0.06)
        );
        assert_eq!(direction_changes(&trace), 0);
    }

    #[test]
    fn sim_very_io_bound_saturates_at_ceiling() {
        let trace = simulate(&[(40, 0.05, true)]);
        assert_eq!(trace[39].0, 64);
        assert_eq!(direction_changes(&trace), 0);
    }

    #[test]
    fn sim_idle_never_grows() {
        let trace = simulate(&[(40, 0.05, false)]);
        assert!(trace.iter().all(|&(s, _)| s == 8));
        // Idle after growth holds the count rather than growing further.
        let trace = simulate(&[(20, 0.2, true), (20, 0.2, false)]);
        assert_eq!(trace[19].0, trace[39].0);
    }

    #[test]
    fn sim_phase_switch_to_cpu_bound_returns_to_floor() {
        let trace = simulate(&[(30, 0.2, true), (30, 1.0, true)]);
        assert!(trace[29].0 >= 30);
        assert_eq!(trace[59].0, 8);
    }

    #[cfg(unix)]
    #[test]
    fn process_cpu_measures_busy_work() {
        let mut cpu = ProcessCpu::new(1.0);
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(100) {
            std::hint::black_box(0u64.wrapping_add(1));
        }
        let u = cpu.sample().expect("measurable on unix");
        assert!(u > 0.3, "busy loop should use CPU, got {u}");
    }

    struct FakeCpu(Arc<AtomicU64>);

    impl CpuUtilization for FakeCpu {
        fn sample(&mut self) -> Option<f64> {
            Some(f64::from_bits(self.0.load(Ordering::Relaxed)))
        }
    }

    fn set(cpu: &AtomicU64, v: f64) {
        cpu.store(v.to_bits(), Ordering::Relaxed);
    }

    #[tokio::test(start_paused = true)]
    async fn semaphore_grows_and_shrinks_without_revoking_running_tasks() {
        let sem = Arc::new(Semaphore::new(0));
        let cpu = Arc::new(AtomicU64::new(0));
        set(&cpu, 0.2);
        let mut config = AdaptiveSlotsConfig::new(2, 8);
        config.interval = Duration::from_secs(1);
        let slots = config
            .start(
                sem.clone(),
                FakeCpu(cpu.clone()),
                None,
                &tokio::runtime::Handle::current(),
            )
            .expect("valid config");
        assert_eq!(sem.available_permits(), 2);

        // Running tasks hold every permit, so slots are saturated and grow 2 -> 4.
        let mut held = vec![
            sem.clone()
                .try_acquire_many_owned(2)
                .expect("floor permits"),
        ];
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(slots.slots(), 4);
        assert_eq!(sem.available_permits(), 2);
        held.push(
            sem.clone()
                .try_acquire_many_owned(2)
                .expect("grown permits"),
        );

        // Shrink 4 -> 3 while all four permits are held: nothing is revoked.
        set(&cpu, 0.9);
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(slots.slots(), 3);
        assert_eq!(sem.available_permits(), 0);

        // Released permits are taken out of circulation until three remain.
        drop(held.pop());
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(held.pop());
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(sem.available_permits(), 3);
        assert_eq!(slots.floor(), 2);
        assert_eq!(slots.ceiling(), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_handle_stops_the_controller() {
        let sem = Arc::new(Semaphore::new(0));
        let cpu = Arc::new(AtomicU64::new(0));
        set(&cpu, 0.2);
        let mut config = AdaptiveSlotsConfig::new(2, 8);
        config.interval = Duration::from_secs(1);
        let slots = config
            .start(
                sem.clone(),
                FakeCpu(cpu),
                None,
                &tokio::runtime::Handle::current(),
            )
            .expect("valid config");
        drop(slots);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(sem.available_permits(), 2);
    }
}
