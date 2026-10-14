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

//! Session-scoped reuse of executor runtime state.

use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ballista_core::RuntimeProducer;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use lru::LruCache;
use parking_lot::Mutex;

/// Derives a task's runtime from a shared base [`RuntimeEnv`] by installing a
/// fresh per-task memory pool sized to the task's vcore claim. The base's
/// object-store registry and the cache manager's underlying file-metadata
/// (footer) cache are preserved via
/// [`RuntimeEnvBuilder::from_runtime_env`](datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env)
/// — the outer `CacheManager` is rebuilt around that same inner cache — so
/// read-side state is shared while the memory pool stays per task.
///
/// `vcores_consumed` is the number of vcores this task claims from the
/// executor's budget (`min(input_partition_ids.len(), budget)` for non-collapse
/// stages, `1` for collapse). The policy scales the pool proportionally so a
/// task claiming N/total vcores gets N/total of the executor's memory budget.
pub type MemoryPoolPolicy = Arc<
    dyn Fn(
            Arc<RuntimeEnv>,
            &SessionConfig,
            u32,
        ) -> datafusion::error::Result<Arc<RuntimeEnv>>
        + Send
        + Sync,
>;

/// The memory every task's pool holds, against the executor's whole memory
/// budget.
///
/// Each task's own pool enforces its limit; this only adds them up, so the
/// executor can tell whether it has room for another task before lending a
/// waiting task's vcore. It never refuses a reservation.
#[derive(Debug)]
pub struct ExecutorMemory {
    budget: usize,
    reserved: AtomicUsize,
}

impl ExecutorMemory {
    /// Counts against an executor budget of `budget` bytes.
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            reserved: AtomicUsize::new(0),
        }
    }
}

impl Display for ExecutorMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "executor(reserved: {}, budget: {})",
            self.reserved(),
            self.budget
        )
    }
}

impl MemoryPool for ExecutorMemory {
    fn name(&self) -> &str {
        "executor"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.reserved.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.reserved.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.budget)
    }
}

/// A task's own memory pool, whose reservations are also counted in the
/// executor's [`ExecutorMemory`].
#[derive(Debug)]
pub struct TaskMemoryPool {
    task: Arc<dyn MemoryPool>,
    executor: Arc<ExecutorMemory>,
}

impl TaskMemoryPool {
    /// Wraps the task's `task` pool, counting what it holds in `executor`.
    pub fn new(task: Arc<dyn MemoryPool>, executor: Arc<ExecutorMemory>) -> Self {
        Self { task, executor }
    }
}

impl Display for TaskMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "task({}, {})", self.task, self.executor)
    }
}

impl MemoryPool for TaskMemoryPool {
    fn name(&self) -> &str {
        self.task.name()
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.task.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.task.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.task.grow(reservation, additional);
        self.executor.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.task.shrink(reservation, shrink);
        self.executor.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.task.try_grow(reservation, additional)?;
        self.executor.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.task.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.task.memory_limit()
    }
}

/// Produces a task's [`RuntimeEnv`], optionally reusing read-side state across
/// the tasks of a session.
///
/// The executor calls [`produce_runtime`](Self::produce_runtime) for every task;
/// implementors decide whether and how to share base runtime state (object-store
/// clients, file-metadata cache) between a session's tasks.
/// [`DefaultSessionRuntimeCache`] is the built-in implementation — provide a
/// custom one to change the caching/sharing strategy.
pub trait SessionRuntimeCache: Send + Sync {
    /// Returns the per-task runtime for `session_id`, sized to `vcores_consumed`
    /// vcores of the executor's memory budget.
    fn produce_runtime(
        &self,
        session_id: &str,
        config: &SessionConfig,
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>>;
}

/// A bounded, session-keyed cache of shared *base* [`RuntimeEnv`]s.
///
/// A base env carries the read-side state safe to share across all tasks of a
/// session: the object-store registry and the cache manager (whose Parquet
/// footer cache is thereby reused across the session's tasks and queries).
/// `RuntimeEnvBuilder::from_runtime_env` also carries over the disk manager
/// (rooted at the executor's `work_dir`), so a session's tasks share one
/// `DiskManager` too; this is safe because spill temp files are uniquely
/// named, matching the standard one-`RuntimeEnv`-per-`SessionContext` model.
/// Each task's real runtime is produced by applying [`MemoryPoolPolicy`] to
/// the shared base, which installs a fresh per-task memory pool — so memory
/// isolation is unchanged.
///
/// The cache is bounded by an LRU of `capacity` sessions. A capacity of `0`
/// disables caching entirely: every call builds a fresh base env, matching the
/// behavior of building a runtime per task.
pub struct DefaultSessionRuntimeCache {
    base_producer: RuntimeProducer,
    pool_policy: MemoryPoolPolicy,
    cache: Option<Mutex<LruCache<String, Arc<RuntimeEnv>>>>,
}

impl DefaultSessionRuntimeCache {
    /// Creates a new cache that produces per-task runtimes from `base_producer`
    /// (invoked at most once per cached session) and `pool_policy` (invoked on
    /// every call). `capacity` bounds the number of distinct sessions kept in
    /// the LRU; `0` disables caching.
    pub fn new(
        base_producer: RuntimeProducer,
        pool_policy: MemoryPoolPolicy,
        capacity: usize,
    ) -> Self {
        let cache = NonZeroUsize::new(capacity).map(|cap| Mutex::new(LruCache::new(cap)));
        Self {
            base_producer,
            pool_policy,
            cache,
        }
    }
}

impl SessionRuntimeCache for DefaultSessionRuntimeCache {
    /// Returns the per-task runtime for `session_id`, reusing a cached base env
    /// when present and building + caching one on miss.
    fn produce_runtime(
        &self,
        session_id: &str,
        config: &SessionConfig,
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        let base = match &self.cache {
            None => (self.base_producer)(config)?,
            Some(cache) => {
                if let Some(base) = cache.lock().get(session_id) {
                    base.clone()
                } else {
                    // Build the base env without holding the lock, so a miss
                    // never stalls other sessions' lookups. A rare concurrent
                    // first-miss for the same session may build twice; that is
                    // harmless (idempotent, cheap) — the last writer wins and
                    // the extra env is dropped.
                    let base = (self.base_producer)(config)?;
                    cache.lock().put(session_id.to_string(), base.clone());
                    base
                }
            }
        };
        (self.pool_policy)(base, config, vcores_consumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn base_producer() -> RuntimeProducer {
        Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())))
    }

    /// Identity policy: the task shares the base env unchanged (no pool swap).
    fn identity_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _, _| Ok(base))
    }

    /// Rebuilding policy: mimics production — a fresh per-task pool layered onto
    /// the shared base, preserving the base's read-side state.
    fn per_task_pool_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _, _| {
            RuntimeEnvBuilder::from_runtime_env(&base)
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024)))
                .build_arc()
        })
    }

    #[test]
    fn same_session_shares_cache_manager() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn different_sessions_get_different_base() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s2", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn per_task_pool_shares_footer_cache_but_not_env() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), per_task_pool_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        // Different runtime envs (fresh per-task pool)...
        assert!(!Arc::ptr_eq(&e1, &e2));
        // ...but the shared read-side state is reused: object-store registry
        // passes through unchanged, and the inner footer (file-metadata) cache
        // is the same instance even though the outer CacheManager wrapper is
        // rebuilt. Do NOT compare the outer Arc<CacheManager>.
        assert!(Arc::ptr_eq(
            &e1.object_store_registry,
            &e2.object_store_registry
        ));
        assert!(Arc::ptr_eq(
            &e1.cache_manager.get_file_metadata_cache(),
            &e2.cache_manager.get_file_metadata_cache(),
        ));
    }

    #[test]
    fn capacity_zero_disables_cache() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 0);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn evicts_least_recently_used() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 2);
        let cfg = SessionConfig::new();
        let s1_a = cache.produce_runtime("s1", &cfg, 1).unwrap();
        cache.produce_runtime("s2", &cfg, 1).unwrap();
        // Inserting s3 (capacity 2) evicts the least-recently-used, s1.
        cache.produce_runtime("s3", &cfg, 1).unwrap();
        let s1_b = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&s1_a.cache_manager, &s1_b.cache_manager));
    }
}
