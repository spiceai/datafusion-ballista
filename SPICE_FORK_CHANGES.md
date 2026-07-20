<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Spice Ballista Fork — Logical Changes vs Upstream

Living inventory of intentional differences between
[`spiceai/datafusion-ballista`](https://github.com/spiceai/datafusion-ballista)
and [`apache/datafusion-ballista`](https://github.com/apache/datafusion-ballista).

Status snapshot: **2026-07-20** (post Ballista 54 merge). Working tip:
`phillip/merge-upstream-ballista-54` (merge of `spiceai-54` × upstream `54.0.0`
`18566b9c`). Cross-checked against the
[upstreaming audit](https://ember-reef-w54j.here.now/).

## How to use this document

- Update this file whenever a Spice fork PR lands, an upstream PR opens/merges,
  or an upgrade merge drops/restores a change.
- Prefer **sentinel symbols** (grepable identifiers) over prose-only descriptions
  so upgrade merges can be validated mechanically.
- Status values:
  - `present` — on current Spice tip
  - `lost` — was on a prior Spice tip; missing from current tip (restore or adopt upstream)
  - `upstreamed` — equivalent (or better) exists on upstream tip/tag
  - `open-pr` — upstream PR open
  - `spice-only` — keep in fork (product/embedder-specific)
  - `abandoned` — do not re-port
  - `adopt-upstream` — next merge should take upstream's form, not re-land the fork patch

## Current tip reality (important)

`spiceai-54` is **not** a merge of upstream Ballista 54. It is the Spice 53-era
tree plus a manual DataFusion 54 dependency bump (`cb3385e5`). Relative to
upstream `54.0.0`:

| Fact | Value |
|---|---|
| Merge-base with `54.0.0` | Ballista `53.0.0` (`e0a78666`) |
| Upstream commits not in tip | ~173 (entire 53→54 cycle) |
| File delta vs `54.0.0` | ~327 files |
| Published crate versions on tip | still `52.0.0` (cosmetic drift) |
| DF / Arrow deps on tip | DataFusion 54 / Arrow 58 |

The in-progress merge branch is `phillip/merge-upstream-ballista-54`.

### Post-review repair commit (on top of the merge)

The merge suffered a delete/modify pathology: files the fork had deleted
relative to base 53.0.0 stayed deleted whenever upstream hadn't touched them
during 53→54, and several conflict resolutions kept stale fork-side versions.
A follow-up commit repaired the tree: restored upstream's TUI files
(`tui/domain/mod.rs`, `tui/domain/executors.rs`, `ui/main/jobs/dot_parser.rs`),
`exec.rs`, the python docs dir + example notebooks + `test_jupyter.py`, TUI
screenshots, `standalone-broadcast-join.rs`, `tpch-gen.sh`, the vendored
`datafusion*.proto` stubs, and upstream's `scheduler/src/api/` + `display.rs`;
deleted orphaned fork files (`aqe/optimizer_rule/{datafusion_patch,
eliminate_empty}.rs`, dead `executor/src/client_pool.rs`, stale
`.pending-snap` snapshots); and re-adopted the clobbered upstream features
listed below. `take.yml`/`stale.yml` (ASF probot configs) stay deleted.

## Status legend for the inventory

| Column | Meaning |
|---|---|
| Fork | Spice PR number(s) |
| Tip | Present on `spiceai-54`? |
| Upstream | Upstream disposition |
| Disposition | What to do on the Ballista 54 merge / afterward |

---

## Inventory

### Embedding / integration APIs

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| Catalog + UDF stub sync | #1 #4 | present | Rejected (`apache#1333`); hook invited | `spice-only` until gRPC service-extension hook lands | `remote_catalog`, `RemoteScalarUDF` |
| `poll_loop` readiness oneshot | #2 | present | Not upstreamed; fold into #1893 API | keep; fold when #1893 consolidates | `readiness: Option<OneShotSender<String>>` |
| TLS + metadata interceptors | #3 | present | **Upstreamed** `apache#1400` (evolved) | `adopt-upstream` TLS config shape on merge | `server_tls_config`, interceptors |
| Executor disconnect backoff + log suppression | #6 | present | Not upstreamed | keep; P2 upstream candidate | `ExponentialBackoff` in `execution_loop` |
| Heartbeat / push status modes | #5 | absent | Abandoned (dropped at DF51 rebase) | `abandoned` — poll_work-as-heartbeat superseded it | — |
| `BallistaBuilder` custom object-store | #11 | present | Upstream `remote_with_state` covers most | `spice-only` / minor | `BallistaBuilder` |
| `poll_now_notify` + `on_work_available` | #12 | present | **Open** `apache#1893` (needs rework) | keep; finish upstream PR | `poll_now_notify`, `on_work_available` |
| Pending-tasks lock fix | #13 | present | Spice-only instrumentation fix | `spice-only` | — |
| External executor semaphore | #14 | present | **Open** `apache#1892` (approved) | keep; finish upstream PR | `AvailableTaskSlots` |
| Job-state event broadcast channel | #15 | present | **Open** `apache#1891` | keep; finish upstream PR | `JobStateEvent` broadcast |
| Task-cancellation routing hook | #19 | present | Complementary to `apache#1903` | keep; P2 upstream | `OnCancelTasksFn` |
| `executor_id` on `TaskInfo` + public graph | #38 #49 | present | Not upstreamed | keep; P1 upstream (merge-clobbered twice) | `executor_id` on task info / `get_job_execution_graph` |

### Scheduler correctness / HA

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| None task-slot after executor lost | #23 | present | **Upstreamed** `apache#1523` | parity | — |
| Heartbeat while all slots busy | #26 | present | Bug still live upstream | keep; P0 upstream | `HEARTBEAT_POLL_INTERVAL` (5s) |
| Job-state broadcast ordering | #30 | present | Lesson for #1891 / #2037 | `spice-only` ordering detail | — |
| Ignore stale TaskStatus for reset partitions | #53 | present | Not upstreamed | keep | — |
| Execution-graph serde + `recover_job` + `submit_job_with_id` | #56 | present | Design-first → `apache#2030` | keep; RFC then upstream | `recover_job`, `submit_job_with_id` |
| Buffer task statuses across failed polls + reconcile sweep + scan/subquery ports | #57 | present | Scan/subquery already upstream (`#1906`); buffer/reconcile not | keep buffer/reconcile; adopt upstream scan form on merge | `reconcile_running_jobs`, pending status buffer |
| Persist terminal status before active-cache eviction | #59 | present | **Open / approved** `apache#2037` | keep; finish upstream PR | `persist_terminal_and_evict` |
| Lock hygiene (persist outside lock, DashMap-across-await, sample loop) | #60 | present | Partly in #2037; residuals still live upstream | keep; follow-up upstream after #2037 | DashMap guard / `JOB_PERSIST_TIMEOUT` |
| Stuck-query watchdog | #39 | **lost** | Concept → `#2030` | low urgency restore or redesign | was progress-sampling loop |
| Consistent-hash task binding restore | (restore) | present | Upstream removed policy; leans on `#1911` | keep fork-side until benchmarks justify re-proposal | consistent-hash policy |

### Shuffle storage

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| Vortex shuffle format | #7 | present | Reframe on `#1980` later | `spice-only` (git-fork dep) | `vortex_shuffle` |
| In-memory shuffle | #8 #17 | present | Needs MemoryPool design upstream | keep; design issue only | `memory://`, `shuffle_manager` |
| Object-store shuffle (S3/Azure) + streaming IPC | #9 #18 #42 #43 #48 | present | RFC on `#1539` | keep; RFC not raw diff | `shuffle_storage`, `ShuffleStorage`, `PrefixStore` |
| Env-credential / URL-strip S3 client fixes | #40 #41 | present | Fixes fork-only code | `spice-only` | — |
| Missing partition file → empty partition | #54 | present | Debatable (`#412`/`#2027`) | keep; NotFound typing upstreamable | missing-partition → empty |

### Shuffle fetch / transport

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| Lazy `BatchCoalescer` init | #24 | present (restored on merge) | Absent upstream too | keep | `Option<LimitedBatchCoalescer>` lazy init in shuffle reader |
| `find_fetch_failed` error drill-through | #36 | present (restored on merge) | Upstream `#1578`/`#1951` different form | keep drill-through | `find_fetch_failed` |
| Evict + retry on fresh connection | #61 | **replaced** by upstream `#1951` pool `discard()` + reduce-side `with_retry` | **adopted** | done | `with_retry`, `PooledClient::discard` |
| h2 receive-window sizing | #62a | **adopted** upstream config keys (`ballista.client.initial_*_window_size`, defaults 64MB/16MB) | **Upstreamed** `#1951` | done | `BALLISTA_CLIENT_INITIAL_*_WINDOW_SIZE` |
| `InactivityTimeoutStream` | #62b | present (wraps streams inside `BallistaClient` fetch) | Not upstreamed; protects `#1951` | keep; P1 upstream | `InactivityTimeoutStream` |
| Unordered stream drain | #63a | **removed** — remote partitions are fully buffered (`fetch_partition_buffered`), so h2 credit never pins; reader uses upstream's ordered `try_flatten` | Superseded by `#1951` buffering | done | — |
| Transport on I/O runtime + 60s keepalive-ack | #63b | present (pool misses connect via `connect_ballista_client` on the registered transport runtime; keepalive tuning lives in `BallistaClient::try_new`) | Not upstreamed | keep; P1 upstream | `set_shuffle_transport_runtime`, `connect_ballista_client` |

### Planner / correctness / perf

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| INNER broadcast joins | #27 | present (upstream thresholds via merge) | **Fixed differently** `#1900`/`#1904` | `adopt-upstream` | non-zero thresholds in `extension.rs` |
| TopK single-stage for small `fetch` | #28.1 | present (restored on merge) | No upstream equivalent | keep | `TOPK_FETCH_THRESHOLD` in `planner.rs` |
| TopK executor dynamic-filter re-link | #28.2 | **lost** | Propose on `#1375` | P2 | — |
| Parquet `metadata_size_hint` round-trip workaround | #29 | present (restored on merge) | Root bug still in DF54 proto | keep; file DF fix | `fix_parquet_metadata_size_hint` in `execution_engine.rs` |
| HashJoin dynamic-filter strip | #33 | present (upstream default) | Upstream globally disables (`enable_dynamic_filter_pushdown=false` on `54.0.0`) | `adopt-upstream` | `enable_dynamic_filter_pushdown` false in `extension.rs` |
| Null-aware anti-join guards | #58 | present | Gaps still live upstream (+ `#1900` demotion hazard) | keep; P0 upstream | `null_aware` guards + tests in vendored `join_selection.rs` |

### Observability

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| Collector traits + Prometheus (~62 metrics) | #10 | present | Upstream invested in different surfaces (`#1968`/`#1949`/`#1999`) | keep; P1 upstream candidate | `MetricsCollector`, `override_metrics_collector` |
| Distributed EXPLAIN ANALYZE + `GetJobMetrics` | #34a | present (adopted on merge) | **Upstreamed** `#1567`/`#1635` (and more) | `adopt-upstream` | `DistributedExplainAnalyzeExec`, `GetJobMetrics` |
| EXPLAIN FORMAT TREE round-trip | #34b | **lost** | Appetite via `#1627` | restore from history (`07be66a8` / #34) as P2 | FORMAT TREE codec |
| Executor system/process metrics (`memory-stats`) | upstream `#1547` | present (adopted on merge) | Present on `54.0.0` | `adopt-upstream` | `ExecutorMetricCollectionPolicy`, `os_info`, expanded `ExecutorMetric` |

---

## Silently lost at tip — restore checklist

These were present before the lossy `53.0.0` whole-tree merge (`0b95c9d7`)
and/or were never carried through the DF54 bump. Independent of upstreaming.

| # | Action on Ballista 54 merge | Source commit(s) | Merge status |
|---|---|---|---|
| #29 parquet `metadata_size_hint` | **Restore** fork workaround | `a4b0db68` | **done** |
| #34 EXPLAIN ANALYZE | **Adopt upstream** `#1567`/`#1635` via merge | upstream | **done** (`GetJobMetrics` RPC + client exec) |
| #27/#33 broadcast + dynamic filters | **Adopt upstream** `#1900`/`#1904` + `enable_dynamic_filter_pushdown=false` | upstream | **done** (extension defaults) |
| #28.1 TopK single-stage | **Restore** | `8bc4d752` / `42f17f88` | **done** |
| #36 `find_fetch_failed` drill-through | **Verify** still needed; restore if yes | `c14e3e7c` (error conversion hunk) | **done** |
| #24 lazy `BatchCoalescer` | **Restore** | `ad88031f` | **done** (`LimitedBatchCoalescer`) |
| #39 stuck-query watchdog | Low urgency; concept → `#2030` | `7e9872a5` | deferred |
| `#1547` executor metrics | **Adopt upstream** via merge | upstream | **done** (Spice `ExecutorMetricsCollector` retained alongside) |
| Crate versions `52.0.0` → `54.0.0` | Bump during merge | upstream `#2004` | **done** |
| Upstream AQE coalesce / `CoalescePlan` | Adopt upstream AQE; add coalesce/broadcast APIs to Spice shuffle reader | upstream | **done** (Spice fetch transport kept) |

Process rule for this merge and future ones: after resolving conflicts, run a
sentinel grep pass over this inventory and fail the upgrade if any `present`
row's sentinel disappears without an explicit disposition update.

## Upstream features the merge must pick up

Not Spice patches — features the fork never absorbed because it skipped the
53→54 development cycle. Status after the post-review repair commit:

- `#1567` / `#1635` distributed EXPLAIN ANALYZE — **adopted** (merge)
- `#1900` / `#1904` broadcast-safe joins + demotion — **adopted** (AQE path via
  merge; the static-planner half — `maybe_promote_to_broadcast`, CollectLeft
  demotion guard, SMJ→hash conversion, broadcast stage lowering, the
  `ballista.optimizer.broadcast_join_threshold_bytes` /
  `broadcast_sort_merge_join_enabled` config keys and
  `with_ballista_broadcast_join_threshold_bytes` — was clobbered and re-added
  in the repair commit, with upstream's 12 planner broadcast tests)
- `#1951` shuffle-fetch governor + h2 windows + retry — **ADOPTED**
  (follow-up commit): the remote-fetch path now uses upstream's mechanics —
  three-gate reduce-side governor (`max_bytes_in_flight`,
  `max_blocks_in_flight_per_address`, max concurrent requests), fully
  buffered fetches (`fetch_partition_buffered` + `GovernedStream`),
  reduce-side `with_retry` with `ballista.client.io_retries_times` /
  `io_retry_wait_time_ms` at upstream defaults (3 / 3000 ms), the
  `BallistaClientPool` / `DefaultBallistaClientPool` client pool with the
  executor `--client-ttl` option (default 0 = no pooling, upstream default),
  and config-driven h2 windows. Retained fork deltas inside that path:
  (a) pool misses connect via `connect_ballista_client`, which places the
  channel's h2 driver on the registered shuffle transport runtime (#63b);
  (b) `BallistaClient::try_new` keeps the fork's keepalive tuning (60s
  keepalive-ack) and `InactivityTimeoutStream` wrapping (#62b);
  (c) NotFound-from-fetch keeps the fork's missing-partition-is-empty
  semantics (#54) and does not discard the pooled client;
  (d) the fetch action stays path-based (fork location model), and the
  memory:// / object-store / vortex reader classes sit outside the governor.
  **Deployment note:** pooling is opt-in via `--client-ttl` (or
  `DefaultExecutionEngine::with_client_pool` for embedders); the spiceai
  runtime MUST configure a pool when repinning or fetches connect per
  request (the pre-#57 connection-storm regime).
  `ballista.shuffle.remote_read_prefer_flight` still defaults to **true** —
  the fork's block-IO transport cannot serve sort-based shuffle (enabled by
  default), so the upstream sort-shuffle test's block-IO cases stay removed.
- `#1911` partition pruning — **adopted** (repair commit; active under
  `disable-stage-plan-cache`, ignored when the stage-plan cache is on)
- `#1902` preserve user session config overrides — **adopted** (merge)
- `#1982` selective shuffle cleanup on job success — **adopted** (repair
  commit: `intermediate_stage_ids` → `remove_stage_ids` end-to-end; the
  executor also applies the selective semantics to the in-memory shuffle
  manager)
- `#1995` share read-side runtime state — **adopted** (repair commit:
  `runtime_cache` wired into executor lib/config/process, session-keyed LRU;
  also restored upstream's `--memory-pool-size` FairSpillPool option)
- `#1547` executor system/process metrics — **adopted** (merge)
- `#1968` shuffle-read / per-operator metrics — **adopted** (repair commit:
  `ShuffleReadMetrics` mapped onto the Spice fetch pipeline — memory + local
  count as `local_partitions`, object-store + flight as `remote_partitions`;
  writer Displays render child metrics)
- `#1949` failed-task surfacing in TUI/REST — **adopted** (repair commit:
  upstream `scheduler/src/api/` restored wholesale, incl. `get_job`,
  `get_job_config`, `get_executor_info`, `get_scheduler_version`, typed
  failed-task reasons, CORS options `--cors-allowed-origins/-methods` and
  `--disable-rest-api` from `#1818`; `ExecutorManager::get_executors_state`
  and `TaskManager::get_all_jobs` renamed to upstream form; **embedder note:**
  the `JobState` trait gained `get_all_jobs` — external implementations must
  add it)
- Ballista crate version bump to `54.0.0` — **adopted** (merge)
- `#1999` task duration in finished-task log — **adopted** (repair commit)

## Open upstream PRs (finish first when possible)

| Apache PR | Fork source | State |
|---|---|---|
| [#2037](https://github.com/apache/datafusion-ballista/pull/2037) | #59 (+ part of #60) | Approved / mergeable |
| [#1892](https://github.com/apache/datafusion-ballista/pull/1892) | #14 | Approved; needs small fixes |
| [#1891](https://github.com/apache/datafusion-ballista/pull/1891) | #15 | Needs design answers |
| [#1893](https://github.com/apache/datafusion-ballista/pull/1893) | #12 | Needs rework per review |

## Structural gaps to full upstream reliance

The individual patches above are the small stuff — each is a rebase or a
one-off upstream PR. What actually decides whether this fork can shrink to a
thin re-pin (or disappear) are the five structural gaps below. Track them
here across upgrade cycles; update `Status` whenever an RFC, upstream PR, or
benchmark moves one of them.

Goal state: **unforked Ballista source consumed via `[patch]` against the
Spice DataFusion fork**, with Spice-only pieces (Vortex codec, remote
catalog, Prometheus collector) living as plugins behind upstream extension
points instead of in-tree edits.

### Gap 1 — Pluggable shuffle storage (the long pole)

The fork's center of gravity: `memory://`, S3/Azure object-store shuffle,
and Vortex all hang off the **path-based `PartitionLocation` model**, while
upstream committed to `file_id`/`is_sort_shuffle`. This proto divergence is
the recurring merge-conflict source, and it is why block-IO sort-shuffle is
unsupported on the fork.

- **Exit**: upstream's own invitations — the
  [#1539](https://github.com/apache/datafusion-ballista/issues/1539)
  object-store-shuffle RFC and
  [#1980](https://github.com/apache/datafusion-ballista/issues/1980) format
  hooks. A `ShuffleStorage`-style trait upstream lets memory/object-store/
  Vortex become backends and retires the path-vs-file_id fork.
- **Prereq for Vortex specifically**: a non-git-fork dependency story
  (upstream will not take a git dep on `spiceai/vortex`).
- **Effort**: design doc + months; community project, not a patch.
- **Status**: not started. Next step: write the RFC against #1539.

### Gap 2 — Embedding API upstream

Spice runs the scheduler and executor **in-process**; upstream is
binary-first. The embedder hooks (`readiness` oneshot, `OnCancelTasksFn`,
`override_metrics_collector`, `get_job_execution_graph`,
`submit_job_with_id`/`recover_job`, `BallistaBuilder`, `poll_now_notify`)
are individually small but need upstream to accept *embedders as a
first-class consumer* as a design stance.

- **Exit**: one umbrella "embedding API" proposal rather than ten drive-by
  hook PRs; #56 (graph serde / recovery) already has a path via the
  [#2030](https://github.com/apache/datafusion-ballista/issues/2030) HA RFC.
- **Status**: not started as an umbrella; individual pieces tracked in the
  inventory above. Next step: draft the umbrella issue, fold #1891/#1893
  review feedback into it.

### Gap 3 — Catalog/UDF sync as an extension point

`remote_catalog` / `RemoteScalarUDF` were **rejected upstream**
([apache#1333](https://github.com/apache/datafusion-ballista/pull/1333))
with a counter-offer: a gRPC service-extension hook. This is a redesign,
not a rebase — the sync logic moves into Spice, implemented against an
upstream extension point.

- **Exit**: upstream service-extension hook lands; fork keeps only a plugin.
- **Status**: waiting on hook design. Next step: propose the hook shape
  upstream (can cite the fork's implementation as the motivating consumer).

### Gap 4 — Consistent-hash task binding: benchmark, then decide

Upstream deleted the consistent-hash policy and bet on `#1911` partition
pruning instead. The fork now has **both** (pruning adopted in the repair
commit), which makes the question empirical for the first time.

- **Exit A**: SF100 benchmark shows pruning covers it → drop the fork
  policy.
- **Exit B**: benchmark shows a real win → re-propose upstream with the
  numbers (exactly the evidence upstream asked for when removing it).
- **Status**: testable now. Next step: SF100 run with consistent-hash
  disabled, compare shuffle-read locality + QPH.

### Gap 5 — DataFusion pin alignment

Spice runs on its own DataFusion fork; upstream Ballista pins apache
releases. This does **not** force a Ballista source fork — `[patch]`-
substituting the DF crates works as long as the Spice DF fork stays
API-compatible — but it fixes the endgame as "unforked Ballista source,
rebuilt against our DF", never "crates.io binaries".

- **Exit**: keep the Spice DF fork patch-thin (API-additive only); verify
  each Ballista upgrade builds with `[patch.crates-io]` substitution.
- **Status**: ongoing discipline, no one-time fix. Next step: try building
  this branch against unmodified upstream Ballista source + `[patch]`ed DF
  to measure how far away that already is.

### Burndown order

1. Finish the four open upstream PRs (mechanical; deletes 4 fork patches).
2. Batch-upstream the correctness fixes (#26, #58, #60 residuals, #53,
   #57-buffer, #36, #24, #54 NotFound typing) — small, evidence-backed,
   high acceptance likelihood. File the #29 root-cause fix in DataFusion.
3. Start Gap 1 (shuffle-storage RFC) immediately — it is the long pole and
   gates the proto convergence every future merge pays for.
4. Gap 4 benchmark next SF100 cycle (cheap, may delete a subsystem).
5. Gap 2 umbrella proposal once #1891/#1893 conclude.

## Related maps

- Merged Spice PR number list (talk artifact): `ballista-ha-talk-copy/fork-pr-map.md` in `spiceai-project` / `ballista-talk`
- Full upstreaming roadmap + analysis: https://ember-reef-w54j.here.now/
