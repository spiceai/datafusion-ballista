# Spice Ballista Fork — Logical Changes vs Upstream

Living inventory of intentional differences between
[`spiceai/datafusion-ballista`](https://github.com/spiceai/datafusion-ballista)
and [`apache/datafusion-ballista`](https://github.com/apache/datafusion-ballista).

Status snapshot: **2026-07-20**. Verified against tip bookmark `spiceai-54`
(`ab207a1c`) and upstream tag `54.0.0` (`18566b9c`). Cross-checked against the
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
| Lazy `BatchCoalescer` init | #24 | **lost** | Absent upstream too | **restore** (latent panic) | `Option<…BatchCoalescer>` / lazy init in `CoalescedShuffleReaderStream` |
| `find_fetch_failed` error drill-through | #36 | **lost** (drill-through); retry/pool parts superseded | Upstream `#1578`/`#1951` different form | **decide**: restore drill-through if still needed with shared streams | `find_fetch_failed` |
| Evict + retry on fresh connection | #61 | present | Superseded by pool `discard()` + `with_retry` | `adopt-upstream` on merge | — |
| h2 receive-window sizing | #62a | present | **Upstreamed** `#1951` (same defaults) | `adopt-upstream` config form | window sizes 16MB/64MB |
| `InactivityTimeoutStream` | #62b | present | Not upstreamed; protects `#1951` | keep; P1 upstream | `InactivityTimeoutStream` |
| Unordered stream drain | #63a | present | Superseded by `#1951` buffering | design input only after merge | — |
| Transport on I/O runtime + 60s keepalive-ack | #63b | present | Not upstreamed | keep; P1 upstream | I/O runtime handle for pooled channels |

### Planner / correctness / perf

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| INNER broadcast joins | #27 | **lost** (thresholds forced to `0`) | **Fixed differently** `#1900`/`#1904` | `adopt-upstream` on merge | thresholds currently `0` in `extension.rs` |
| TopK single-stage for small `fetch` | #28.1 | **lost** | No upstream equivalent | **restore** after/during merge | planner skip stage break when `fetch ≤ 1000` |
| TopK executor dynamic-filter re-link | #28.2 | **lost** | Propose on `#1375` | P2 | — |
| Parquet `metadata_size_hint` round-trip workaround | #29 | **lost** | Root bug still in DF54 proto | **restore** workaround; file DF fix | `fix_parquet_metadata_size_hint` in `execution_engine.rs` |
| HashJoin dynamic-filter strip | #33 | **lost** | Upstream globally disables (`enable_dynamic_filter_pushdown=false` on `54.0.0`) | `adopt-upstream` on merge | — |
| Null-aware anti-join guards | #58 | present | Gaps still live upstream (+ `#1900` demotion hazard) | keep; P0 upstream | `null_aware` guards + tests in vendored `join_selection.rs` |

### Observability

| Change | Fork | Tip | Upstream | Disposition | Sentinel |
|---|---|---|---|---|---|
| Collector traits + Prometheus (~62 metrics) | #10 | present | Upstream invested in different surfaces (`#1968`/`#1949`/`#1999`) | keep; P1 upstream candidate | `MetricsCollector`, `override_metrics_collector` |
| Distributed EXPLAIN ANALYZE + `GetJobMetrics` | #34a | **lost** | **Upstreamed** `#1567`/`#1635` (and more) | `adopt-upstream` on merge | `DistributedExplainAnalyzeExec`, `GetJobMetrics` |
| EXPLAIN FORMAT TREE round-trip | #34b | **lost** | Appetite via `#1627` | restore from history (`07be66a8` / #34) as P2 | FORMAT TREE codec |
| Executor system/process metrics (`memory-stats`) | upstream `#1547` | **lost** (fork dropped) | Present on `54.0.0` | `adopt-upstream` on merge | `memory-stats` dep |

---

## Silently lost at tip — restore checklist

These were present before the lossy `53.0.0` whole-tree merge (`0b95c9d7`)
and/or were never carried through the DF54 bump. Independent of upstreaming.

| # | Action on Ballista 54 merge | Source commit(s) |
|---|---|---|
| #29 parquet `metadata_size_hint` | **Restore** fork workaround | `a4b0db68` |
| #34 EXPLAIN ANALYZE | **Adopt upstream** `#1567`/`#1635` via merge | upstream |
| #27/#33 broadcast + dynamic filters | **Adopt upstream** `#1900`/`#1904` + `enable_dynamic_filter_pushdown=false` | upstream |
| #28.1 TopK single-stage | **Restore** | `8bc4d752` / `42f17f88` |
| #36 `find_fetch_failed` drill-through | **Verify** still needed; restore if yes | `c14e3e7c` (error conversion hunk) |
| #24 lazy `BatchCoalescer` | **Restore** | `ad88031f` |
| #39 stuck-query watchdog | Low urgency; concept → `#2030` | `7e9872a5` |
| `#1547` executor metrics | **Adopt upstream** via merge | upstream |
| Crate versions `52.0.0` → `54.0.0` | Bump during merge | upstream `#2004` |

Process rule for this merge and future ones: after resolving conflicts, run a
sentinel grep pass over this inventory and fail the upgrade if any `present`
row's sentinel disappears without an explicit disposition update.

## Upstream features the merge must pick up

Not Spice patches — features the fork never absorbed because it skipped the
53→54 development cycle:

- `#1567` / `#1635` distributed EXPLAIN ANALYZE
- `#1900` / `#1904` broadcast-safe joins + demotion
- `#1951` shuffle-fetch governor + h2 windows + retry
- `#1911` partition pruning
- `#1902` preserve user session config overrides
- `#1982` shuffle cleanup on job success
- `#1995` share read-side runtime state
- `#1547` executor system/process metrics
- `#1968` shuffle-read / per-operator metrics
- `#1949` failed-task surfacing in TUI/REST
- Ballista crate version bump to `54.0.0`

## Open upstream PRs (finish first when possible)

| Apache PR | Fork source | State |
|---|---|---|
| [#2037](https://github.com/apache/datafusion-ballista/pull/2037) | #59 (+ part of #60) | Approved / mergeable |
| [#1892](https://github.com/apache/datafusion-ballista/pull/1892) | #14 | Approved; needs small fixes |
| [#1891](https://github.com/apache/datafusion-ballista/pull/1891) | #15 | Needs design answers |
| [#1893](https://github.com/apache/datafusion-ballista/pull/1893) | #12 | Needs rework per review |

## Related maps

- Merged Spice PR number list (talk artifact): `ballista-ha-talk-copy/fork-pr-map.md` in `spiceai-project` / `ballista-talk`
- Full upstreaming roadmap + analysis: https://ember-reef-w54j.here.now/
