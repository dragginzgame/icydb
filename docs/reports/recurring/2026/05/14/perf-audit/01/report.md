# Query Instruction Footprint Audit - 2026-05-14

## 0. Run Metadata + Comparability Note

- scope: refreshed recurring query-instruction audit definition and current
  PocketIC SQL/fluent perf harness run
- definition path:
  `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-03/perf-audit.md`
- secondary SQL-lane baseline:
  `docs/audits/reports/2026-05/2026-05-01/sql-perf-audit.md`
- code snapshot identifier: `499a8478a` plus local uncommitted audit/report
  work
- method tag/version: `PERF-0.4-pocketic-sql-fluent-harness`
- comparability status: `partial`
- auditor: `Codex`
- run timestamp (UTC): `2026-05-14T14:05:56Z`
- branch: `main`
- worktree: `dirty`
- execution environment: `PocketIC`
- entities in scope: `PerfAuditUser`, `PerfAuditAccount`, `PerfAuditBlob`
- entry surfaces in scope: dedicated SQL perf harness and dedicated
  typed/fluent perf harness
- query shapes in scope: scalar reads, field-list projections, expression
  order, grouped aggregates, explain/metadata SQL, repeated cache paths, update
  warm paths, blob payload shapes, and typed/fluent grouped/load paths

The broad April baseline used `demo_rpg` generated dispatch sampling. This run
switches the recurring audit to the dedicated SQL and fluent PocketIC harnesses,
so broad historical deltas are method-shifted. SQL harness rows remain useful
against the May 1 SQL-lane report for overlapping blob scenarios. The current
SQL/fluent absolute rows are the baseline to compare future `PERF-0.4` runs
against.

## 1. Coverage Table

| Scenario Family | Surfaces Covered | Missing Surfaces | Attribution Depth | Risk |
| --------------- | ---------------- | ---------------- | ----------------- | ---- |
| SQL scalar/projection | SQL PocketIC harness | generated dispatch compatibility lane | compile, execute, store, cache | medium |
| SQL grouped/aggregate | SQL PocketIC harness | grouped cursor second-page explicit row in primary capture | grouped stream/fold/finalize, grouped-count internals | medium |
| SQL explain/metadata | SQL PocketIC harness | none for logical/json/execution metadata lanes sampled | compile and execute split | low |
| SQL blob payload | SQL PocketIC harness | no separate raw artifact persisted in this report | compile, execute, store/cache | medium |
| typed/fluent scalar | fluent PocketIC harness | paged cursor-invalid compatibility lane | compile, runtime, direct scan/read/order/page/finalize/decode | medium |
| typed/fluent grouped | fluent PocketIC harness | grouped cursor second-page explicit row | grouped stream/fold/finalize, grouped-count internals | medium |
| repeat/cache | SQL and fluent PocketIC harnesses | none for current repeat rows | cache hits/misses plus averaged totals | low |

## 2. Current Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ------------ | ------------- | ----: | ---: | ----- |
| `user.pk.key_only.asc.limit1` | SQL | 1 | 48,741,839 | compile 17,548,643; execute 31,193,196 |
| `user.name.eq.order_id.limit1` | SQL | 1 | 64,487,111 | secondary equality; no store.get calls |
| `user.grouped.age_count.limit10` | SQL | 1 | 49,221,318 | grouped stream 25,374; fold 328,937; finalize 45,646 |
| `user.describe` | SQL | 1 | 27,867,302 | metadata lane |
| `account.active.lower.order_handle.asc.limit3` | SQL | 1 | 57,333,792 | expression-order account row |
| `repeat.user.grouped.age_count.limit10.runs10` | SQL | 10 | 47,026,878 | compiled/shared cache hits 9/1 |
| `user.id.order_only.asc.limit2` | fluent | 1 | 33,356,992 | compile 25,350,664; execute 8,006,328 |
| `user.age.order_only.asc.limit3` | fluent | 1 | 33,714,451 | direct scan/read/order attribution present |
| `user.grouped.age_count.limit10` | fluent | 1 | 31,632,268 | grouped stream 25,374; fold 323,219; finalize 45,511 |
| `repeat.user.age.order_only.asc.limit3.runs100` | fluent | 100 | 31,548,398 | cache hits 99/1 |
| `account.active_true.order_handle.asc.limit3` | fluent | 1 | 33,913,263 | account predicate/order row |
| `repeat.account.active_true.order_handle.asc.limit3.runs100` | fluent | 100 | 31,633,671 | cache hits 99/1 |

## 3. Comparison Highlights

The old embedded baseline values in the harness output are not a trustworthy
regression authority for this recurring report. Current cold SQL rows are around
`48M-64M` instructions for representative scalar/grouped reads, while current
fluent rows are around `31M-34M`. The gap is dominated by the current canister
measurement envelope and harness method, not a like-for-like April regression.

The useful comparison going forward is `PERF-0.4` to `PERF-0.4`: dedicated
PocketIC SQL harness plus dedicated PocketIC fluent harness, with generated
dispatch only as optional compatibility context.

## 4. Phase Attribution Read

| Scenario Key | Compile | Planner | Store | Executor | Projection/Finalize | Notes |
| ------------ | ------: | ------: | ----: | -------: | ------------------: | ----- |
| `user.pk.key_only.asc.limit1` | 17,548,643 | PARTIAL | 0 gets | 31,193,196 | PARTIAL | SQL scalar |
| `user.name.eq.order_id.limit1` | 17,602,751 | PARTIAL | 0 gets | 46,884,360 | PARTIAL | SQL secondary equality |
| `user.grouped.age_count.limit10` | 17,582,180 | PARTIAL | 6 gets | 31,639,138 | 45,646 | SQL grouped count |
| `repeat.user.grouped.age_count.limit10.runs10` | 15,609,639 | PARTIAL | 6 gets | 31,417,238 | 45,689 | SQL repeat cache row |
| `user.id.order_only.asc.limit2` | 25,350,664 | PARTIAL | 48,940 direct-store | 8,006,328 | 36,153 finalize; 47,886 decode | fluent scalar |
| `user.grouped.age_count.limit10` | 23,452,831 | PARTIAL | N/A | 8,179,437 | 45,511 | fluent grouped count |
| `repeat.user.age.order_only.asc.limit3.runs100` | 23,269,496 | PARTIAL | 138,025 direct-store | 8,278,901 | 36,284 finalize; 72,286 decode | fluent repeat cache row |

## 5. Hotspot Localization

- SQL compile/cache and execute boundary:
  `crates/icydb-core/src/db/session/sql/mod.rs`
- SQL compiled execution and phase attribution:
  `crates/icydb-core/src/db/session/sql/execute/mod.rs`
- SQL lowering:
  `crates/icydb-core/src/db/sql/lowering/mod.rs`
- SQL projection runtime:
  `crates/icydb-core/src/db/session/sql/projection/runtime/mod.rs`
- fluent query attribution:
  `crates/icydb-core/src/db/session/query/diagnostics.rs`
- scalar/grouped executor phase attribution:
  `crates/icydb-core/src/db/executor/pipeline/entrypoints/`
- recurring harnesses:
  `testing/pocket-ic/tests/sql_perf_audit.rs`,
  `testing/pocket-ic/tests/fluent_perf_audit.rs`

## 6. Coverage Gaps

- The generated `demo_rpg` dispatch lane was intentionally demoted to optional
  compatibility context and was not rerun.
- The SQL stdout was large and was summarized in this report rather than stored
  as a full raw text artifact.
- Planner attribution is still `PARTIAL`; compile and execute are available,
  but planner subphases are not uniformly isolated across all primary rows.
- Cursor-invalid and explicit second-page rows are covered by related tests and
  older broad audit context, but are not first-class rows in the two primary
  harness summary captures.

## 7. Overall Read

The recurring perf audit is worth keeping, but the old broad `demo_rpg`
generated-dispatch method is no longer the right primary lane. The audit now
has a cleaner repeatable shape: dedicated PocketIC SQL harness for SQL
query/update/explain/cache rows, and dedicated PocketIC fluent harness for
typed/fluent query/cache rows.

No product-code perf fix was made from this run. The main improvement is audit
quality: future runs can compare stable SQL/fluent harness rows directly instead
of mixing generated dispatch, demo data, and current session-owner boundaries.

## Verification Readout

- SQL perf harness registration check passed.
- Fluent perf harness registration check passed.
- SQL perf harness compile check passed.
- Fluent perf harness compile check passed.
- SQL instruction capture passed:
  `test sql_perf_audit_harness_reports_instruction_samples ... ok`
- Fluent instruction capture passed:
  `test fluent_perf_audit_harness_reports_instruction_samples ... ok`
