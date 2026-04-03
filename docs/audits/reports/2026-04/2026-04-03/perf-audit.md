# Query Instruction Footprint Audit - 2026-04-03

## Report Preamble

- scope: refreshed quickstart-canister PocketIC baseline for generated SQL
  dispatch, typed SQL surfaces, fluent load/paged surfaces, metadata lanes,
  explain, grouped aggregate, global aggregate, delete, write, batch insert,
  computed projection, and direct `STARTS_WITH(...)` predicate paths
- definition path: `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-31/perf-audit.md`
- code snapshot identifier: `625d1d31`
- method tag/version: `PERF-0.3-quickstart-pocketic-surface-sampling-expanded`
- comparability status: `partial`
  - the broad quickstart surface matrix below is a fresh same-method rerun and
    is directly comparable for scenario keys that already existed on
    `2026-03-31`
  - the matrix also includes newer write and direct `STARTS_WITH(...)` rows that
    did not exist in the March 31 baseline
  - the earlier same-day focused `x1` / `x10` / `x100` operation-repeat matrix
    remains useful reference material, but it was not rerun during this refresh
    because the quickstart canister build helper hit a crates.io resolution
    mismatch on `canic-cdk = ^0.22.3`
- auditor: `Codex`
- run timestamp (UTC): `2026-04-03T08:56:18.740254Z`
- branch: `main`
- worktree: `dirty`
- execution environment: `PocketIC + quickstart test canister`
- entities in scope: `User`
- entry surfaces in scope: generated `sql_dispatch::query(...)`, typed
  `execute_sql_dispatch::<User>(...)`, typed `query_from_sql::<User>(...)` +
  `execute_query(...)`, typed `execute_sql::<User>(...)`, typed
  `execute_sql_grouped::<User>(...)`, typed `execute_sql_aggregate::<User>(...)`,
  fluent `load::<User>()`, fluent paged `load::<User>()`, fluent count-only
  delete, typed write helpers, typed batch-insert helpers
- query shapes in scope: scalar projection, scalar whole-row load, filtered
  fluent load, metadata lanes, explain, grouped aggregate, grouped `HAVING`,
  grouped continuation, grouped invalid cursor, global aggregate, paged fluent
  continuation, paged fluent invalid cursor, computed projection, direct
  `STARTS_WITH(...)`, delete projection, count-only delete, one-row write, and
  batched insert

## Initial Read

This rerun establishes the first broad perf baseline after the generated query
route collapse landed. The current matrix says:

- generated and typed scalar dispatch got slightly more expensive than the
  March 31 baseline
- typed structural whole-row SQL (`query_from_sql + execute_query` and
  `execute_sql`) got slightly cheaper
- grouped execution and grouped invalid-cursor rejection got cheaper
- delete got materially cheaper on generated, typed, and fluent count-only
  paths
- aggregate execution is still the largest obvious regression versus the March
  31 baseline
- direct `STARTS_WITH(...)` and batch-insert rows are now recorded as numeric
  baseline scenarios instead of ad hoc local notes

## Current Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ---- | ---- | ----: | ----: | ---- |
| `generated.dispatch.projection.user_name_eq_limit` | generated dispatch projection | `5` | `601,188` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.projection.user_name_eq_limit` | typed dispatch projection | `5` | `621,315` | success, `row_count=1`, `detail_count=2` |
| `typed.query_from_sql.execute.scalar_limit` | `query_from_sql + execute_query` | `5` | `542,651` | success, `row_count=2` |
| `typed.execute_sql.scalar_limit` | typed `execute_sql::<User>(...)` | `5` | `542,349` | success, `row_count=2` |
| `generated.dispatch.describe.user` | `DESCRIBE User` | `5` | `27,219` | success, `detail_count=5` |
| `generated.dispatch.explain.user_name_eq_limit` | explain scalar projection | `5` | `193,977` | success, `detail_count=10` |
| `generated.dispatch.explain.grouped.user_age_count` | explain grouped aggregate | `5` | `186,290` | success, `detail_count=10` |
| `generated.dispatch.explain.aggregate.user_count` | explain global aggregate | `5` | `96,532` | success, `detail_count=10` |
| `typed.execute_sql_grouped.user_age_count` | grouped full page | `5` | `677,246` | success, `row_count=3`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.having_empty` | grouped `HAVING` empty result | `5` | `706,461` | success, `row_count=0`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.limit2.first_page` | grouped paged first page | `5` | `710,393` | success, `row_count=2`, `has_cursor=true` |
| `typed.execute_sql_grouped.user_age_count.limit2.second_page` | grouped paged second page | `5` | `713,600` | success, `row_count=1`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.invalid_cursor` | grouped invalid cursor | `5` | `178,203` | fail-closed `Query(Plan)` cursor decode rejection |
| `typed.execute_sql_aggregate.user_count` | global aggregate | `5` | `207,494` | success, `Uint(3)` |
| `typed.insert.user_single` | typed single insert | `1` | `491,572` | success, `row_count=1` |
| `typed.insert_many_atomic.user_10` | typed atomic insert-many `x10` | `1` | `5,381,990` | success, `row_count=10` |
| `typed.insert_many_atomic.user_100` | typed atomic insert-many `x100` | `1` | `56,325,216` | success, `row_count=100` |
| `typed.insert_many_atomic.user_1000` | typed atomic insert-many `x1000` | `1` | `601,696,818` | success, `row_count=1000` |
| `typed.insert_many_non_atomic.user_10` | typed non-atomic insert-many `x10` | `1` | `5,386,949` | success, `row_count=10` |
| `typed.insert_many_non_atomic.user_100` | typed non-atomic insert-many `x100` | `1` | `59,128,782` | success, `row_count=100` |
| `typed.insert_many_non_atomic.user_1000` | typed non-atomic insert-many `x1000` | `1` | `694,945,365` | success, `row_count=1000` |
| `typed.update.user_single` | typed single update | `1` | `763,545` | success, `row_count=1` |
| `fluent.delete.user_order_id_limit1.count` | fluent delete count-only | `1` | `990,152` | success, `row_count=1`, `result_kind=delete_count` |
| `generated.dispatch.show_indexes.user` | `SHOW INDEXES User` | `5` | `21,853` | success, `detail_count=2` |
| `generated.dispatch.show_columns.user` | `SHOW COLUMNS User` | `5` | `34,949` | success, `detail_count=5` |
| `generated.dispatch.show_entities` | `SHOW ENTITIES` | `5` | `12,887` | success, `detail_count=3` |
| `generated.dispatch.computed_projection.lower_name_limit2` | generated computed projection | `5` | `597,564` | success, `row_count=2`, `detail_count=1` |
| `typed.dispatch.computed_projection.lower_name_limit2` | typed computed projection | `5` | `595,525` | success, `row_count=2`, `detail_count=1` |
| `generated.dispatch.predicate.starts_with_name_limit2` | generated direct `STARTS_WITH(name, ...)` | `5` | `685,989` | success, `row_count=1`, `detail_count=2` |
| `generated.dispatch.predicate.lower_starts_with_name_limit2` | generated direct `STARTS_WITH(LOWER(name), ...)` | `5` | `694,912` | success, `row_count=1`, `detail_count=2` |
| `generated.dispatch.predicate.upper_starts_with_name_limit2` | generated direct `STARTS_WITH(UPPER(name), ...)` | `5` | `694,944` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.starts_with_name_limit2` | typed direct `STARTS_WITH(name, ...)` | `5` | `706,481` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.lower_starts_with_name_limit2` | typed direct `STARTS_WITH(LOWER(name), ...)` | `5` | `715,958` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.upper_starts_with_name_limit2` | typed direct `STARTS_WITH(UPPER(name), ...)` | `5` | `715,990` | success, `row_count=1`, `detail_count=2` |
| `fluent.load.user_order_id_limit2` | fluent whole-row load | `5` | `496,037` | success, `row_count=2` |
| `fluent.load.user_name_eq_limit1` | fluent filtered load | `5` | `479,589` | success, `row_count=1` |
| `fluent.paged.user_order_id_limit2.first_page` | fluent paged first page | `5` | `652,858` | success, `row_count=2`, `has_cursor=true` |
| `fluent.paged.user_order_id_limit2.second_page` | fluent paged second page | `5` | `691,055` | success, `row_count=1`, `has_cursor=false` |
| `fluent.paged.user_order_id_limit2.invalid_cursor` | fluent paged invalid cursor | `5` | `91,468` | fail-closed `Query(Plan)` cursor decode rejection |
| `generated.dispatch.explain_delete` | generated `EXPLAIN DELETE` | `5` | `106,052` | success, `detail_count=10` |
| `generated.dispatch.delete` | generated `DELETE` projection | `1` | `1,034,770` | success, `row_count=1`, `detail_count=5` |
| `typed.dispatch.delete` | typed dispatch `DELETE` projection | `1` | `1,064,060` | success, `row_count=1`, `detail_count=5` |

## Comparison Highlights

Rows that existed on March 31 now look like this:

- generated dispatch projection: `592,238 -> 601,188` (`+8,950`, `+1.51%`)
- typed dispatch projection: `616,671 -> 621,315` (`+4,644`, `+0.75%`)
- `query_from_sql + execute_query`: `549,921 -> 542,651` (`-7,270`, `-1.32%`)
- typed `execute_sql`: `549,496 -> 542,349` (`-7,147`, `-1.30%`)
- grouped full page: `686,532 -> 677,246` (`-9,286`, `-1.35%`)
- grouped invalid cursor: `186,354 -> 178,203` (`-8,151`, `-4.37%`)
- global aggregate: `188,276 -> 207,494` (`+19,218`, `+10.21%`)
- generated `DELETE`: `1,095,056 -> 1,034,770` (`-60,286`, `-5.51%`)
- typed `DELETE`: `1,129,340 -> 1,064,060` (`-65,280`, `-5.78%`)
- fluent delete count-only: `1,053,912 -> 990,152` (`-63,760`, `-6.05%`)
- generated computed projection: `592,842 -> 597,564` (`+4,722`, `+0.80%`)
- typed computed projection: `590,898 -> 595,525` (`+4,627`, `+0.78%`)

Main read:

- the route-collapse and facade-thinning work did not create a broad perf
  regression across every SQL path
- grouped and delete execution clearly improved
- aggregate execution is still the main pressure point in this matrix
- scalar generated dispatch and explain lanes moved up slightly, but not by the
  same order of magnitude as the wasm reduction

## Operation Repeat Status

The segregated `x1` / `x10` / `x100` operation-repeat benchmark remains at:

- `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/operation-repeat-samples.json`

That focused matrix was captured earlier on `2026-04-03` and still shows:

- generated select: `x1 597,431`, `x10 596,632`, `x100 596,916`
- typed select: `x1 621,186`, `x10 620,360`, `x100 619,738`
- typed insert: `x1 492,029`, `x10 573,845`, `x100 632,727`
- typed update: `x1 764,562`, `x10 873,410`, `x100 930,727`
- fluent count-only delete: `x1 708,869`, `x10 708,496`, `x100 708,956`

I did not treat those rows as freshly rerun in this refresh. The dedicated
repeat rerun was blocked when the quickstart canister build helper attempted a
fresh Cargo resolution and hit the current crates.io mismatch on
`canic-cdk = ^0.22.3`.

## Artifacts

- fresh broad sample matrix:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/quickstart-samples.json`
- earlier same-day focused operation-repeat matrix:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/operation-repeat-samples.json`
- verification notes:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/verification-readout.md`

## Next Read

The broad runtime baseline is refreshed again. The next useful follow-up is:

- fix the current `canic-cdk = ^0.22.3` Cargo resolution mismatch so the
  focused operation-repeat matrix can be rerun on the same current tree, and
- keep watching aggregate execution separately from the broader grouped/delete
  improvements because it is now the clearest remaining regression in the
  quickstart surface matrix.
