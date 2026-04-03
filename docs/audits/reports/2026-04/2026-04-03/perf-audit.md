# Query Instruction Footprint Audit - 2026-04-03

## Report Preamble

- scope: refreshed quickstart-canister PocketIC baseline for generated SQL
  dispatch, typed SQL surfaces, fluent load/paged surfaces, metadata lanes,
  explain, grouped aggregate, global aggregate, delete, write, batch insert,
  computed projection, and direct `STARTS_WITH(...)` predicate paths
- definition path: `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-31/perf-audit.md`
- code snapshot identifier: `b7f86bac`
- method tag/version: `PERF-0.3-quickstart-pocketic-surface-sampling-expanded`
- comparability status: `partial`
  - the broad quickstart surface matrix below is a fresh same-method rerun and
    is directly comparable for scenario keys that already existed on
    `2026-03-31`
  - the matrix also includes newer write and direct `STARTS_WITH(...)` rows that
    did not exist in the March 31 baseline
  - the earlier same-day focused `x1` / `x10` / `x100` operation-repeat matrix
    remains useful reference material, but it was not rerun during this
    refresh because the current pass was focused on the broad matrix plus the
    shared scalar query hot path
- auditor: `Codex`
- run timestamp (UTC): `2026-04-03T11:34:50Z`
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

This rerun refreshes the broad baseline after the accepted `0.67.4` shared
query-runtime cuts landed. The current matrix says:

- generated and typed scalar dispatch are now materially cheaper than the
  March 31 baseline
- typed structural whole-row SQL (`query_from_sql + execute_query` and
  `execute_sql`) also moved down further
- computed projection now tracks the same broad scalar read improvement
- grouped execution and grouped invalid-cursor rejection remain cheaper
- delete remains materially cheaper on generated, typed, and fluent count-only
  paths
- aggregate execution is still the clearest remaining regression versus the
  March 31 baseline

## Current Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ---- | ---- | ----: | ----: | ---- |
| `generated.dispatch.projection.user_name_eq_limit` | generated dispatch projection | `5` | `502,733` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.projection.user_name_eq_limit` | typed dispatch projection | `5` | `500,817` | success, `row_count=1`, `detail_count=2` |
| `typed.query_from_sql.execute.scalar_limit` | `query_from_sql + execute_query` | `5` | `535,095` | success, `row_count=2` |
| `typed.execute_sql.scalar_limit` | typed `execute_sql::<User>(...)` | `5` | `534,942` | success, `row_count=2` |
| `generated.dispatch.describe.user` | `DESCRIBE User` | `5` | `27,018` | success, `detail_count=5` |
| `generated.dispatch.explain.user_name_eq_limit` | explain scalar projection | `5` | `192,769` | success, `detail_count=10` |
| `generated.dispatch.explain.grouped.user_age_count` | explain grouped aggregate | `5` | `184,598` | success, `detail_count=10` |
| `generated.dispatch.explain.aggregate.user_count` | explain global aggregate | `5` | `95,226` | success, `detail_count=10` |
| `typed.execute_sql_grouped.user_age_count` | grouped full page | `5` | `676,211` | success, `row_count=3`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.having_empty` | grouped `HAVING` empty result | `5` | `704,436` | success, `row_count=0`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.limit2.first_page` | grouped paged first page | `5` | `710,850` | success, `row_count=2`, `has_cursor=true` |
| `typed.execute_sql_grouped.user_age_count.limit2.second_page` | grouped paged second page | `5` | `714,890` | success, `row_count=1`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.invalid_cursor` | grouped invalid cursor | `5` | `177,715` | fail-closed `Query(Plan)` cursor decode rejection |
| `typed.execute_sql_aggregate.user_count` | global aggregate | `5` | `207,759` | success, `Uint(3)` |
| `typed.insert.user_single` | typed single insert | `1` | `492,568` | success, `row_count=1` |
| `typed.insert_many_atomic.user_10` | typed atomic insert-many `x10` | `1` | `5,346,879` | success, `row_count=10` |
| `typed.insert_many_atomic.user_100` | typed atomic insert-many `x100` | `1` | `56,179,429` | success, `row_count=100` |
| `typed.insert_many_atomic.user_1000` | typed atomic insert-many `x1000` | `1` | `601,732,245` | success, `row_count=1000` |
| `typed.insert_many_non_atomic.user_10` | typed non-atomic insert-many `x10` | `1` | `5,386,862` | success, `row_count=10` |
| `typed.insert_many_non_atomic.user_100` | typed non-atomic insert-many `x100` | `1` | `59,323,257` | success, `row_count=100` |
| `typed.insert_many_non_atomic.user_1000` | typed non-atomic insert-many `x1000` | `1` | `696,482,354` | success, `row_count=1000` |
| `typed.update.user_single` | typed single update | `1` | `763,229` | success, `row_count=1` |
| `fluent.delete.user_order_id_limit1.count` | fluent delete count-only | `1` | `986,270` | success, `row_count=1`, `result_kind=delete_count` |
| `generated.dispatch.show_indexes.user` | `SHOW INDEXES User` | `5` | `21,640` | success, `detail_count=2` |
| `generated.dispatch.show_columns.user` | `SHOW COLUMNS User` | `5` | `34,736` | success, `detail_count=5` |
| `generated.dispatch.show_entities` | `SHOW ENTITIES` | `5` | `12,686` | success, `detail_count=3` |
| `generated.dispatch.computed_projection.lower_name_limit2` | generated computed projection | `5` | `488,716` | success, `row_count=2`, `detail_count=1` |
| `typed.dispatch.computed_projection.lower_name_limit2` | typed computed projection | `5` | `486,790` | success, `row_count=2`, `detail_count=1` |
| `generated.dispatch.predicate.starts_with_name_limit2` | generated direct `STARTS_WITH(name, ...)` | `5` | `597,922` | success, `row_count=1`, `detail_count=2` |
| `generated.dispatch.predicate.lower_starts_with_name_limit2` | generated direct `STARTS_WITH(LOWER(name), ...)` | `5` | `605,351` | success, `row_count=1`, `detail_count=2` |
| `generated.dispatch.predicate.upper_starts_with_name_limit2` | generated direct `STARTS_WITH(UPPER(name), ...)` | `5` | `605,383` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.starts_with_name_limit2` | typed direct `STARTS_WITH(name, ...)` | `5` | `596,181` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.lower_starts_with_name_limit2` | typed direct `STARTS_WITH(LOWER(name), ...)` | `5` | `603,496` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.predicate.upper_starts_with_name_limit2` | typed direct `STARTS_WITH(UPPER(name), ...)` | `5` | `603,528` | success, `row_count=1`, `detail_count=2` |
| `fluent.load.user_order_id_limit2` | fluent whole-row load | `5` | `488,786` | success, `row_count=2` |
| `fluent.load.user_name_eq_limit1` | fluent filtered load | `5` | `467,539` | success, `row_count=1` |
| `fluent.paged.user_order_id_limit2.first_page` | fluent paged first page | `5` | `644,438` | success, `row_count=2`, `has_cursor=true` |
| `fluent.paged.user_order_id_limit2.second_page` | fluent paged second page | `5` | `683,896` | success, `row_count=1`, `has_cursor=false` |
| `fluent.paged.user_order_id_limit2.invalid_cursor` | fluent paged invalid cursor | `5` | `91,068` | fail-closed `Query(Plan)` cursor decode rejection |
| `generated.dispatch.explain_delete` | generated `EXPLAIN DELETE` | `5` | `104,824` | success, `detail_count=10` |
| `generated.dispatch.delete` | generated `DELETE` projection | `1` | `1,032,547` | success, `row_count=1`, `detail_count=5` |
| `typed.dispatch.delete` | typed dispatch `DELETE` projection | `1` | `1,057,008` | success, `row_count=1`, `detail_count=5` |

## Comparison Highlights

Rows that existed on March 31 now look like this:

- generated dispatch projection: `592,238 -> 502,733` (`-89,505`, `-15.11%`)
- typed dispatch projection: `616,671 -> 500,817` (`-115,854`, `-18.79%`)
- `query_from_sql + execute_query`: `549,921 -> 535,095` (`-14,826`, `-2.70%`)
- typed `execute_sql`: `549,496 -> 534,942` (`-14,554`, `-2.65%`)
- grouped full page: `686,532 -> 676,211` (`-10,321`, `-1.50%`)
- grouped invalid cursor: `186,354 -> 177,715` (`-8,639`, `-4.64%`)
- global aggregate: `188,276 -> 207,759` (`+19,483`, `+10.35%`)
- generated `DELETE`: `1,095,056 -> 1,032,547` (`-62,509`, `-5.71%`)
- typed `DELETE`: `1,129,340 -> 1,057,008` (`-72,332`, `-6.40%`)
- fluent delete count-only: `1,053,912 -> 986,270` (`-67,642`, `-6.42%`)
- generated computed projection: `592,842 -> 488,716` (`-104,126`, `-17.56%`)
- typed computed projection: `590,898 -> 486,790` (`-104,108`, `-17.62%`)

Main read:

- the accepted `0.67.4` shared-query runtime cuts now show up clearly in the
  broad matrix instead of only in the focused attribution benchmark
- scalar dispatch, whole-row load, and computed-projection reads all moved
  down materially
- grouped and delete execution remain clear wins
- aggregate execution is still the main pressure point in this matrix

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
repeat matrix remains the earlier same-day reference capture while this pass
focused on the broad surface matrix plus the scalar execute hot path.

## Artifacts

- fresh broad sample matrix:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/quickstart-samples.json`
- earlier same-day focused operation-repeat matrix:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/operation-repeat-samples.json`
- verification notes:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/perf-audit/verification-readout.md`

## Next Read

The broad runtime baseline is refreshed again. The next useful follow-up is:

- rerun the focused `x1` / `x10` / `x100` operation-repeat matrix on this same
  current tree so the published repeat rows catch up with the lower scalar
  query baseline, and
- keep watching aggregate execution separately from the broader scalar/grouped
  /delete improvements because it is still the clearest remaining regression in
  the quickstart surface matrix.
