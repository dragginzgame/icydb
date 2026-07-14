# Query Instruction Footprint Audit - 2026-03-31

## Report Preamble

- scope: numeric demo_rpg-canister baseline for query instruction footprint across generated SQL dispatch, typed SQL surfaces, fluent load surfaces, metadata lanes, explain, grouped aggregate, grouped `HAVING`, global aggregate, cursor continuation, and cursor rejection
- definition path: `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path: `N/A (first numeric run for this audit scope)`
- code snapshot identifier: `9a4c1c31`
- method tag/version: `PERF-0.3-demo_rpg-pocketic-surface-sampling-expanded`
- comparability status: `non-comparable`
  - this is the first same-method numeric baseline for this audit scope
  - future reruns using the same demo_rpg `sql_perf(...)` harness, fresh-canister-per-scenario topology, stable scenario keys, and the normalized artifact script should be comparable
- auditor: `Codex`
- run timestamp (UTC): `2026-03-31T13:58:23Z`
- branch: `main`
- worktree: `dirty`
- execution environment: `PocketIC + demo_rpg test canister`
- entities in scope: `User` for numeric measurements; fixture presence still verified for `Order` and `Character`
- entry surfaces in scope: demo_rpg generated `sql(...)`, typed `execute_sql_dispatch::<User>(...)`, typed `query_from_sql::<User>(...)` + `execute_query(...)`, typed `execute_sql::<User>(...)`, typed `execute_sql_grouped::<User>(...)`, typed `execute_sql_aggregate::<User>(...)`, fluent `load::<User>()`, fluent paged `load::<User>()`
- query shapes in scope: scalar projection, scalar whole-row load, filtered fluent load, delete mutation projection, metadata lane, explain, grouped aggregate, grouped `HAVING`, grouped continuation, grouped invalid cursor, global aggregate, paged fluent continuation, paged fluent invalid cursor, computed projection parity, rejection / unsupported path

## Initial Read

This rerun refreshes the demo_rpg numeric baseline after generated query-lane
real `DELETE` support and computed text projection parity landed.

The matrix now includes:

- fluent load totals
- paged first-page and second-page totals
- grouped first-page and second-page totals
- grouped empty-result `HAVING` totals
- grouped invalid-cursor rejection totals
- fluent invalid-cursor rejection totals
- metadata totals for `SHOW INDEXES`, `SHOW COLUMNS`, and `SHOW ENTITIES`
- grouped and global aggregate `EXPLAIN` totals
- generated `EXPLAIN DELETE` totals on the canister query surface
- generated and typed `DELETE ... LIMIT 1` totals on the `User` fixture path
- fluent count-only `DELETE ... LIMIT 1` totals on the same `User` fixture path
- one computed-projection parity check across generated vs typed dispatch

The method remains intentionally narrow:

- all authoritative totals come from demo_rpg wasm-side sampling via `performance_counter(1)`
- every scenario uses a fresh canister install plus fresh fixture load
- most scenarios execute `5` repeated calls inside one canister query to expose first-run and warmed-run spread
- the two mutating `DELETE` scenarios execute `1` call each so the sample does not fold repeated fixture mutation into one misleading warmed-run number

This is now a useful drift baseline for the demo_rpg canister path, but it is
still partial for the full audit definition because it does not yet isolate
parse/lower/plan phases and it still does not cover fluent grouped builders,
SQL cursor signature mismatch rejection, or host-only `icydb-core` surfaces
numerically.

Three follow-up continuation-path optimization reruns were captured after this
baseline:

- `cursor-hex-opt`: replace per-byte formatting in `encode_cursor(...)` with
  direct nibble encoding
- `cursor-hex-borrow-opt`: keep the hex change and stop cloning scalar/grouped
  cursor payloads just to serialize them
- `cursor-final-boundary-opt`: keep the earlier cursor encoding changes, move the
  final scalar cursor boundary into token construction, and clone grouped final
  keys only when a continuation cursor is actually needed

Comparison artifacts for those reruns are recorded under
`artifacts/perf-audit/optimization-comparison.cursor-reruns.{json,tsv}` plus the
three saved sample matrices.

Those saved cursor-optimization reruns were captured before generated
`EXPLAIN DELETE` support landed, so only the refreshed baseline matrix in this
report should be treated as current for that scenario.

## Query Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ---- | ---- | ----: | ----: | ---- |
| `generated.dispatch.projection.user_name_eq_limit` | generated `sql(...)` projection | `5` | `592,238` | success, `row_count=1`, `detail_count=2` |
| `typed.dispatch.projection.user_name_eq_limit` | typed `execute_sql_dispatch::<User>(...)` | `5` | `616,671` | success, `row_count=1`, `detail_count=2` |
| `typed.query_from_sql.execute.scalar_limit` | `query_from_sql + execute_query` | `5` | `549,921` | success, `row_count=2` |
| `typed.execute_sql.scalar_limit` | typed `execute_sql::<User>(...)` | `5` | `549,496` | success, `row_count=2` |
| `generated.dispatch.describe.user` | `DESCRIBE User` | `5` | `26,158` | success, `detail_count=5` |
| `generated.dispatch.explain.user_name_eq_limit` | explain scalar projection | `5` | `189,703` | success, `detail_count=10` |
| `generated.dispatch.explain.grouped.user_age_count` | explain grouped aggregate | `5` | `181,807` | success, `detail_count=10` |
| `generated.dispatch.explain.aggregate.user_count` | explain global aggregate | `5` | `92,134` | success, `detail_count=10` |
| `typed.execute_sql_grouped.user_age_count` | grouped full page | `5` | `686,532` | success, `row_count=3`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.having_empty` | grouped `HAVING` empty result | `5` | `716,441` | success, `row_count=0`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.limit2.first_page` | grouped paged first page | `5` | `719,721` | success, `row_count=2`, `has_cursor=true` |
| `typed.execute_sql_grouped.user_age_count.limit2.second_page` | grouped paged second page | `5` | `724,523` | success, `row_count=1`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.invalid_cursor` | grouped invalid cursor | `5` | `186,354` | fail-closed `Query(Plan)` cursor decode rejection |
| `typed.execute_sql_aggregate.user_count` | global aggregate | `5` | `188,276` | success, `Uint(3)` |
| `fluent.delete.user_order_id_limit1.count` | fluent delete count-only | `1` | `1,053,912` | success, `row_count=1`, `result_kind=delete_count` |
| `generated.dispatch.show_indexes.user` | `SHOW INDEXES User` | `5` | `21,319` | success, `detail_count=2` |
| `generated.dispatch.show_columns.user` | `SHOW COLUMNS User` | `5` | `34,666` | success, `detail_count=5` |
| `generated.dispatch.show_entities` | `SHOW ENTITIES` | `5` | `12,244` | success, `detail_count=3` |
| `generated.dispatch.computed_projection.lower_name_limit2` | generated computed projection | `5` | `592,842` | success, `row_count=2`, `detail_count=1` |
| `typed.dispatch.computed_projection.lower_name_limit2` | typed computed projection | `5` | `590,898` | success, `row_count=2`, `detail_count=1` |
| `fluent.load.user_order_id_limit2` | fluent load whole-row | `5` | `497,082` | success, `row_count=2` |
| `fluent.load.user_name_eq_limit1` | fluent filtered load | `5` | `478,501` | success, `row_count=1` |
| `fluent.paged.user_order_id_limit2.first_page` | fluent paged first page | `5` | `653,022` | success, `row_count=2`, `has_cursor=true` |
| `fluent.paged.user_order_id_limit2.second_page` | fluent paged second page | `5` | `693,435` | success, `row_count=1`, `has_cursor=false` |
| `fluent.paged.user_order_id_limit2.invalid_cursor` | fluent paged invalid cursor | `5` | `91,289` | fail-closed `Query(Plan)` cursor decode rejection |
| `generated.dispatch.explain_delete` | generated `EXPLAIN DELETE` | `5` | `103,157` | success, `detail_count=10` |
| `generated.dispatch.delete` | generated `DELETE` projection | `1` | `1,095,056` | success, `row_count=1`, `detail_count=5` |
| `typed.dispatch.delete` | typed `execute_sql_dispatch::<User>(DELETE ...)` | `1` | `1,129,340` | success, `row_count=1`, `detail_count=5` |

## Phase Attribution

| Phase Bucket | Current Signal | Status | Notes |
| ---- | ---- | ---- | ---- |
| `parse_sql` | included in end-to-end canister totals | `PARTIAL` | not phase-isolated |
| `lower_sql` | included in end-to-end canister totals | `PARTIAL` | not phase-isolated |
| `normalize/canonicalize` | structural only | `PARTIAL` | still no stable checkpoint transport |
| `plan_access` | explain totals plus structural localization | `PARTIAL` | measurable only as part of whole-call totals |
| `compile_runtime` | no direct phase transport | `FAIL` | still not exposed |
| `execute_access` | included in end-to-end totals | `PARTIAL` | no isolated access-only totals |
| `execute_post_access` | included in end-to-end totals | `PARTIAL` | no isolated post-access totals |
| `assemble_projection` | included in projection totals | `PARTIAL` | not split from general query execution |
| `grouped_aggregate` | direct grouped totals and grouped `HAVING` totals present | `PARTIAL` | grouped fold/output still combined |
| `global_aggregate` | direct aggregate totals present | `PARTIAL` | aggregate terminal still combined |
| `cursor_continuation` | second-page and invalid-cursor totals now present | `PARTIAL` | continuation/rejection are measurable, but not phase-isolated |
| `render_explain` | scalar, grouped, and aggregate explain totals present | `PARTIAL` | render-only split still unavailable |
| `map_rejection` | unsupported and invalid-cursor totals present | `PARTIAL` | rejection mapping still combined with full call |

## Phase Coverage Gaps

- Host-side query metrics remain unusable for instruction totals because `read_perf_counter()` still returns `0` outside `wasm32`.
- Demo RPG totals are still end-to-end surface totals, not internal phase checkpoints.
- Fluent grouped builders are not yet sampled numerically.
- SQL canister dynamic dispatch is only numerically sampled on the demo_rpg topology, not across other fixture canisters.
- The current matrix is still anchored to one representative entity (`User`) for numeric comparisons.
- Cursor rejection now has invalid-payload totals, but query-signature mismatch cursor totals are still missing.

## Structural Hotspots

Measured totals now exist, but structural audits still explain where regressions
are likely to land first.

| Shared Hub | Evidence | Why It Matters For Perf |
| ---- | ---- | ---- |
| `db::sql::lowering` | all sampled SQL lanes still pay it | lowering drift remains shared tax across SQL entry surfaces |
| `db::query::plan::access_choice::evaluator` | top query-relevant hotspot in the latest complexity audit | scalar query totals will move here before leaf APIs diverge |
| `db::predicate::runtime` | shared runtime hotspot in the latest complexity audit | fluent and SQL filtered paths both concentrate here |
| `db::executor::explain::descriptor::shared` | explain hotspot in the latest complexity audit | explain is now measured across scalar, grouped, and aggregate shapes |
| `db::session::sql::dispatch` | convergence surface for typed SQL lanes | dispatch skew and unsupported-lane drift show up here early |

## Planner / Lowering Pressure

- The parser split means parser itself is no longer the main shared perf-risk hotspot for this audit.
- `db::sql::lowering` still dominates the shared SQL cost story because every successful SQL path in this matrix pays it.
- The tiny gap between `query_from_sql + execute_query` and `execute_sql` (`549,921` vs `549,496`) suggests the typed lowered-query shell itself is not a meaningful extra runtime tax for this sampled scalar shape.

## Executor / Predicate Pressure

- Real `DELETE` is now the heaviest successful path in the matrix, with generated dispatch at `1,095,056` average local instructions and typed dispatch at `1,129,340`.
- Count-only fluent delete on the same `ORDER BY id LIMIT 1` shape measured `1,053,912`, so most of the current delete cost is mutation and commit work rather than deleted-row projection.
- Grouped execution remains the heaviest successful non-mutation path in the matrix, with the paged first page at `719,721` average local instructions.
- Adding a grouped `HAVING COUNT(*) > 1000` empty-result path moved grouped cost from `686,532` to `716,441` (`+4.4%`) on this fixture, so the grouped fold still dominates that lane even after the empty-result filter.
- Fluent whole-row load is materially cheaper than equivalent SQL whole-row execution on the sampled shape.
- Invalid cursor rejection is not free: grouped invalid cursor averaged `186,354`, while fluent invalid cursor averaged `91,289`.
- Explain remains much cheaper than successful projection/load/grouped paths, but it is still large enough to monitor as real runtime cost.

## Entry Surface Skew

Measured skew in this run:

- typed dispatch projection vs generated dispatch projection: `616,671` vs `592,238` (`+4.1%`)
- typed dispatch delete vs generated dispatch delete: `1,129,340` vs `1,095,056` (`+3.1%`)
- generated dispatch delete projection vs fluent count-only delete: `1,095,056` vs `1,053,912` (`+3.9%`)
- typed dispatch delete projection vs fluent count-only delete: `1,129,340` vs `1,053,912` (`+7.2%`)
- typed `query_from_sql + execute_query` vs typed `execute_sql`: `549,921` vs `549,496` (`+0.1%`, effectively parity)
- fluent whole-row load vs typed `execute_sql` whole-row load: `497,082` vs `549,496` (`-9.5%`)
- fluent paged first page vs fluent non-paged whole-row load: `653,022` vs `497,082` (`+31.4%`)
- grouped first page with continuation vs grouped full page: `719,721` vs `686,532` (`+4.8%`)
- grouped second page vs grouped first page with continuation: `724,523` vs `719,721` (`+0.7%`)
- grouped invalid cursor vs fluent invalid cursor: `186,354` vs `91,289` (`+104.1%`)
- grouped `HAVING` empty result vs grouped full page: `716,441` vs `686,532` (`+4.4%`)
- grouped `EXPLAIN` vs grouped execute: `181,807` vs `686,532` (`-73.5%`)
- aggregate `EXPLAIN` vs aggregate execute: `92,134` vs `188,276` (`-51.1%`)
- generated `EXPLAIN DELETE` vs generated scalar `EXPLAIN`: `103,157` vs `189,703` (`-45.6%`)
- generated `DELETE` vs generated `EXPLAIN DELETE`: `1,095,056` vs `103,157` (`10.6x`)
- generated computed projection vs typed computed projection: `592,842` vs `590,898` (`+0.3%`)

Most important surface finding:

- typed and generated dispatch now keep computed projection in near-parity at `590,898` vs `592,842`
- generated demo_rpg `sql(...)` now supports `EXPLAIN DELETE` at `103,157` average local instructions
- generated demo_rpg `sql(...)` now supports real `DELETE` at `1,095,056` average local instructions and stays slightly cheaper than typed dispatch on the same sampled shape
- count-only fluent delete on the same sampled shape is `1,053,912`, so deleted-row projection is currently only a small fraction of end-to-end delete cost on this path

The main remaining surface skew in this matrix is aggregate handling, not computed projection.

## Optimization Reruns

The continuation path was a good optimization target because the baseline showed
that first-page cursor emission was materially more expensive than equivalent
non-paged paths.

The comparison artifacts have now been refreshed against the current baseline on
the comparable scenario set. That means the refreshed `base` column is the
post-`EXPLAIN DELETE` and post-computed-projection-parity baseline in this
report, while `opt1` / `opt2` / `opt3` remain the saved historical cursor
optimization sample matrices.

Refreshed comparison summary against the current baseline:

- `cursor-hex-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `653,022 -> 654,621`
    (`+1,599`, `+0.24%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `719,721 -> 731,102`
    (`+11,381`, `+1.58%`)
- `cursor-hex-borrow-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `653,022 -> 653,841`
    (`+819`, `+0.13%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `719,721 -> 729,874`
    (`+10,153`, `+1.41%`)
- `cursor-final-boundary-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `653,022 -> 650,688`
    (`-2,334`, `-0.36%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `719,721 -> 727,451`
    (`+7,730`, `+1.07%`)

Current read:

- the current baseline now beats every saved grouped first-page cursor snapshot,
  so the grouped continuation improvements from the current tree are stronger
  than the old saved cursor-only reruns
- only the final-boundary snapshot still beats the refreshed baseline on the
  fluent first-page cursor hotspot, and only slightly
- the earlier `cursor-hex-opt` and `cursor-hex-borrow-opt` snapshots now land
  slightly above the refreshed baseline, which is expected drift once later
  unrelated changes are included in the current tree
- non-target scenarios are no longer a pure noise band in this rebased view;
  several explain/metadata lanes now differ by a few percent, so these refreshed
  comparison artifacts should be read as a consistency check, not as a clean
  replacement for rerunning the old code states
- `generated.dispatch.explain_delete` is intentionally excluded from the
  refreshed comparison set because the saved cursor sample matrices predate that
  surface becoming a successful explain path
- `generated.dispatch.computed_projection.lower_name_limit2` is also excluded
  because the saved cursor sample matrices predate generated computed
  projection becoming a successful path instead of a fail-closed rejection

## Early Warning Signals

- Real `DELETE` is now measured on both projection and count-only surfaces, but still only on one representative `User ORDER BY id LIMIT 1` shape.
- Cursor continuation now has both second-page totals and invalid-payload rejection totals, and both fluent and grouped pagination remain materially more expensive than their non-paged counterparts.
- Metadata lanes remain cheap enough to serve as useful “sanity floor” scenarios in future reruns.
- Grouped and aggregate explain totals are now present, so future planner/explain drift will be easier to localize than in the first numeric pass.
- The next shared SQL or planner refactor should be rerun against this matrix so the project gets its first true regression deltas.

## Risk Score

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| authoritative instruction capture availability | `4` | `2` | `8` |
| phase attribution coverage | `7` | `2` | `14` |
| entry-surface verification breadth | `2` | `1` | `2` |
| structural hotspot localization quality | `3` | `1` | `3` |
| freshness / comparability readiness | `4` | `2` | `8` |

`overall_index = 35 / 8 = 4.4`

Interpretation: moderate risk. The audit now has a meaningful numeric baseline
for one representative canister path, plus grouped `HAVING`, grouped/global
`EXPLAIN`, and cursor rejection totals, but it still needs broader surface
coverage and phase isolation.

## Verification Readout

- comparability status: `non-comparable` to history because this is still the first same-method numeric baseline
- authoritative instruction rows: `present` for `27` measured demo_rpg canister scenarios
- structural coverage scan: `PASS`
- runtime verification: `PASS`
- overall audit status: `PARTIAL`

Verification commands:

- `cargo check -p icydb-core` -> PASS
- `cargo test -p canister_demo_rpg --features sql -- --nocapture` -> PASS
- `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture` -> PASS

## Follow-Up Actions

- Add one fluent grouped-builder scenario.
- Add one cursor signature-mismatch rejection scenario.
- Add one additional global aggregate beyond `COUNT(*)` so the aggregate lane is not a single-shape baseline.
- Add optional phase checkpoints only if they can be captured without distorting the measured surfaces.
- Re-run this matrix after the next shared SQL/planner change and treat that rerun as the first real regression comparison.
