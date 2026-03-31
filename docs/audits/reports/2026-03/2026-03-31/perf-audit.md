# Query Instruction Footprint Audit - 2026-03-31

## Report Preamble

- scope: numeric quickstart-canister baseline for query instruction footprint across generated SQL dispatch, typed SQL surfaces, fluent load surfaces, metadata lanes, explain, grouped aggregate, grouped `HAVING`, global aggregate, cursor continuation, and cursor rejection
- definition path: `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path: `N/A (first numeric run for this audit scope)`
- code snapshot identifier: `9efaa1bb`
- method tag/version: `PERF-0.3-quickstart-pocketic-surface-sampling-expanded`
- comparability status: `non-comparable`
  - this is the first same-method numeric baseline for this audit scope
  - future reruns using the same quickstart `sql_perf(...)` harness, fresh-canister-per-scenario topology, stable scenario keys, and the normalized artifact script should be comparable
- auditor: `Codex`
- run timestamp (UTC): `2026-03-31T08:44:08Z`
- branch: `main`
- worktree: `dirty`
- execution environment: `PocketIC + quickstart test canister`
- entities in scope: `User` for numeric measurements; fixture presence still verified for `Order` and `Character`
- entry surfaces in scope: quickstart generated `sql(...)`, typed `execute_sql_dispatch::<User>(...)`, typed `query_from_sql::<User>(...)` + `execute_query(...)`, typed `execute_sql::<User>(...)`, typed `execute_sql_grouped::<User>(...)`, typed `execute_sql_aggregate::<User>(...)`, fluent `load::<User>()`, fluent paged `load::<User>()`
- query shapes in scope: scalar projection, scalar whole-row load, filtered fluent load, metadata lane, explain, grouped aggregate, grouped `HAVING`, grouped continuation, grouped invalid cursor, global aggregate, paged fluent continuation, paged fluent invalid cursor, computed projection parity, rejection / unsupported path

## Initial Read

This run materially improves the quickstart numeric baseline.

The matrix now includes:

- fluent load totals
- paged first-page and second-page totals
- grouped first-page and second-page totals
- grouped empty-result `HAVING` totals
- grouped invalid-cursor rejection totals
- fluent invalid-cursor rejection totals
- metadata totals for `SHOW INDEXES`, `SHOW COLUMNS`, and `SHOW ENTITIES`
- grouped and global aggregate `EXPLAIN` totals
- one computed-projection parity check across generated vs typed dispatch

The method remains intentionally narrow:

- all authoritative totals come from quickstart wasm-side sampling via `performance_counter(1)`
- every scenario uses a fresh canister install plus fresh fixture load
- each scenario executes `5` repeated calls inside one canister query to expose first-run and warmed-run spread

This is now a useful drift baseline for the quickstart canister path, but it is
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

## Query Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ---- | ---- | ----: | ----: | ---- |
| `generated.dispatch.projection.user_name_eq_limit` | generated `sql(...)` projection | `5` | `593,469` | success, `row_count=1` |
| `typed.dispatch.projection.user_name_eq_limit` | typed `execute_sql_dispatch::<User>(...)` | `5` | `615,207` | success, `row_count=1` |
| `typed.query_from_sql.execute.scalar_limit` | `query_from_sql + execute_query` | `5` | `548,444` | success, `row_count=2` |
| `typed.execute_sql.scalar_limit` | typed `execute_sql::<User>(...)` | `5` | `548,019` | success, `row_count=2` |
| `fluent.load.user_order_id_limit2` | fluent load whole-row | `5` | `495,963` | success, `row_count=2` |
| `fluent.load.user_name_eq_limit1` | fluent filtered load | `5` | `477,430` | success, `row_count=1` |
| `fluent.paged.user_order_id_limit2.first_page` | fluent paged first page | `5` | `733,210` | success, `row_count=2`, `has_cursor=true` |
| `fluent.paged.user_order_id_limit2.second_page` | fluent paged second page | `5` | `690,190` | success, `row_count=1`, `has_cursor=false` |
| `fluent.paged.user_order_id_limit2.invalid_cursor` | fluent paged invalid cursor | `5` | `91,040` | fail-closed `Query(Plan)` cursor decode rejection |
| `typed.execute_sql_grouped.user_age_count` | grouped full page | `5` | `701,949` | success, `row_count=3`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.having_empty` | grouped `HAVING` empty result | `5` | `714,666` | success, `row_count=0`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.limit2.first_page` | grouped paged first page | `5` | `786,619` | success, `row_count=2`, `has_cursor=true` |
| `typed.execute_sql_grouped.user_age_count.limit2.second_page` | grouped paged second page | `5` | `729,025` | success, `row_count=1`, `has_cursor=false` |
| `typed.execute_sql_grouped.user_age_count.invalid_cursor` | grouped invalid cursor | `5` | `185,702` | fail-closed `Query(Plan)` cursor decode rejection |
| `typed.execute_sql_aggregate.user_count` | global aggregate | `5` | `187,699` | success, `Uint(3)` |
| `generated.dispatch.explain.user_name_eq_limit` | explain scalar projection | `5` | `184,902` | success, `detail_count=10` |
| `generated.dispatch.explain.grouped.user_age_count` | explain grouped aggregate | `5` | `177,820` | success, `detail_count=10` |
| `generated.dispatch.explain.aggregate.user_count` | explain global aggregate | `5` | `91,473` | success, `detail_count=10` |
| `generated.dispatch.describe.user` | `DESCRIBE User` | `5` | `26,373` | success, `detail_count=5` |
| `generated.dispatch.show_indexes.user` | `SHOW INDEXES User` | `5` | `21,534` | success, `detail_count=2` |
| `generated.dispatch.show_columns.user` | `SHOW COLUMNS User` | `5` | `34,873` | success, `detail_count=5` |
| `generated.dispatch.show_entities` | `SHOW ENTITIES` | `5` | `12,450` | success, `detail_count=3` |
| `typed.dispatch.computed_projection.lower_name_limit2` | typed computed projection | `5` | `589,334` | success, `row_count=2` |
| `generated.dispatch.computed_projection.lower_name_limit2` | generated computed projection | `5` | `66,779` | fail-closed unsupported |
| `generated.dispatch.rejection.explain_delete` | generated rejection path | `5` | `35,290` | fail-closed unsupported |

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
- Quickstart totals are still end-to-end surface totals, not internal phase checkpoints.
- Fluent grouped builders are not yet sampled numerically.
- SQL canister dynamic dispatch is only numerically sampled on the quickstart topology, not across other fixture canisters.
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
- The tiny gap between `query_from_sql + execute_query` and `execute_sql` (`548,444` vs `548,019`) suggests the typed lowered-query shell itself is not a meaningful extra runtime tax for this sampled scalar shape.

## Executor / Predicate Pressure

- Grouped execution remains the heaviest successful path in the matrix, with the paged first page at `786,619` average local instructions.
- Adding a grouped `HAVING COUNT(*) > 1000` empty-result path only moved grouped cost from `701,949` to `714,666` (`+1.8%`) on this fixture, so the grouped fold itself still dominates that lane more than the empty-result filter.
- Fluent whole-row load is materially cheaper than equivalent SQL whole-row execution on the sampled shape.
- Invalid cursor rejection is not free: grouped invalid cursor averaged `185,702`, while fluent invalid cursor averaged `91,040`.
- Explain remains much cheaper than successful projection/load/grouped paths, but it is still large enough to monitor as real runtime cost.

## Entry Surface Skew

Measured skew in this run:

- typed dispatch projection vs generated dispatch projection: `615,207` vs `593,469` (`+3.7%`)
- typed `query_from_sql + execute_query` vs typed `execute_sql`: `548,444` vs `548,019` (`+0.1%`, effectively parity)
- fluent whole-row load vs typed `execute_sql` whole-row load: `495,963` vs `548,019` (`-9.5%`)
- fluent paged first page vs fluent non-paged whole-row load: `733,210` vs `495,963` (`+47.8%`)
- grouped first page with continuation vs grouped full page: `786,619` vs `701,949` (`+12.1%`)
- grouped second page vs grouped first page with continuation: `729,025` vs `786,619` (`-7.3%`)
- grouped invalid cursor vs fluent invalid cursor: `185,702` vs `91,040` (`+104.0%`)
- grouped `HAVING` empty result vs grouped full page: `714,666` vs `701,949` (`+1.8%`)
- grouped `EXPLAIN` vs grouped execute: `177,820` vs `701,949` (`-74.7%`)
- aggregate `EXPLAIN` vs aggregate execute: `91,473` vs `187,699` (`-51.3%`)
- generated rejection vs generated `DESCRIBE`: `35,290` vs `26,373` (`+33.8%`)

Most important surface finding:

- typed dispatch supports computed projection at `589,334` average local instructions
- generated quickstart `sql(...)` still rejects the same computed projection shape fail-closed at `66,779`

That is a real product-surface skew, not just a perf difference.

## Optimization Reruns

The continuation path was a good optimization target because the baseline showed
that first-page cursor emission was materially more expensive than equivalent
non-paged paths.

Rerun summary against the saved baseline:

- `cursor-hex-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `733,210 -> 654,621`
    (`-78,589`, `-10.7%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `786,619 -> 731,102`
    (`-55,517`, `-7.1%`)
- `cursor-hex-borrow-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `733,210 -> 653,841`
    (`-79,369`, `-10.8%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `786,619 -> 729,874`
    (`-56,745`, `-7.2%`)
- `cursor-final-boundary-opt`
  - `fluent.paged.user_order_id_limit2.first_page`: `733,210 -> 650,688`
    (`-82,522`, `-11.3%`)
  - `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `786,619 -> 727,451`
    (`-59,168`, `-7.5%`)

What did not move meaningfully:

- most non-target scenarios stayed inside roughly `-0.75%` to `+0.72%`
- second-page scalar pagination stayed effectively flat, and grouped second-page
  moved slightly down with the same patch family
- metadata and unsupported generated dispatch lanes stayed near their prior
  floor, with only trace-level noise

Interpretation:

- the direct hex encoder change is a strong, credible win because only the
  cursor-emitting first-page scenarios moved materially
- the borrowed-wire encode change adds only a marginal extra gain beyond the
  hex optimization (`-0.12%` on fluent first page and `-0.17%` on grouped first
  page versus the first optimization rerun), so its incremental effect is close
  to the current run-to-run noise floor
- the final-boundary / grouped-last-key cleanup adds another small but credible
  step on the same targeted first-page paths (`-0.48%` fluent and `-0.33%`
  grouped versus the borrowed-wire rerun) without introducing any broad
  regression pattern in the rest of the matrix

## Early Warning Signals

- Generated quickstart `sql(...)` is still behind typed dispatch for computed projection support.
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
- authoritative instruction rows: `present` for `25` measured quickstart canister scenarios
- structural coverage scan: `PASS`
- runtime verification: `PASS`
- overall audit status: `PARTIAL`

Verification commands:

- `cargo check -p icydb-core` -> PASS
- `cargo test -p canister_quickstart --features sql -- --nocapture` -> PASS
- `POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_canister sql_canister_perf_harness_reports_positive_instruction_samples -- --nocapture` -> PASS

## Follow-Up Actions

- Add one fluent grouped-builder scenario.
- Add one cursor signature-mismatch rejection scenario.
- Add one additional global aggregate beyond `COUNT(*)` so the aggregate lane is not a single-shape baseline.
- Add optional phase checkpoints only if they can be captured without distorting the measured surfaces.
- Re-run this matrix after the next shared SQL/planner change and treat that rerun as the first real regression comparison.
