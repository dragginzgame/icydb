# Complexity Accretion Audit - 2026-04-30

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-27/complexity-accretion.md`
- code snapshot identifier: `2513e1311` (`dirty` working tree before audit artifacts were generated)
- dirty-tree caveat: this rerun includes the updated expression-planning work in `db/query/plan/expr`, including the canonicalize split, predicate bridge extraction, and predicate/projection cleanup
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable with caveat` against `2026-04-27/complexity-accretion.md` because the method manifest, runtime scope, and metric generator are unchanged; the caveat is that this snapshot is not a clean-tree release baseline

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-30/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-30/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 - Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`733` modules):
  - `docs/audits/reports/2026-04/2026-04-30/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-30/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::sql::parser::projection` | `crates/icydb-core/src/db/sql/parser/projection.rs` | 919 | 11 | 64 | 5.82 | 64 | 64 | 3 | 2 | 75 |
| `db::query::plan::expr::type_inference` | `crates/icydb-core/src/db/query/plan/expr/type_inference/mod.rs` | 856 | 22 | 110 | 5.00 | 32 | 30 | 3 | 3 | 52 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 407 | 10 | 26 | 2.60 | 34 | 34 | 3 | 2 | 44 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 856 | 21 | 135 | 6.43 | 20 | 20 | 2 | 3 | 41 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 365 | 10 | 39 | 3.90 | 30 | 30 | 2 | 3 | 40 |
| `db::query::plan::expr::projection_eval` | `crates/icydb-core/src/db/query/plan/expr/projection_eval.rs` | 571 | 22 | 82 | 3.73 | 14 | 12 | 2 | 2 | 34 |
| `db::query::plan::expr::predicate_compile` | `crates/icydb-core/src/db/query/plan/expr/predicate_compile.rs` | 569 | 20 | 82 | 4.10 | 14 | 14 | 2 | 2 | 34 |
| `db::query::explain::render` | `crates/icydb-core/src/db/query/explain/render.rs` | 282 | 1 | 1 | 1.00 | 33 | 33 | 2 | 1 | 34 |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime/mod.rs` | 849 | 18 | 98 | 5.44 | 15 | 15 | 2 | 3 | 33 |
| `db::sql::parser::order_expr` | `crates/icydb-core/src/db/sql/parser/order_expr.rs` | 556 | 5 | 66 | 13.20 | 32 | 28 | 2 | 2 | 33 |

## STEP 0 - Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 624 | 733 | +109 |
| Runtime LOC | `[M]` | primary | 119,813 | 125,235 | +5,422 |
| Runtime fanout (sum) | `[M]` | primary | 1,293 | 1,472 | +179 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 10 | 5 | -5 |
| Total branch sites | `[M]` | primary | 4,259 | 4,398 | +139 |
| Top-10 branch concentration | `[D]` | primary | 0.1219 | 0.0955 | -0.0264 |
| Top-10 fanout concentration | `[D]` | primary | 0.0472 | 0.0414 | -0.0058 |
| Modules with `max_branch_depth >= 3` | `[D]` | primary | 27 | 27 | 0 |

## Current Complexity Signals

- Runtime scope is larger than the April 27 baseline:
  - files: `624 -> 733`
  - LOC: `119,813 -> 125,235`
  - total branch sites: `4,259 -> 4,398`
- Branch pressure grew in aggregate but de-concentrated materially:
  - modules with `branch_sites_total >= 40`: `10 -> 5`
  - top-10 branch concentration: `0.1219 -> 0.0955`
  - modules with `max_branch_depth >= 3`: `27 -> 27`
- Fanout pressure remains structurally bounded:
  - modules with `fanout > 12`: `0 -> 0`
  - top-10 fanout concentration: `0.0472 -> 0.0414`
- Compared with the earlier same-day snapshot, the updated plan/expr work nudged total size up but improved top-tier branch concentration:
  - files: `732 -> 733`
  - LOC: `125,153 -> 125,235`
  - total branch sites: `4,393 -> 4,398`
  - top-10 branch concentration: `0.0967 -> 0.0955`
  - `db::query::plan::expr::predicate_compile`: `38 -> 34`
  - `db::query::plan::expr::projection_eval`: `35 -> 34`
- The largest removed or split hotspots from the prior report are no longer monolithic:
  - `db::query::plan::expr::canonicalize` moved from `996 LOC / 64 branch sites` to a directory split; the root now has `44 LOC / 0 branch sites`, with branch pressure distributed into `normalize`, `truth_admission`, and `case`
  - `db::sql::lowering::aggregate` moved from `1,272 LOC / 45 branch sites` to a directory split; semantic, strategy, terminal, projection, command, lowering, and grouped files now carry smaller local responsibilities
  - `db::executor::aggregate::contracts::state` moved from `868 LOC / 62 branch sites` to a split state subsystem; the remaining branch pressure is concentrated in `state::reducer = 28` and `state::grouped = 24`
  - `db::data::structural_field::value_storage` moved from `1,439 LOC / 56 branch sites` to a split value-storage subsystem; the largest child currently visible is `decode::value = 19`
  - `db::data::persisted_row::codec` remains split into scalar/by-kind/structured/meta/strategy/traversal files, with no single codec file in the current top-10 branch tier
- The strongest current branch sites are now narrower domain owners:
  - `db::sql::parser::projection = 75`
  - `db::query::plan::expr::type_inference = 52`
  - `db::query::plan::access_choice::evaluator::range = 44`
  - `value = 41`
  - `db::access::canonical = 40`

## Structural Interpretation

- This run shows successful hotspot de-concentration, not a global contraction:
  - files increased by `+109`
  - LOC increased by `+5,422`
  - total branch sites increased by `+139`
  - modules over the branch-hotspot threshold dropped by `-5`
- The main complexity movement is from large files into smaller owners:
  - expression canonicalization is now split, and the refreshed predicate/projection cleanup lowered two expression files below the prior same-day levels; expression planning remains a watch zone because type inference is still the second-largest branch owner
  - aggregate lowering is structurally cleaner after semantic sealing and identity convergence; it no longer appears as one overloaded hotspot
  - codec and value-storage complexity has been pushed into narrower lane-specific owners, which is healthier than one flat persistence file
- Fanout remains under control:
  - no module crosses `fanout > 12`
  - top-10 fanout concentration declined despite the larger file count
  - layer authority checks still report `0` upward tracked imports and `0` cross-layer policy re-derivations
- The main residual risk is semantic density inside narrow owners:
  - parser projection remains the largest branch owner
  - type inference remains the expression planning choke point
  - access/range/canonical planning remains sensitive because schema migration/layout work may route through those decision surfaces

## Overall Complexity Risk Index

**5.1/10**

Interpretation:

- Risk is slightly lower than the April 27 report because hotspot count and branch concentration improved sharply, with an additional same-day improvement from the refreshed plan/expr cleanup.
- Risk remains medium because runtime size, file count, fanout sum, and total branch sites all grew.
- The current shape is healthier for maintenance than the previous snapshot, but it is not a net complexity reduction.

## Outcome

- complexity trajectory: `larger but better distributed`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - treat `db::sql::parser::projection` and `db::sql::parser::order_expr` as the SQL grammar watch zone
  - treat `db::query::plan::expr::type_inference`, `predicate_compile`, and `projection_eval` as the expression planning watch zone
  - protect `db::query::plan::access_choice::evaluator::range` and `db::access::canonical` from absorbing schema migration/layout complexity

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `2513e1311` with a dirty working tree, compared against `docs/audits/reports/2026-04/2026-04-27/complexity-accretion.md`, and marked `comparable with caveat`.

1. Overall complexity risk index
- overall complexity risk index is `5.1/10`, driven by larger runtime size offset by materially lower branch concentration and fewer branch-hotspot modules.

2. Fastest growing concept families
- expression planning remains the fastest-growing visible family, with `type_inference`, `predicate_compile`, and `projection_eval` clustered near the top branch tier.
- parser projection/order parsing remains the SQL surface growth family.
- access/range/canonical planning remains a smaller but sensitive family because it owns route and range decisions.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `db::sql::parser::projection = 75`, `db::query::plan::expr::type_inference = 52`, `db::query::plan::access_choice::evaluator::range = 44`, `value = 41`, and `db::access::canonical = 40`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new fanout-led `AccessPath` or `RouteShape` drift appeared; the layer-health snapshot reports `AccessPath decision owners = 2` and `RouteShape decision owners = 2`.

5. Flow multiplication risks (axis-based)
- expression planning is the main flow-multiplication risk because type inference, predicate compilation, and projection evaluation each carry related branch pressure.
- SQL parser projection remains a grammar-flow risk, but aggregate lowering is no longer a monolithic peer hotspot.

6. Semantic authority vs execution spread risks
- layer invariants remain bounded with `0` upward tracked imports, `0` cross-layer policy re-derivations, and `0` cross-layer predicate duplication count.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and top-10 fanout concentration improved `0.0472 -> 0.0414` even while total fanout rose by `+179`.

8. Super-node + call-depth warnings
- no fanout super-node emerged.
- modules with `max_branch_depth >= 3` stayed flat at `27`, so branch depth did not improve but also did not regress.

9. Trend-interpretation filter outcomes
- the trend is decomposition plus moderate semantic expansion: total runtime size is up, but the largest decision files were split and the branch-hotspot threshold count fell by half.

10. Complexity trend table
- against the latest comparable baseline, runtime files and LOC are up, total branch sites are up slightly, branch hotspots are down sharply, and branch/fanout concentration both improved.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `db::query::plan::expr::{type_inference,predicate_compile,projection_eval}`; action: keep expression-family work behind small owner-specific patches and re-run this audit after adding new expression or scalar-function families; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::sql::parser::{projection,order_expr}`; action: avoid per-function parser special cases and route new grammar additions through existing parse-shape helpers; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::query::plan::access_choice::evaluator::range` and `db::access::canonical`; action: before schema migration/layout work, keep access-shape decisions in their current owners and avoid duplicating range/canonical checks in migration/session layers; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-30/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
