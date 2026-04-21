# Complexity Accretion Audit - 2026-04-21

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-17/complexity-accretion.md`
- code snapshot identifier: `7c1946c04` (`dirty` working tree)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` against `2026-04-17/complexity-accretion.md` because the method manifest, runtime scope, and metric generator are unchanged; this run re-measures the current working tree on the same generator-backed basis

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-21/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-21/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`574` modules):
  - `docs/audits/reports/2026-04/2026-04-21/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-21/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::sql::parser::projection` | `crates/icydb-core/src/db/sql/parser/projection.rs` | 1,046 | 14 | 71 | 5.07 | 74 | 72 | 3 | 2 | 86 |
| `db::predicate::bool_expr` | `crates/icydb-core/src/db/predicate/bool_expr.rs` | 1,321 | 50 | 220 | 4.40 | 28 | 28 | 2 | 2 | 78 |
| `db::session::sql::parameter` | `crates/icydb-core/src/db/session/sql/parameter.rs` | 1,714 | 31 | 137 | 4.42 | 45 | 45 | 2 | 4 | 76 |
| `db::query::plan::expr::ast` | `crates/icydb-core/src/db/query/plan/expr/ast.rs` | 1,110 | 22 | 166 | 7.55 | 52 | 46 | 3 | 2 | 68 |
| `db::sql::lowering::prepare` | `crates/icydb-core/src/db/sql/lowering/prepare.rs` | 1,278 | 29 | 131 | 4.52 | 34 | 33 | 3 | 4 | 62 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 878 | 32 | 104 | 3.25 | 32 | 30 | 2 | 4 | 62 |
| `db::query::plan::expr::type_inference` | `crates/icydb-core/src/db/query/plan/expr/type_inference/mod.rs` | 828 | 29 | 143 | 4.93 | 32 | 31 | 3 | 3 | 60 |
| `db::sql::lowering::aggregate` | `crates/icydb-core/src/db/sql/lowering/aggregate.rs` | 1,523 | 20 | 109 | 5.45 | 34 | 34 | 2 | 4 | 54 |
| `db::data::structural_field::value_storage` | `crates/icydb-core/src/db/data/structural_field/value_storage.rs` | 978 | 7 | 64 | 9.14 | 40 | 39 | 2 | 4 | 46 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 407 | 10 | 26 | 2.60 | 34 | 34 | 3 | 2 | 44 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 563 | 574 | +11 |
| Runtime LOC | `[M]` | primary | 101,875 | 112,890 | +11,015 |
| Runtime fanout (sum) | `[M]` | primary | 1,154 | 1,176 | +22 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 5 | 12 | +7 |
| Total branch sites | `[M]` | primary | 3,559 | 4,129 | +570 |
| Top-10 branch concentration | `[D]` | primary | 0.1203 | 0.1540 | +0.0338 |
| Top-10 fanout concentration | `[D]` | primary | 0.0520 | 0.0510 | -0.0010 |
| Modules with `max_branch_depth >= 3` | `[D]` | primary | 26 | 32 | +6 |

## Current Complexity Signals

- Runtime scope is materially larger than the April 17 baseline:
  - files: `563 -> 574`
  - LOC: `101,875 -> 112,890`
  - total branch sites: `3,559 -> 4,129`
- Hotspot pressure rose instead of contracting:
  - modules with `branch_sites_total >= 40`: `5 -> 12`
  - top-10 branch concentration: `0.1203 -> 0.1540`
  - modules with `max_branch_depth >= 3`: `26 -> 32`
- Fanout pressure still did not widen with it:
  - modules with `fanout > 12`: `0 -> 0`
  - top-10 fanout concentration: `0.0520 -> 0.0510`
- The new hotspot tier is no longer anchored by decimal/value/access only. It is now dominated by SQL parser/lowering, prepared fallback, and planner expression typing:
  - `db::sql::parser::projection = 86`
  - `db::predicate::bool_expr = 78`
  - `db::session::sql::parameter = 76`
  - `db::query::plan::expr::ast = 68`
  - `db::sql::lowering::prepare = 62`
  - `db::query::plan::expr::type_inference = 60`
  - `db::sql::lowering::aggregate = 54`
- The older core-runtime hotspots remain active too:
  - `db::executor::aggregate::contracts::state = 62`
  - `db::data::structural_field::value_storage = 46`
  - `db::query::plan::access_choice::evaluator::range = 44`
  - `value = 41`
  - `db::access::canonical = 40`

## Structural Interpretation

- This run still does not show classic dependency-hub sprawl:
  - no module crossed `fanout > 12`
  - top-10 fanout concentration stayed flat-to-down
  - layer snapshot still reports `0` upward imports and `0` cross-layer policy re-derivations
- But the branch-pressure story is materially worse than April 17:
  - total branch sites rose `+570`
  - hotspot count more than doubled `5 -> 12`
  - the top-10 branch tier is more concentrated again
- The character of the current pressure is different from the earlier April 17 read:
  - then: broader runtime with successful hotspot contraction
  - now: branch-heavy accretion concentrated in parser/lowering/prepared/planner expression owners
- This is a cleaner failure mode than fanout-led architecture sprawl, but it is still a real structural risk because multiple adjacent semantic owners now sit in the top hotspot tier at the same time:
  - parser projection
  - grouped/predicate boolean canonicalization
  - prepared parameter contracts
  - prepared fallback lowering
  - planner expression AST and type inference

## Overall Complexity Risk Index

**6.2/10**

Interpretation:

- Risk moves into the high-monitoring band.
- The score rises because branch surface, hotspot count, and branch concentration all deteriorated together against the April 17 baseline.
- Risk stays below the severe band because fanout and layer-authority invariants did not deteriorate in parallel.

## Outcome

- complexity trajectory: `upward and newly re-concentrating`
- release risk from complexity accretion: `Medium-High`
- blocking recommendation: `none`
- follow-up recommendation:
  - stop treating the current parser/lowering/prepared/planner growth as harmless decomposition
  - treat the `0.112` prepared fallback line as a real structural contraction target, not only a semantic-ownership cleanup
  - rerun the audit after the next prepared/planner cleanup slice because the current pressure is concentrated enough to become sticky if more expression families land first

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `7c1946c04` (`dirty` working tree), compared against `docs/audits/reports/2026-04/2026-04-17/complexity-accretion.md`, and marked `comparable`.

1. Overall complexity risk index
- overall complexity risk index is `6.2/10`, driven by `+570` total branch sites, hotspot-count growth `5 -> 12`, and top-10 branch concentration rising to `0.1540`.

2. Fastest growing concept families
- the fastest visible growth family is now the parser/lowering/prepared/planner-expression cluster: `db::sql::parser::projection`, `db::predicate::bool_expr`, `db::session::sql::parameter`, `db::sql::lowering::prepare`, `db::query::plan::expr::ast`, and `db::query::plan::expr::type_inference`.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `db::sql::parser::projection = 86`, `db::predicate::bool_expr = 78`, `db::session::sql::parameter = 76`, `db::query::plan::expr::ast = 68`, `db::sql::lowering::prepare = 62`, and `db::executor::aggregate::contracts::state = 62`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new fanout-led `AccessPath` or `RouteShape` drift appeared; the branch growth is concentrated in parser/lowering/prepared/planner expression owners instead of expanded route hubs.

5. Flow multiplication risks (axis-based)
- the current growth cluster mixes parser clause handling, prepared parameter contracts, planner expression typing, and fallback lowering in adjacent owners; that is a stronger flow-multiplication signal than the April 17 branch profile.

6. Semantic authority vs execution spread risks
- layer invariants still show bounded authority anchors (`AccessPath owners = 2`, `RouteShape owners = 3`, `Predicate coercion owners = 4`) with `0` cross-layer policy re-derivations.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and top-10 fanout concentration improved slightly `0.0520 -> 0.0510` even while total fanout rose `+22`.

8. Super-node + call-depth warnings
- no fanout super-node emerged, but modules with `max_branch_depth >= 3` rose `26 -> 32`, and several of the new parser/prepared/planner hotspots now sit in that deep-branch tier.

9. Trend-interpretation filter outcomes
- this is not just broad redistribution under decomposition anymore; the branch tier is larger and more concentrated at the same time, so the current signal is real structural pressure rather than naming churn.

10. Complexity trend table
- against the latest comparable baseline, the runtime is larger, more branch-heavy, and more hotspot-concentrated, with the pressure now centered on parser/lowering/prepared/planner expression owners.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `db::sql::lowering::prepare` + `db::session::sql::parameter` + `db::query::plan::expr::type_inference`; action: treat prepared fallback consolidation as a structural contraction target as well as a semantic-owner cleanup, and reduce prepared/planner branch pressure before widening prepared SQL further; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::sql::parser::projection` + `db::sql::lowering::aggregate`; action: stop adding new clause-family branching until parser/lowering branch sites are decomposed or reduced enough that the top parser hotspot falls below the current `86` branch-site tier; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::predicate::bool_expr` + `db::query::plan::expr::ast`; action: keep boolean canonicalization and planner expression growth owner-local and avoid letting further normalization or expression-family widening push both files upward together without an offsetting contraction pass; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-21/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
