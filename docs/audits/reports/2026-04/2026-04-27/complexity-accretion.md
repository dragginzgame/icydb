# Complexity Accretion Audit - 2026-04-27

## Report Preamble

- scope: conceptual growth, branch pressure, hotspot concentration, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-21/complexity-accretion.md`
- code snapshot identifier: `9890a4d7a` (`clean` working tree before audit artifacts were generated)
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` against `2026-04-21/complexity-accretion.md` because the method manifest, runtime scope, and metric generator are unchanged; this run re-measures the current working tree on the same generator-backed basis

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-27/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-04/2026-04-27/artifacts/complexity-accretion/module-branch-hotspots.tsv`

## STEP -1 - Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`624` modules):
  - `docs/audits/reports/2026-04/2026-04-27/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-04/2026-04-27/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::sql::parser::projection` | `crates/icydb-core/src/db/sql/parser/projection.rs` | 908 | 11 | 63 | 5.73 | 63 | 63 | 3 | 2 | 74 |
| `db::query::plan::expr::canonicalize` | `crates/icydb-core/src/db/query/plan/expr/canonicalize.rs` | 996 | 34 | 162 | 4.76 | 30 | 30 | 2 | 2 | 64 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 868 | 32 | 101 | 3.16 | 32 | 30 | 2 | 4 | 62 |
| `db::data::structural_field::value_storage` | `crates/icydb-core/src/db/data/structural_field/value_storage.rs` | 1,439 | 11 | 75 | 6.82 | 47 | 45 | 2 | 4 | 56 |
| `db::query::plan::expr::type_inference` | `crates/icydb-core/src/db/query/plan/expr/type_inference/mod.rs` | 847 | 23 | 120 | 5.22 | 32 | 30 | 3 | 3 | 53 |
| `db::sql::lowering::aggregate` | `crates/icydb-core/src/db/sql/lowering/aggregate.rs` | 1,272 | 12 | 62 | 5.17 | 33 | 33 | 2 | 3 | 45 |
| `db::query::plan::access_choice::evaluator::range` | `crates/icydb-core/src/db/query/plan/access_choice/evaluator/range.rs` | 407 | 10 | 26 | 2.60 | 34 | 34 | 3 | 2 | 44 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 856 | 21 | 135 | 6.43 | 20 | 20 | 2 | 3 | 41 |
| `db::query::plan::expr::predicate_compile` | `crates/icydb-core/src/db/query/plan/expr/predicate_compile.rs` | 730 | 26 | 111 | 4.27 | 14 | 14 | 2 | 2 | 40 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 365 | 10 | 39 | 3.90 | 30 | 30 | 2 | 3 | 40 |

## STEP 0 - Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 574 | 624 | +50 |
| Runtime LOC | `[M]` | primary | 112,890 | 119,813 | +6,923 |
| Runtime fanout (sum) | `[M]` | primary | 1,176 | 1,293 | +117 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 12 | 10 | -2 |
| Total branch sites | `[M]` | primary | 4,129 | 4,259 | +130 |
| Top-10 branch concentration | `[D]` | primary | 0.1540 | 0.1219 | -0.0321 |
| Top-10 fanout concentration | `[D]` | primary | 0.0510 | 0.0472 | -0.0038 |
| Modules with `max_branch_depth >= 3` | `[D]` | primary | 32 | 27 | -5 |

## Current Complexity Signals

- Runtime scope is larger than the April 21 baseline:
  - files: `574 -> 624`
  - LOC: `112,890 -> 119,813`
  - total branch sites: `4,129 -> 4,259`
- Branch pressure rose modestly in aggregate but de-concentrated:
  - modules with `branch_sites_total >= 40`: `12 -> 10`
  - top-10 branch concentration: `0.1540 -> 0.1219`
  - modules with `max_branch_depth >= 3`: `32 -> 27`
- Fanout pressure remains structurally bounded:
  - modules with `fanout > 12`: `0 -> 0`
  - top-10 fanout concentration: `0.0510 -> 0.0472`
- The strongest remaining branch sites are concentrated in expression/parser/value-shape owners:
  - `db::sql::parser::projection = 74`
  - `db::query::plan::expr::canonicalize = 64`
  - `db::query::plan::expr::type_inference = 53`
  - `db::sql::lowering::aggregate = 45`
- The older storage/value hotspots remain visible:
  - `db::data::structural_field::value_storage = 56`
  - `value = 41`
  - `db::access::canonical = 40`
- The executor aggregate state machine remains a stable hotspot:
  - `db::executor::aggregate::contracts::state = 62`

## Structural Interpretation

- This run does not show dependency-hub sprawl:
  - no module crossed `fanout > 12`
  - top-10 fanout concentration dropped
  - layer authority checks still report `0` upward tracked imports and `0` cross-layer policy re-derivations
- The main change is decomposition plus moderate total branch growth:
  - runtime module count rose by `+50`
  - total branch sites rose by `+130`
  - hotspot count and concentration both improved
- Compared with April 21, the parser/lowering/prepared pressure has partially relaxed:
  - `db::sql::parser::projection` fell from `86` to `74`
  - `db::session::sql::parameter` and `db::sql::lowering::prepare` left the top-10 branch tier
  - top-10 branch concentration fell by `0.0321`
- The new pressure center is expression canonicalization and typed numeric/function handling:
  - `db::query::plan::expr::canonicalize = 64`
  - `db::query::plan::expr::type_inference = 53`
  - `db::query::plan::expr::predicate_compile = 40`
- That is a manageable failure mode because the branch growth is not paired with widened fanout or new upward dependencies, but it should be watched before adding more SQL function families or expression variants.

## Overall Complexity Risk Index

**5.4/10**

Interpretation:

- Risk drops from the April 21 high-monitoring band because branch hotspots are less concentrated and fewer modules sit in deep branch tiers.
- Risk remains medium because runtime size, fanout sum, and total branch sites all grew.
- The current trend is healthier than the previous report, but not a full contraction.

## Outcome

- complexity trajectory: `larger but less concentrated`
- release risk from complexity accretion: `Medium`
- blocking recommendation: `none`
- follow-up recommendation:
  - keep expression canonicalization/type inference as the primary watch zone before adding more SQL expression families
  - keep parser projection below the current `74` branch-site tier during the next SQL surface expansion
  - avoid reintroducing prepared/session SQL branch growth now that those files have dropped out of the top hotspot tier

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `9890a4d7a` (`clean` before audit artifacts), compared against `docs/audits/reports/2026-04/2026-04-21/complexity-accretion.md`, and marked `comparable`.

1. Overall complexity risk index
- overall complexity risk index is `5.4/10`, driven by larger runtime size but offset by reduced hotspot count, reduced top-10 branch concentration, and fewer deep-branch modules.

2. Fastest growing concept families
- the main visible growth family is expression canonicalization/type handling, with `db::query::plan::expr::canonicalize`, `db::query::plan::expr::type_inference`, and `db::query::plan::expr::predicate_compile` now clustered in or near the branch hotspot tier.

3. Highest branch multipliers
- the strongest current branch-pressure anchors are `db::sql::parser::projection = 74`, `db::query::plan::expr::canonicalize = 64`, `db::executor::aggregate::contracts::state = 62`, `db::data::structural_field::value_storage = 56`, and `db::query::plan::expr::type_inference = 53`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- no new fanout-led `AccessPath` or `RouteShape` drift appeared; route-shape authority improved from `3` owners in the previous report snapshot to `2` owners in the current layer-health snapshot.

5. Flow multiplication risks (axis-based)
- expression canonicalization/type inference is the main flow-multiplication risk; parser and SQL lowering are still present but less dominant than in the prior report.

6. Semantic authority vs execution spread risks
- layer invariants still show bounded authority anchors with `0` upward tracked imports, `0` cross-layer policy re-derivations, and `0` cross-layer predicate duplication count.

7. Ownership drift + fanout pressure
- fanout pressure remains low (`0` modules above `fanout > 12`), and top-10 fanout concentration improved `0.0510 -> 0.0472` even while total fanout rose `+117`.

8. Super-node + call-depth warnings
- no fanout super-node emerged, and modules with `max_branch_depth >= 3` fell `32 -> 27`.

9. Trend-interpretation filter outcomes
- this looks like decomposition plus moderate semantic expansion rather than branch re-concentration; the top hotspot tier got smaller and less concentrated while total runtime size grew.

10. Complexity trend table
- against the latest comparable baseline, runtime size is up, total branch sites are up slightly, but hotspot concentration and deep-branch pressure are down.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- runtime metrics generation passed, both architecture invariant checks passed, and `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `db::query::plan::expr::canonicalize` + `db::query::plan::expr::type_inference`; action: treat expression-family expansion as the current complexity watch zone and avoid adding more function families without checking whether canonicalization/type-inference branches rise again; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::sql::parser::projection`; action: keep the parser projection hotspot below the current `74` branch-site tier by routing new scalar functions through existing grouped parsing paths instead of per-function special cases; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::data::structural_field::value_storage` + `db::data::persisted_row::codec`; action: keep storage/value branching isolated from query-expression growth so the data layer does not absorb semantic branch pressure; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh docs/audits/reports/2026-04/2026-04-27/artifacts/complexity-accretion/runtime-metrics.tsv` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
