# Complexity Accretion Audit - 2026-03-24

## Report Preamble

- scope: conceptual growth, branch pressure, flow multiplication, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/complexity-accretion.md`
- code snapshot identifier: `3f453012`
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `non-comparable` (the canonical generator is still missing, and the fallback extractor was refined again for fanout and switch-site filtering, so historical deltas that depend on those derived metrics remain `N/A (method change)`)

## Method Changes

- `scripts/audit/runtime_metrics.sh` is still absent in this checkout.
- this run regenerated the runtime metrics with the same fallback family as the
  2026-03-15 report, but refined two noisy heuristics:
  - fanout now counts coarse internal module edges instead of every imported symbol token
  - enum switch sites now count branch-bearing files that mention the enum, not
    every source line carrying an enum variant
- because those fallback heuristics changed, fanout, super-node, branch-module,
  and branch-multiplier deltas are marked `N/A (method change)` where needed

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/function-branch-hotspots.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/enum-surface.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/concept-branch-summary.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/concept-branch-map.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (all `485` modules):
  - `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/runtime-metrics.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime.rs` | 2,073 | 68 | 295 | 4.34 | 54 | 54 | 5 | 0 | 122 |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 1,156 | 5 | 22 | 4.40 | 72 | 71 | 5 | 0 | 76 |
| `db::query::plan::access_choice` | `crates/icydb-core/src/db/query/plan/access_choice/mod.rs` | 896 | 16 | 76 | 4.75 | 54 | 54 | 5 | 0 | 70 |
| `db::executor::explain::descriptor` | `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 969 | 20 | 75 | 3.75 | 34 | 32 | 5 | 0 | 52 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 866 | 20 | 118 | 5.90 | 31 | 31 | 6 | 0 | 51 |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser/mod.rs` | 671 | 3 | 15 | 5.00 | 50 | 47 | 5 | 0 | 50 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering/mod.rs` | 1,099 | 23 | 73 | 3.17 | 26 | 26 | 5 | 0 | 49 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 628 | 9 | 36 | 4.00 | 29 | 29 | 5 | 0 | 38 |
| `db::reduced_sql` | `crates/icydb-core/src/db/reduced_sql/mod.rs` | 631 | 9 | 149 | 16.56 | 30 | 29 | 6 | 3 | 38 |
| `db::executor::aggregate::contracts::state` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 663 | 22 | 73 | 3.32 | 13 | 13 | 5 | 0 | 35 |
| `db::index::key::build` | `crates/icydb-core/src/db/index/key/build.rs` | 559 | 16 | 36 | 2.25 | 19 | 19 | 6 | 0 | 35 |
| `db::executor::aggregate::projection` | `crates/icydb-core/src/db/executor/aggregate/projection/mod.rs` | 735 | 16 | 46 | 2.88 | 17 | 17 | 6 | 0 | 33 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 467 | 485 | N/A (method change) |
| Runtime LOC | `[M]` | primary | 78,574 | 92,096 | N/A (method change) |
| Runtime fanout (sum) | `[M]` | primary | 496 | 155 | N/A (method change) |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | N/A (method change) |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | 4 | 18 | N/A (method change) |
| Continuation decision owners | `[C]` | primary | 2 | 2 | 0 |
| Continuation execution consumers | `[C]` | primary | 22 | 22 | 0 |
| Continuation plumbing modules | `[C]` | primary | 131 | 171 | N/A (method change) |
| AccessPath decision owners | `[C]` | primary | 3 | 3 | 0 |
| AccessPath executor dispatch sites | `[M]` | primary | 1 | 1 | 0 |
| AccessPath branch modules | `[M]` | primary | 6 | 30 | N/A (method change) |
| RouteShape decision owners | `[C]` | primary | 3 | 2 | -1 |
| RouteShape branch modules | `[M]` | primary | 2 | 6 | N/A (method change) |
| Predicate coercion decision owners | `[C]` | primary | 4 | 4 | 0 |
| Continuation mentions (context only) | `[M]` | weak | 1,313 | 3,907 | N/A (method change) |

## STEP 1 — Variant Surface Growth + Branch Multiplier

Evidence mode: `semi-mechanical`

- enum surface artifact:
  - `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/enum-surface.tsv`

| Enum [M] | Variants [M] | Switch Sites [M] | Branch Multiplier [D] | Decision Owners [C] | Domain Scope [C] | Mixed Domains? [C] | Growth Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |
| `Predicate` | 13 | 52 | 676 | 4 | predicate + index + query + executor | yes | High |
| `AccessPath` | 7 | 30 | 210 | 3 | access + executor + query/explain | yes | High |
| `CursorPlanError` | 9 | 12 | 108 | 2 | cursor + query/session | yes | Medium-High |
| `ErrorClass` | 7 | 13 | 91 | 1 | shared error taxonomy | yes | Medium |
| `QueryError` | 5 | 9 | 45 | 2 | query + session facade | yes | Medium |
| `RouteShapeKind` | 5 | 6 | 30 | 2 | executor routing | no | Low-Medium |
| `PreparedIndexDeltaKind` | 5 | 5 | 25 | 1 | commit/index mutation | no | Low-Medium |
| `StoreError` | 3 | 1 | 3 | 1 | storage error boundary | no | Low |

## STEP 2 — Local Branching Pressure (Function-Level)

Evidence mode: `semi-mechanical`

- hotspot artifact:
  - `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/function-branch-hotspots.tsv`

| Function [M] | Module [M] | Branch Layers [D] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_chain_count [M] | max_branch_depth [M] | Axis Count [C] | Previous Branch Layers [M] | Delta [D] | Domains Mixed [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `plan_compare` | `db::query::plan::planner::compare` | 14 | 2 | 10 | 5.00 | 12 | 6 | 3 | N/A | N/A (method change) | yes | High |
| `apply_post_access_to_kernel_rows_dyn` | `db::executor::terminal::page` | 14 | 0 | 0 | 0.00 | 14 | 5 | 3 | N/A | N/A (method change) | yes | High |
| `canonical_cmp` | `db::access::canonical` | 13 | 1 | 7 | 7.00 | 12 | 4 | 3 | N/A | N/A (method change) | no | Medium-High |
| `write_execution_node_json` | `db::query::explain::json` | 12 | 11 | 22 | 2.00 | 1 | 4 | 3 | N/A | N/A (method change) | yes | Medium |
| `render_text_tree_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 2 | 2 | N/A | N/A (method change) | yes | Medium |
| `render_text_tree_verbose_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 2 | 2 | N/A | N/A (method change) | yes | Medium |
| `evaluate_range_compare_candidate` | `db::query::plan::access_choice` | 11 | 0 | 0 | 0.00 | 11 | 4 | 2 | N/A | N/A (method change) | yes | Medium |
| `bounds_for_prefix_component_range_with_kind` | `db::index::key::build` | 10 | 4 | 8 | 2.00 | 6 | 3 | 3 | N/A | N/A (method change) | yes | Medium |
| `eval_text_scalar_compare` | `db::predicate::runtime` | 10 | 2 | 14 | 7.00 | 8 | 3 | 4 | N/A | N/A (method change) | yes | Medium |
| `eval_text_scalar_literal_compare` | `db::predicate::runtime` | 10 | 2 | 14 | 7.00 | 8 | 3 | 4 | N/A | N/A (method change) | yes | Medium |

## STEP 2A — Concept Branch Distribution Across Modules

Evidence mode: `semi-mechanical`

- concept branch artifacts:
  - `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/concept-branch-summary.tsv`
  - `docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/concept-branch-map.tsv`

| Concept [M] | Branch Modules [M] | Decision Owners [C] | Branch/Owner Ratio [D] | Previous Branch Modules [M] | Delta [D] |
| ---- | ----: | ----: | ----: | ----: | ----: |
| `AccessPath` | 36 | 3 | 12.00 | N/A | N/A (method change) |
| `RouteShape` | 10 | 2 | 5.00 | N/A | N/A (method change) |
| `Continuation` | 120 | 2 | 60.00 | N/A | N/A (method change) |
| `PredicateCoercion` | 127 | 4 | 31.75 | N/A | N/A (method change) |

## STEP 3 — Execution Path Multiplicity (Effective Flows)

Evidence mode: `semi-mechanical`

No new runtime feature axes landed in the late `0.63` cleanup line, so the
effective-flow table remains structurally flat relative to the 2026-03-15
classified baseline.

| Operation [M] | Axes Used [M] | Axis Cardinalities [M] | Theoretical Space [D] | Effective Flows [D] | Previous Effective Flows [M] | Delta [D] | Shared Core? [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `save` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | 4 | 0 | yes | Medium |
| `replace` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | 4 | 0 | yes | Medium |
| `delete` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | 4 | 0 | yes | Medium |
| `load` | access path, cursor, ordering | `7x2x3` | 42 | 6 | 6 | 0 | yes | High |
| `recovery replay` | operation subtype, uniqueness | `3x2` | 6 | 3 | 3 | 0 | yes | Medium |
| `cursor continuation` | access path, cursor, ordering | `4x2x3` | 24 | 4 | 4 | 0 | yes | Medium-High |
| `index mutation` | operation subtype, recovery, uniqueness | `3x2x2` | 12 | 5 | 5 | 0 | yes | Medium-High |

## STEP 4 — Semantic Authority vs Execution Spread

Evidence mode: `classified`

| Concept [M] | Decision Owners [C] | Execution Consumers [C] | Plumbing Modules [C] | Owner Count [D] | Consumer Count [D] | Plumbing Count [D] | Semantic Layers [C] | Transport Layers [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ----: | ----: | ----: | ---- | ---- | ---- |
| continuation / cursor anchor semantics | `db::cursor::envelope`, `db::cursor::continuation` | branch-bearing continuation modules from concept map | continuation/cursor mention modules in runtime scope | 2 | 120 | 171 | 2 | 3 | High |
| `AccessPath` decision semantics | `db::access::path`, `db::access::dispatch`, `db::access::lowering` | branch-bearing `AccessPath` modules from concept map | `AccessPath` mention modules in runtime scope | 3 | 36 | 39 | 2 | 2 | Medium-High |
| `RouteShape` decision semantics | `db::executor::route::contracts::shape`, route planner execution boundaries | branch-bearing `RouteShape` modules from concept map | `RouteShape` mention modules in runtime scope | 2 | 10 | 12 | 2 | 2 | Medium |
| predicate coercion decision semantics | predicate normalization + planner coercion boundaries + SQL lowering boundaries | branch-bearing predicate/coercion modules from concept map | predicate/coercion mention modules in runtime scope | 4 | 127 | 166 | 3 | 3 | High |
| error origin mapping | error taxonomy + session mapping + planner mapping | error-bearing branches in query/session/runtime layers | error transport modules | 3 | 13 | 26 | 2 | 2 | Medium |

## STEP 4A — Concept Ownership Drift (Only)

Evidence mode: `classified`

| Concept [M] | Decision Owners [C] | Previous Owners [C] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| continuation | 2 | N/A | N/A (method change) | Medium |
| `AccessPath` | 3 | N/A | N/A (method change) | High |
| `RouteShape` | 2 | N/A | N/A (method change) | Medium |
| predicate coercion | 4 | N/A | N/A (method change) | High |
| index range | 2 | N/A | N/A (method change) | Medium |
| canonicalization | 1 | N/A | N/A (method change) | Medium-Low |

## STEP 4B — Fanout Pressure

Evidence mode: `mechanical`

| Module [M] | Fanout [M] | Previous Fanout [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| `db::data::structural_field::storage_key` | 4 | N/A | N/A (method change) | Low |
| `testing::fixtures` | 4 | N/A | N/A (method change) | Low |
| `db::access::execution_contract::types` | 3 | N/A | N/A (method change) | Low |
| `db::access::path` | 3 | N/A | N/A (method change) | Low |
| `db::commit` | 3 | N/A | N/A (method change) | Low |
| `db::executor::aggregate::contracts::spec` | 3 | N/A | N/A (method change) | Low |
| `db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink` | 3 | N/A | N/A (method change) | Low |
| `db::executor::route::contracts::shape` | 3 | N/A | N/A (method change) | Low |

## STEP 5 — Cognitive Load Indicators (Super-Node + Call Depth)

Evidence mode: `mechanical`

| Module/Operation [M] | LOC or Call Depth [M] | Fanout [M] | Domain Count [D] | Previous [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| functions with logical length > 80 | 39 | N/A | N/A | N/A | N/A (method change) | Medium |
| branch hotspot functions (`branch_layers >= 8`) | 19 | N/A | N/A | N/A | N/A (method change) | Medium-High |
| max observed branch nesting depth (function-level) | 6 | N/A | N/A | N/A | N/A (method change) | Medium |
| super-node count | 18 | N/A | N/A | N/A | N/A (method change) | Medium |
| `db::predicate::runtime` | 2,073 | 0 | 3 | N/A | N/A (method change) | High |
| `db::sql::lowering` | 1,099 | 0 | 3 | N/A | N/A (method change) | Medium |
| `db::executor::aggregate::projection` | 735 | 0 | 3 | N/A | N/A (method change) | Medium |
| `db::index::key::build` | 559 | 0 | 3 | N/A | N/A (method change) | Medium |

## STEP 5A — Complexity Concentration Ratios

Evidence mode: `mechanical`

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) | 0.2065 | N/A | N/A (method change) | Low-Medium |
| Branch-site concentration (top 10 modules) | 0.2157 | N/A | N/A (method change) | Medium |
| AccessPath branch concentration (top 3 modules) | 0.3548 | N/A | N/A (method change) | Medium |
| RouteShape branch concentration (top 3 modules) | 0.8696 | N/A | N/A (method change) | High |

## STEP 6 — Drift Sensitivity (Axis Count)

Evidence mode: `semi-mechanical`

| Area [M] | Decision Axes [M] | Axis Count [D] | Branch Multiplier [D] | Drift Sensitivity [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ---- |
| access-choice + planner route eligibility | access path variant, predicate shape, order mode, cursor mode | 4 | 210 (`AccessPath`) | high coupling between planner/explain/runtime | High |
| continuation envelope eligibility | boundary kind, anchor placement, order direction, cursor token presence | 4 | 108 (`CursorPlanError` proxy) | invariant drift risk if owner leaks | Medium-High |
| predicate coercion + normalization | predicate node kind, coercion path, canonicalization path, target index capability | 4 | 676 (`Predicate`) | high multiplication across planner/index/runtime surfaces | High |
| explain/render route shape | route shape, access-path rendering mode, projection mode | 3 | 30 (`RouteShapeKind`) | bounded but concentrated in explain nodes | Medium |

## STEP 7 — Complexity Risk Index (Rubric-Guided)

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| Variant explosion risk | 6 | 2 | 12 |
| Branching pressure + centralization trend | 6 | 2 | 12 |
| Flow multiplicity | 5 | 2 | 10 |
| Cross-layer spread | 5 | 3 | 15 |
| Authority fragmentation | 5 | 2 | 10 |
| Fanout pressure + super-node load | 4 | 2 | 8 |
| Call-depth pressure | 4 | 1 | 4 |

`overall_index = 71 / 14 = 5.1`

## STEP 8 — Trend Interpretation Filter (Structural Noise Filter)

Evidence mode: `semi-mechanical`

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| late `0.63` test/harness cleanup since the previous shipped patch | no `icydb-core` feature-surface widening in `0.63.7` / `0.63.8` | benign surface growth outside runtime scope | does not increase runtime complexity risk by itself |
| fallback fanout and switch-site heuristic refinement | fanout sum fell from `496` to `155`; branch-module counts rose sharply for concept scans | method-noise | treat fanout and branch-module deltas as non-comparable for this run |
| RouteShape owner count from layer check | `2` owners | structural improvement | owner pressure is flatter than the prior report, even though branch-bearing file count is higher under the refined scan |
| branch hotspots (`>= 8` layers) | `19` functions | localized concentration | moderate pressure, but still concentrated rather than workspace-wide runaway |

## STEP 8A — Complexity Trend Table (Required)

Evidence mode: `mechanical` (primary) + `classified` (secondary)

| Metric [M/C] | 2026-03-08 | 2026-03-09 | 2026-03-12 | 2026-03-15 | 2026-03-24 |
| ---- | ----: | ----: | ----: | ----: | ----: |
| continuation decision-owner count `[C]` | 10 | N/A | N/A | 2 | 2 |
| continuation execution-consumer count `[C]` | 48 | N/A | N/A | 22 | 22 |
| AccessPath branch-module count `[M]` | N/A | N/A | N/A | 6 | 30 |
| RouteShape branch-module count `[M]` | N/A | N/A | N/A | 2 | 6 |
| branch hotspots (count) `[M]` | N/A | N/A | N/A | 10 | 19 |
| super-node count `[D]` | N/A | N/A | N/A | 4 | 18 |
| AccessPath variants `[M]` | 7 | 7 | N/A | 7 | 7 |
| continuation mentions (weak context) `[M]` | 925 | 1,016 | N/A | 1,313 | 3,907 |

## STEP 8B — Invalidating Signals (Required)

Evidence mode: `classified`

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves | no | none | no adjustment |
| file splits without semantic change | yes | can inflate module and mention counts while lowering local complexity | interpret with owner-count and hotspot concentration checks |
| generated code expansion | no | none | no adjustment |
| parser/table-driven conversion replacing branch expressions | no | none | no adjustment |
| branch consolidation into helper functions | yes | can lower local branch density in one file while leaving concept spread flat | keep concept branch maps and owner counts as the primary drift signal |

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `3f453012`, marked `non-comparable` because `scripts/audit/runtime_metrics.sh` is still missing and the fallback extractor was refined again for fanout and switch-site filtering.

1. Overall complexity risk index
- overall complexity risk index is `5.1/10` from the weighted rubric (`71/14`), which keeps the line in the moderate band.

2. Fastest growing concept families
- current broadest concept spread is `PredicateCoercion` (`127` branch-bearing modules, `3,801` mention hits) followed by `Continuation` (`120` branch-bearing modules, `3,907` weak-context mentions).

3. Highest branch multipliers
- the highest branch multipliers are `Predicate = 676`, `AccessPath = 210`, and `CursorPlanError = 108`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- current branch-module counts are `30` for `AccessPath` and `6` for `RouteShape`, but both are `N/A (method change)` against the prior run because the concept scan was refined to coarse branch-bearing-file counting.

5. Flow multiplication risks (axis-based)
- `load` still has `42` theoretical combinations and `6` effective flows, while `index mutation` still has `12` theoretical combinations and `5` effective flows.

6. Semantic authority vs execution spread risks
- continuation remains the most asymmetric concept (`2` owners vs `120` branch-bearing consumers vs `171` mention modules), and predicate coercion remains the broadest multi-layer spread (`4` owners, `127` branch-bearing consumers).

7. Ownership drift + fanout pressure
- layer checks still report stable key owners (`AccessPath = 3`, `RouteShape = 2`, `predicate coercion = 4`), and the refined fanout extractor still shows `0` modules over the `fanout > 12` threshold.

8. Super-node + call-depth warnings
- there are `19` hotspot functions at `branch_layers >= 8`, `39` functions over `80` logical lines, and the fallback scan sees max branch nesting depth `6`.

9. Trend-interpretation filter outcomes
- most raw growth signals in this run are method-sensitive and non-comparable, but the owner-count anchors stayed flat or improved (`RouteShape` owners `3 -> 2`), so the audit does not indicate structural runaway.

10. Complexity trend table
- a five-date trend table is included with explicit `N/A` history where prior runs are not comparable under the refined fallback method.

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- fallback runtime extraction passed, both architecture/layer invariant checks passed, and `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `crosscutting audit tooling`; action: restore or reintroduce `scripts/audit/runtime_metrics.sh` so future complexity runs can return to the canonical generator; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `crosscutting fallback method`; action: freeze one documented fallback extractor so fanout and branch-module trend lines can become comparable again if the canonical generator stays absent; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh` -> BLOCKED (missing in repository)
- fallback mechanical runtime extraction -> PASS (`docs/audits/reports/2026-03/2026-03-24/artifacts/complexity-accretion/runtime-metrics.tsv`)
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
