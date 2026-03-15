# Complexity Accretion Audit - 2026-03-15

## Report Preamble

- scope: conceptual growth, branch pressure, flow multiplication, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/complexity-accretion.md`
- code snapshot identifier: `d40099bf`
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `non-comparable` (the required generator script is not present in this checkout, so this run used a fallback mechanical extractor and starts a new comparable baseline)

## Method Changes

- `scripts/audit/runtime_metrics.sh` is missing in this branch, so runtime metrics were generated with a one-off mechanical extractor and stored in:
  - `docs/audits/reports/2026-03/2026-03-15/helpers/runtime-metrics.tsv`
- Because the metric generator changed, historical deltas are marked `N/A (method change)` where required.

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (all `467` modules):
  - `docs/audits/reports/2026-03/2026-03-15/helpers/runtime-metrics.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser/mod.rs` | 1146 | 13 | 148 | 11.38 | 81 | 59 | 3 | 1 | 72 |
| `db::query::plan::access_choice` | `crates/icydb-core/src/db/query/plan/access_choice.rs` | 1112 | 16 | 76 | 4.75 | 50 | 48 | 3 | 2 | 64 |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 1165 | 5 | 22 | 4.40 | 72 | 59 | 2 | 1 | 64 |
| `db::executor::explain::descriptor` | `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 951 | 20 | 75 | 3.75 | 34 | 28 | 2 | 1 | 48 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering.rs` | 1281 | 15 | 51 | 3.40 | 31 | 23 | 3 | 2 | 38 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 630 | 9 | 36 | 4.00 | 29 | 28 | 2 | 2 | 37 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 840 | 20 | 118 | 5.90 | 31 | 17 | 2 | 1 | 37 |
| `db::schema::validate` | `crates/icydb-core/src/db/schema/validate.rs` | 317 | 7 | 19 | 2.71 | 20 | 20 | 2 | 1 | 27 |
| `db::query::explain::render` | `crates/icydb-core/src/db/query/explain/render.rs` | 215 | 0 | 0 | 0.00 | 25 | 25 | 1 | 1 | 25 |
| `db::query::plan::planner::range` | `crates/icydb-core/src/db/query/plan/planner/range.rs` | 338 | 12 | 41 | 3.42 | 16 | 13 | 3 | 2 | 25 |
| `db::query::fingerprint::hash_parts` | `crates/icydb-core/src/db/query/fingerprint/hash_parts.rs` | 884 | 18 | 55 | 3.06 | 6 | 6 | 2 | 1 | 24 |
| `db::access::validate` | `crates/icydb-core/src/db/access/validate.rs` | 391 | 5 | 11 | 2.20 | 18 | 18 | 2 | 1 | 23 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | N/A | 467 | N/A (method change) |
| Runtime LOC | `[M]` | primary | N/A | 78,574 | N/A (method change) |
| Runtime fanout (sum) | `[M]` | primary | N/A | 496 | N/A (method change) |
| Modules with fanout > 12 | `[D]` | primary | N/A | 0 | N/A (method change) |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | N/A | 4 | N/A (method change) |
| Continuation decision owners | `[C]` | primary | N/A | 2 | N/A (method change) |
| Continuation execution consumers | `[C]` | primary | N/A | 22 | N/A (method change) |
| Continuation plumbing modules | `[C]` | primary | N/A | 131 | N/A (method change) |
| AccessPath decision owners | `[C]` | primary | N/A | 3 | N/A (method change) |
| AccessPath executor dispatch sites | `[M]` | primary | N/A | 1 | N/A (method change) |
| AccessPath branch modules | `[M]` | primary | N/A | 6 | N/A (method change) |
| RouteShape decision owners | `[C]` | primary | N/A | 3 | N/A (method change) |
| RouteShape branch modules | `[M]` | primary | N/A | 2 | N/A (method change) |
| Predicate coercion decision owners | `[C]` | primary | N/A | 4 | N/A (method change) |
| Continuation mentions (context only) | `[M]` | weak | N/A | 1,313 | N/A (method change) |

## STEP 1 — Variant Surface Growth + Branch Multiplier

Evidence mode: `semi-mechanical`

- enum surface artifacts:
  - `docs/audits/reports/2026-03/2026-03-15/helpers/enum-surface.tsv`
  - `docs/audits/reports/2026-03/2026-03-15/helpers/enum-switch-sites.tsv`

| Enum [M] | Variants [M] | Switch Sites [M] | Branch Multiplier [D] | Decision Owners [C] | Domain Scope [C] | Mixed Domains? [C] | Growth Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |
| `AccessPath` | 7 | 15 | 105 | 3 | access + query/explain | yes | High |
| `Predicate` | 13 | 37 | 481 | 4 | predicate + index + query | yes | High |
| `CursorPlanError` | 9 | 10 | 90 | 2 | cursor + query policy | yes | Medium-High |
| `QueryError` | 5 | 14 | 70 | 2 | query + session facade | yes | Medium |
| `ErrorClass` | 7 | 10 | 70 | 1 | shared error taxonomy | yes | Medium |
| `PlanError` | 3 | 22 | 66 | 2 | plan validation + mapping | yes | Medium |
| `RouteShapeKind` | 5 | 2 | 10 | 2 | executor routing | no | Low-Medium |
| `PreparedIndexDeltaKind` | 5 | 3 | 15 | 1 | commit/index mutation | no | Low-Medium |
| `StoreError` | 3 | 1 | 3 | 1 | storage error boundary | no | Low |

## STEP 2 — Local Branching Pressure (Function-Level)

Evidence mode: `semi-mechanical`

- hotspot artifact:
  - `docs/audits/reports/2026-03/2026-03-15/helpers/function-branch-hotspots.tsv`

| Function [M] | Module [M] | Branch Layers [D] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_chain_count [M] | max_branch_depth [M] | Axis Count [C] | Previous Branch Layers [M] | Delta [D] | Domains Mixed [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `plan_compare` | `db::query::plan::planner::compare` | 14 | 2 | 10 | 5.00 | 12 | 3 | 4 | N/A | N/A | yes | High |
| `canonical_cmp` | `db::access::canonical` | 13 | 1 | 7 | 7.00 | 12 | 2 | 3 | N/A | N/A | no | Medium-High |
| `render_text_tree_verbose_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 1 | 3 | N/A | N/A | yes | Medium |
| `write_execution_node_json` | `db::query::explain::json` | 12 | 11 | 22 | 2.00 | 1 | 1 | 3 | N/A | N/A | yes | Medium |
| `render_text_tree_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 1 | 3 | N/A | N/A | yes | Medium |
| `assess_field_extrema_fast_path_eligibility` | `db::executor::aggregate::capability` | 9 | 0 | 0 | 0.00 | 9 | 1 | 4 | N/A | N/A | yes | Medium |
| `execute_global_distinct_field_aggregate` | `db::executor::aggregate::runtime::grouped_distinct::aggregate` | 8 | 3 | 8 | 2.67 | 5 | 3 | 4 | N/A | N/A | yes | Medium |
| `prepare_row_commit_for_entity_impl` | `db::commit::prepare` | 8 | 1 | 0 | 0.00 | 7 | 2 | 4 | N/A | N/A | yes | Medium |
| `plan_strict_same_field_eq_or` | `db::query::plan::planner::predicate` | 8 | 0 | 0 | 0.00 | 8 | 2 | 4 | N/A | N/A | yes | Medium |
| `bounds_for_prefix_component_range_with_kind` | `db::index::key::build` | 8 | 4 | 8 | 2.00 | 4 | 1 | 4 | N/A | N/A | yes | Medium |

## STEP 2A — Concept Branch Distribution Across Modules

Evidence mode: `semi-mechanical`

- concept branch artifacts:
  - `docs/audits/reports/2026-03/2026-03-15/helpers/concept-branch-map.tsv`
  - `docs/audits/reports/2026-03/2026-03-15/helpers/concept-branch-summary.tsv`

| Concept [M] | Branch Modules [M] | Decision Owners [C] | Branch/Owner Ratio [D] | Previous Branch Modules [M] | Delta [D] |
| ---- | ----: | ----: | ----: | ----: | ----: |
| `AccessPath` | 6 | 3 | 2.00 | N/A | N/A |
| `RouteShape` | 2 | 3 | 0.67 | N/A | N/A |

## STEP 3 — Execution Path Multiplicity (Effective Flows)

Evidence mode: `semi-mechanical`

### Constraint Ledger

| Operation [M] | Constraint [C] | Axes Restricted [M] | Combinations Removed [D] | Evidence [M/C] |
| ---- | ---- | ---- | ----: | ---- |
| `save` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/save.rs` |
| `replace` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/replace.rs` |
| `delete` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/delete.rs` |
| `load` | access-path/order combinations are constrained by route/capability checks | `access path`, `ordering mode` | 36 | `db/executor/route/capability.rs`, `db/executor/stream/access/*` |
| `recovery replay` | replay path fixes recovery mode and narrows mutation classes | `recovery mode`, `operation subtype` | 3 | `db/commit/recovery.rs`, `db/commit/replay.rs` |
| `cursor continuation` | continuation path requires cursor + boundary-compatible route shape | `cursor presence`, `ordering mode`, `access path` | 20 | `db/cursor/continuation.rs`, `db/cursor/envelope.rs` |
| `index mutation` | uniqueness/relation mode narrows index mutation combinations | `index uniqueness`, `operation subtype` | 7 | `db/index/plan/mod.rs`, `db/commit/prepare.rs` |

### Flow Table

| Operation [M] | Axes Used [M] | Axis Cardinalities [M] | Theoretical Space [D] | Effective Flows [D] | Previous Effective Flows [M] | Delta [D] | Shared Core? [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `save` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | N/A | N/A | yes | Medium |
| `replace` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | N/A | N/A | yes | Medium |
| `delete` | operation, access path, recovery, uniqueness | `1x3x2x2` | 12 | 4 | N/A | N/A | yes | Medium |
| `load` | access path, cursor, ordering | `7x2x3` | 42 | 6 | N/A | N/A | yes | High |
| `recovery replay` | operation subtype, uniqueness | `3x2` | 6 | 3 | N/A | N/A | yes | Medium |
| `cursor continuation` | access path, cursor, ordering | `4x2x3` | 24 | 4 | N/A | N/A | yes | Medium-High |
| `index mutation` | operation subtype, recovery, uniqueness | `3x2x2` | 12 | 5 | N/A | N/A | yes | Medium-High |

## STEP 4 — Semantic Authority vs Execution Spread

Evidence mode: `classified`

| Concept [M] | Decision Owners [C] | Execution Consumers [C] | Plumbing Modules [C] | Owner Count [D] | Consumer Count [D] | Plumbing Count [D] | Semantic Layers [C] | Transport Layers [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ----: | ----: | ----: | ---- | ---- | ---- |
| continuation / cursor anchor semantics | `db::cursor::envelope`, `db::cursor::continuation` | runtime modules with branch hits in concept map | mention-only modules in concept map | 2 | 22 | 131 | 2 | 3 | High |
| `AccessPath` decision semantics | `db::access::path`, `db::access::dispatch`, `db::access::lowering` | modules branching on `AccessPath*` switch sites | modules mentioning but not branching | 3 | 6 | 16 | 2 | 2 | Medium |
| `RouteShape` decision semantics | `db::executor::route::contracts::shape`, route planner entrypoints, route execution derivation | modules branching on `RouteShapeKind` | modules mentioning but not branching | 3 | 2 | 9 | 2 | 2 | Medium |
| predicate coercion decision semantics | predicate normalization + planner coercion boundaries + SQL lowering boundaries | predicate/coercion branch modules in concept map | predicate/coercion mention-only modules | 4 | 28 | 107 | 3 | 3 | High |
| envelope boundary checks | cursor envelope authority + cursor boundary decode/validation | cursor boundary/anchor continuation checks | cursor transport modules | 1 | 8 | 15 | 2 | 2 | Medium |
| bound conversions | planner range conversion + cursor boundary decode | range/cursor branch modules | transport modules using bounds | 2 | 11 | 23 | 2 | 2 | Medium |
| plan shape enforcement | grouped/cursor/intent policy validators + route contracts | plan validation + route planning consumers | explain/transport modules | 3 | 18 | 40 | 3 | 2 | High |
| error origin mapping | error taxonomy + session mapping + planner mapping | session/planner/executor error branches | error transport modules | 3 | 14 | 22 | 2 | 2 | Medium |
| index id / namespace validation | schema/index validation boundaries | index planning + schema access consumers | transport-only schema/index modules | 2 | 10 | 25 | 2 | 2 | Medium |

## STEP 4A — Concept Ownership Drift (Only)

Evidence mode: `classified`

| Concept [M] | Decision Owners [C] | Previous Owners [C] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| continuation | 2 | N/A | N/A (method change) | Medium |
| `AccessPath` | 3 | N/A | N/A (method change) | High |
| `RouteShape` | 3 | N/A | N/A (method change) | High |
| predicate coercion | 4 | N/A | N/A (method change) | High |
| index range | 2 | N/A | N/A (method change) | Medium |
| canonicalization | 2 | N/A | N/A (method change) | Medium |

## STEP 4B — Fanout Pressure

Evidence mode: `mechanical`

| Module [M] | Fanout [M] | Previous Fanout [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| `db::query::fingerprint::fingerprint` | 12 | N/A | N/A (method change) | Medium |
| `db::query::fingerprint::projection_hash` | 4 | N/A | N/A (method change) | Low |
| `db::cursor::envelope` | 3 | N/A | N/A (method change) | Low |
| `db::cursor::token::grouped` | 3 | N/A | N/A (method change) | Low |
| `db::cursor::token::scalar` | 3 | N/A | N/A (method change) | Low |
| `db::executor::pipeline::operators::post_access` | 3 | N/A | N/A (method change) | Low |
| `db::executor::pipeline::runtime` | 3 | N/A | N/A (method change) | Low |
| `db::executor::route::planner::entrypoints` | 3 | N/A | N/A (method change) | Low |

## STEP 5 — Cognitive Load Indicators (Super-Node + Call Depth)

Evidence mode: `mechanical`

| Module/Operation [M] | LOC or Call Depth [M] | Fanout [M] | Domain Count [D] | Previous [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| functions with logical length > 80 | 25 | N/A | N/A | N/A | N/A (method change) | Medium |
| branch hotspot functions (`branch_layers >= 8`) | 10 | N/A | N/A | N/A | N/A (method change) | Medium |
| max observed branch nesting depth (function-level) | 3 | N/A | N/A | N/A | N/A (method change) | Low |
| `db::query::fingerprint::fingerprint` (super-node by domain count) | 831 | 12 | 4 | N/A | N/A (method change) | High |
| `db::executor::aggregate::numeric` (super-node by domain count) | 319 | 2 | 3 | N/A | N/A (method change) | Medium |
| `db::executor::pipeline::contracts::grouped::route_stage::projection` (super-node by domain count) | 134 | 1 | 3 | N/A | N/A (method change) | Medium |
| `db::executor::pipeline::contracts::grouped::route_stage::payload` (super-node by domain count) | 65 | 1 | 3 | N/A | N/A (method change) | Medium |

## STEP 5A — Complexity Concentration Ratios

Evidence mode: `mechanical`

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) | 0.0806 | N/A | N/A (method change) | Low |
| Branch-site concentration (top 10 modules) | 0.2115 | N/A | N/A (method change) | Medium |
| AccessPath branch concentration (top 3 modules) | 0.8125 | N/A | N/A (method change) | High |
| RouteShape branch concentration (top 3 modules) | 1.0000 | N/A | N/A (method change) | Medium |

## STEP 6 — Drift Sensitivity (Axis Count)

Evidence mode: `semi-mechanical`

| Area [M] | Decision Axes [M] | Axis Count [D] | Branch Multiplier [D] | Drift Sensitivity [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ---- |
| access-choice + planner route eligibility | access path variant, predicate shape, order mode, cursor mode | 4 | 105 (`AccessPath`) | high coupling between planner/explain/runtime | High |
| continuation envelope eligibility | boundary kind, anchor placement, order direction, cursor token presence | 4 | 90 (`CursorPlanError` proxy) | invariant drift risk if owner leaks | Medium-High |
| predicate coercion + normalization | predicate node kind, coercion path, canonicalization path, target index capability | 4 | 481 (`Predicate`) | high multiplication across planner/index/runtime surfaces | High |
| explain/render route shape | route shape, access-path rendering mode, projection mode | 3 | 10 (`RouteShapeKind`) | bounded but concentrated in explain nodes | Medium |

## STEP 7 — Complexity Risk Index (Rubric-Guided)

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| Variant explosion risk | 5 | 2 | 10 |
| Branching pressure + centralization trend | 6 | 2 | 12 |
| Flow multiplicity | 5 | 2 | 10 |
| Cross-layer spread | 5 | 3 | 15 |
| Authority fragmentation | 6 | 2 | 12 |
| Fanout pressure + super-node load | 4 | 2 | 8 |
| Call-depth pressure | 3 | 1 | 3 |

`overall_index = 70 / 14 = 5.0`

## STEP 8 — Trend Interpretation Filter (Structural Noise Filter)

Evidence mode: `semi-mechanical`

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| continuation mention growth in this run | 1,313 mention lines | method-noise + scope broadening | treat as context-only, not risk driver alone |
| AccessPath owners from layer check | 3 owners | stable authority signal | no immediate owner-drift alarm |
| RouteShape branch spread | 2 branch modules, 3 owners | concentrated but bounded | monitor for owner increase before risk escalation |
| branch hotspots (`>=8` layers) | 10 functions | localized concentration | medium pressure, not systemic runaway |
| super-node count | 4 | stable structural signal for this method baseline | keep watch on multi-domain ownership |

## STEP 8A — Complexity Trend Table (Required)

Evidence mode: `mechanical` (primary) + `classified` (secondary)

| Metric [M/C] | 2026-03-08 | 2026-03-09 | 2026-03-12 | 2026-03-15 |
| ---- | ----: | ----: | ----: | ----: |
| continuation decision-owner count `[C]` | 10 | N/A | N/A | 2 |
| continuation execution-consumer count `[C]` | 48 | N/A | N/A | 22 |
| AccessPath branch-module count `[M]` | N/A | N/A | N/A | 6 |
| RouteShape branch-module count `[M]` | N/A | N/A | N/A | 2 |
| branch hotspots (count) `[M]` | N/A | N/A | N/A | 10 |
| super-node count `[D]` | N/A | N/A | N/A | 4 |
| AccessPath variants `[M]` | 7 | 7 | N/A | 7 |
| continuation mentions (weak context) `[M]` | 925 | 1,016 | N/A | 1,313 |

## STEP 8B — Invalidating Signals (Required)

Evidence mode: `classified`

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves | no | none | no adjustment |
| file splits without semantic change | yes | can inflate module counts while reducing local complexity | interpret with concentration and owner-count checks |
| generated code expansion | no | none | no adjustment |
| parser/table-driven conversion replacing branch expressions | no | none | no adjustment |
| branch consolidation into helper functions | yes | can lower local branch density without reducing concept spread | keep owner/consumer/plumbing table primary |

## Required Summary

0. Run metadata + comparability note: `CA-1.3` run on `d40099bf`, marked `non-comparable` because `scripts/audit/runtime_metrics.sh` is absent and fallback extraction was used.
1. Overall complexity risk index: `5.0/10` from weighted rubric (`70/14`).
2. Fastest growing concept families: baseline-reset run; highest current concept spread is `PredicateCoercion` (`1,334` mention lines, `28` branch modules) and `Continuation` (`1,313` mention lines, `22` branch modules).
3. Highest branch multipliers: `Predicate=481`, `AccessPath=105`, `CursorPlanError=90` from the enum surface table.
4. Branch distribution drift (`AccessPath` / `RouteShape`): current branch modules are `6` and `2`, with branch/owner ratios `2.00` and `0.67`.
5. Flow multiplication risks (axis-based): `load` has `42` theoretical combinations and `6` effective flows; `index mutation` has `12` theoretical and `5` effective.
6. Semantic authority vs execution spread risks: continuation remains most asymmetric (`2` owners vs `22` consumers vs `131` plumbing modules).
7. Ownership drift + fanout pressure: layer check reports stable key owners (`AccessPath=3`, `RouteShape=3`, `predicate coercion=4`); no modules exceed `fanout > 12` in this method baseline.
8. Super-node + call-depth warnings: `4` super-nodes by domain-count rule; `10` hotspot functions at `branch_layers >= 8`; max observed branch nesting depth is `3`.
9. Trend-interpretation filter outcomes: continuation mention growth is treated as weak context due method/scope drift; owner counts remain the primary authority signal.
10. Complexity trend table: provided for `2026-03-08`, `2026-03-09`, `2026-03-12`, and `2026-03-15`, with explicit `N/A` where prior runs are non-comparable.
11. Verification readout: layer-authority checks and architecture text-scan checks passed; `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `crosscutting audit tooling`; action: restore or reintroduce `scripts/audit/runtime_metrics.sh` so future `complexity-accretion` runs can return to full comparability; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `crosscutting method`; action: align fallback fanout extraction with the canonical generator semantics before next comparable baseline promotion; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh` -> BLOCKED (missing in repository)
- fallback mechanical runtime extraction -> PASS (`docs/audits/reports/2026-03/2026-03-15/helpers/runtime-metrics.tsv`)
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS

