# Complexity Accretion Audit - 2026-03-28

## Report Preamble

- scope: conceptual growth, branch pressure, flow multiplication, and authority spread in `crates/icydb-core/src` runtime modules (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-26/complexity-accretion.md`
- code snapshot identifier: `d38b29fa`
- method tag/version: `CA-1.3`
- method manifest:
  - `method_version = CA-1.3`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`
- comparability status: `comparable` for generator-backed runtime totals, concentration ratios, and layer-authority anchors against `2026-03-26`; `semi-mechanical extension` for the enum-surface and concept-branch tables because the prior canonical run did not record those sub-artifacts explicitly

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/runtime-metrics.tsv`
- `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/module-branch-hotspots.tsv`
- `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/enum-surface.tsv`
- `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/concept-branch-summary.tsv`
- `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/concept-branch-map.tsv`

## STEP -1 — Runtime Module Enumeration

Evidence mode: `mechanical`

- full runtime dataset (`483` modules):
  - `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/runtime-metrics.tsv`
- derived branch-hotspot view:
  - `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/module-branch-hotspots.tsv`

Top branch-site modules from the required enumeration table:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 842 | 4 | 18 | 4.50 | 66 | 65 | 2 | 2 | 69 |
| `db::query::plan::access_choice` | `crates/icydb-core/src/db/query/plan/access_choice/mod.rs` | 788 | 17 | 76 | 4.47 | 49 | 49 | 2 | 3 | 66 |
| `db::executor::explain::descriptor` | `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 1052 | 21 | 78 | 3.71 | 44 | 42 | 2 | 3 | 63 |
| `db::predicate::runtime` | `crates/icydb-core/src/db/predicate/runtime.rs` | 998 | 39 | 164 | 4.21 | 20 | 20 | 3 | 3 | 59 |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser/mod.rs` | 552 | 3 | 15 | 5.00 | 49 | 46 | 2 | 2 | 49 |
| `db::data::persisted_row` | `crates/icydb-core/src/db/data/persisted_row.rs` | 1793 | 23 | 124 | 5.39 | 24 | 24 | 2 | 7 | 47 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering/mod.rs` | 988 | 21 | 73 | 3.48 | 26 | 26 | 2 | 2 | 47 |
| `db::access::canonical` | `crates/icydb-core/src/db/access/canonical.rs` | 395 | 9 | 36 | 4.00 | 32 | 32 | 2 | 2 | 41 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 724 | 19 | 86 | 4.53 | 20 | 20 | 2 | 3 | 39 |
| `db::reduced_sql` | `crates/icydb-core/src/db/reduced_sql/mod.rs` | 618 | 9 | 147 | 16.33 | 29 | 28 | 2 | 3 | 37 |

## STEP 0 — Baseline Capture

Evidence mode: `semi-mechanical`

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 483 | 483 | 0 |
| Runtime LOC | `[M]` | primary | 71,835 | 72,799 | +964 |
| Runtime fanout (sum) | `[M]` | primary | 973 | 973 | 0 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | N/A (canonical dataset does not emit `domain_count`) | N/A (manual full recount not run) | N/A |
| Continuation decision owners | `[C]` | primary | N/A (not recorded in 2026-03-26) | 2 | N/A |
| Continuation execution consumers | `[C]` | primary | N/A (not recorded in 2026-03-26) | 32 | N/A |
| Continuation plumbing modules | `[C]` | primary | N/A (not recorded in 2026-03-26) | 78 | N/A |
| AccessPath decision owners | `[C]` | primary | 3 | 3 | 0 |
| AccessPath executor dispatch sites | `[M]` | primary | N/A (not recorded in 2026-03-26) | 1 | N/A |
| AccessPath branch modules | `[M]` | primary | N/A (not recorded in 2026-03-26) | 10 | N/A |
| RouteShape decision owners | `[C]` | primary | 2 | 2 | 0 |
| RouteShape branch modules | `[M]` | primary | N/A (not recorded in 2026-03-26) | 7 | N/A |
| Predicate coercion decision owners | `[C]` | primary | 4 | 4 | 0 |
| Continuation mentions (context only) | `[M]` | weak | N/A (not recorded in 2026-03-26) | 4,002 | N/A |

## Current Complexity Signals

- Total branch sites rose only slightly: `2,531 -> 2,547` (`+16`, about `+0.6%`), while hotspot modules at `branch_sites_total >= 40` fell `9 -> 8`.
- Concentration stayed nearly flat:
  - top-10 branch-site concentration: `0.2027 -> 0.2030` (`+0.0003`)
  - top-10 fanout concentration: `0.0606 -> 0.0606` (`flat`)
- The main hotspot family stayed concentrated rather than broadening:
  - `db::query::plan::access_choice`: `65 -> 66` branch sites
  - `db::executor::explain::descriptor`: `60 -> 63`
  - `db::predicate::runtime`: `64 -> 59`
  - `db::data::persisted_row`: `40 -> 47`
- `db::data::persisted_row` is the clearest growth vector in this run:
  - LOC: `1522 -> 1793` (`+271`)
  - branch sites: `40 -> 47` (`+7`)
  - fanout: unchanged at `7`
- Layer-authority anchors stayed flat:
  - `AccessPath` decision owners: `3`
  - `RouteShape` decision owners: `2`
  - predicate coercion owners: `4`
  - cross-layer policy re-derivations: `0`

## STEP 1 — Variant Surface Growth + Branch Multiplier

Evidence mode: `semi-mechanical`

- enum surface artifact:
  - `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/enum-surface.tsv`

| Enum [M] | Variants [M] | Switch Sites [M] | Branch Multiplier [D] | Decision Owners [C] | Domain Scope [C] | Mixed Domains? [C] | Growth Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |
| `Predicate` | 13 | 24 | 312 | 4 | predicate + index + query + executor | yes | High |
| `ErrorClass` | 7 | 17 | 119 | 1 | shared error taxonomy | yes | Medium |
| `CursorPlanError` | 9 | 12 | 108 | 2 | cursor + query/session | yes | Medium-High |
| `PlanError` | 3 | 27 | 81 | 2 | plan validation + query mapping | yes | Medium |
| `AccessPath` | 7 | 10 | 70 | 3 | access + query/explain | yes | High |
| `QueryError` | 5 | 7 | 35 | 2 | query + session facade | yes | Medium |
| `RouteShapeKind` | 5 | 7 | 35 | 2 | executor routing | no | Low-Medium |
| `PreparedIndexDeltaKind` | 5 | 4 | 20 | 1 | commit/index mutation | no | Low-Medium |
| `StoreError` | 3 | 1 | 3 | 1 | storage error boundary | no | Low |

Interpretation:

- No new large runtime enum appeared, but the mixed-domain switch surfaces are still the same ones:
  - `Predicate` at multiplier `312`
  - `ErrorClass` at `119`
  - `CursorPlanError` at `108`
  - `PlanError` at `81`
- `AccessPath` is not variant-exploding (`7` variants), but it still has mixed-domain pressure because the `10` branch-bearing modules straddle access, planning, and explain.

## STEP 2 — Local Branching Pressure (Function-Level)

Evidence mode: `semi-mechanical`

This run did not regenerate a full function-level extractor under the canonical
dataset, so the function table below is a current-source reinspection of the
known hotspot family from the last detailed fallback-era run.

| Function [M] | Module [M] | Branch Layers [D] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_chain_count [M] | max_branch_depth [M] | Axis Count [C] | Previous Branch Layers [M] | Delta [D] | Domains Mixed [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `plan_compare` | `db::query::plan::planner::compare` | 14 | 2 | 10 | 5.00 | 12 | 3 | 4 | 14 | 0 | yes | High |
| `write_execution_node_json` | `db::query::explain::json` | 12 | 11 | 22 | 2.00 | 1 | 1 | 3 | 12 | 0 | yes | Medium |
| `render_text_tree_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 1 | 3 | 12 | 0 | yes | Medium |
| `render_text_tree_verbose_into` | `db::query::explain::render` | 12 | 0 | 0 | 0.00 | 12 | 1 | 3 | 12 | 0 | yes | Medium |
| `canonical_cmp` | `db::access::canonical` | 13 | 1 | 7 | 7.00 | 12 | 2 | 3 | 13 | 0 | no | Medium-High |
| `eval_text_scalar_compare` | `db::predicate::runtime` | 10 | 2 | 14 | 7.00 | 8 | 3 | 4 | 10 | 0 | yes | Medium |

Interpretation:

- The function hotspot family is stable rather than expanding.
- No reinspection surfaced a new function with deeper branch layering than the existing `12-14` layer cluster.
- The current pressure remains concentrated in:
  - planner compare / access choice
  - explain JSON / render surfaces
  - canonical compare
  - text predicate runtime

## STEP 2A — Concept Branch Distribution Across Modules

Evidence mode: `semi-mechanical`

- concept branch artifacts:
  - `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/concept-branch-summary.tsv`
  - `docs/audits/reports/2026-03/2026-03-28/artifacts/complexity-accretion/concept-branch-map.tsv`

| Concept [M] | Branch Modules [M] | Decision Owners [C] | Branch/Owner Ratio [D] | Previous Branch Modules [M] | Delta [D] |
| ---- | ----: | ----: | ----: | ----: | ----: |
| `AccessPath` | 10 | 3 | 3.33 | N/A (not recorded in 2026-03-26) | N/A |
| `RouteShape` | 7 | 2 | 3.50 | N/A (not recorded in 2026-03-26) | N/A |
| `Continuation` | 32 | 2 | 16.00 | N/A (not recorded in 2026-03-26) | N/A |
| `PredicateCoercion` | 27 | 4 | 6.75 | N/A (not recorded in 2026-03-26) | N/A |

Interpretation:

- The branch/owner ratio is still highest on continuation (`16.00`) and predicate coercion (`6.75`), but owner counts did not increase.
- `AccessPath` and `RouteShape` remain concentrated enough that semantic ownership is still contained:
  - `AccessPath`: `10` branch modules over `3` owners
  - `RouteShape`: `7` branch modules over `2` owners

## STEP 3 — Execution Path Multiplicity (Effective Flows)

Evidence mode: `semi-mechanical`

No new runtime axis landed in the late `0.65` line. The effective-flow table
remains structurally flat against the prior classified baseline.

### Constraint Ledger

| Operation [M] | Constraint [C] | Axes Restricted [M] | Combinations Removed [D] | Evidence [M/C] |
| ---- | ---- | ---- | ----: | ---- |
| `save` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/save.rs` |
| `replace` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/replace.rs` |
| `delete` | cursor axis is disabled for mutation execution | `cursor presence` | 4 | `db/executor/mutation/delete.rs` |
| `load` | access-path/order combinations are constrained by route/capability checks | `access path`, `ordering mode` | 36 | `db/executor/route/*`, `db/executor/stream/access/*` |
| `recovery replay` | replay path fixes recovery mode and narrows mutation classes | `recovery mode`, `operation subtype` | 3 | `db/commit/recovery.rs`, `db/commit/replay.rs` |
| `cursor continuation` | continuation path requires cursor + boundary-compatible route shape | `cursor presence`, `ordering mode`, `access path` | 20 | `db/cursor/*`, `db/query/plan/continuation.rs` |
| `index mutation` | uniqueness/relation mode narrows index mutation combinations | `index uniqueness`, `operation subtype` | 7 | `db/index/plan/*`, `db/commit/prepare.rs` |

### Flow Table

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
| continuation / cursor anchor semantics | `db::query::plan::continuation`, `db::cursor::continuation` | branch-bearing continuation modules from concept map | current mention modules minus consumers | 2 | 32 | 78 | 2 | 3 | High |
| `AccessPath` decision semantics | `db::access::path`, `db::access::dispatch`, `db::access::lowering` | branch-bearing `AccessPath` modules from concept map | current mention modules minus consumers | 3 | 10 | 27 | 2 | 2 | Medium-High |
| `RouteShape` decision semantics | `db::executor::route::contracts::shape`, `db::executor::route::planner::execution` | branch-bearing `RouteShape` modules from concept map | current mention modules minus consumers | 2 | 7 | 5 | 2 | 2 | Medium |
| predicate coercion decision semantics | `db::predicate::coercion`, `db::predicate::normalize`, `db::query::plan::planner::compare`, `db::sql::lowering` | branch-bearing predicate/coercion modules from concept map | current mention modules minus consumers | 4 | 27 | 20 | 3 | 3 | High |
| envelope boundary checks | `db::cursor::boundary` | cursor boundary and token/runtime consumers | cursor transport modules | 1 | 9 | 11 | 2 | 2 | Medium |
| bound conversions | `db::query::plan::planner::range`, `db::cursor::boundary` | range/cursor branch modules | transport modules using bounds | 2 | 11 | 17 | 2 | 2 | Medium |
| error origin mapping | `error::mod`, `db::query::intent::errors`, planner validation mapping | session/planner/executor error branches | error transport modules | 3 | 17 | 20 | 2 | 2 | Medium |
| index id / namespace validation | `db::schema::validate`, `db::index::validate` | index planning + schema validation consumers | schema/index transport modules | 2 | 9 | 18 | 2 | 2 | Medium |

Interpretation:

- Continuation is still the broadest execution-spread concept in the current tree:
  - `2` owners
  - `32` branch-bearing consumers
  - `78` transport/plumbing modules
- Predicate coercion remains the broadest mixed-domain semantic surface:
  - `4` owners
  - `27` consumers
  - `20` plumbing modules
  - `3` semantic layers and `3` transport layers
- `AccessPath` and `RouteShape` stayed contained enough that the branch spread is still owner-led, not owner-fragmented.

## STEP 4A — Concept Ownership Drift (Only)

Evidence mode: `classified`

| Concept [M] | Decision Owners [C] | Previous Owners [C] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| continuation | 2 | N/A (not recorded in 2026-03-26) | N/A | Medium |
| `AccessPath` | 3 | 3 | 0 | High |
| `RouteShape` | 2 | 2 | 0 | Medium |
| predicate coercion | 4 | 4 | 0 | High |
| index range | 2 | N/A (not recorded in 2026-03-26) | N/A | Medium |
| canonicalization | 1 | 1 | 0 | Medium-Low |

## STEP 4B — Fanout Pressure

Evidence mode: `mechanical`

No module crossed the `fanout > 12` threshold. Fanout pressure remains flat and
localized.

| Module [M] | Fanout [M] | Previous Fanout [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| `db::data::persisted_row` | 7 | 7 | 0 | Medium |
| `db::executor::aggregate::projection` | 5 | 5 | 0 | Medium |
| `db::executor::terminal::bytes` | 5 | 5 | 0 | Medium |
| `db::scalar_expr` | 5 | 5 | 0 | Medium |
| `db::index::key::build` | 5 | 5 | 0 | Medium |
| `db::query::plan::access_choice` | 3 | 3 | 0 | Low-Medium |
| `db::executor::explain::descriptor` | 3 | 3 | 0 | Low-Medium |
| `db::predicate::runtime` | 3 | 3 | 0 | Low-Medium |

## STEP 5 — Cognitive Load Indicators (Super-Node + Call Depth)

Evidence mode: `mechanical`

The canonical dataset gives strong module-level complexity signals but does not
emit `domain_count` or a full current function-length distribution. This step
therefore stays explicit about what is measured and what remains `N/A`.

| Module/Operation [M] | LOC or Call Depth [M] | Fanout [M] | Domain Count [D] | Previous [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| branch hotspot modules (`branch_sites_total >= 40`) | 8 | N/A | N/A | 9 | -1 | Medium |
| modules with `max_branch_depth >= 3` | 17 | N/A | N/A | 18 | -1 | Medium |
| max observed module branch depth | 3 | N/A | N/A | 3 | 0 | Medium-Low |
| `db::data::persisted_row` | 1793 | 7 | N/A | 1522 | +271 | High |
| `db::executor::explain::descriptor` | 1052 | 3 | N/A | 997 | +55 | Medium-High |
| `db::predicate::runtime` | 998 | 3 | N/A | 983 | +15 | Medium-High |
| `db::query::plan::access_choice` | 788 | 3 | N/A | 754 | +34 | High |

Interpretation:

- The dominant cognitive load warning in this run is not fanout growth; it is
  owner-local accretion inside already hot modules.
- `db::data::persisted_row` is now clearly in the cognitive-load watch band:
  `1793` LOC, `47` branch sites, `7` fanout.

## STEP 5A — Complexity Concentration Ratios

Evidence mode: `mechanical`

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) | 0.0606 | 0.0606 | 0.0000 | Low |
| Branch-site concentration (top 10 modules) | 0.2030 | 0.2027 | +0.0003 | Medium |
| AccessPath branch concentration (top 3 modules) | 0.6048 | N/A (not recorded in 2026-03-26) | N/A | Medium-High |
| RouteShape branch concentration (top 3 modules) | 0.8000 | N/A (not recorded in 2026-03-26) | N/A | High |

## STEP 6 — Drift Sensitivity (Axis Count)

Evidence mode: `semi-mechanical`

| Area [M] | Decision Axes [M] | Axis Count [D] | Branch Multiplier [D] | Drift Sensitivity [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ---- |
| access-choice + planner route eligibility | access path variant, predicate shape, order mode, cursor mode | 4 | 70 (`AccessPath`) | planner/explain/runtime coupling remains concentrated in one family | High |
| continuation envelope eligibility | boundary kind, anchor placement, order direction, cursor token presence | 4 | 108 (`CursorPlanError` proxy) | invariant drift risk if cursor semantics leak outside the owner pair | Medium-High |
| predicate coercion + normalization | predicate node kind, coercion path, canonicalization path, target index capability | 4 | 312 (`Predicate`) | highest multiplication pressure across planner/index/runtime surfaces | High |
| explain/render route shape | route shape, access-path rendering mode, projection mode | 3 | 35 (`RouteShapeKind`) | bounded but concentrated in explain nodes | Medium |

## STEP 7 — Complexity Risk Index (Rubric-Guided)

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| Variant explosion risk | 6 | 2 | 12 |
| Branching pressure + centralization trend | 5 | 2 | 10 |
| Flow multiplicity | 5 | 2 | 10 |
| Cross-layer spread | 5 | 3 | 15 |
| Authority fragmentation | 4 | 2 | 8 |
| Fanout pressure + super-node load | 4 | 2 | 8 |
| Call-depth pressure | 4 | 1 | 4 |

`overall_index = 67 / 14 = 4.8`

## STEP 8 — Trend Interpretation Filter (Structural Noise Filter)

Evidence mode: `semi-mechanical`

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| owner-local row-boundary hardening in `db::data::persisted_row` | `40 -> 47` branch sites, `1522 -> 1793` LOC | benign surface growth | complexity increased, but it stayed inside one canonical boundary rather than creating new decision owners |
| route snapshot/hint helper consolidation | small helper growth in route planner/hints, owner counts flat (`RouteShape = 2`) | structural improvement | route boilerplate compression offsets some local branch noise and does not represent new semantic spread |
| hotspot module count | `9 -> 8` modules at `branch_sites_total >= 40` | structural improvement | the hotspot family is slightly narrower even though total branch sites rose by `16` |
| file count in canonical runtime scope | `483 -> 483` | no structural diffusion | there was no broad runtime sprawl during this window |

## STEP 8A — Complexity Trend Table (Required)

Evidence mode: `mechanical` (primary) + `classified` (secondary)

Only two comparable dates are available under the canonical runtime-metrics
generator in the current branch line, so this table uses the full comparable
set.

| Metric [M/C] | 2026-03-26 | 2026-03-28 |
| ---- | ----: | ----: |
| continuation decision-owner count `[C]` | N/A (not recorded in report) | 2 |
| continuation execution-consumer count `[C]` | N/A (not recorded in report) | 32 |
| AccessPath branch-module count `[M]` | N/A (not recorded in report) | 10 |
| RouteShape branch-module count `[M]` | N/A (not recorded in report) | 7 |
| branch hotspot modules (`>= 40` branch sites) `[M]` | 9 | 8 |
| super-node count `[D]` | N/A | N/A |
| AccessPath variants `[M]` | N/A (not recorded in report) | 7 |
| continuation mentions (weak context) `[M]` | N/A (not recorded in report) | 4002 |

Extended context from the last fallback-era detailed runs:

| Metric [M/C] | 2026-03-15 (fallback) | 2026-03-24 (fallback refined) | 2026-03-26 (canonical) | 2026-03-28 (canonical) |
| ---- | ----: | ----: | ----: | ----: |
| continuation decision-owner count `[C]` | 2 | 2 | N/A | 2 |
| continuation execution-consumer count `[C]` | 22 | 22 | N/A | 32 |
| AccessPath branch-module count `[M]` | 6 | 30 | N/A | 10 |
| RouteShape branch-module count `[M]` | 2 | 6 | N/A | 7 |
| branch hotspots (context only) `[M]` | 10 functions | 19 functions | 9 modules (`>=40`) | 8 modules (`>=40`) |
| super-node count `[D]` | 4 | 18 | N/A | N/A |
| AccessPath variants `[M]` | 7 | 7 | N/A | 7 |
| continuation mentions (weak context) `[M]` | 1313 | 3907 | N/A | 4002 |

## STEP 8B — Invalidating Signals (Required)

Evidence mode: `classified`

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves | no | none | no adjustment |
| file splits without semantic change | no material runtime file-count shift (`483 -> 483`) | low | interpret branch drift directly |
| generated code expansion | no | none | no adjustment |
| parser/table-driven conversion replacing branch expressions | no | none | no adjustment |
| branch consolidation into helper functions | yes | can lower local branch density while keeping concept spread flat | owner counts and branch concentration remain primary drift signals |

## Required Summary

0. Run metadata + comparability note
- `CA-1.3` run on `d38b29fa`, comparable to `2026-03-26` for canonical runtime totals and concentration ratios, with semi-mechanical extension tables added for enum and concept branch maps.

1. Overall complexity risk index
- overall complexity risk index is `4.8/10` from the weighted rubric (`67/14`), which keeps the runtime in the moderate band and slightly below the `4.9/10` signal from `2026-03-26`.

2. Fastest growing concept families
- the fastest growing owner-local concept family is the canonical row boundary: `db::data::persisted_row` grew `+271` LOC and `+7` branch sites (`40 -> 47`) without any owner-count increase.
- the next visible local growth points are `db::executor::explain::descriptor` (`60 -> 63` branch sites) and `db::query::plan::access_choice` (`65 -> 66`).

3. Highest branch multipliers
- the highest current branch multipliers are `Predicate = 312`, `ErrorClass = 119`, `CursorPlanError = 108`, `PlanError = 81`, and `AccessPath = 70`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- current `AccessPath` spread is `10` branch-bearing modules over `3` owners (`3.33` branch/owner ratio).
- current `RouteShape` spread is `7` branch-bearing modules over `2` owners (`3.50` branch/owner ratio).
- both remain concentrated in a small planner/explain/route slice rather than diffusing across runtime.

5. Flow multiplication risks (axis-based)
- `load` remains the highest-pressure operation at `42` theoretical combinations and `6` effective flows.
- `index mutation` remains the next highest at `12` theoretical combinations and `5` effective flows.
- no effective-flow delta appeared in this run.

6. Semantic authority vs execution spread risks
- continuation remains the most asymmetric concept with `2` owners, `32` branch-bearing consumers, and `78` plumbing modules.
- predicate coercion remains the widest mixed-domain semantic surface with `4` owners, `27` consumers, and `3` semantic layers.

7. Ownership drift + fanout pressure
- key owner counts stayed flat:
  - `AccessPath = 3`
  - `RouteShape = 2`
  - predicate coercion = `4`
- runtime fanout stayed exactly flat:
  - total fanout `973 -> 973`
  - modules with `fanout > 12`: `0 -> 0`

8. Super-node + call-depth warnings
- current cognitive-load pressure is still hotspot-local, not graph-wide:
  - branch hotspot modules (`>=40` branch sites) dropped `9 -> 8`
  - modules with depth `>=3` dropped `18 -> 17`
  - `db::data::persisted_row` is the clearest watchpoint at `1793` LOC, `47` branch sites, and `7` fanout

9. Trend-interpretation filter outcomes
- the main raw growth signal (`db::data::persisted_row`) is owner-local and therefore classifies as benign surface growth, not semantic leakage.
- the route helper cleanup reduces boilerplate without increasing semantic-owner count, so it reads as structural improvement rather than new complexity.

10. Complexity trend table
- the canonical comparable trend is two-point only in this branch line: `2026-03-26 -> 2026-03-28`.
- within that comparable window:
  - files stayed flat at `483`
  - total branch sites rose `2531 -> 2547`
  - top-10 branch concentration stayed effectively flat `0.2027 -> 0.2030`
  - hotspot modules fell `9 -> 8`

11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
- canonical runtime metrics generation passed, both architecture/layer invariant scans passed, and `cargo check -p icydb-core` passed.

## Follow-Up Actions

- owner boundary: `db::data::persisted_row`; action: keep monitoring owner-local growth and trigger a focused structure follow-up if `branch_sites_total` crosses `50` without offsetting decomposition; target report date/run: next `crosscutting-complexity-accretion` run.
- owner boundary: `db::query::plan::access_choice` + `db::executor::explain::descriptor`; action: treat any further top-10 branch concentration increase above `0.21` as a cue for a planner/explain hotspot follow-up; target report date/run: next `crosscutting-complexity-accretion` run.

## Verification Readout

- `scripts/audit/runtime_metrics.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
