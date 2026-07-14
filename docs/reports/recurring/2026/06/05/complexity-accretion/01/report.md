# Complexity Accretion Audit - 2026-06-05

## Report Preamble

| Field [M] | Value |
| ---- | ---- |
| `method_version` | `CA-1.4` |
| `completion_status` | `complete` |
| `risk_index_kind` | `overall` |
| `baseline_report` | `docs/audits/reports/2026-05/2026-05-10/complexity-accretion.md` |
| `comparability_status` | `comparable with caveat` |
| `missing_sections` | `none` |

- scope: `crates/icydb-core/src` runtime modules, excluding tests, benches,
  and examples.
- code snapshot identifier: `fc4dc729e` plus local uncommitted audit,
  module-split cleanup, and value-surface cleanup changes.
- baseline caveat: the previous CA-1.4 report was `mechanical-only`; mechanical
  metrics are comparable, while classified owner and flow deltas use `N/A`.
- method manifest:
  - `method_version = CA-1.4`
  - `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
  - `domain_taxonomy = D-2`
  - `flow_axis_model = F-1`
  - `switch_site_rule = S-1`
  - `risk_rubric = R-1`
  - `trend_filter_rule = T-1`

## Artifact Coverage

| Artifact [M] | Status [C] | Reason [C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| `runtime-metrics.tsv` | PASS | canonical generator completed | comparable mechanical metrics |
| `module-branch-hotspots.tsv` | PASS | derived from runtime metrics | comparable mechanical metrics |
| `enum-surface.tsv` | PASS | semi-mechanical enum extraction | current-run baseline for enum surface |
| `enum-switch-sites.tsv` | PASS | direct enum-qualified reference sites | switch-site caveat: constructor references included |
| `function-branch-hotspots.tsv` | PASS | semi-mechanical function block extraction | current-run baseline for function hotspots |
| `concept-branch-summary.tsv` | PASS | AccessPath/RouteShapeKind branch map summary | current-run baseline for concept spread |
| `concept-branch-map.tsv` | PASS | AccessPath/RouteShapeKind site evidence | current-run baseline for concept spread |
| `flow-constraint-ledger.tsv` | PASS | classified flow constraints with source anchors | prior classified baseline unavailable |
| `flow-counts.tsv` | PASS | classified/derived effective flow totals | prior classified baseline unavailable |
| `semantic-spread.tsv` | PASS | classified semantic role table | prior classified baseline unavailable |
| `ownership-drift.tsv` | PASS | classified owner counts | prior classified baseline unavailable |
| `concentration-ratios.tsv` | PASS | derived from runtime metrics | comparable mechanical metrics |
| `invalidating-signals.tsv` | PASS | classified trend filter signals | comparable with caveat |
| `risk-buckets.tsv` | PASS | rubric score from current artifacts | overall score with baseline caveat |
| `issue-ledger.tsv` | PASS | owner-scoped follow-up rows | current-run action ledger |

## Step Status

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP -1 | PASS | `runtime-metrics.tsv` | comparable mechanical metrics |
| STEP 0 | PASS | baseline table below | classified previous values often `N/A` |
| STEP 1 | PASS | `enum-surface.tsv`, `enum-switch-sites.tsv` | current-run enum baseline |
| STEP 2 | PASS | `function-branch-hotspots.tsv` | current-run function baseline |
| STEP 2A | PASS | `concept-branch-summary.tsv`, `concept-branch-map.tsv` | current-run concept baseline |
| STEP 3 | PASS | `flow-constraint-ledger.tsv`, `flow-counts.tsv` | classified previous values `N/A` |
| STEP 4 | PASS | `semantic-spread.tsv` | classified previous values `N/A` |
| STEP 4A | PASS | `ownership-drift.tsv` | classified previous values `N/A` |
| STEP 4B | PASS | `runtime-metrics.tsv` | comparable mechanical metrics |
| STEP 5 | PASS | `runtime-metrics.tsv`, `function-branch-hotspots.tsv` | call-depth uses function hotspot proxy |
| STEP 5A | PASS | `concentration-ratios.tsv` | comparable mechanical metrics |
| STEP 6 | PASS | `flow-counts.tsv`, `enum-surface.tsv` | current-run axis baseline |
| STEP 7 | PASS | `risk-buckets.tsv` | overall score with caveat |
| STEP 8 | PASS | trend filter table below | comparable with caveat |
| STEP 8A | PASS | trend table below | only two CA-1.4 points available |
| STEP 8B | PASS | `invalidating-signals.tsv` | comparable with caveat |
| STEP 9 | PASS | `issue-ledger.tsv` | current-run action ledger |

## STEP -1 - Runtime Module Enumeration

Evidence mode: `mechanical`.

The canonical generator completed and produced
`docs/audits/reports/2026-06/2026-06-05/artifacts/complexity-accretion/runtime-metrics.tsv`.

Top branch-site modules:

| module [M] | LOC [M] | fanout [M] | branch_sites_total [D] |
| ---- | ----: | ----: | ----: |
| `db::sql::parser::statement` | 611 | 1 | 65 |
| `db::schema::transition` | 930 | 3 | 56 |
| `db::query::plan::access_choice::evaluator::range` | 472 | 2 | 49 |
| `db::data::persisted_row::contract` | 871 | 5 | 47 |
| `db::index::key::build` | 1142 | 5 | 46 |

## STEP 0 - Baseline Capture

Evidence mode: `semi-mechanical`.

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary | 779 | 819 | +40 |
| Runtime LOC | `[M]` | primary | 141,121 | 168,553 | +27,432 |
| Runtime fanout (sum) | `[M]` | primary | 1,546 | 1,592 | +46 |
| Modules with fanout > 12 | `[D]` | primary | 0 | 0 | 0 |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary | N/A | 0 by fanout criterion | N/A |
| Continuation decision owners | `[C]` | primary | N/A | 1 | N/A |
| Continuation execution consumers | `[C]` | primary | N/A | 2 | N/A |
| Continuation plumbing modules | `[C]` | primary | N/A | 2 | N/A |
| AccessPath decision owners | `[C]` | primary | N/A | 2 | N/A |
| AccessPath executor dispatch sites | `[M]` | primary | N/A | 112 direct references | N/A |
| AccessPath branch modules | `[M]` | primary | N/A | 27 | N/A |
| RouteShape decision owners | `[C]` | primary | N/A | 1 | N/A |
| RouteShape branch modules | `[M]` | primary | N/A | 6 | N/A |
| Predicate coercion decision owners | `[C]` | primary | N/A | 2 | N/A |
| Continuation mentions (context only) | `[M]` | weak | N/A | not scored | N/A |
| Total branch sites | `[M]` | primary | 4,777 | 5,712 | +935 |
| Modules with `branch_sites_total >= 40` | `[D]` | primary | 4 | 14 | +10 |

## STEP 1 - Variant Surface Growth + Branch Multiplier

Evidence mode: `semi-mechanical`.

| Enum [M] | Variants [M] | Switch/Variant Sites [M] | Branch Multiplier [D] | Decision Owners [C] | Domain Scope [C] | Mixed Domains? [C] | Growth Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |
| `Predicate` | 14 | 668 | 9,352 | 1 | predicate AST | no | medium |
| `QueryError` | 6 | 246 | 1,476 | 1 | query intent/public detail | no | medium |
| `AccessPath` | 7 | 112 | 784 | 1 | access planning/runtime path | no | medium |
| `ErrorClass` | 7 | 101 | 707 | 1 | public error class | no | medium |
| `PlanError` | 3 | 233 | 699 | 1 | query plan validation | no | medium |
| `CursorPlanError` | 8 | 57 | 456 | 1 | cursor planning policy | no | medium |
| `AccessPathKind` | 7 | 55 | 385 | 1 | access discriminator | no | medium |
| `RouteShapeKind` | 5 | 22 | 110 | 1 | executor route shape | no | medium |

The switch-site artifact records enum-qualified references, so the multiplier
is a pressure signal rather than a pure direct-match count.

## STEP 2 - Local Branching Pressure

Evidence mode: `semi-mechanical`.

| Function [M] | Module [M] | Branch Layers [D] | match_count [M] | if_count [M] | Axis Count [C] | Domains Mixed [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `scalar_slot_value_ref_into_value` | `db::data::persisted_row::reader::structural_slot_reader` | 35 | 6 | 29 | 0 | local | medium |
| `canonicalize_lossless_field_literal_for_kind` | `model::field_kind_semantics` | 17 | 16 | 1 | 0 | local | low |
| `visit_journaled_entries_in_bounds` | `db::data::store` | 14 | 3 | 11 | 0 | local | low |
| `write_execution_node_json` | `db::query::explain::json` | 14 | 13 | 1 | 0 | local | low |
| `parse_sql_expr_prefix` | `db::sql::parser::projection` | 14 | 0 | 14 | 0 | local | low |
| `plan_ordered_compare` | `db::query::plan::planner::compare` | 8 | 3 | 5 | 4 | access,predicate,plan,index | medium |

## STEP 2A - Concept Branch Distribution Across Modules

Evidence mode: `semi-mechanical`.

| Concept [M] | Branch Modules [M] | Decision Owners [C] | Branch/Owner Ratio [D] | Previous Branch Modules [M] | Delta [D] |
| ---- | ----: | ----: | ----: | ----: | ----: |
| `AccessPath` | 27 | 2 | 13.50 | N/A | N/A |
| `RouteShape` | 6 | 1 | 6.00 | N/A | N/A |

## STEP 3 - Execution Path Multiplicity

Evidence mode: `semi-mechanical`.

| Operation [M] | Axes Used [M] | Theoretical Space [D] | Effective Flows [D] | Previous Effective Flows [M] | Delta [D] | Shared Core? [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `save` | operation type,index uniqueness,recovery mode | 4 | 2 | N/A | N/A | yes | low-medium |
| `replace` | operation type,index uniqueness,recovery mode | 4 | 2 | N/A | N/A | yes | low-medium |
| `delete` | operation type,index uniqueness,recovery mode | 4 | 2 | N/A | N/A | yes | low-medium |
| `load` | access path type,cursor presence,ordering mode | 28 | 6 | N/A | N/A | yes | medium |
| `recovery replay` | operation type,recovery mode,index mutation | 12 | 4 | N/A | N/A | yes | medium |
| `cursor continuation` | cursor presence,ordering mode,access path type | 28 | 5 | N/A | N/A | yes | medium |
| `index mutation` | operation type,index uniqueness,recovery mode | 8 | 4 | N/A | N/A | yes | medium |

The constraint ledger anchors these reductions to commit preparation, cursor
signature/boundary checks, schema mutation runner preflight, and relation
reverse-index validation.

## STEP 4 - Semantic Authority vs Execution Spread

Evidence mode: `classified`.

| Concept [M] | Owner Count [D] | Consumer Count [D] | Plumbing Count [D] | Semantic Layers [C] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- | ---- |
| continuation / cursor anchor semantics | 1 | 2 | 2 | cursor/executor/session | medium |
| `AccessPath` decision semantics | 2 | 2 | 2 | access/query/executor | medium |
| `RouteShape` decision semantics | 1 | 1 | 1 | executor/session | low-medium |
| predicate coercion decision semantics | 2 | 2 | 2 | predicate/value/query | medium |
| index id / namespace validation | 2 | 2 | 1 | index/schema/executor | medium |

## STEP 4A - Concept Ownership Drift

Evidence mode: `classified`.

| Concept [M] | Decision Owners [C] | Previous Owners [C] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| continuation | 1 | N/A | N/A | medium |
| `AccessPath` | 2 | N/A | N/A | medium |
| `RouteShape` | 1 | N/A | N/A | low-medium |
| predicate coercion | 2 | N/A | N/A | medium |
| index range | 2 | N/A | N/A | medium |
| canonicalization | 2 | N/A | N/A | medium |

## STEP 4B - Fanout Pressure

Evidence mode: `mechanical`.

| Module [M] | Fanout [M] | Previous Fanout [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| max runtime fanout | 10 | N/A | N/A | low |
| modules with fanout > 12 | 0 | 0 | 0 | low |
| runtime fanout sum | 1,592 | 1,546 | +46 | low |

## STEP 5 - Cognitive Load Indicators

Evidence mode: `mechanical`.

| Module/Operation [M] | LOC or Call Depth [M] | Fanout [M] | Domain Count [D] | Previous [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `metrics::sink` | 2,004 LOC | 3 | 0 | 1,991 LOC | +13 | medium |
| `db::key_taxonomy` | 1,180 LOC | 4 | 0 | N/A | N/A | medium |
| `db::schema::store` | 1,176 LOC | 4 | 0 | N/A | N/A | medium |
| `db::index::key::build` | 1,142 LOC | 5 | 1 | 592 LOC | +550 | medium |
| `scalar_slot_value_ref_into_value` | 35 branch layers | n/a | 0 | N/A | N/A | medium |

No fanout super-node was found under the `fanout > 20` criterion.

## STEP 5A - Complexity Concentration Ratios

Evidence mode: `mechanical`.

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) | 0.0396 | 0.0408 | -0.0012 | low |
| Branch-site concentration (top 10 modules) | 0.0842 | 0.0808 | +0.0034 | medium |

## STEP 6 - Drift Sensitivity

Evidence mode: `semi-mechanical`.

| Area [M] | Decision Axes [M] | Axis Count [D] | Branch Multiplier [D] | Drift Sensitivity [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ---- |
| load/cursor execution | access path type,cursor presence,ordering mode | 3 | 28 theoretical flows | new access variants multiply cursor paths | medium |
| predicate runtime/planner | predicate AST,coercion family,expression form | 3 | `Predicate` multiplier 9,352 | new predicate variants touch parse/plan/runtime | medium |
| SQL statement parsing | statement kind,DDL/query/write,version contracts | 3 | 65 module branch sites | new verbs add parser branches quickly | medium |
| schema transition admission | transition class,physical work,ownership,version | 4 | 56 module branch sites | new DDL classes multiply admission/publication checks | medium |

## STEP 7 - Complexity Risk Index

Evidence mode: `semi-mechanical`.

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| Variant explosion risk | 5 | 2 | 10 |
| Branching pressure + centralization trend | 6 | 2 | 12 |
| Flow multiplicity | 5 | 2 | 10 |
| Cross-layer spread | 5 | 3 | 15 |
| Authority fragmentation | 5 | 2 | 10 |
| Fanout pressure + super-node load | 3 | 2 | 6 |
| Call-depth pressure | 4 | 1 | 4 |

Overall complexity risk index: **4.8/10** (`67 / 14`), moderate.

## STEP 8 - Trend Interpretation Filter

Evidence mode: `semi-mechanical`.

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| Runtime files | `779 -> 819` (`+40`) | module-split caveat | file growth includes owner-local cleanup splits |
| Runtime LOC | `141,121 -> 168,553` (`+27,432`) | broad growth | treat as pressure, not violation |
| Branch sites | `4,777 -> 5,712` (`+935`) | real pressure | branch hotspot count needs follow-up tracking |
| Fanout hotspots | `0 -> 0` | stable | no hub-sprawl signal under fanout threshold |
| Branch concentration | `0.0808 -> 0.0842` (`+0.0034`) | mild concentration increase | moderate branch centralization pressure |
| Module splits | present | structural improvement where thresholds improved | do not treat all file-count growth as entropy |

## STEP 8A - Complexity Trend Table

Evidence mode: `mechanical` plus `classified`.

Only two CA-1.4 points are available.

| Metric [M/C] | 2026-05-10 | 2026-06-05 |
| ---- | ----: | ----: |
| continuation decision-owner count `[C]` | N/A | 1 |
| continuation execution-consumer count `[C]` | N/A | 2 |
| AccessPath branch-module count `[M]` | N/A | 27 |
| RouteShape branch-module count `[M]` | N/A | 6 |
| branch hotspots (count) `[M]` | 4 | 14 |
| super-node count `[D]` | 0 by fanout | 0 by fanout |
| AccessPath variants `[M]` | N/A | 7 |
| continuation mentions (weak context) `[M]` | N/A | not scored |

## STEP 8B - Invalidating Signals

Evidence mode: `classified`.

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves | yes | 0.178/0.179 schema DDL and runner splits move branch sites | compare owner pressure, not only file count |
| file splits without semantic change | yes | module count rises while root hubs shrink | classify threshold reductions as structural improvement |
| generated code expansion | no evidence | none observed in runtime metrics scope | no adjustment |
| parser/table-driven conversion replacing branch expressions | unknown | branch counts may fall without semantic simplification | do not infer correctness from lower branch count |
| branch consolidation into helper functions | yes | hotspot movement may look like improvement | use issue ledger owner boundaries |

## STEP 9 - Issue Ledger

Evidence mode: `classified`.

| Finding [C] | Anchor Metric [M/D] | Owner Boundary [C] | Trigger Threshold [M/D] | Action [C] | Next Check [M/C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| branch hotspot count rose materially | `14` modules with branch_sites_total >= 40; previous CA-1.4 partial had `4` | runtime owner-local hotspot modules | `branch_sites_total >= 40` | keep new feature work in existing owner-specific modules; split only when branch families are separable by authority | next complexity run |
| metrics sink remains large | `metrics::sink`: `45` branch sites, `2,004` LOC | metrics sink/reporting boundary | branch_sites_total >= 40 and LOC > 2,000 | avoid adding runtime policy decisions to metrics reporting | next complexity run |
| SQL statement parser is the top branch hotspot | `db::sql::parser::statement`: `65` branch sites, `611` LOC | SQL statement parser | branch_sites_total >= 40 | keep new SQL statement families in parser-owned child modules; avoid adding semantic admission logic to statement dispatch | next complexity and flow-convergence runs |
| SQL DDL reconciliation child is a new medium hotspot | `db::schema::reconcile::sql_ddl`: `41` branch sites, `833` LOC | schema-owned SQL DDL publication envelope | branch_sites_total >= 40 | keep SQL DDL gates in this child or split by field/index publication class if it grows | next module-structure and complexity runs |

## Post-Audit Cleanup Applied

The immediate cleanup pass split SQL DDL statement parsing out of the root SQL
statement parser, split SQL DDL field-metadata publication validation out of
the schema-owned SQL DDL reconciliation child, moved schema transition
admission identity/version-fingerprint gating plus generated-extension
compatibility policy into transition child modules, and split range access-choice
per-key constraint classification out of the range evaluator root. The pass
also split persisted schema index/relation integrity checks out of the schema
integrity root. These changes preserve existing runtime behavior while moving
branch pressure into owner-local children.
The post-cleanup metrics artifact is
`docs/audits/reports/2026-06/2026-06-05/artifacts/complexity-accretion/runtime-metrics-after-cleanup.tsv`.

| Area | Before | After | Result |
| ---- | ---- | ---- | ---- |
| `db::sql::parser::statement` | `611` LOC, `65` branch sites | `212` LOC, `28` branch sites | root statement router dropped below the hotspot threshold |
| `db::sql::parser::statement::ddl` | n/a | `410` LOC, `37` branch sites | DDL statement parsing isolated below the hotspot threshold |
| `db::schema::reconcile::sql_ddl` | `833` LOC, `41` branch sites | `371` LOC, `11` branch sites | SQL DDL index/publication envelope dropped below the hotspot threshold |
| `db::schema::reconcile::sql_ddl::field_metadata` | n/a | `479` LOC, `30` branch sites | SQL DDL field metadata validation isolated below the hotspot threshold |
| `db::schema::transition` | `930` LOC, `56` branch sites | `584` LOC, `35` branch sites | transition root dropped below the hotspot threshold |
| `db::schema::transition::admission` | n/a | `161` LOC, `7` branch sites | admission identity/version-fingerprint gate isolated below the hotspot threshold |
| `db::schema::transition::compatibility` | n/a | `208` LOC, `14` branch sites | generated-extension compatibility policy isolated below the hotspot threshold |
| `db::query::plan::access_choice::evaluator::range` | `472` LOC, `49` branch sites | `311` LOC, `28` branch sites | range evaluator root dropped below the hotspot threshold |
| `db::query::plan::access_choice::evaluator::range::constraints` | n/a | `172` LOC, `21` branch sites | per-key range constraint classification isolated below the hotspot threshold |
| `db::schema::integrity` | `415` LOC, `45` branch sites | `189` LOC, `23` branch sites | schema integrity root dropped below the hotspot threshold |
| `db::schema::integrity::index` | n/a | `165` LOC, `16` branch sites | index integrity validation isolated below the hotspot threshold |
| `db::schema::integrity::relation` | n/a | `68` LOC, `6` branch sites | relation integrity validation isolated below the hotspot threshold |

Post-cleanup branch hotspots dropped from `14` in the audit snapshot to `9`.

## Required Summary

0. Run metadata + comparability note
- `CA-1.4` completed on `fc4dc729e` plus local uncommitted audit/cleanup
  changes; mechanical comparison uses the 2026-05-10 CA-1.4 partial baseline.

1. Overall complexity risk index
- overall complexity risk index is `4.8/10`, anchored by `5,712` branch sites,
  `14` branch hotspots, and `0` fanout hotspots.

2. Fastest growing concept families
- branch hotspots rose `4 -> 14`; the strongest current concept pressures are
  predicate AST (`14` variants, multiplier `9,352`), AccessPath (`7` variants,
  `27` branch modules), and schema transition admission (`56` branch sites).

3. Highest branch multipliers
- `Predicate` has multiplier `9,352`; `QueryError` has multiplier `1,476`;
  `AccessPath` has multiplier `784`.

4. Branch distribution drift (`AccessPath` / `RouteShape`)
- `AccessPath` appears across `27` branch/reference modules with `2` owner
  boundaries; `RouteShapeKind` appears across `6` modules with `1` owner.

5. Flow multiplication risks
- load and cursor continuation each have theoretical flow space `28`, reduced
  by explicit constraints to `6` and `5` effective flows.

6. Semantic authority vs execution spread risks
- `AccessPath`, predicate coercion, and index namespace validation each have
  owner count `2`, consumer count `2`, and medium risk.

7. Ownership drift + fanout pressure
- classified prior owners are `N/A`, but fanout pressure remains low:
  `0` modules above `fanout > 12` and top-10 fanout concentration `0.0396`.

8. Super-node + call-depth warnings
- no fanout super-node was found; function pressure is concentrated in
  `scalar_slot_value_ref_into_value` at `35` branch layers.

9. Trend-interpretation filter outcomes
- file count growth `+40` is partly explained by owner-local splits; branch
  growth `+935` and hotspot growth `+10` remain real complexity pressure.

10. Complexity trend table
- only two CA-1.4 points exist, so classified trend deltas are mostly `N/A`;
  mechanical branch hotspot growth is the strongest comparable signal.

11. Verification readout
- `PASS`: canonical runtime metrics, artifact generation, and report tables
  completed.

12. Issue ledger summary
- follow-up pressure centers are branch hotspot count,
  `db::sql::parser::statement`, `metrics::sink`, and
  `db::schema::reconcile::sql_ddl`.

## Verification Readout

- `PASS`: canonical runtime metrics generated.
- `PASS`: required CA-1.4 artifact set generated.
- `PASS`: report generated with baseline and invalidating-signal caveats.
