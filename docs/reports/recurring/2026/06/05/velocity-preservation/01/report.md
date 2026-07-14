# Velocity Preservation Audit - 2026-06-05

## 1. Run Metadata + Method / Comparability

- scope: `icydb-core` plus facade/adapters touched by the 0.179 cleanup line
- compared baseline report path: `docs/audits/reports/2026-05/2026-05-01/velocity-preservation.md`
- code snapshot identifier: `532840f98`
- method tag/version: `VP-1.0`
- feature-slice selection source: 0.179 patch content commits and `docs/changelog/0.179.md`
- comparability status: `partially comparable`

The previous comparable report used a CI slice-shape shortcut and did not run
the full weekly 3-slice method. This run uses the current recurring audit
definition, so raw deltas against the May baseline are context only.

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | 0.179 patch content ranges, release commits excluded | direct release-head and prior-content slice audit | Partial |
| subsystem taxonomy | fixed recurring taxonomy, facade/adapters included | CI primary-domain classifier | Partial |
| boundary crossing rule set | recurring VP boundary regexes with runtime/test filtering | route-planner import grep | Partial |
| fan-in definition | runtime metric fanout plus source-reference candidates | not measured | No |
| hub-family taxonomy | fixed VP hub family taxonomy | route-planner controlled hub only | Partial |
| independent-axis rule | VP-1.0 independent-axis table | not measured | No |
| facade/adapters inclusion | included because 0.179.2 touched CLI and public facade modules | included only when slice touched facade/adapters | Partial |

## 2. Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | high slice / low release commit | `5.0/10` | N/A |
| Cross-layer suspect-review candidates | N/A | `28` | N/A |
| Avg files touched per feature slice | `84` prior content slice | `75.67` | N/A |
| Median files touched | N/A | `54` | N/A |
| p95 files touched | N/A | `163` | N/A |
| Top gravity-well fanout | N/A | `7` | N/A |
| Route-planner high-impact cross-layer families | `0` direct SQL/session leakage | `1` contract family | N/A |
| Edit concentration in top 5 modules (%) | N/A | `58.1%` | N/A |
| Fan-in concentration in top 5 modules (%) | N/A | N/A | N/A |
| Decision-site concentration in top 3 enums (%) | N/A | high mechanical concentration in core scalar/schema enums | N/A |

Artifacts:

- `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/feature-slices.tsv`
- `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/feature-files.tsv`
- `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/edit-concentration.tsv`
- `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/runtime-metrics.tsv`

## 3. Feature Slice Selection

| Feature Slice | Source Type | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| 0.179.0 dry cleanup foundation | manual/changelog | `41f89f5c9..0551df09e` | DRY audit follow-through and schema/relation/SQL cleanup | release metadata commit |
| 0.179.1 structure and complexity cleanup | manual/changelog | `fc4dc729e..6a18baf24` | module-structure and complexity-accretion cleanup | release metadata commit |
| 0.179.2 audit cleanup and hub split | manual/changelog | `200ad67ca..afe313e03` | CLI/core hub split, test relocation, lint expectation cleanup | release metadata commit |

## 4. Empirical Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| 0.179.0 dry cleanup foundation | `10` | `2` | `1` | `2` | `2` | `4` | `0.500` | `0.333` | Low |
| 0.179.1 structure and complexity cleanup | `54` | `4` | `3` | `4` | `4` | `20` | `0.463` | `0.667` | High raw / Medium adjusted |
| 0.179.2 audit cleanup and hub split | `163` | `5` | `4` | `5` | `5` | `30` | `0.325` | `0.833` | High raw / Medium adjusted |

The 0.179.1 and 0.179.2 CAF values are high because the selected slices are
cleanup/meta slices that deliberately touch many owners. The adjusted
interpretation is lower than the raw CAF because the edits are mostly
owner-local module extraction, test relocation, and lint-suppression tightening
rather than new feature axes that must evolve together.

## 5. Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| patch content ranges | 0.179 patch content commits | `3` | `0.179.0`, `0.179.1`, `0.179.2` | Partial |

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `75.67` | N/A | N/A |
| median files touched | `54` | N/A | N/A |
| p95 files touched | `163` | N/A | N/A |

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules | `58.1%` | N/A | N/A | Medium |
| % of fan-in in top 5 modules | N/A | N/A | N/A | Unmeasured |
| % of decision sites in top 3 enums | high mechanical concentration | N/A | N/A | Medium |

The blast-radius SLO gates are missed in raw file-count terms. The miss is
structural-cleanup noise rather than routine feature work, but future cleanup
slices should still avoid repeating a 100+ file review unit unless the unit is
almost entirely mechanical test/module relocation.

## 6. Boundary Leakage

Artifact: `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/boundary-leakage-candidates.tsv`

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect-Review Crossings | Previous Suspect | Delta | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/sql -> executor | `1` | `0` | `1` | N/A | N/A | Low |
| executor -> query/sql | `37` | `9` | `28` | N/A | N/A | Medium |
| index -> query/sql | `0` | `0` | `0` | N/A | N/A | Low |
| cursor -> plan internals | `12` | `12` | `0` | N/A | N/A | Low |
| recovery -> query semantics | `0` | `0` | `0` | N/A | N/A | Low |

The one planner/sql crossing is a documentation comment, not runtime coupling.
The executor-to-query/sql candidates are the real velocity pressure: executor
projection, aggregate, prepared-plan, and metrics code still consume query-plan
types directly. Most are intentional handoff contracts, but the surface is wide
enough to keep feature changes sensitive to query-plan DTO movement.

## 7. Gravity Wells + Hub Containment

Artifact: `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/gravity-wells.tsv`

| Module | Class | LOC | LOC Delta | Fanout | Fanout Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `db::executor::mutation::save_validation` | stable candidate | `706` | N/A | `7` | N/A | `1` | N/A | Low |
| `db::session::sql::execute::write::insert` | stable candidate | `393` | N/A | `7` | N/A | `1` | N/A | Low |
| `traits` | stable candidate | `966` | N/A | `6` | N/A | `1` | N/A | Medium |
| `db::index::key::build` | stable candidate | `1142` | N/A | `5` | N/A | `1` | N/A | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` | planner -> route shape -> executor dispatch | `1` | N/A | N/A | `1` | PASS | Low |
| `crates/icydb-core/src/db/session/query/paging.rs` | session query paging boundary | `3` | N/A | N/A | `1` | REVIEW | Medium |

The route planner hub is well contained after prior cleanup. The active hub
pressure has shifted to session query paging and executor/query-plan handoff
contracts, not route-planner leakage.

## 8. Enum Shock Radius

Artifact: `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/enum-shock-radius.tsv`

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `PersistedFieldKind` | `32` | `670` | `30` | `22.33` | `4` | `2858.7` | Medium |
| `FieldKind` | `32` | `792` | `62` | `12.77` | `5` | `2043.9` | Medium |
| `Keyword` | `59` | `246` | `25` | `9.84` | `2` | `1161.1` | Medium |
| `ExplainExecutionNodeType` | `33` | `84` | `8` | `10.50` | `3` | `1039.5` | Medium |
| `Value` | `24` | `2163` | `398` | `5.43` | `7` | `913.0` | Medium |

The enum shock surface is broad but unsurprising: scalar/schema/value enums are
the durable semantic axes. No new critical enum family was introduced by the
0.179 cleanup line, but future scalar/schema additions should expect wide
decision-surface updates.

## 9. Subsystem Independence

Artifact: `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/subsystem-independence.tsv`

| Subsystem | Internal Imports | External Imports | LOC | Independence | Adjusted Independence | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/query | `848` | `16` | `48337` | `0.981` | `10.59` | Low |
| executor/runtime | `1860` | `124` | `53603` | `0.938` | `10.21` | Low |
| cursor/continuation | `54` | `4` | `2711` | `0.931` | `7.36` | Low |
| access/index | `31` | `4` | `7493` | `0.886` | `7.90` | Low |
| storage/recovery | `674` | `4` | `38132` | `0.994` | `10.49` | Low |

The independence signal is healthy. Executor/runtime has the largest external
import count, which matches the boundary-leakage finding: execution owns many
handoffs from query-plan contracts.

## 10. Independent-Axis Growth

Artifact: `docs/audits/reports/2026-06/2026-06-05/artifacts/velocity-preservation/decision-axis-growth.tsv`

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| SQL DDL publication | schema version, fingerprint identity, mutation class, runner capability, publication identity | `5` | `4` | N/A | N/A | Medium |
| SQL query compile/execute | surface, accepted schema identity, cache attribution, statement family | `4` | `3` | N/A | N/A | Low |
| CLI reporting | command group, target resolution, render surface, schema-check domain | `4` | `3` | N/A | N/A | Low |
| Metrics sink | event family, cache kind, outcome, report rendering | `4` | `3` | N/A | N/A | Low |

## 11. Decision Surface Size

| Enum | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `PersistedFieldKind` / `FieldKind` | high, scalar/schema-wide | N/A | N/A | Medium |
| `Value` | high, value/runtime-wide | N/A | N/A | Medium |
| `Keyword` | moderate, parser-focused | N/A | N/A | Medium |
| `SqlDdlBindError` | moderate, DDL diagnostics-focused | N/A | N/A | Low-Medium |

Decision size is manageable as long as additions land through schema/value
owners first and do not duplicate semantic decisions in executor or facade code.

## 12. Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| 0.179.2 file count | `163` files | structural cleanup transient | broad but mostly test/module relocation and hub split |
| revised CAF | high on `.1` and `.2` | cleanup-slice amplification | not representative of ordinary feature extension cost |
| executor/query-plan crossings | medium candidate count | real recurring handoff pressure | keep contracts explicit; avoid widening direct plan-type dependencies |
| route-planner hub | contained | structural improvement | prior route-planner cleanup held |

## 13. Velocity Risk Index

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| enum shock radius | `5` | `2` | `10` |
| CAF trend | `6` | `2` | `12` |
| cross-layer leakage | `5` | `2` | `10` |
| gravity-well growth/stability | `3` | `2` | `6` |
| hub contract containment | `5` | `2` | `10` |
| edit blast radius | `6` | `2` | `12` |

Overall velocity risk index: `5.0/10`.

Interpretation: moderate and manageable. The current risk is dominated by
wide cleanup slices and executor/query-plan handoff breadth, not by route
planner leakage, subsystem independence failure, or new feature-axis
multiplication.

Follow-up actions:

- owner boundary: `db::executor` query-plan handoff contracts; action: when
  touching projection, aggregate, or prepared-plan execution, prefer executor
  contract DTOs or existing pipeline contract modules over adding new direct
  `db::query::plan` dependencies; target report date/run: next
  `crosscutting-velocity-preservation` run.
- owner boundary: cleanup slice governance; action: keep future module/test
  relocation slices split by owner when possible, especially if raw file count
  would exceed `50`; target report date/run: next 0.179 cleanup slice.

## 14. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| method comparability status recorded | PASS | marked partially comparable against May baseline |
| mandatory steps/tables present | PASS | all required VP sections included |
| SLO gates evaluated | PASS | median `54`, p95 `163`, both raw gates missed with refactor-noise adjustment |
| runtime metrics generated | PASS | `scripts/audit/runtime_metrics.sh` wrote `runtime-metrics.tsv` |
| boundary scans completed | PASS | `rg` scans recorded in `boundary-leakage-candidates.tsv` |
| full tests | BLOCKED | not required for this audit; no runtime behavior changed |
