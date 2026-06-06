# Velocity Preservation Audit - 2026-06-06

## 1. Run Metadata + Method / Comparability

- scope: `icydb-core` plus facade/adapters touched by the 0.179 cleanup line and the current unpushed worktree.
- compared baseline report path: `docs/audits/reports/2026-06/2026-06-05/velocity-preservation.md`
- code snapshot identifier: `c373182f3` with dirty working tree at scan time
- method tag/version: `VP-1.0`
- feature-slice selection source: 0.179 patch content ranges, `docs/changelog/0.179.md`, and current dirty-worktree candidate slice
- comparability status: `comparable`, snapshot-qualified because the current dirty tree includes user work outside this audit cleanup

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | 0.179 patch content ranges plus current dirty candidate slice | 0.179 patch content ranges | Yes |
| subsystem taxonomy | fixed recurring taxonomy, facade/adapters included | fixed recurring taxonomy, facade/adapters included | Yes |
| boundary crossing rule set | recurring VP boundary regexes with runtime/test filtering | recurring VP boundary regexes with runtime/test filtering | Yes |
| fan-in definition | runtime metric fanout plus source-reference candidates | runtime metric fanout plus source-reference candidates | Yes |
| hub-family taxonomy | fixed VP hub family taxonomy | fixed VP hub family taxonomy | Yes |
| independent-axis rule | VP-1.0 independent-axis table | VP-1.0 independent-axis table | Yes |
| facade/adapters inclusion | included when slice touched facade/adapters | included when slice touched facade/adapters | Yes |

## 2. Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | `5.0/10` | `4.7/10` | `-0.3` |
| Cross-layer suspect-review candidates | `28` | `10` | `-18` |
| Avg files touched per feature slice | `75.67` | `94.25` | `+18.58` |
| Median files touched | `54` | `75` | `+21` |
| p95 files touched | `163` | `166` | `+3` |
| Top gravity-well fanout | `7` | `7` | `0` |
| Route-planner high-impact cross-layer families | `1` | `1` | `0` |
| Edit concentration in top 5 module/path families (%) | `58.1%` | `88.7%` for current dirty slice | `+30.6` |
| Fan-in concentration in top 5 modules (%) | N/A | N/A | N/A |
| Decision-site concentration in top 3 enums (%) | high mechanical concentration | high mechanical concentration | stable |

The velocity score improves despite raw file-count pressure because the prior
executor/query-plan handoff follow-up landed: projection, prepared-plan, and
aggregate production imports now concentrate in executor-owned contract modules.
The raw blast-radius metrics remain high because this cleanup line includes
audit artifacts, module relocation, generated fixture churn, and macro test
surface updates rather than ordinary feature slices.

## 3. Feature Slice Selection

| Feature Slice | Source Type | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| 0.179.1 structure and complexity cleanup | commits/changelog | `41f89f5c9..200ad67ca` | module-structure and complexity cleanup follow-through | release tag metadata interpretation only |
| 0.179.2 audit cleanup and hub split | commits/changelog | `200ad67ca..532840f98` | CLI/core hub split, test relocation, lint expectation cleanup | release tag metadata interpretation only |
| 0.179.3 velocity cleanup | commits/changelog | `532840f98..c373182f3` | executor/query-plan handoff and session paging follow-through | release tag metadata interpretation only |
| current 0.179 candidate dirty slice | worktree | `git diff --name-only` at `c373182f3` | current cleanup/version-key/macro-fixture work in progress | untracked audit report files excluded from file-count metrics |

## 4. Empirical Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| 0.179.1 structure and complexity cleanup | `61` | `5` | `4` | `5` | `4` | `20` | `0.48` | `0.71` | High raw / Medium adjusted |
| 0.179.2 audit cleanup and hub split | `166` | `6` | `4` | `5` | `5` | `30` | `0.60` | `0.86` | High raw / Medium adjusted |
| 0.179.3 velocity cleanup | `79` | `3` | `3` | `3` | `3` | `9` | `0.78` | `0.43` | Medium raw / Low adjusted |
| current dirty candidate slice | `71` tracked files | `4` | `2` | `3` | `2` | `8` | `0.56` | `0.57` | Medium |

The current dirty candidate slice is broad mostly because generated fixtures,
macro-test UI files, and schema examples move together. It should remain a
review-governance watch item, but it is not evidence of core planner/executor/
storage feature amplification.

## 5. Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| patch content ranges plus dirty candidate | 0.179 patch content commits and current worktree | `4` | `0.179.1`, `0.179.2`, `0.179.3`, current candidate | Yes, snapshot-qualified |

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `94.25` | `75.67` | `+18.58` |
| median files touched | `75` | `54` | `+21` |
| p95 files touched | `166` | `163` | `+3` |

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of current dirty edits in top 5 path families | `88.7%` | `58.1%` slice-edit concentration | `+30.6` | Medium |
| % of fan-in in top 5 modules | N/A | N/A | N/A | Unmeasured |
| % of decision sites in top 3 enums | high mechanical concentration | high mechanical concentration | stable | Medium |

The raw file-count SLOs remain missed. For this run, the missed SLO is caused
by deliberate audit/cleanup and generated fixture work; future ordinary feature
slices should still stay below the median `<= 8`, p95 `<= 15` expectations.

## 6. Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Previous Suspect | Delta | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner/sql -> executor | `1` | `1` | `0` | `1` | `-1` | Low |
| executor -> query/sql | `19` | `10` | `9` | `28` | `-19` | Low-Medium |
| index -> query/sql | `0` | `0` | `0` | `0` | `0` | Low |
| cursor -> plan internals | delegating references only | delegating references only | `0` | `0` | `0` | Low |
| recovery -> query semantics | `0` | `0` | `0` | `0` | `0` | Low |

The executor/query-plan handoff cleanup materially reduced the suspect surface.
Remaining production references are mostly explain/metrics descriptors,
pipeline contracts, continuation route contracts, and route observability.
Projection, prepared-plan, and aggregate handoffs are now concentrated in
executor-owned contract modules.

## 7. Gravity Wells + Hub Containment

| Module | Class | LOC | Fanout | Domains | Risk |
| ---- | ---- | ----: | ----: | ----: | ---- |
| `db::session::sql::execute` | monitored hub | `854` | `4` | `1` | Low-Medium |
| `db::relation::reverse_index` | monitored hub | `1041` | `5` | `1` | Medium |
| `db::session::sql::execute::write::insert` | stable candidate | `393` | `7` | `1` | Low-Medium |
| `db::executor::mutation::save_validation` | stable candidate | `706` | `7` | `1` | Low-Medium |
| `db::index::key::build` | stable candidate | `1142` | `5` | `1` | Medium |

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` | planner -> route shape -> executor dispatch | `1` | `1` | `0` | `1` | PASS | Low |
| `crates/icydb-core/src/db/session/query/paging/*` | session query paging boundary | split scalar/grouped children | `3` family root pressure | improved | `1` per child owner | PASS | Low |
| `crates/icydb-core/src/db/session/sql/execute/*` | SQL execution shell | diagnostics split from root | root `905` LOC before cleanup | root `-51` LOC | threshold `950` | PASS | Low-Medium |

## 8. Enum Shock Radius

| Enum | Variants / Surface | Switch Sites | Modules Using Enum | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- |
| `PersistedFieldKind` / `FieldKind` | schema/scalar-wide | high | high | high | Medium |
| `Value` | runtime value-wide | high | high | high | Medium |
| `Keyword` | parser-focused | moderate | moderate | moderate | Medium |
| SQL/schema-version declaration key | macro/schema surface | fixture-heavy | broad generated tests | low semantic, high fixture count | Medium |

No new critical enum-like decision family was introduced. The current dirty
version-key work is broad in generated fixtures but does not appear to create a
new runtime decision axis.

## 9. Subsystem Independence

| Subsystem | Independence Signal | Current Assessment | Risk |
| ---- | ---- | ---- | ---- |
| planner/query | no upward executor import found in layer guard | healthy | Low |
| executor/runtime | query-plan references now mostly owner contract modules | improved but still broad handoff owner | Low-Medium |
| cursor/continuation | validation remains cursor/query-plan delegated | healthy | Low |
| access/index | no query/sql import leakage found | healthy | Low |
| storage/recovery | no query semantic import leakage found | healthy | Low |
| facade/adapters | current dirty fixture/macro surface is broad | review-size pressure, not semantic coupling | Medium |

## 10. Independent-Axis Growth

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| SQL DDL publication | schema version, fingerprint identity, mutation class, runner capability, publication identity | `5` | `4` | `4` | `0` | Medium |
| SQL query compile/execute | surface, accepted schema identity, cache attribution, statement family, diagnostics attribution | `5` | `3` | `3` | `0` material | Low |
| generated schema declaration version key | macro parse key, fixture declarations, UI diagnostics | `3` | `2` | N/A | N/A | Medium |
| metrics/diagnostics entrypoints | owner visibility, report rendering, instrumentation | `3` | `2` | `3` | `-1` | Low |

The SQL diagnostics split reduces structural pressure without adding an
execution axis: attribution is now a child concern under the same SQL execution
owner.

## 11. Decision Surface Size

| Enum / Decision Surface | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `PersistedFieldKind` / `FieldKind` | high, scalar/schema-wide | high | stable | Medium |
| `Value` | high, value/runtime-wide | high | stable | Medium |
| `Keyword` | moderate, parser-focused | moderate | stable | Medium |
| generated entity declaration version key | broad fixtures/UI expectations | N/A | N/A | Medium |

## 12. Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| 0.179.2 file count | `166` files | structural cleanup transient | broad but mostly owner-local CLI/core/test relocation |
| 0.179.3 file count | `79` files | owner-contract cleanup | real velocity improvement despite moderate file count |
| current dirty file count | `71` tracked files | generated fixture and macro-test churn | review-size pressure; not a core semantic-boundary failure |
| executor/query-plan crossings | down from `28` suspect to `9` suspect | real improvement | prior velocity follow-up worked |

## 13. Velocity Risk Index

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| enum shock radius | `5` | `2` | `10` |
| CAF trend | `6` | `2` | `12` |
| cross-layer leakage | `3` | `2` | `6` |
| gravity-well growth/stability | `4` | `2` | `8` |
| hub contract containment | `4` | `2` | `8` |
| edit blast radius | `6` | `2` | `12` |

Overall velocity risk index: `4.7/10`.

Interpretation: moderate and improved. The biggest remaining velocity risk is
review-size governance for cleanup/generated-fixture slices, while the prior
executor/query-plan handoff pressure has improved enough to drop out of the
highest follow-up tier.

Follow-up actions:

- owner boundary: current generated schema declaration/version-key slice;
  action: keep it as one intentional fixture/UI update if it is all mechanical,
  otherwise split semantic parser changes from fixture rewrites before push;
  target report date/run: next 0.179 cleanup slice.
- owner boundary: `db::relation::reverse_index`; action: monitor size only and
  split when a real child owner appears, not for line count alone; target report
  date/run: next module-structure/velocity run.

## 14. Verification Readout

| Check | Status | Notes |
| ---- | ---- | ---- |
| method comparability status recorded | PASS | comparable, snapshot-qualified for dirty worktree |
| mandatory steps/tables present | PASS | all required VP sections included |
| SLO gates evaluated | PASS | median `75`, p95 `166`, both raw gates missed with refactor-noise adjustment |
| runtime metrics generated | PASS | `scripts/audit/runtime_metrics.sh` used for current SQL execution metrics |
| boundary scans completed | PASS | executor/query-plan crossings reduced to `19` mechanical / `9` suspect |
| route planner boundary | PASS | `check-route-planner-import-boundary.sh` passed |
| structure/layer guards | PASS | module thresholds, layer authority, and architecture text-scan passed in this cleanup run |
| compile checks | PASS | `cargo check -p icydb-core --features sql` and `cargo check -p icydb-core --features 'sql diagnostics'` passed in this cleanup run |
