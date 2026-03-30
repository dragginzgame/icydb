# Velocity Preservation Audit - 2026-03-30

## Report Preamble

- scope: feature agility and cross-layer amplification risk in the shipped `0.66.x` line
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/velocity-preservation.md`
- code snapshot identifier: `12fdfa03`
- method tag/version: `Method V4`
- comparability status: `non-comparable`
  - the `2026-03-24` report used the older loose `Method V3` summary format and did not record the same feature-slice, blast-radius, or hub-containment tables
  - this run should be treated as the new structured velocity baseline for the `0.66.x` line

## STEP 0 - Run Metadata + Method / Comparability

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | changelog-guided release slices with manual file filtering for code-bearing files | loose recent-change summary | No |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | not recorded | No |
| boundary crossing rule set | invariant scripts plus route/load hub import review | invariant scripts plus qualitative hub note | Partial |
| fan-in definition | manual hub pressure proxy using import families and 30d edit frequency | not recorded | No |
| hub-family taxonomy | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | partial narrative only | No |
| independent-axis rule | slice-local axis count using distinct semantic change families | not recorded | No |
| facade/adapters inclusion | included for canister/bootstrap/generated-surface slices; docs excluded from subsystem counts | implicit | Partial |

## STEP 1 - Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | `4.0` | `5.2` | `+1.2` |
| Cross-layer suspect crossings | `0` (qualitative) | `0` | `0` |
| Avg files touched per feature slice | `N/A` | `28.7` | `N/A` |
| Median files touched | `N/A` | `31` | `N/A` |
| p95 files touched | `N/A` | `34` | `N/A` |
| Top gravity-well fan-in | `route planner (qualitative)` | `route planner (stable)` | `stable` |
| Route-planner high-impact cross-layer families | `2` | `2` | `0` |
| Edit concentration in top 5 modules (%) | `N/A` | `72.1` | `N/A` |
| Fan-in concentration in top 5 modules (%) | `N/A` | `N/A` | `N/A` |
| Decision-site concentration in top 3 enums (%) | `N/A` | `97.7` (mechanical upper bound) | `N/A` |

## STEP 2 - Feature Slice Selection

| Feature Slice | Source Type (`PR`/`tracker`/`commits`/`manual`) | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| `0.66.1` text semantics session lane | `commits` | `v0.66.0..c757f12f` | first shipped text-function SQL lane plus session-SQL decomposition | root docs/readme/design text excluded from locality counts |
| `0.66.2` Canic bootstrap alignment | `manual` | `docs/changelog/0.66.md` + filtered `v0.66.1..bdf58cd9` bootstrap/canister/schema files | runtime/bootstrap integration is the main `0.66.2` shipped objective | parser cleanup, compile-fail fixture, and complexity-report refresh excluded |
| `0.66.3` parser and Canic facade cleanup | `manual` | `docs/changelog/0.66.md` + filtered `v0.66.1..bdf58cd9` parser/import/audit files | cleanup patch directly affects extension cost and release maintenance | unrelated schema-range/bootstrap files excluded |

## STEP 3 - Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.66.1` text semantics session lane | `34` | `4` | `5` | `4` | `3` | `15` | `0.56` | `0.67` | High |
| `0.66.2` Canic bootstrap alignment | `21` | `2` | `2` | `3` | `2` | `4` | `0.90` | `0.33` | Medium-Low |
| `0.66.3` parser and Canic facade cleanup | `31` | `3` | `3` | `3` | `2` | `6` | `0.55` | `0.50` | Medium |

Interpretation:

- `0.66.1` was the expensive slice: it crossed parser/lowering/session/facade/testing boundaries and then paid the expected decomposition cost to keep the new lane contained.
- `0.66.2` stayed cleanly localized around generated bootstrap, canister wiring, and schema fixture ranges.
- `0.66.3` was a cleanup patch, but it still touched many files because the `canic::cdk` import migration and parser hotspot split were both cross-cutting maintenance work.

## STEP 4 - Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| release-slice sample | `0.66.x` shipped slices | `3` | `0.66.1`, `0.66.2`, `0.66.3` | No |

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `28.7` | `N/A` | `N/A` |
| median files touched | `31` | `N/A` | `N/A` |
| p95 files touched | `34` | `N/A` | `N/A` |

SLO evaluation:

- median files touched `<= 8`: `FAIL`
- p95 files touched `<= 15`: `FAIL`

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules | `72.1` | `N/A` | `N/A` | High |
| % of fan-in in top 5 modules | `N/A` | `N/A` | `N/A` | Baseline not yet normalized |
| % of decision sites in top 3 enums | `97.7` | `N/A` | `N/A` | Medium |

Top 5 edit buckets in this run:

- `canisters/*`: `18`
- `db/session/sql/*`: `16`
- `db/core-other`: `14`
- `core-other`: `8`
- `schema/*`: `6`

## STEP 5 - Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Previous Suspect | Delta | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| planner -> executor types | `present` | `present` | `0` | `0` | `0` | Medium-Low |
| executor -> planner validation helpers | `present` | `present` | `0` | `0` | `0` | Medium-Low |
| index -> query-layer AST/types | `present` | `present` | `0` | `0` | `0` | Low |
| cursor -> executable plan internals | `present` | `present` | `0` | `0` | `0` | Low |
| recovery -> query semantics | `present` | `present` | `0` | `0` | `0` | Low |

Readout:

- `check-layer-authority-invariants.sh` stayed green.
- `check-architecture-text-scan-invariants.sh` stayed green.
- No new suspect boundary crossings were needed to ship the `0.66.x` slices.

## STEP 6 - Gravity Wells + Hub Containment

### Hub Pressure

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `executor/route/planner/mod.rs` | route planner consumes access-route contracts and emits route execution shape | `2` | `2` | `0` | `1` | Over | Medium |
| `executor/pipeline/orchestrator/mod.rs` | load-surface runtime orchestration only | `1` | `1` | `0` | `1` | Within | Low |

### Stable Gravity Wells

| Module | Class | LOC | Edit Frequency (30d) | Domains | Risk |
| ---- | ---- | ----: | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | stable gravity well | `109` | `34` | `3` | Medium |
| `db/sql/parser/mod.rs` | cleanup target under active containment | `793` | `9` | `2` | Medium |
| `db/session/sql/dispatch/mod.rs` | contained dispatch hub | `194` | `2` | `2` | Medium-Low |

Interpretation:

- The route planner is still the one live velocity hotspot that crosses more than one high-impact family.
- The load hub remains healthier after the earlier orchestrator split and did not re-accumulate mixed policy responsibilities in the `0.66.x` line.
- Parser and session-SQL pressure showed up as size/change hotspots, but both were actively decomposed rather than left to accumulate inside one monolith.

## STEP 7 - Enum Shock Radius

Mechanical upper-bound scan:

| Enum | Variants | Switch/Variant Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `AccessPath` / `AccessPathKind` | `7` | `378` | `25` | `15.12` | `2` | `211.7` | High |
| `AggregateKind` | `8` | `328` | `33` | `9.94` | `4` | `318.3` | High |
| `RouteShapeKind` | `5` | `19` | `7` | `2.71` | `3` | `40.7` | Medium |
| `ContinuationMode` | `3` | `17` | `4` | `4.25` | `1` | `12.8` | Low |

Interpretation:

- `AggregateKind` and `AccessPath` still dominate change-relevant branch pressure.
- `RouteShapeKind` remains intentionally smaller; the live route budget test is still keeping that shock radius bounded.

## STEP 8 - Subsystem Independence

| Subsystem | Qualitative Independence | Risk |
| ---- | ---- | ---- |
| planner/query | moderate; still depends on executor-facing route/access contracts at feature edges | Medium |
| executor/runtime | moderate-high; large surface but mostly owner-local once plans are prepared | Medium-Low |
| access/index | high; boundary stayed contract-driven in this run | Low |
| storage/recovery | high; no new release slice pulled recovery semantics into feature work | Low |
| facade/adapters | moderate; Canic integration changes are still wide when public facade contracts move | Medium |

## STEP 9 - Decision-Axis Growth

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| reduced SQL dispatch growth | parser surface, lowering gate, session dispatch lane, explain parity | `4` | `3` | `N/A` | `N/A` | Medium |
| canister bootstrap integration | runtime bootstrap API, export-candid macro surface, reserved memory ranges | `3` | `3` | `N/A` | `N/A` | Medium-Low |
| parser cleanup and facade cleanup | parser split, Canic facade import cleanup, compile-fail/changelog alignment | `3` | `2` | `N/A` | `N/A` | Medium-Low |

## STEP 10 - Decision Surface Size

| Enum | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AggregateKind` | `18` | `N/A` | `N/A` | Medium |
| `AccessPath` / `AccessPathKind` | `14` | `N/A` | `N/A` | Medium |
| `RouteShapeKind` | `7` | `N/A` | `N/A` | Medium-Low |

These counts are final triaged counts rather than raw mechanical matches. They reflect the places most likely to need intentional edits when variants grow.

## STEP 11 - Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| `0.66.1` touched `34` code-bearing files | high blast radius | feature + containment refactor | expensive slice, but much of the cost bought down future session-SQL change cost |
| parser cleanup touched many files in `0.66.3` | broad cleanup patch | structural improvement | hotspot reduction matters more than file-count breadth here |
| Canic integration patch touched every shipped canister harness | wide facade touch set | expected facade blast | this is adapter breadth, not core semantic sprawl |

## STEP 12 - Velocity Risk Index

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| enum shock radius | `5` | `2` | `10` |
| CAF trend | `6` | `2` | `12` |
| cross-layer leakage (suspect) | `2` | `2` | `4` |
| gravity-well growth/stability | `5` | `2` | `10` |
| hub contract containment | `5` | `2` | `10` |
| edit blast radius (SLO-based) | `8` | `2` | `16` |

`overall_index = 62 / 12 = 5.2`

Interpretation: moderate risk and manageable pressure. The main drag is not boundary breakage; it is that even contained `0.66.x` work still landed as large release slices. The route planner remains the main stable hotspot, and the `0.66.1` SQL lane shows that new feature work still becomes expensive when parser, lowering, session dispatch, canister wiring, and integration tests all move together.

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority boundaries remain intact | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Route-shape feature-budget guard executes in the live route owner boundary | `cargo test -p icydb-core db::executor::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` | PASS | Medium |

## Follow-Up Actions

- No immediate structural blocker is exposed by this run.
- Monitoring-only: keep `executor/route/planner/mod.rs` from gaining a third high-impact cross-layer family.
- Monitoring-only: if the next comparable run still has median files touched above `8`, split future SQL feature work into smaller landed slices rather than carrying combined parser/lowering/session/facade bundles.

## Verification Readout

- method comparability status: `non-comparable` against `2026-03-24` because the earlier report did not record the same structured slice metrics
- all mandatory velocity sections for this run are present
- SLO gates were evaluated against the current sample and both blast-radius gates failed
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo test -p icydb-core db::executor::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
