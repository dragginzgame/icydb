# Velocity Preservation Audit - 2026-04-22

## Report Preamble

- scope: feature agility and cross-layer amplification risk in the shipped `0.110` / `0.111` / `0.112` slices
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-17/velocity-preservation.md`
- code snapshot identifier: `b43bba078` (`dirty` working tree)
- method tag/version: `Method V4`
- comparability status: `comparable`

## STEP 0 - Run Metadata + Method / Comparability

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | changelog-guided release slices with manual file filtering for code-bearing files | changelog-guided release slices with manual file filtering for code-bearing files | Yes |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | Yes |
| boundary crossing rule set | invariant scripts plus route/load hub import review | invariant scripts plus route/load hub import review | Yes |
| fan-in definition | manual hub pressure proxy plus coarse runtime-module reference upper bound, tests/generated excluded | manual hub pressure proxy plus coarse runtime-module reference upper bound, tests/generated excluded | Yes |
| hub-family taxonomy | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | Yes |
| independent-axis rule | slice-local axis count using distinct semantic change families | slice-local axis count using distinct semantic change families | Yes |
| facade/adapters inclusion | included only when shipped slices actually touched session/canister/bootstrap/generated surfaces; docs excluded from subsystem counts | included only when shipped slices actually touched canister/bootstrap/generated surfaces; docs excluded from subsystem counts | Yes |

## STEP 1 - Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | `5.5` | `4.7` | `-0.8` |
| Cross-layer suspect crossings | `0` | `0` | `0` |
| Avg files touched per feature slice | `32.7` | `10.3` | `-22.4` |
| Median files touched | `27` | `10` | `-17` |
| p95 files touched | `62` | `14` | `-48` |
| Top gravity-well fan-in | `types::decimal (69-module upper bound)` | `route planner (11-module coarse upper bound); prepared files are hotter by edit frequency, not by breadth` | `shifted` |
| Route-planner high-impact cross-layer families | `1` | `1` | `0` |
| Edit concentration in top 5 modules (%) | `88.8` | `83.9` | `-4.9` |
| Fan-in concentration in top 5 modules (%) | `N/A` | `N/A` | `N/A` |
| Decision-site concentration in top 3 enums (%) | `96.5` | `96.8` | `+0.3` |

## STEP 2 - Feature Slice Selection

| Feature Slice | Source Type (`PR`/`tracker`/`commits`/`manual`) | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| `0.110.0` grouped semantic alignment | `manual` | `docs/changelog/0.110.md` + filtered `v0.109.2..v0.110.0` | first shipped grouped semantic canonicalization slice in the current line | docs, changelog, cargo metadata excluded from locality counts |
| `0.111.0` grouped omitted-`ELSE` follow-through | `manual` | `docs/changelog/0.111.md` + filtered `v0.110.0..v0.111.0` | captures the shipped follow-through and identity hardening slice after `0.110.0` | docs, changelog, cargo metadata excluded from locality counts |
| `0.112.0` prepared fallback consolidation | `manual` | `docs/changelog/0.112.md` + filtered `v0.111.0..v0.112.0` | captures the shipped prepared-lane contraction slice before the new `0.114` authority-collapse line | docs, changelog, cargo metadata excluded from locality counts |

## STEP 3 - Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.110.0` grouped semantic alignment | `14` | `2` | `2` | `3` | `2` | `4` | `0.79` | `0.33` | Medium-Low |
| `0.111.0` grouped omitted-`ELSE` follow-through | `10` | `3` | `4` | `4` | `3` | `12` | `0.60` | `0.50` | Medium |
| `0.112.0` prepared fallback consolidation | `7` | `2` | `3` | `4` | `3` | `9` | `0.57` | `0.33` | Medium-Low |

Interpretation:

- `0.110.0` is a healthy contained slice: grouped semantic ownership moved across planner/query plus session-facing parity without dragging executor, access, or storage layers into the change.
- `0.111.0` is the widest slice in this sample, but it is still far tighter than the April 17 baseline outlier. The breadth came from identity and explain follow-through, not from a broad route/load or recovery reopening.
- `0.112.0` is the current healthy contraction shape: small file count, contained subsystem spread, and no new surface widening.

## STEP 4 - Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| release-slice sample | shipped `0.110.0`, `0.111.0`, `0.112.0` slices | `3` | `0.110.0`, `0.111.0`, `0.112.0` | Yes |

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `10.3` | `32.7` | `-22.4` |
| median files touched | `10` | `27` | `-17` |
| p95 files touched | `14` | `62` | `-48` |

SLO evaluation:

- median files touched `<= 8`: `FAIL`
- p95 files touched `<= 15`: `PASS`

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules | `83.9` | `88.8` | `-4.9` | Medium-High |
| % of fan-in in top 5 modules | `N/A` | `N/A` | `N/A` | Baseline not yet normalized |
| % of decision sites in top 3 enums | `96.8` | `96.5` | `+0.3` | Medium |

Top 5 edit buckets in this run:

- `db/query/*`: `10`
- `db/session/*`: `7`
- `db/sql/*`: `6`
- `db/predicate/*`: `2`
- `db/executor/*`: `1`

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
- `check-route-planner-import-boundary.sh` stayed green.
- No sampled `0.110` / `0.111` / `0.112` slice needed a new suspect boundary crossing to ship.

## STEP 6 - Gravity Wells + Hub Containment

### Gravity Wells

| Module | Class | LOC | LOC Delta | Fan-In | Fan-In Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `db/session/sql/parameter.rs` | stable gravity well | `2131` | `+61` | `2` | `N/A` | `2` | `15` | High |
| `db/sql/lowering/prepare.rs` | stable gravity well | `1649` | `+191` | `2` | `N/A` | `2` | `20` | High |
| `db/data/structural_field/value_storage.rs` | stable gravity well | `1296` | `0` | `7` | `N/A` | `2` | `7` | Medium-High |
| `db/access/canonical.rs` | stable gravity well | `610` | `0` | `9` | `N/A` | `2` | `7` | Medium |
| `db/executor/planning/route/planner/mod.rs` | contained hub | `26` | `0` | `11` | `N/A` | `2` | `3` | Low |
| `db/executor/pipeline/orchestrator/mod.rs` | contained hub | `46` | `0` | `10` | `N/A` | `1` | `8` | Low-Medium |

### Hub Contract Containment

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `executor/planning/route/planner/mod.rs` | route planner consumes access-route contracts and emits route execution shape | `1` | `1` | `0` | `1` | Within | Low |
| `executor/pipeline/orchestrator/mod.rs` | load-surface runtime orchestration only | `1` | `1` | `0` | `1` | Within | Low |

Interpretation:

- The route/load hubs stayed contained. The April 17 concern about route/load contract sprawl did not re-accumulate in the current shipped line.
- The active velocity drag has moved to prepared-path gravity wells instead: [parameter.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/parameter.rs:1) and [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:1) are large, repeatedly edited, and cross the planner/query plus facade boundary, even though their fan-in is still modest.

## STEP 7 - Enum Shock Radius

Mechanical upper-bound scan (runtime source only, tests excluded):

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `AggregateKind` | `8` | `531` | `59` | `9.00` | `3` | `216.0` | High |
| `AccessPath` / `AccessPathKind` | `7` | `438` | `45` | `9.73` | `3` | `204.3` | High |
| `RouteShapeKind` | `5` | `38` | `10` | `3.80` | `2` | `38.0` | Medium-Low |
| `ContinuationMode` | `3` | `33` | `6` | `5.50` | `1` | `16.5` | Low |

Interpretation:

- `AggregateKind` and `AccessPath` still dominate the mechanical decision surface.
- The top 3 enums still account for almost all change-relevant decision density in the current scan.
- The route-shape budget remains materially calmer than the aggregate and access surfaces.

## STEP 8 - Subsystem Independence

| Subsystem | Qualitative Independence | Risk |
| ---- | ---- | ---- |
| planner/query | moderate; all three sampled slices still landed primarily here, but they were materially smaller and more contained than the April 17 baseline bundles | Medium |
| executor/runtime | high; the sampled `0.110` / `0.111` / `0.112` line barely touched executor owners outside focused follow-through proofs | Low |
| access/index | high; no sampled slice reopened access internals or route/access contract breadth | Low |
| storage/recovery | high; no sampled slice needed recovery or storage-boundary amplification | Low |
| facade/adapters | moderate; session-facing parity and prepared binding still participate in every slice, which keeps the adapter boundary in the change path more often than ideal | Medium |

## STEP 9 - Decision-Axis Growth

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| grouped semantic alignment delivery | grouped `HAVING` canonicalization, preserve-shape SQL lowering path, explain/cache/session parity | `3` | `2` | `N/A` | `N/A` | Medium-Low |
| grouped omitted-`ELSE` follow-through | admitted grouped boolean family expansion, explain/hash/cache identity, global aggregate `HAVING` parity, fail-closed rejection hardening | `4` | `3` | `N/A` | `N/A` | Medium |
| prepared fallback consolidation | compare-contract ownership, dynamic fallback-family inference, `WHERE` / `HAVING` fallback alignment, grouped template-lane guard retention | `4` | `3` | `N/A` | `N/A` | Medium |

## STEP 10 - Decision Surface Size

| Enum | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AggregateKind` | `18` | `18` | `0` | Medium |
| `AccessPath` / `AccessPathKind` | `14` | `14` | `0` | Medium |
| `RouteShapeKind` | `7` | `7` | `0` | Medium-Low |

These are triaged change-relevant counts rather than raw mechanical matches. The sampled `0.110` / `0.111` / `0.112` line did not materially widen these intentional update surfaces.

## STEP 11 - Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| `0.110.0` touched `14` code-bearing files | moderate blast radius | feature + parity follow-through | this slice is above the hard median target, but it stayed semantically contained and did not widen into route/load or recovery work |
| `0.111.0` touched `10` code-bearing files | moderate blast radius | semantic follow-through | the slice carried identity, explain, and hash hardening together, but still avoided the broad cleanup-bundle shape seen in the April 17 baseline |
| `0.112.0` touched `7` code-bearing files | low blast radius | structural contraction | prepared-path cleanup landed with healthy locality and without broadening admitted surface area |

## STEP 12 - Velocity Risk Index

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| enum shock radius | `5` | `2` | `10` |
| CAF trend | `5` | `2` | `10` |
| cross-layer leakage (suspect) | `2` | `2` | `4` |
| gravity-well growth/stability | `7` | `2` | `14` |
| hub contract containment | `2` | `2` | `4` |
| edit blast radius (SLO-based) | `7` | `2` | `14` |

`overall_index = 56 / 12 = 4.7`

Interpretation: moderate risk, improving. The current shipped line is materially healthier than the April 17 baseline because the slices are smaller, hub containment stayed intact, and p95 blast radius dropped back under the hard ceiling. The remaining velocity tax is concentrated in prepared-path gravity wells and the long-lived enum decision surfaces, not in new suspect boundary leakage or route/load coordination sprawl.

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority boundaries remain intact | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Route planner import boundary stayed fenced away from frontend/session concerns | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS | Low |
| Route-shape feature-budget guard executes in the live route owner boundary | `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` | PASS | Medium |

## Follow-Up Actions

- owner boundary: `db/sql/lowering` + `db/session/sql`; action: treat [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:1) and [parameter.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/parameter.rs:1) as the next explicit velocity contraction pair and keep the `0.114` slice narrow enough that it does not reopen route/load/storage surfaces; target report date/run: `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`
- owner boundary: `planner/query`; action: keep grouped semantic follow-through patches under the median file-count gate by separating identity/explain/hash tightening from unrelated cleanup; target report date/run: `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`

## Verification Readout

- method comparability status: `comparable` against `2026-04-17`
- all mandatory velocity sections for this run are present
- SLO gates were evaluated against the current sample; median blast radius still failed, but p95 passed
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
