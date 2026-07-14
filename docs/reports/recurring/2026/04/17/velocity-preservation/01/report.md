# Velocity Preservation Audit - 2026-04-17

## Report Preamble

- scope: feature agility and cross-layer amplification risk in the shipped `0.88.x` line
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-30/velocity-preservation.md`
- code snapshot identifier: `8ffba6a5c` (`dirty` working tree)
- method tag/version: `Method V4`
- comparability status: `non-comparable`
  - slice selection, subsystem taxonomy, and boundary checks stayed aligned with the March 30 structured baseline
  - this run tightened the gravity-well readout by adding a coarse runtime-module reference upper bound for current hubs instead of leaving fan-in fully qualitative
  - treat deltas as directional rather than strict trend evidence

## STEP 0 - Run Metadata + Method / Comparability

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | changelog-guided release slices with manual file filtering for code-bearing files | changelog-guided release slices with manual file filtering for code-bearing files | Yes |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | Yes |
| boundary crossing rule set | invariant scripts plus route/load hub import review | invariant scripts plus route/load hub import review | Yes |
| fan-in definition | manual hub pressure proxy plus coarse runtime-module reference upper bound, tests/generated excluded | manual hub pressure proxy using import families and 30d edit frequency | No |
| hub-family taxonomy | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | Yes |
| independent-axis rule | slice-local axis count using distinct semantic change families | slice-local axis count using distinct semantic change families | Yes |
| facade/adapters inclusion | included only when shipped slices actually touched canister/bootstrap/generated surfaces; docs excluded from subsystem counts | included for canister/bootstrap/generated-surface slices; docs excluded from subsystem counts | Yes |

## STEP 1 - Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | `5.2` | `5.5` | `+0.3` |
| Cross-layer suspect crossings | `0` | `0` | `0` |
| Avg files touched per feature slice | `28.7` | `32.7` | `+4.0` |
| Median files touched | `31` | `27` | `-4` |
| p95 files touched | `34` | `62` | `+28` |
| Top gravity-well fan-in | `route planner (stable qualitative hotspot)` | `types::decimal (69-module upper bound)` | `shifted` |
| Route-planner high-impact cross-layer families | `2` | `1` | `-1` |
| Edit concentration in top 5 modules (%) | `72.1` | `88.8` | `+16.7` |
| Fan-in concentration in top 5 modules (%) | `N/A` | `N/A` | `N/A` |
| Decision-site concentration in top 3 enums (%) | `97.7` | `96.5` | `-1.2` |

## STEP 2 - Feature Slice Selection

| Feature Slice | Source Type (`PR`/`tracker`/`commits`/`manual`) | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| `0.88.0` grouped aggregate ordered execution groundwork | `manual` | `docs/changelog/0.88.md` + filtered `v0.87.1..v0.88.0` | first shipped grouped ordered-fold execution slice in the `0.88.x` line | docs, changelog, cargo metadata excluded from locality counts |
| `0.88.1` bounded grouped Top-K plus cleanup tranche | `manual` | `docs/changelog/0.88.md` + filtered `v0.88.0..v0.88.1` | this is the dominant shipped patch and the real velocity pressure point in the line | docs, changelog, cargo metadata excluded from locality counts |
| `0.88.2` alias parity and follow-through cleanup | `manual` | `docs/changelog/0.88.md` + filtered `v0.88.1..v0.88.2` | captures the shipped follow-through slice after `0.88.1` broad cleanup landed | docs, changelog, cargo metadata excluded from locality counts |

## STEP 3 - Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.88.0` grouped aggregate ordered execution groundwork | `9` | `2` | `2` | `3` | `2` | `4` | `0.89` | `0.33` | Medium-Low |
| `0.88.1` bounded grouped Top-K plus cleanup tranche | `62` | `4` | `5` | `6` | `5` | `25` | `0.68` | `0.67` | High |
| `0.88.2` alias parity and follow-through cleanup | `27` | `3` | `4` | `4` | `3` | `12` | `0.74` | `0.50` | Medium |

Interpretation:

- `0.88.0` was a healthy routine slice: grouped aggregate execution and planner semantics moved together without dragging in parser/session/build surfaces.
- `0.88.1` is the velocity outlier. It combined a real shipped executor/planner objective with a large cleanup tranche across predicate parsing/runtime, session projection, storage-key codecs, fingerprint hashing, and the shared lexer.
- `0.88.2` was smaller and more localized than `0.88.1`, but it still landed above the hard file-count ceiling because parser/storage follow-through work remained bundled into one release patch.

## STEP 4 - Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| release-slice sample | `0.88.x` shipped slices | `3` | `0.88.0`, `0.88.1`, `0.88.2` | No |

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `32.7` | `28.7` | `+4.0` |
| median files touched | `27` | `31` | `-4` |
| p95 files touched | `62` | `34` | `+28` |

SLO evaluation:

- median files touched `<= 8`: `FAIL`
- p95 files touched `<= 15`: `FAIL`

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules | `88.8` | `72.1` | `+16.7` | High |
| % of fan-in in top 5 modules | `N/A` | `N/A` | `N/A` | Baseline not yet normalized |
| % of decision sites in top 3 enums | `96.5` | `97.7` | `-1.2` | Medium |

Top 5 edit buckets in this run:

- `db/predicate/*`: `23`
- `db/executor/*`: `22`
- `db/query/*`: `18`
- `db/data/*`: `12`
- `db/session/*`: `12`

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
- No `0.88.x` slice needed a new suspect boundary crossing to ship.

## STEP 6 - Gravity Wells + Hub Containment

### Gravity Wells

| Module | Class | LOC | LOC Delta | Fan-In | Fan-In Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `types/decimal.rs` | stable gravity well | `1342` | `-38` | `69` | `N/A` | `1` | `5` | High |
| `db/executor/aggregate/contracts/state.rs` | stable gravity well | `979` | `+221` | `2` | `N/A` | `1` | `12` | Medium-High |
| `db/data/structural_field/value_storage.rs` | stable gravity well | `1296` | `+733` | `3` | `N/A` | `2` | `9` | Medium-High |
| `db/access/canonical.rs` | stable gravity well | `624` | `-87` | `6` | `N/A` | `2` | `7` | Medium |
| `db/executor/planning/route/planner/mod.rs` | contained hub | `26` | `+26` | `16` | `N/A` | `2` | `2` | Medium-Low |
| `db/executor/pipeline/orchestrator/mod.rs` | contained hub | `43` | `-65` | `9` | `N/A` | `1` | `16` | Low |

### Hub Contract Containment

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `executor/planning/route/planner/mod.rs` | route planner consumes access-route contracts and emits route execution shape | `1` | `2` | `-1` | `1` | Within | Low |
| `executor/pipeline/orchestrator/mod.rs` | load-surface runtime orchestration only | `1` | `1` | `0` | `1` | Within | Low |

Interpretation:

- The old route-planner overage is gone at the root boundary. The planner root now imports only executor-local families and stays within the configured ceiling.
- The load-side successor remains narrow. The former broad load-hub gravity well did not re-accumulate under `orchestrator/mod.rs`.
- Velocity pressure has shifted away from route/load coordination and back toward older dense core owners: decimal arithmetic, aggregate state contracts, structural value storage, and canonical access normalization.

## STEP 7 - Enum Shock Radius

Mechanical upper-bound scan (runtime source only, tests excluded):

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `AggregateKind` | `8` | `305` | `54` | `5.65` | `3` | `135.6` | High |
| `AccessPath` / `AccessPathKind` | `7` | `146` | `44` | `3.32` | `3` | `69.7` | Medium-High |
| `RouteShapeKind` | `5` | `21` | `10` | `2.10` | `2` | `21.0` | Medium-Low |
| `ContinuationMode` | `3` | `17` | `6` | `2.83` | `1` | `8.5` | Low |

Interpretation:

- `AggregateKind` is now the clearest live decision hotspot from a velocity lens.
- `AccessPath` remains large, but the main route-planner hub pressure around it is lower than in March because the planner root itself is now better contained.
- The top 3 enums still account for almost all change-relevant decision density in the current mechanical scan.

## STEP 8 - Subsystem Independence

| Subsystem | Qualitative Independence | Risk |
| ---- | ---- | ---- |
| planner/query | moderate; `0.88.x` work still bundled predicate, query, session-SQL, and lexer concerns together in `0.88.1`, even though those follow-up splits stayed inside one semantic family | Medium |
| executor/runtime | moderate-high; grouped aggregate work stayed mostly owner-local and did not re-grow route/load hub pressure | Medium-Low |
| access/index | high; route planning stayed contract-driven and no `0.88.x` slice reopened access internals broadly | Low |
| storage/recovery | moderate; storage-key cleanup stayed local, but dense storage/value owners remain live contraction targets | Medium |
| facade/adapters | high; no shipped `0.88.x` slice needed canister/build/bootstrap expansion | Low |

## STEP 9 - Decision-Axis Growth

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| grouped aggregate ordered execution delivery | grouped planner semantics, grouped fold state, projection materialization, explain/session parity | `4` | `3` | `N/A` | `N/A` | Medium |
| bounded grouped Top-K plus cleanup tranche | route payload contracts, predicate parse/runtime, session projection covering, fingerprint/lexer/storage-key cleanup | `4` | `3` | `N/A` | `N/A` | Medium |
| alias parity follow-through | explain alias normalization, parser atom/operand normalization, fingerprint profile hashing | `3` | `2` | `N/A` | `N/A` | Medium-Low |

## STEP 10 - Decision Surface Size

| Enum | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AggregateKind` | `18` | `18` | `0` | Medium |
| `AccessPath` / `AccessPathKind` | `14` | `14` | `0` | Medium |
| `RouteShapeKind` | `7` | `7` | `0` | Medium-Low |

These are triaged change-relevant counts rather than raw mechanical matches. The current `0.88.x` line did not materially widen these intentional update surfaces.

## STEP 11 - Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| `0.88.1` touched `62` code-bearing files | very high blast radius | feature + containment refactor | this patch was genuinely expensive, but a large share of the breadth bought down future parser/session/lexer/fingerprint hotspot pressure |
| `0.88.2` touched `27` code-bearing files | still above hard slice ceiling | structural improvement | broad follow-through cleanup continued, but it stayed out of route/load hubs and kept the remaining pressure inside parser/storage/query owners |
| route-planner cross-layer family count moved `2 -> 1` | lower hub pressure | containment improvement | the route planner is no longer the main active overage in the velocity picture |

## STEP 12 - Velocity Risk Index

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| enum shock radius | `5` | `2` | `10` |
| CAF trend | `8` | `2` | `16` |
| cross-layer leakage (suspect) | `2` | `2` | `4` |
| gravity-well growth/stability | `6` | `2` | `12` |
| hub contract containment | `2` | `2` | `4` |
| edit blast radius (SLO-based) | `10` | `2` | `20` |

`overall_index = 66 / 12 = 5.5`

Interpretation: moderate risk and manageable pressure, but the drag moved. The March run was held back by route/load coordination hotspots. The `0.88.x` line shows better hub containment there, but worse slice locality because `0.88.1` and `0.88.2` still shipped as broad cleanup bundles. The main velocity tax is now large landed slices plus a few dense long-lived core owners, not new suspect layer leakage.

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority boundaries remain intact | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Route planner import boundary stayed fenced away from frontend/session concerns | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS | Low |
| Route-shape feature-budget guard executes in the live route owner boundary | `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` | PASS | Medium |

## Follow-Up Actions

- owner boundary: `planner/query`; action: do not ship another combined predicate/session/query cleanup slice above the hard limit without an explicit slice override and justification; target report date/run: `docs/audits/reports/2026-04/2026-04-24/velocity-preservation.md`
- owner boundary: `types` + `db/data`; action: treat `types::decimal` and `db::data::structural_field::value_storage` as separate contraction candidates instead of bundling them with executor or parser work; target report date/run: `docs/audits/reports/2026-04/2026-04-24/velocity-preservation.md`

## Verification Readout

- method comparability status: `non-comparable` against `2026-03-30` because the gravity-well/fan-in readout is more explicit in this run even though the slice taxonomy and boundary checks are the same
- all mandatory velocity sections for this run are present
- SLO gates were evaluated against the current sample and both blast-radius gates failed
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
