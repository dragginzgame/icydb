# Velocity Preservation Audit - 2026-04-22 (Same-Day Rerun 3)

## Report Preamble

- scope: feature agility and cross-layer amplification risk in the shipped
  slice line currently sampled by the 2026-04-22 velocity audit, with a same-day
  hotspot reread against the live tree
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/velocity-preservation-2.md`
- code snapshot identifier: `47e55a7897` (`dirty` working tree)
- method tag/version: `Method V4`
- comparability status: `comparable`
  - this rerun keeps the same shipped slice sample, subsystem taxonomy,
    boundary-crossing rules, hub-family taxonomy, and SLO gates as the earlier
    same-day reruns
  - because the sampled shipped slice set is unchanged, the score-bearing blast
    radius metrics remain directly comparable to the earlier 2026-04-22 runs
  - the main purpose of this rerun is to refresh the hotspot interpretation in
    light of the current tree and the newer `0.118` flow-collapse framing

## STEP 0 - Run Metadata + Method / Comparability

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | unchanged from same-day baseline | unchanged from same-day baseline | Yes |
| subsystem taxonomy | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | planner/query, executor/runtime, cursor/continuation, access/index, storage/recovery, facade/adapters | Yes |
| boundary crossing rule set | invariant scripts plus route/load hub import review | invariant scripts plus route/load hub import review | Yes |
| fan-in definition | manual hub pressure proxy plus coarse runtime-module reference upper bound, tests/generated excluded | manual hub pressure proxy plus coarse runtime-module reference upper bound, tests/generated excluded | Yes |
| hub-family taxonomy | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | Yes |
| independent-axis rule | slice-local axis count using distinct semantic change families | slice-local axis count using distinct semantic change families | Yes |
| facade/adapters inclusion | included only when shipped slices actually touched session/canister/bootstrap/generated surfaces; docs excluded from subsystem counts | included only when shipped slices actually touched session/canister/bootstrap/generated surfaces; docs excluded from subsystem counts | Yes |

## STEP 1 - Baseline Capture

| Metric | Same-Day Baseline | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | `4.7` | `4.7` | `0.0` |
| Cross-layer suspect crossings | `0` | `0` | `0` |
| Avg files touched per feature slice | `10.3` | `10.3` | `0.0` |
| Median files touched | `10` | `10` | `0` |
| p95 files touched | `14` | `14` | `0` |
| Top gravity-well read | `broader expression cluster now reads as co-equal velocity drag` | `same cluster remains active, with predicate-flow pressure now slightly more tractable in the live tree` | `interpretive refinement` |
| Route-planner high-impact cross-layer families | `1` | `1` | `0` |
| Edit concentration in top 5 modules (%) | `83.9` | `83.9` | `0.0` |
| Fan-in concentration in top 5 modules (%) | `N/A` | `N/A` | `N/A` |
| Decision-site concentration in top 3 enums (%) | `96.8` | `96.8` | `0.0` |

## STEP 2 - Feature Slice Selection

| Feature Slice | Source Type (`PR`/`tracker`/`commits`/`manual`) | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |
| `0.110.0` grouped semantic alignment | `manual` | unchanged from same-day baseline | retained to preserve comparability | docs, changelog, cargo metadata excluded from locality counts |
| `0.111.0` grouped omitted-`ELSE` follow-through | `manual` | unchanged from same-day baseline | retained to preserve comparability | docs, changelog, cargo metadata excluded from locality counts |
| `0.112.0` prepared fallback consolidation | `manual` | unchanged from same-day baseline | retained to preserve comparability | docs, changelog, cargo metadata excluded from locality counts |

Current-read note:

- No new shipped slice was added to the score-bearing sample.
- Same-day design and audit work remains noise for historical blast-radius
  scoring.
- This rerun therefore focuses on whether the current code makes the next
  locality contraction target clearer.

## STEP 3 - Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.110.0` grouped semantic alignment | `14` | `2` | `2` | `3` | `2` | `4` | `0.79` | `0.33` | Medium-Low |
| `0.111.0` grouped omitted-`ELSE` follow-through | `10` | `3` | `4` | `4` | `3` | `12` | `0.60` | `0.50` | Medium |
| `0.112.0` prepared fallback consolidation | `7` | `2` | `3` | `4` | `3` | `9` | `0.57` | `0.33` | Medium-Low |

Interpretation:

- These score-bearing slice metrics are unchanged.
- The rerun therefore does not revise the historical blast-radius read.
- The useful delta is live targeting: the expression cluster is still the right
  contraction zone, but the current tree now points more specifically at
  predicate-flow collapse as the first bounded move inside it.

## STEP 4 - Edit Blast Radius Summary

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |
| release-slice sample | shipped `0.110.0`, `0.111.0`, `0.112.0` slices | `3` | `0.110.0`, `0.111.0`, `0.112.0` | Yes |

| Metric | Same-Day Baseline | Current | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | `10.3` | `10.3` | `0.0` |
| median files touched | `10` | `10` | `0` |
| p95 files touched | `14` | `14` | `0` |

SLO evaluation:

- median files touched `<= 8`: `FAIL`
- p95 files touched `<= 15`: `PASS`

| Concentration Metric | Same-Day Baseline | Current | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules | `83.9` | `83.9` | `0.0` | Medium-High |
| % of fan-in in top 5 modules | `N/A` | `N/A` | `N/A` | Baseline not yet normalized |
| % of decision sites in top 3 enums | `96.8` | `96.8` | `0.0` | Medium |

Top 5 edit buckets remain:

- `db/query/*`: `10`
- `db/session/*`: `7`
- `db/sql/*`: `6`
- `db/predicate/*`: `2`
- `db/executor/*`: `1`

## STEP 5 - Boundary Leakage

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Same-Day Baseline | Delta | Risk |
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
- No same-day evidence suggests route/load/storage sprawl has re-accumulated.

## STEP 6 - Gravity Wells + Hub Containment

### Gravity Wells

| Module | Current Read | Current LOC | Same-Day Delta | Risk |
| ---- | ---- | ----: | ---- | ---- |
| `db/sql/lowering/prepare.rs` | large prepared-path gravity well with planner/query plus facade pressure | `1780` | unchanged | High |
| `db/session/sql/parameter.rs` | large session-facing prepared execution/binding surface | `1302` | slightly down from the earlier same-day rerun | Medium-High |
| `db/predicate/bool_expr.rs` | truth-lane adapter cluster still in the hot path, but less oversized than before | `1148` | materially down from the earlier same-day rerun (`1912`) | High |
| `db/query/plan/expr/type_inference/mod.rs` | planner-owned expression-family typing/classification seam | `1070` | unchanged | High |
| `db/executor/planning/route/planner/mod.rs` | contained hub | `26` | unchanged containment story | Low |
| `db/executor/pipeline/orchestrator/mod.rs` | contained hub | `46` | unchanged containment story | Low-Medium |

### Hub Contract Containment

| Hub Module | Contract Boundary | Cross-Layer Families | Same-Day Baseline | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `executor/planning/route/planner/mod.rs` | route planner consumes access-route contracts and emits route execution shape | `1` | `1` | `0` | `1` | Within | Low |
| `executor/pipeline/orchestrator/mod.rs` | load-surface runtime orchestration only | `1` | `1` | `0` | `1` | Within | Low |

Interpretation:

- The route/load hubs are still not the velocity problem.
- The broader expression cluster remains the active drag.
- The live tree now slightly sharpens that read:
  - prepared/session gravity wells are still real
  - planner type inference is still central
  - predicate-flow pressure looks more tractable than it did in the previous
    same-day rerun because `bool_expr.rs` is materially smaller
- That makes the tightened `0.118` framing look better grounded:
  predicate-flow collapse is the best first bounded contraction inside the
  broader expression cluster.

## STEP 7 - Enum Shock Radius

Mechanical upper-bound scan read remains unchanged from the same-day baseline:

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `AggregateKind` | `8` | `531` | `59` | `9.00` | `3` | `216.0` | High |
| `AccessPath` / `AccessPathKind` | `7` | `438` | `45` | `9.73` | `3` | `204.3` | High |
| `RouteShapeKind` | `5` | `38` | `10` | `3.80` | `2` | `38.0` | Medium-Low |
| `ContinuationMode` | `3` | `33` | `6` | `5.50` | `1` | `16.5` | Low |

Interpretation:

- The route-shape budget remains materially calmer than aggregate and access.
- No new evidence suggests the next contraction should reopen route-shape work.

## STEP 8 - Subsystem Independence

| Subsystem | Same-Day Rerun Read | Risk |
| ---- | ---- | ---- |
| planner/query | still moderate; the next likely contraction line remains here, now with a more explicit predicate-flow-first shape | Medium |
| executor/runtime | still high independence; the rerun found no new reason to pull executor into the next contraction slice | Low |
| access/index | still high independence; no new route/access breadth signal appeared | Low |
| storage/recovery | still high independence; no new storage/recovery amplification signal appeared | Low |
| facade/adapters | still moderate; session-facing prepared binding and SQL lowering remain in the path of expression-related slices more often than ideal | Medium |

## STEP 9 - Decision-Axis Growth

Historical slice-axis counts remain unchanged from the same-day baseline:

| Operation | Axes | Axis Count | Independent Axes | Same-Day Baseline | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| grouped semantic alignment delivery | grouped `HAVING` canonicalization, preserve-shape SQL lowering path, explain/cache/session parity | `3` | `2` | `2` | `0` | Medium-Low |
| grouped omitted-`ELSE` follow-through | admitted grouped boolean family expansion, explain/hash/cache identity, global aggregate `HAVING` parity, fail-closed rejection hardening | `4` | `3` | `3` | `0` | Medium |
| prepared fallback consolidation | compare-contract ownership, dynamic fallback-family inference, `WHERE` / `HAVING` fallback alignment, grouped template-lane guard retention | `4` | `3` | `3` | `0` | Medium |

Current planning implication:

- `0.118` now reads more credibly as a bounded velocity slice because the live
  tree supports the narrower reading: predicate-flow collapse first, broader
  flow cleanup second.

## STEP 10 - Decision Surface Size

| Enum | Decision Sites Requiring Feature Updates | Same-Day Baseline | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AggregateKind` | `18` | `18` | `0` | Medium |
| `AccessPath` / `AccessPathKind` | `14` | `14` | `0` | Medium |
| `RouteShapeKind` | `7` | `7` | `0` | Medium-Low |

No same-day evidence suggests these intentional update surfaces widened.

## STEP 11 - Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| same-day docs and audit reruns touched the worktree | visible churn | docs-only noise | this does not count as a velocity regression in shipped code locality |
| the shipped slice sample is unchanged | stable score-bearing dataset | expected stability | unchanged metrics are a comparability feature here, not an audit blind spot |
| `0.118` is now framed more explicitly around predicate-flow collapse | sharper design language | planning signal with live-code support | this still does not change historical scoring, but it does better match the current hotspot read |
| `bool_expr.rs` is materially smaller in the live tree | concrete code-side delta | real signal, not noise | this strengthens the case that predicate-flow collapse is the right first bounded contraction rather than a broad pipeline rewrite |

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

Interpretation:

- Moderate risk, unchanged from the earlier same-day reruns.
- The rerun does not change the risk band.
- It does refine the locality diagnosis:
  - route/load hubs remain contained
  - prepared-path gravity wells are still real
  - the broader expression cluster remains the active drag
  - predicate-flow collapse now looks like the cleanest first bounded move
    inside that cluster

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority boundaries remain intact | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Route planner import boundary stayed fenced away from frontend/session concerns | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS | Low |
| Route-shape feature-budget guard executes in the live route owner boundary | `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` | PASS | Medium |
| Current hotspot read supports the narrowed `0.118` first-step shape | direct source review plus current LOC scan | PASS | Medium |

## Follow-Up Actions

- owner boundary: `db/predicate` + `planner/query` + `db/sql/lowering`; action:
  treat [0.118-design.md](/home/adam/projects/icydb/docs/design/0.118-expression-pipeline-flow-collapse/0.118-design.md:1)
  as the next explicit velocity contraction target, with
  [bool_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/bool_expr.rs:1)
  as the first and primary contraction surface; target report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`
- owner boundary: `db/sql/lowering` + `db/session/sql`; action: keep
  [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:1)
  and [parameter.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/parameter.rs:1)
  bounded as planner-consuming contract mappers while predicate-flow collapse
  lands; target report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`
- owner boundary: `planner/query`; action: keep the broader expression-flow
  collapse line sequenced after the predicate hop rather than trying to flatten
  the full pipeline in one slice; target report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`

## Verification Readout

- method comparability status: `comparable` against the earlier 2026-04-22
  reruns
- all mandatory velocity sections for this rerun are present
- SLO gates were reevaluated against the unchanged shipped sample; median blast
  radius still failed, but p95 still passed
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
