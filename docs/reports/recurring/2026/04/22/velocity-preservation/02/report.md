# Velocity Preservation Audit - 2026-04-22 (Same-Day Rerun)

## Report Preamble

- scope: feature agility and cross-layer amplification risk in the shipped
  slice line currently sampled by the 2026-04-22 velocity audit, with a same-day
  hotspot reread against the live tree
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/velocity-preservation.md`
- code snapshot identifier: `424eb9848f` (`dirty` working tree)
- method tag/version: `Method V4`
- comparability status: `comparable`
  - this rerun keeps the same shipped slice sample, subsystem taxonomy,
    boundary-crossing rules, hub-family taxonomy, and SLO gates as the earlier
    2026-04-22 baseline
  - because the sampled shipped slice set is unchanged, the score-bearing blast
    radius metrics are expected to remain stable unless current hotspot review
    forces a risk reinterpretation

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
| Top gravity-well fan-in | `route planner (11-module coarse upper bound); prepared files hotter by edit frequency, not by breadth` | `unchanged numerically; hotspot interpretation is narrower and now includes the expression cluster` | `interpretive shift` |
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

- No new shipped slice was added to the scoring sample in this rerun.
- Same-day doc and audit churn is intentionally treated as noise for velocity
  scoring, not as a shipped locality signal.

## STEP 3 - Change Surface Mapping

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `0.110.0` grouped semantic alignment | `14` | `2` | `2` | `3` | `2` | `4` | `0.79` | `0.33` | Medium-Low |
| `0.111.0` grouped omitted-`ELSE` follow-through | `10` | `3` | `4` | `4` | `3` | `12` | `0.60` | `0.50` | Medium |
| `0.112.0` prepared fallback consolidation | `7` | `2` | `3` | `4` | `3` | `9` | `0.57` | `0.33` | Medium-Low |

Interpretation:

- These score-bearing slice metrics are unchanged from the same-day baseline.
- The rerun therefore does not revise the blast-radius history.
- The value in this pass is hotspot targeting: the next contraction target
  should be chosen based on which cluster is most likely to reduce module-touch
  follow-through, not based on a changed historical score.

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

| Module | Current Read | Current LOC | Same-Day Relevance | Risk |
| ---- | ---- | ----: | ---- | ---- |
| `db/sql/lowering/prepare.rs` | large prepared-path gravity well with planner/query plus facade pressure | `1780` | still hot, but no longer the only obvious follow-through drag | High |
| `db/session/sql/parameter.rs` | large session-facing prepared execution/binding surface | `1318` | still a gravity well, but less singular than the earlier prepared-only read suggested | Medium-High |
| `db/predicate/bool_expr.rs` | truth-lane adapter cluster carrying substantial structural shaping | `1912` | now clearly velocity-relevant because truth-condition follow-through crosses predicate, planner, and lowering seams here | High |
| `db/query/plan/expr/type_inference/mod.rs` | planner-owned expression-family typing/classification seam | `1070` | now clearly velocity-relevant because expression-family follow-through converges here | High |
| `db/executor/planning/route/planner/mod.rs` | contained hub | `26` | unchanged containment story | Low |
| `db/executor/pipeline/orchestrator/mod.rs` | contained hub | `46` | unchanged containment story | Low-Medium |

### Hub Contract Containment

| Hub Module | Contract Boundary | Cross-Layer Families | Same-Day Baseline | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `executor/planning/route/planner/mod.rs` | route planner consumes access-route contracts and emits route execution shape | `1` | `1` | `0` | `1` | Within | Low |
| `executor/pipeline/orchestrator/mod.rs` | load-surface runtime orchestration only | `1` | `1` | `0` | `1` | Within | Low |

Interpretation:

- The route/load hubs are still not the velocity problem.
- The earlier same-day baseline correctly identified prepared-path gravity
  wells, but the current reread shows that diagnosis is too narrow on its own.
- The expression cluster now reads as co-equal velocity drag:
  `bool_expr.rs`, planner type inference, expression-related lowering, and
  prepared/session consumers form the broader follow-through path.
- This matters because the next slice should reduce touched-module count across
  that cluster, not just shrink one prepared file in isolation.

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
- This rerun found no new evidence that the next velocity contraction should
  reopen route-shape work.

## STEP 8 - Subsystem Independence

| Subsystem | Same-Day Rerun Read | Risk |
| ---- | ---- | ---- |
| planner/query | still moderate; the next likely contraction line remains here, especially around expression-family follow-through | Medium |
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

- If `0.116` lands as designed, it should improve this table indirectly by
  turning routine truth-condition follow-through into a narrower one-family
  contraction rather than another multi-owner expression patch.

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
| current design docs make `0.116` and `0.117` more explicit | stronger sequencing signal | planning signal, not shipped breadth | this helps select the next locality-improving slice, but does not itself improve or worsen velocity |
| hotspot reread now includes `bool_expr.rs` and planner type inference as explicit drag surfaces | interpretive widening | real signal, not noise | the next contraction should target the broader expression follow-through seam rather than only the prepared pair |

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

- Moderate risk, unchanged from the earlier same-day baseline.
- The rerun does not change the risk band.
- It does sharpen the locality diagnosis:
  - route/load hubs remain contained
  - prepared-path gravity wells are still real
  - the broader expression cluster is now the clearer next velocity contraction
    target because it is where routine truth-condition and expression-family
    follow-through still crosses planner, predicate, lowering, and session
    boundaries

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority boundaries remain intact | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Route planner import boundary stayed fenced away from frontend/session concerns | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS | Low |
| Route-shape feature-budget guard executes in the live route owner boundary | `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` | PASS | Medium |
| Current hotspot read supports expression-cluster contraction before any route/load work | direct source review plus current LOC scan | PASS | Medium |

## Follow-Up Actions

- owner boundary: `planner/query` + `db/predicate` + `db/sql/lowering`; action:
  treat [0.116-design.md](/home/adam/projects/icydb/docs/design/0.116-truth-condition-semantics-centralization/0.116-design.md:1)
  as the next explicit velocity contraction target because truth-condition
  centralization is the cleanest way to reduce routine filter-related
  follow-through across `bool_expr.rs`, planner expression owners, and
  truth-related lowering adapters; target report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`
- owner boundary: `db/sql/lowering` + `db/session/sql`; action: keep
  [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:1)
  and [parameter.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/parameter.rs:1)
  bounded as consumers of planner-owned expression contracts, not independent
  semantic-expansion sites; target report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`
- owner boundary: `planner/query`; action: treat
  [0.117-design.md](/home/adam/projects/icydb/docs/design/0.117-expression-family-semantics-centralization/0.117-design.md:1)
  as the likely post-`0.116` locality follow-on rather than parallel work, so
  the expression-cluster contraction remains one bounded line at a time; target
  report date/run:
  `docs/audits/reports/2026-04/2026-04-29/velocity-preservation.md`

## Verification Readout

- method comparability status: `comparable` against the earlier 2026-04-22
  baseline
- all mandatory velocity sections for this rerun are present
- SLO gates were reevaluated against the unchanged shipped sample; median blast
  radius still failed, but p95 still passed
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::executor::planning::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
