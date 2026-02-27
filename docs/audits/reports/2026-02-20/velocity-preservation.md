# Velocity Preservation Audit - 2026-02-20

Scope: feature agility, cross-layer amplification, and extension friction.

## Step 1 - Change Surface Mapping (Empirical)

| Feature | Files Modified | Subsystems Touched | Cross-Layer? | Localized? | Change Amplification Factor |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor offset continuation semantics (`initial_offset`, effective offset) | 13 core files in latest change set | query policy + plan + executor + tests | Yes | Partial | 5 subsystems x 3 flows = 15 |
| Boundary centralization around `KeyEnvelope` | 3 primary modules (`cursor_spine`, `index/range`, lookup call sites) | planner/index/executor | Yes | Mostly | 3 x 3 = 9 |
| Load executor route decomposition (`execute/route/page/physical_path/composite_stream`) | multi-file but one subsystem root | executor/load + context | Mostly internal | Yes | 2 x 3 = 6 |
| Error-origin consolidation (`InternalError` constructors) | error + commit/index/relation call sites | error/core + executor + commit/index | Yes | Partial | 4 x 2 = 8 |

CAF threshold flags:
- Cursor offset continuation work remains high amplification (>6).

## Step 2 - Layer Boundary Integrity (Velocity-Oriented)

| Boundary | Leakage Type | Velocity Impact | Severity |
| ---- | ---- | ---- | ---- |
| planner -> executor | low leakage (typed plan only) | low friction | Low |
| executor -> plan internals | moderate (fast-path routing depends on plan shape fields) | medium friction for new route types | Medium |
| recovery -> prepare hooks | intentional coupling through `prepare_row_commit_op` | medium friction when commit row-op shape grows | Medium |
| index -> query abstractions | limited helper coupling (`Direction`, range helpers) | medium | Medium |
| cursor codec -> executable plan | constrained via cursor spine | low-medium | Low-Medium |

## Step 3 - Growth Vector & Gravity Well Detection

| Module | Responsibilities | Import Fan-In | Import Fan-Out | Growth Rate | Bottleneck Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/planner.rs` | predicate -> path planning + range shaping | High | High | High | High |
| `crates/icydb-core/src/db/query/plan/logical.rs` | post-access semantics + pagination + cursor filtering | High | Medium | High | High |
| `crates/icydb-core/src/db/query/plan/cursor_spine.rs` | token validation + envelope checks + anchor typing | Medium-High | Medium | High | Medium-High |
| `crates/icydb-core/src/db/executor/load/mod.rs` (+ submodules) | orchestration and route dispatch | High | Medium | Moderate | Medium |
| `crates/icydb-core/src/db/commit/recovery.rs` | replay + startup rebuild + rollback restore | Medium | High | Moderate | Medium-High |

## Step 4 - Change Multiplier Analysis

| Feature | Subsystems Likely Impacted | Change Surface Size | Friction Level |
| ---- | ---- | ---- | ---- |
| Composite pagination | planner, cursor, executor/load, tests | large | High |
| DESC support expansion | planner, cursor spine, logical, index traversal, tests | large | High |
| Secondary index ordering expansion | planner, validate pushdown, executor/load, index store | medium-large | High |
| Query caching | query intent/plan, executor, invalidation hooks | medium | Moderate |
| Multi-index intersection growth | planner + composite_stream + ordered streams + tests | medium-large | High |
| New commit phase | commit marker, guard, recovery replay, mutation entrypoints | large | High |
| New AccessPath variant | plan types, planner, executor, explain, tests | large | High |

## Step 5 - Amplification Hotspots

| Amplification Source | Why It Multiplies Change | Risk |
| ---- | ---- | ---- |
| `AccessPath` fan-out (22 non-test db files, 210 references) | every new path changes planner + executor + explain + cursor support | High |
| Cursor protocol shape | new token fields require encode/decode/plan/revalidate/tests updates | High |
| Commit marker authority model | row-op schema changes propagate through prepare/apply/replay | High |
| Planner/logical large modules | single-feature edits often span multiple phases in same file | Medium-High |

## Step 6 - Predictive Structural Stress Points

| Subsystem | Stress Vector | Risk Level |
| ---- | ---- | ---- |
| query/plan | growing boundary semantics + branch density | High |
| executor/load | route matrix growth and continuation interplay | Medium-High |
| commit/recovery | replay equivalence sensitivity to row-op evolution | Medium-High |
| pagination test surface | high cognitive maintenance cost | High |

## Step 7 - Velocity Risk Table

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy (High-Level Only) |
| ---- | ---- | ---- | ---- | ---- |
| AccessPath expansion | broad branch fan-out | High | High | keep path routing centralized and explicit |
| Cursor protocol evolution | multi-boundary compatibility rules | High | High | keep token validation in single cursor spine authority |
| Commit protocol changes | recovery parity requirements | High | High | preserve marker-as-authority and mechanical apply |
| Planner hotspot size | high local cognitive load | Medium | Medium-High | continue localized submodule extraction where semantics stay explicit |

## Step 8 - Drift Sensitivity Index

| Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- |
| AccessPath growth | High | High |
| Error variant growth | Medium | Medium |
| Recovery evolution | High | High |
| Cursor complexity | High | High |
| Index type expansion | Medium-High | Medium-High |

## Final Output

1. Velocity Risk Index (1-10, lower is better): **6/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

2. Architectural Drag Sources
- AccessPath fan-out, cursor protocol coupling, commit/recovery parity obligations.

3. Layer Leakage Findings
- No critical upward leaks; main pressure is valid cross-layer coordination, not illegal dependencies.

4. Gravity Wells
- planner/logical/cursor-spine modules.

5. Feature Friction Map
- Highest friction: new AccessPath variants, DESC expansion, commit phase additions.

6. Change Amplification Summary
- Cursor and access-path features still require multi-subsystem edits and are the primary velocity constraint.
