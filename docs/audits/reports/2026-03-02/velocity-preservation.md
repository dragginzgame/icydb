# Velocity Preservation Audit - 2026-03-02

Scope: feature agility, change amplification, and extension friction across `icydb-core` architecture.

## Step 1 - Change Surface Mapping (Empirical)

Feature windows sampled from release tags:

| Feature | Files Modified | Subsystems Touched | Cross-Layer? | Localized? | Change Amplification Factor |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Projection hardening (`v0.38.0..v0.38.1`) | 20 | 2 (`executor`, `query`) | Yes | Mostly | 4 (2 x 2 flows) |
| Projection identity + grouped projection migration (`v0.38.1..v0.38.3`) | 50 | 4 (`executor`, `query`, `response`, `db root`) | Yes | No | 12 (4 x 3 flows) |
| Numeric convergence start (`v0.38.3..v0.39.1`) | 8 | 5 (`executor`, `query`, `predicate`, `numeric`, `db root`) | Yes | Mostly | 15 (5 x 3 flows) |
| Numeric boundary hardening (`v0.39.1..v0.39.2`) | 7 | 3 (`query`, `executor`, `numeric`) | Yes | Mostly | 9 (3 x 3 flows) |
| Numeric identity guards (`v0.39.2..v0.39.3`) | 4 | 2 (`query`, `executor`) | Yes | Mostly | 4 (2 x 2 flows) |

CAF flags:
- `v0.38.1..v0.38.3` and `v0.38.3..v0.39.1` exceed CAF > 6.

## Step 2 - Layer Boundary Integrity (Velocity-Oriented)

| Boundary | Leakage Type | Velocity Impact | Severity |
| ---- | ---- | ---- | ---- |
| query -> executor runtime internals | none detected (`0` non-comment references) | Low friction | Low |
| index/data/commit -> query | none detected (`0` non-comment references) | Low friction | Low |
| executor depending on plan shape details | expected typed coupling in load/aggregate execution boundaries | Medium coordination cost | Medium |
| continuation policy coupling | continuation signatures and cursor boundaries span planner/runtime/cursor surfaces | Medium-High coordination cost | Medium-High |

## Step 3 - Growth Vector & Gravity Well Detection

| Module | Responsibilities | Import Fan-In | Import Fan-Out | Growth Rate | Bottleneck Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | scalar + grouped orchestration, continuation, trace, paging, grouped folds | High | High | High (9 commits since `v0.37.0`) | High |
| `query/plan/validate.rs` | plan-shape + grouped policy + expression compatibility checks | Medium-High | Medium | Medium-High (5 commits since `v0.37.0`) | Medium-High |
| `query/intent/mod.rs` | intent construction + lowering + policy checks | High | Medium | Medium (3 commits since `v0.37.0`) | Medium-High |
| `executor/aggregate/mod.rs` | aggregate dispatch and execution-mode routing | Medium | Medium-High | Medium (2 commits since `v0.37.0`) | Medium |

## 3A - Hub Import Pressure (Required Metric)

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | `query(13)`, `executor(11)`, `predicate(2)`, `access(2)`, `cursor(1)` | 11 | 10 | unique `+2`, cross-layer `+1` | High |
| `query/intent/mod.rs` | `query(7)`, `access(1)`, `cursor(1)`, `predicate(1)`, `response(1)` | 5 | 4 | unique `+2`, cross-layer `+1` | Medium-High |
| `query/plan/validate.rs` | `access(1)`, `cursor(1)`, `executor(1)`, `predicate(1)`, `query(1)` | 5 | 4 | unique `+2`, cross-layer `+1` | Medium-High |
| `executor/aggregate/mod.rs` | `executor(4)`, `contracts(1)`, `data(1)`, `direction(1)`, `index(1)` | 7 | 6 | unique `+3`, cross-layer `+2` | Medium-High |

## Step 4 - Change Multiplier Analysis

| Feature | Subsystems Likely Impacted | Change Surface Size | Friction Level |
| ---- | ---- | ---- | ---- |
| Composite pagination | `query`, `cursor`, `executor`, `route` | Medium-High | Moderate/High |
| DESC support expansion | `query`, `executor`, `route`, `index`, `cursor` | High | High |
| Secondary index ordering | `query`, `index`, `executor`, `cursor`, `explain` | High | High |
| Query caching | `query`, `fingerprint`, `session`, `executor`, `response` | High | High |
| Multi-index intersection changes | `planner`, `access`, `executor`, `index`, `predicate` | High | High |
| New commit phase | `commit`, `executor/mutation`, `recovery`, `diagnostics`, `db root` | High | High |
| New `AccessPath` variant | `access`, `planner`, `executor`, `route`, `cursor`, `explain`, `fingerprint` | Very High | Surgical cross-system change required |

## Step 5 - Amplification Hotspots

| Amplification Source | Why It Multiplies Change | Risk |
| ---- | ---- | ---- |
| Access-path fan-out (`AccessPath::` 187 references in 17 runtime files) | planner/executor/cursor/explain/fingerprint all branch on path shape | High |
| Continuation surface (`continuation|anchor` 611 mentions in 66 runtime files) | paging/shape/signature changes cross multiple subsystem boundaries | High |
| Grouped DISTINCT spread (177 references in 26 runtime files) | grouped policy and grouped execution paths evolve together | High |
| Error mapping spread (`map_err(` 168 callsites in 66 runtime files) | behavior-preserving changes still require broad mapping touch points | Medium-High |

## Step 6 - Predictive Structural Stress Points

| Subsystem | Stress Vector | Risk Level |
| ---- | ---- | ---- |
| `executor/load` | grouped + scalar orchestration concentration | High |
| `query/plan` | grouped policy + expression compatibility growth | High |
| `query/intent` | fluent/API growth and policy translation | Medium-High |
| `cursor` | continuation compatibility and grouped/scalar edge cases | Medium-High |
| `commit/recovery` | protocol evolution requiring replay symmetry | Medium |

## Step 7 - Velocity Risk Table

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy (High-Level Only) |
| ---- | ---- | ---- | ---- | ---- |
| Access-path growth | touches planner + route + runtime + cursor | High | High | keep one access-path contract checklist across planner/route/executor |
| Grouped execution hub concentration | high merge/review contention in load/grouped paths | High | High | keep grouped policy ownership planner-side and runtime mechanics executor-side |
| Continuation contract evolution | requires synchronized signature/runtime/policy updates | Medium-High | Medium-High | maintain single continuation-signature authority and parity tests |
| Numeric/predicate boundary evolution | spans planner typing + predicate runtime + projection semantics | Medium | Medium | preserve shared numeric authority and avoid local coercion forks |

## Step 8 - Drift Sensitivity Index

| Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- |
| `AccessPath` growth | High | High |
| policy/error variant growth | Medium-High | Medium-High |
| recovery evolution | Medium | Medium |
| cursor complexity | Medium-High | Medium-High |
| index type/path expansion | High | High |

## Final Output

1. Velocity Risk Index (1-10, lower is better): **7/10**
2. Architectural Drag Sources: access-path fan-out, continuation surface spread, grouped runtime concentration.
3. Layer Leakage Findings: no strict upward dependency leak; drag comes from legitimate multi-subsystem coordination.
4. Gravity Wells: `executor/load/mod.rs`, `query/plan/validate.rs`, `query/intent/mod.rs`.
5. Feature Friction Map: highest friction remains path-shape, ordering, and continuation-evolution work.
6. Change Amplification Summary: architecture remains extendable, but cross-layer coordination cost stays high for route/cursor/grouped changes.
