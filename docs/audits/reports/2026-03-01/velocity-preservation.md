# Velocity Preservation Audit - 2026-03-01

Scope: feature agility, change amplification, and extension friction across `icydb-core` architecture.

## Step 1 - Change Surface Mapping (Empirical)

Feature windows sampled from tagged release history:

| Feature | Files Modified | Subsystems Touched | Cross-Layer? | Localized? | Change Amplification Factor |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `GROUP BY` introduction (`v0.34.3..v0.35.0`) | 74 | 5 (`executor`, `query`, `access`, `cursor`, `session`) | Yes | No | 15 (5 subsystems x 3 execution flows) |
| Ordered grouped strategy foundations (`v0.35.1..v0.36.0`) | 39 | 2 (`executor`, `query`) | Yes | Partly | 6 (2 x 3) |
| Grouped DISTINCT contracts (`v0.36.0..v0.36.2`) | 52 | 3 (`executor`, `query`, `cursor`) | Yes | No | 9 (3 x 3) |
| Grouped hardening (`v0.36.2..v0.36.3`) | 26 | 2 (`executor`, `query`) | Yes | Mostly | 4 (2 x 2) |
| Aggregate fluent consolidation (`v0.36.3..v0.37.0`) | 48 | 7 (`query`, `executor`, `cursor`, `predicate`, `index`, `contracts`, `db root`) | Yes | No | 21 (7 x 3) |
| Builder hardening + follow-up (`v0.37.0..v0.37.2`) | 25 | 2 (`executor`, `query`) | Yes | Mostly | 4 (2 x 2) |

CAF flags:
- `GROUP BY` introduction and `0.37.0` aggregate consolidation exceed CAF > 6 significantly.

## Step 2 - Layer Boundary Integrity (Velocity-Oriented)

| Boundary | Leakage Type | Velocity Impact | Severity |
| ---- | ---- | ---- | ---- |
| planner -> executor details | no runtime import leak in query modules; planner surface remains query-owned | Low friction | Low |
| executor -> plan internals | executor branches on logical/grouped plan shape and grouped handoff contracts | Medium friction | Medium |
| recovery/commit -> planner | no direct planner ownership reimplementation in commit boundary | Low friction | Low |
| index/data/commit -> query | no non-comment direct dependency in audited runtime files | Low friction | Low |
| cursor codec/spine -> plan constraints | cursor validation tied to canonical order/signature contracts | Medium friction | Medium |

## Step 3 - Growth Vector & Gravity Well Detection

| Module | Responsibilities | Import Fan-In | Import Fan-Out | Growth Rate | Bottleneck Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | scalar + grouped load orchestration, continuation, trace, grouped folds | High | High | High | High |
| `query/intent/mod.rs` | typed intent construction, plan lowering, policy gates | High | Medium | Medium-High | Medium-High |
| `query/plan/validate.rs` | plan-shape and grouped semantic validation | Medium-High | Medium | High | Medium-High |
| `executor/aggregate/mod.rs` | aggregate dispatch and execution-mode routing | Medium-High | Medium-High | Medium | Medium-High |

### 3A - Hub Import Pressure (Required Metric)

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | `query`, `predicate`, `cursor`, `access`, `index` | 9 (`access`,`contracts`,`cursor`,`data`,`direction`,`index`,`predicate`,`query`,`response`) | 9 | +9 import-label pressure vs `v0.28.6` proxy baseline (11 -> 20 top import labels) | High |
| `query/intent/mod.rs` | `query`, `predicate`, `access`, `response` | 3 (`access`,`predicate`,`response`) | 3 | +1 import-label pressure vs `v0.28.6` proxy baseline (12 -> 13 labels) | Medium |
| `query/plan/validate.rs` | `access`, `cursor`, `predicate` | 3 | 3 | N/A (module/path not present in `v0.28.6`) | Medium |
| `executor/aggregate/mod.rs` | `data`, `direction`, `index`, `response` | 4 | 4 | N/A (no comparable baseline in previous report lineage) | Medium-High |

Note: prior `2026-02-24` velocity report did not record hub-import-pressure counts, so `Delta vs Previous` uses a tag-proxy baseline where available.

## Step 4 - Change Multiplier Analysis

| Feature | Subsystems Likely Impacted | Change Surface Size | Friction Level |
| ---- | ---- | ---- | ---- |
| Composite pagination | `query`, `cursor`, `executor`, `route` | Medium-High | Moderate/High |
| DESC support expansion | `query`, `executor`, `route`, `index`, `cursor` | High | High |
| Secondary index ordering | `query`, `index`, `executor`, `cursor`, `explain` | High | High |
| Query caching | `query`, `fingerprint`, `session`, `executor`, `response` | High | High |
| Multi-index intersection improvements | `planner`, `access`, `executor`, `index`, `predicate` | High | High |
| New commit phase | `commit`, `executor/mutation`, `recovery`, `diagnostics`, `db root` | High | High |
| New `AccessPath` variant | `access`, `planner`, `executor`, `route`, `cursor`, `explain`, `fingerprint` | Very High | Surgical cross-system change required |

## Step 5 - Amplification Hotspots

| Amplification Source | Why It Multiplies Change | Risk |
| ---- | ---- | ---- |
| Access-path fan-out (`AccessPath::` in 23 non-test modules / 212 references) | new path semantics propagate across planning, routing, execution, explain/fingerprint | High |
| Grouped load orchestration concentration (`execute_grouped_path` ~468 lines) | many grouped feature changes converge in one function | High |
| Cursor continuation spread (31 modules) | continuation contract edits require multi-surface coordination | High |
| Grouped policy surface (`GroupPlanError` 19 variants) | each new grouped rule adds policy + validation + runtime branch pressure | Medium-High |

## Step 6 - Predictive Structural Stress Points

| Subsystem | Stress Vector | Risk Level |
| ---- | ---- | ---- |
| `executor/load` | grouped + scalar orchestration convergence | High |
| `query/plan` | grouped policy growth and branch expansion | High |
| `query/intent` | API-surface expansion and policy translation | Medium-High |
| `cursor` | continuation compatibility and grouped/scalar divergence | Medium-High |
| `commit/recovery` | protocol evolution requires strict replay symmetry | Medium |

## Step 7 - Velocity Risk Table

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy (High-Level Only) |
| ---- | ---- | ---- | ---- | ---- |
| Access-path growth | multi-subsystem edits per variant change | High | High | keep access-path contract explicit and gate changes through one planner+route checklist |
| Grouped execution hub concentration | high merge and review collision surface | High | High | keep grouped routing/validation contracts explicit and narrow interface boundaries |
| Cursor contract evolution | continuation updates require coordinated policy/runtime/test updates | Medium-High | Medium-High | maintain one cursor-spine authority with explicit compatibility tests |
| Fluent aggregate evolution | API changes propagate to intent/planner/runtime mappings | High | Medium-High | preserve builder-to-plan translation boundary and avoid leaking executor concerns upward |

## Step 8 - Drift Sensitivity Index

| Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- |
| `AccessPath` growth | High | High |
| error/policy variant growth | Medium-High | Medium-High |
| recovery/commit evolution | Medium | Medium |
| cursor complexity | Medium-High | Medium-High |
| index type/path expansion | High | High |

## Final Output

1. Velocity Risk Index (1-10, lower is better): **7/10**

2. Architectural Drag Sources
- Access-path fan-out, grouped load orchestration concentration, and broad continuation surface.

3. Layer Leakage Findings
- No strict upward dependency leak; main drag comes from legitimate but broad cross-subsystem coordination.

4. Gravity Wells
- `executor/load/mod.rs`
- `query/plan/validate.rs`
- `query/intent/mod.rs`

5. Feature Friction Map
- Highest friction: new `AccessPath` variant, DESC/ordering expansion, and query caching.

6. Change Amplification Summary
- Architecture remains operable, but aggregate/grouped evolution now incurs consistently high multi-module coordination costs.
