# Velocity Preservation Audit - 2026-03-05 (Method V3 Rerun)

Scope: feature agility and change amplification across planner/route/executor/index/cursor surfaces in the current working tree.

Method changes in this rerun:
- Revised CAF to reduce subsystem/layer double-counting (`max(subsystems, layers) × axes`).
- Added Containment Score (cross-subsystem blast radius).
- Added density-adjusted enum shock radius.
- Added independent-axis tracking (not all axes treated as independent).
- Added gravity-well edit frequency and size-adjusted subsystem independence.
- Added decision surface size metric.
- Updated weighted risk model per method V3.

## Step 0 - Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | 5.0 | 5.0 | 0.0 |
| Cross-layer leakage crossings (tracked boundaries) | 1 | 1 | 0 |
| Average files touched per feature slice | 3.0 | 3.0 | 0.0 |
| p95 files touched (slice-sampled) | 5 | 5 | 0 |
| Top gravity-well fan-in proxy | 30 | 30 | 0 |

## Step 1 - Revised CAF + ELS + Containment

Total subsystems used for containment normalization in this report: `5` (`planner`, `executor`, `cursor`, `index`, `recovery`).

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| Access-stream hub split (`access/mod.rs` -> `bindings.rs` + `traversal.rs`) | 3 | 1 | 1 | 1 | 1 | 1.00 | 0.20 | Low |
| Scalar continuation pre-resolution moved to load entrypoint coordinator | 5 | 3 | 1 | 2 | 6 | 0.40 | 0.60 | Medium |
| Structural guard tests for router/load continuation boundaries | 1 | 1 | 1 | 1 | 1 | 1.00 | 0.20 | Low |

## Step 2 - Boundary Leakage (Mechanical)

| Boundary | Import Crossings | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner -> executor types | 0 | 0 | 0 | Low |
| executor -> planner validation helpers | 1 | 1 | 0 | Medium-Low |
| index -> query-layer types | 0 | 0 | 0 | Low |
| cursor -> executable plan internals | 0 | 0 | 0 | Low |

## Step 3 - Gravity Well Growth + Edit Frequency

Fan-in is proxy-count based in this rerun.

| Module | LOC | LOC Delta | Fan-In Proxy | Fan-In Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `executor/load/mod.rs` | 622 | -242 | 30 | N/A | 5 | 84 | Medium (active coordination hub) |
| `executor/continuation/mod.rs` | 609 | +3 | 8 | N/A | 3 | 9 | Medium |
| `access/execution_contract.rs` | 732 | 0 | 4 | N/A | 3 | 2 | Medium-Low |

## Step 4 - Change Multiplier Matrix (Deterministic)

| Feature Axis | Planner | Executor | Cursor | Index | Recovery | Subsystem Count |
| ---- | ---- | ---- | ---- | ---- | ---- | ----: |
| ordering | yes | yes | yes | yes | no | 4 |
| cursor | yes | yes | yes | no | no | 3 |
| index type | yes | no | no | yes | yes | 3 |
| recovery mode | no | yes | no | yes | yes | 3 |
| uniqueness | yes | yes | no | yes | yes | 4 |

## Step 5 - Enum Shock Radius (Density-Adjusted)

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| `AccessPath` | 6 | 31 | 15 | 2.07 | 5 | 62.0 | High |
| `ExecutionOrdering` | 3 | 1 | 8 | 0.13 | 4 | 1.5 | Medium-Low |
| `ContinuationMode` | 3 | 2 | 3 | 0.67 | 1 | 2.0 | Medium-Low |
| `RouteShapeKind` | 5 | 1 | 7 | 0.14 | 1 | 0.7 | Low |

## Step 6 - Edit Blast Radius (Slice-Sampled)

Feature-slice sample used: `[3, 5, 1]` files modified.

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice | 3.0 | 3.0 | 0.0 |
| median files touched | 3 | 3 | 0 |
| p95 files touched | 5 | 5 | 0 |

## Step 7 - Size-Adjusted Subsystem Independence

| Subsystem | Internal Imports | External Imports | LOC | Independence | Adjusted Independence | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| executor | 166 | 48 | 20,239 | 0.7757 | 7.6913 | Medium |
| query | 158 | 18 | 12,697 | 0.8977 | 8.4825 | Low |
| index | 2 | 2 | 4,172 | 0.5000 | 4.1681 | Medium |
| cursor | 31 | 0 | 2,601 | 1.0000 | 7.8637 | Low |
| access | 1 | 0 | 2,676 | 1.0000 | 7.8921 | Low |

## Step 8 - Decision-Axis Growth (Independence-Aware)

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |
| Load execution | cursor, access path, ordering, shape | 4 | 3 | N/A | N/A | Medium-High |
| Cursor pagination | cursor shape, ordering, resume mode | 3 | 2 | N/A | N/A | Medium |
| Index mutation | index type, uniqueness, recovery mode | 3 | 2 | N/A | N/A | Medium |
| Recovery replay | mutation type, index state, relation mode | 3 | 2 | N/A | N/A | Medium |

## Step 9 - Decision Surface Size

| Enum | Decision Sites | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 31 | 31 | 0 | High |
| `ContinuationMode` | 2 | 2 | 0 | Medium-Low |
| `ExecutionOrdering` | 1 | 1 | 0 | Low |
| `RouteShapeKind` | 1 | 1 | 0 | Low |

## Step 10 - Refactor Noise Filter

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| Access-stream split increased touched files in local slice | Up | Structural improvement | Locality and hub pressure improved |
| Continuation refactor increased mentions but kept boundary leak counters flat | Up | Mixed (not pure transient) | main residual drag is execution fan-out, not new leakage |
| Cross-layer leakage counters | Flat | Stable | no new leakage debt added |

## Step 11 - Weighted Velocity Risk Index (V3 Weights)

Weights:
- enum shock radius x3
- CAF trend x2
- cross-layer leakage x2
- gravity-well growth x2
- edit blast radius x1

| Area | Score (1-10) | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| Enum shock radius | 6.0 | 3 | 18.0 |
| CAF trend | 5.0 | 2 | 10.0 |
| Cross-layer leakage | 4.0 | 2 | 8.0 |
| Gravity-well growth | 5.0 | 2 | 10.0 |
| Edit blast radius | 5.0 | 1 | 5.0 |

Weighted total: `51 / 10 = 5.10`

## Velocity Risk Index

**5/10**

Key conclusion:
- The revised method removes CAF inflation and better captures real velocity drag: `AccessPath` decision surface/shock radius and load-hub coordination frequency remain the principal long-term constraints.
