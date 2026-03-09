# Velocity Preservation Audit - 2026-03-06

Scope: feature agility and change amplification across planner/route/executor/access/cursor surfaces in the current `0.43` BYTES work.

Method: same revised CAF model used in `2026-03-05` (`revised_caf = max(subsystems, layers) × flow_axes`) with slice-sampled metrics from active feature slices.

## Step 0 - Baseline Capture

| Metric | Previous (2026-03-05) | Current (2026-03-06) | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index | 5.0 | 5.0 | 0.0 |
| Cross-layer leakage crossings (tracked boundaries) | 1 | 1 | 0 |
| Avg files touched per feature slice | 3.0 | 2.3 | -0.7 |
| p95 files touched | 5 | 3 | -2 |
| Top gravity-well fan-in proxy | 30 | 35 | +5 |

## Step 1 - Revised CAF + ELS + Containment

Containment normalization denominator in this run: `5` subsystem families (`planner`, `executor`, `cursor`, `index`, `access`).

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |
| Scalar `bytes()` terminal surface + wiring | 3 | 2 | 2 | 1 | 2 | 0.67 | 0.40 | Medium |
| `bytes()` parity matrix expansion | 3 | 1 | 1 | 1 | 1 | 1.00 | 0.20 | Low |
| Saturation helper hardening in load terminal | 1 | 1 | 1 | 1 | 1 | 1.00 | 0.20 | Low |

## Step 2 - Boundary Leakage (Mechanical)

| Boundary | Import/Type Crossings | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| planner -> executor types | 1 | 1 | 0 | Medium-Low |
| executor -> planner validation helpers | 1 | 1 | 0 | Medium-Low |
| index -> query-layer types | 0 | 0 | 0 | Low |
| cursor -> executable-plan internals | 0 | 0 | 0 | Low |

## Step 3 - Gravity Well Growth

| Module | LOC | Previous LOC | Delta | Fan-In Proxy | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- |
| `executor/load/mod.rs` | 625 | 622 | +3 | 35 | Medium |
| `executor/continuation/mod.rs` | 608 | 609 | -1 | 8 | Medium-Low |
| `access/execution_contract.rs` | 1067 | 732 | +335 | 2 | Medium (size pressure) |

## Step 4 - Enum Shock Radius (Mechanical)

| Enum | Variants | Reference Sites | Subsystems Involved | Shock Signal | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- |
| `AccessPath` | 7 | 116 | 4 | high | High |
| `ContinuationMode` | 3 | 2 | 2 | low | Low-Medium |
| `RouteShapeKind` | 5 | 1 | 1 | low | Low |

## Velocity Risk Index

**5/10**

Key conclusion:
- Velocity remains in the same moderate band as `2026-03-05`.
- `0.43` BYTES slices are mostly contained; long-term drag is still `AccessPath` decision-surface size and load-hub concentration.
