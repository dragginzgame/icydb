# Velocity Preservation Audit - 2026-02-24

Scope: change amplification and extension friction.

## Step 1 - Change Surface Mapping (Empirical)

| Feature Slice | Files Modified | Subsystems Touched | Cross-Layer? | Change Amplification Factor |
| ---- | ---- | ---- | ---- | ---- |
| `0.28.0` `values_by` introduction (`348dc512`) | 10 files | executor + fluent query + session + facade + docs | Yes | Medium |
| `0.28.1` projection follow-up (`2e1ab6e7`) | 7 files | same projection stack + tests + changelog/status | Yes | Medium |
| distinct ordering + parity lock tests | concentrated in aggregate tests | executor + session behavior contracts | Yes | Medium |

## Step 2 - Layer Boundary Integrity (Velocity-Oriented)

| Boundary | Leakage Type | Velocity Impact | Severity |
| ---- | ---- | ---- | ---- |
| planner -> executor | low leakage (typed executable plan) | low friction | Low |
| executor -> plan internals | moderate coupling for route gates | medium friction | Medium |
| projection terminal API -> execution core | intentional additive wrappers | low-medium friction | Low-Medium |
| commit/recovery coupling | durable by design; high correctness constraints | medium-high friction | Medium-High |

## Step 3 - Growth Vector & Gravity Well Detection

| Module | Responsibilities | Growth Pressure | Bottleneck Risk |
| ---- | ---- | ---- | ---- |
| `executor/load/aggregate.rs` | aggregates + field projection terminals | High | High |
| `executor/route/mod.rs` | route ownership + dispatch | High | High |
| `query/plan/logical/mod.rs` | post-access semantics and pagination | Medium-High | Medium-High |
| `query/cursor/spine.rs` | compatibility + envelope checks | Medium | Medium |

## Step 4 - Change Multiplier Analysis

| Future Feature | Subsystems Likely Impacted | Friction Level |
| ---- | ---- | ---- |
| multi-field projection (`0.29+`) | fluent/session/executor + tests | Medium-High |
| typed projection wrappers (`0.29+`) | API + conversion + error surface | High |
| `group_by` | planner + executor + terminal APIs + pushdown + tests | High |
| new AccessPath variant | planner + executor + explain + cursor + tests | High |

## Step 5 - Amplification Hotspots

| Amplification Source | Why It Multiplies Change | Risk |
| ---- | ---- | ---- |
| AccessPath fan-out (17 files / 163 refs) | path additions propagate broadly | High |
| Projection implementation concentration | additive terminal APIs currently centralize in one large file | Medium-High |
| Route orchestrator size | multiple feature edits collide in same module | High |
| Cursor compatibility protocol | token evolution needs strict multi-layer updates | Medium-High |

## Step 6 - Predictive Structural Stress Points

| Subsystem | Stress Vector | Risk Level |
| ---- | ---- | ---- |
| executor/load aggregate | terminal surface growth concentration | High |
| executor route | branch coordination cost | High |
| query/cursor | compatibility drift sensitivity | Medium |
| commit/recovery | replay-equivalence maintenance overhead | Medium-High |

## Step 7 - Velocity Risk Table

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy |
| ---- | ---- | ---- | ---- | ---- |
| additive terminal growth in single hub | higher merge/review collision probability | Medium-High | High | keep projection core abstractions narrow |
| route-level coordination | many execution features touch one owner | High | High | preserve route ownership discipline |
| future typed projection | cross-layer conversion/error design required | High | High | defer until projection core stabilizes |
| AccessPath evolution | planner/executor/test impact each change | High | High | preserve explicit path modeling |

## Step 8 - Drift Sensitivity Index

| Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- |
| projection API growth | Medium | Medium |
| route complexity | High | High |
| cursor protocol evolution | Medium | Medium |
| commit/recovery evolution | High | High |

## Final Output

1. Velocity Risk Index (1-10, lower is better): **6/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

2. Architectural Drag Sources
- route ownership hotspot, AccessPath fan-out, aggregate terminal concentration.

3. Layer Leakage Findings
- No critical upward dependency leaks found.

4. Gravity Wells
- `executor/load/aggregate.rs`, `executor/route/mod.rs`.

5. Feature Friction Map
- highest friction: typed projection and `group_by`.

6. Change Amplification Summary
- `0.28.x` projection additions stayed additive and contained, but concentration in executor aggregate path increases medium-term extension friction.
