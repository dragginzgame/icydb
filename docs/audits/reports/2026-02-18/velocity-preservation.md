# Velocity Preservation Audit - 2026-02-18

Scope: architectural change agility and cross-layer amplification risk in `icydb-core`.

## 1. Change Surface Mapping (Baseline Footprint)

| Feature Area | Files Modified Footprint | Subsystems Touched | Cross-Layer? | Localized? | Change Amplification Factor |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Range pushdown / index range execution | 23 | 3 (`query`, `executor`, `index`) | Yes | No | 9 (3 subsystems x 3 flows) |
| Cursor pagination / continuation | 10 | 3 (`db-root`, `query`, `executor`) | Yes | No | 12 (3 subsystems x 4 flows) |
| Reverse relation index lifecycle | 9 | 4 (`relation`, `commit`, `executor`, `db-root`) | Yes | No | 12 (4 subsystems x 3 flows) |
| Unique enforcement | 4 | 1 (`index`) | Mostly no | Yes | 2 (1 subsystem x 2 flows) |
| Commit marker protocol and apply/recovery boundary | 13 | 3 (`commit`, `executor`, `index`) | Yes | No | 9 (3 subsystems x 3 flows) |

CAF flags (`>6`): range pushdown, cursor pagination, reverse relation, commit marker.

## 2. Layer Boundary Integrity (Velocity-Oriented)

| Boundary | Leakage Type | Velocity Impact | Severity |
| ---- | ---- | ---- | ---- |
| Planner -> executor | defensive re-validation required in executor | Medium coordination overhead | Medium |
| Cursor codec -> executable plan | signature/direction coupling | Medium coordination overhead | Medium |
| Recovery -> prepare hooks | replay coupled to prepare semantics | Medium-high change coordination | Medium |
| Index store -> continuation direction | direction-aware API contained but DESC pending | Low currently; future-sensitive | Low-Medium |

Evidence: `crates/icydb-core/src/db/executor/load/mod.rs:132`, `crates/icydb-core/src/db/query/plan/continuation.rs:382`, `crates/icydb-core/src/db/commit/recovery.rs:94`, `crates/icydb-core/src/db/index/store/lookup.rs:177`.

## 3. Growth Vector and Gravity Wells

| Module | Responsibilities | Import Fan-In | Import Fan-Out | Growth Rate | Bottleneck Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `query/plan/executable.rs` | cursor planning, envelope checks, executable boundary | High | Medium | Moderate | High |
| `executor/load/mod.rs` | fast paths, fallback path, cursor pagination | High | High | High | High |
| `commit/recovery.rs` | replay protocol, startup rebuild, rollback-on-fail | Medium | Medium | Moderate | Medium-High |
| `index/store/lookup.rs` | prefix/range resolution + continuation advancement | Medium | Medium | Moderate | Medium |

Signals:
- `ExecutablePlan` referenced in 8 non-test db files.
- `Direction` referenced in 20 non-test db files.
- `AccessPath` referenced in 21 non-test db files.

## 4. Change Multiplier Analysis

| Feature | Subsystems Likely Impacted | Change Surface Size | Friction Level |
| ---- | ---- | ---- | ---- |
| Composite pagination | `query`, `executor`, `cursor`, `index` | Medium-Large | High |
| DESC support activation | `query`, `executor`, `index` | Medium | Moderate-High |
| Secondary index ordering extensions | `query`, `executor`, `index` | Medium | Moderate |
| Query caching | `query`, `executor`, response/session layer | Medium | Moderate |
| Multi-index intersection enhancements | `query`, `executor` | Medium | Moderate |
| New commit phase | `executor`, `commit`, `recovery` | Medium-Large | High |
| New `AccessPath` variant | `query`, `executor`, explain/fingerprint/projection | Large fan-out | High |

## 5. Amplification Hotspots

| Amplification Source | Why It Multiplies Change | Risk |
| ---- | ---- | ---- |
| `AccessPath` fan-out | one variant change ripples across planner, executor, explain, fingerprint, validation | High |
| Cursor + continuation + signature model | continuation changes require codec, planner validation, executor handling updates | Medium-High |
| Commit marker/replay protocol | mutation sequencing changes impact save/delete + recovery equivalence | Medium-High |
| Reverse relation integrity | save/delete/recovery all need consistent updates | Medium-High |

## 6. Drift Sensitivity Index

| Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- |
| AccessPath growth | High | High |
| PlanError growth (24 variants) | Medium | Medium |
| Recovery evolution | Medium-High | Medium-High |
| Cursor complexity | Medium-High | Medium-High |
| Index type expansion | Medium | Medium |

## 7. Velocity Risk Table

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy (High-Level Only) |
| ---- | ---- | ---- | ---- | ---- |
| Access path fan-out | Multi-site edits for one semantic expansion | High | High | Keep execution-direction and bound logic centralized |
| Commit/recovery coupling | Protocol changes span mutation + replay | Medium-High | Medium-High | Preserve shared prepare/apply path |
| Cursor continuation semantics | Token shape and execution checks must stay aligned | Medium-High | Medium-High | Keep structural checks in single plan boundary |
| Loader fast-path matrix | New optimizations expand gating branches | Medium | Medium | Gate optimizations through common projection/validation helpers |

## Velocity Risk Index

Velocity Risk Index (1-10, lower is better): **6/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Summary:
- Velocity is still manageable, but feature work is no longer uniformly localized.
- Main drag is change amplification across `AccessPath`, cursor continuation, and commit/recovery boundaries.
