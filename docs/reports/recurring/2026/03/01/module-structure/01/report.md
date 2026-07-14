# Structure / Module / Visibility Discipline Audit - 2026-03-01

Scope: `icydb-core` + facade (`icydb`) public surface, dependency direction, and visibility hygiene.

## 1. Public Surface Mapping

### 1A. Crate Root Enumeration

| Item | Path | Publicly Reachable From Root? | Intended Public API? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `pub mod` roots in core crate (12 + prelude) | `crates/icydb-core/src/lib.rs` | Yes | Yes | Medium |
| `db` root re-export surface (`pub use` lines: 16) | `crates/icydb-core/src/db/mod.rs` | Yes | Yes | Medium |
| facade root modules + hidden support modules | `crates/icydb/src/lib.rs` | Yes | Mixed (`__macro`, `__reexports` are hidden support) | Medium |
| facade db exports (`Row`, response/session aliases) | `crates/icydb/src/db/mod.rs` | Yes | Yes | Low |

### 1B. Exposure Classification

| Classification | Representative Items | Risk |
| ---- | ---- | ---- |
| API surface | `DbSession`, `Query`, `PlanError`, response types, aggregate builders | Low |
| Facade-support types | `__macro`, `__reexports`, build/schema re-exports | Medium (intentional hidden exports) |
| Internal plumbing with restricted visibility | `pub(in crate::db)`, `pub(crate)` across runtime internals | Medium |
| Accidental exposure candidates | public-field DTOs in diagnostics/explain/runtime trace surfaces | Medium |

### 1C. Public Field Exposure

| Type | Public Fields? | Leaks Internal Representation? | Risk |
| ---- | ---- | ---- | ---- |
| `ExecutionTrace` | Yes (public metrics fields) | No raw storage leakage | Low-Medium |
| explain/diagnostic DTOs (`ExplainPlan`, `StorageReport` subtypes) | Yes | No raw key bytes exposed directly | Low-Medium |
| internal planning structs (`IndexPlan`, relation metadata, etc.) | some public fields under restricted modules | limited to crate boundaries | Medium |

## 2. Subsystem Boundary Mapping

### 2A. Dependency Direction

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `identity` / `types` | foundational types | most higher layers | Yes | Low |
| `serialize` / `data` | identity/types, storage contracts | index/executor/commit | Yes | Low-Medium |
| `index` | data + value/key encoding + predicate helpers | planner/executor/commit | Mostly | Medium |
| `query` (intent/plan/explain/fluent) | access + predicate + model + response | executor/session/facade | Yes (no runtime executor imports) | Medium |
| `executor` | query plan contracts + index/data + cursor + response | db/session entrypoints | Mostly (expected plan-consumption coupling) | Medium |
| `commit` / recovery | data/index/relation + marker protocol | db/executor mutation/read guards | Yes | Medium |
| facade crate (`icydb`) | `icydb-core` + schema/build crates | downstream consumers | Yes | Low |

### 2B. Circular Dependency Check

| Subsystem A | Subsystem B | Cycle? | Risk |
| ---- | ---- | ---- | ---- |
| query | executor | no compile-time cycle observed | Low |
| query | index/data/commit | no upward-back import from lower storage layers | Low |
| commit | query planner | no direct cycle observed | Low |

### 2C. Implementation Leakage

| Violation | Location | Description | Risk |
| ---- | ---- | ---- | ---- |
| Planner referencing executor internals | not found | query modules do not import runtime executor internals | Low |
| Executor relying on query plan internals | `executor/load/*`, `executor/executable_plan.rs` | expected typed coupling at execution boundary | Medium |
| Index layer referencing query abstractions | not found in non-test index/data/commit paths | clean downward direction | Low |

## 3. Visibility Hygiene Audit

### 3A. Overexposure

- Unrestricted `pub` declarations in non-test `db/`: **491**
- Restricted visibility declarations (`pub(crate)`, `pub(in ...)`, `pub(super)`): **1502**
- Public `struct/enum/trait/fn` declarations in non-test `db/`: **304**
- Public field lines in non-test `db/`: **82**

Risk: Medium-High. The API is intentionally broad in DTO/explain surfaces; internal visibility footprint is large and requires discipline to avoid accidental boundary widening.

### 3B. Under-Containment Signals

| Signal | Evidence | Risk |
| ---- | ---- | ---- |
| Large hub modules | `executor/load/mod.rs` (1198 LOC), `query/intent/mod.rs` (1006), `query/plan/validate.rs` (914), `query/plan/planner.rs` (827) | High |
| Subsystems importing many siblings | `executor/load/mod.rs` imports across access/cursor/data/direction/index/predicate/query/response/contracts | High |
| Multi-domain concentration | grouped execution orchestration and policy validation concentrated in few files | Medium-High |

### 3C. Test Leakage

- No runtime module was found importing test-only utilities.
- Test helper exposure appears bounded to test modules.

## 4. Layering Integrity Validation

### 4A. No Upward References

| Layer | Upward Dependency Found? | Description | Risk |
| ---- | ---- | ---- | ---- |
| query -> executor | No (non-comment matches: 0) | clean semantic/runtime split | Low |
| index/data/commit -> query | No (non-comment matches: 0) | lower layers do not depend upward | Low |
| access canonicalization ownership | No violation | implementation remains in `access/` | Low |

### 4B. Plan / Execution Separation

| Rule | Status | Notes |
| ---- | ---- | ---- |
| intent independent of executor runtime types | Holds | intent compiles to logical/access query model |
| planner independent of commit internals | Holds | no commit-layer dependency in planner modules |
| executor mutating plan types | Not observed | executor consumes validated plans and emits responses/traces |

### 4C. Facade Containment

| Facade Item | Leaks Core Internal? | Risk |
| ---- | ---- | ---- |
| `icydb::db` re-exports | no raw storage/internal key bytes | Low |
| `icydb::__macro` hidden module | intentionally exposes narrow runtime wiring | Medium (intentional, hidden) |
| `icydb::__reexports` hidden module | dependency wiring support only | Medium (intentional, hidden) |

## 5. Structural Pressure Indicators

| Area | Pressure Type | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| large runtime hubs | size and multi-domain concentration | High | High |
| access-path fan-out | many modules depend on access-path semantics | High | High |
| grouped policy surface | growing grouped validation and execution branches | High | High |
| public-field DTO spread | broad but mostly intentional explain/diagnostic surfaces | Medium | Medium |

## 6. Structural Risk Index

| Category | Risk Index (1-10, lower is better) |
| ---- | ---- |
| Public Surface Discipline | 5 |
| Layer Directionality | 3 |
| Circularity Safety | 2 |
| Visibility Hygiene | 7 |
| Facade Containment | 4 |

### Overall Structural Risk Index (1-10, lower is better)

**6/10**

Trend vs 2026-02-24: higher pressure from larger runtime hubs and grouped feature surface expansion, while directional layering remains clean.
