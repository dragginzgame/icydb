# Structure / Module / Visibility Discipline Audit - 2026-02-24

Scope: public surface containment, layering discipline, and visibility hygiene.

## 1. Public Surface Mapping

### 1A. Crate Root Enumeration

| Crate Root | Public Surface Style | Risk |
| ---- | ---- | ---- |
| `crates/icydb-core/src/lib.rs` | broad module-level exports; db boundary centralized in `db/mod.rs` | Medium |
| `crates/icydb-core/src/db/mod.rs` | explicit tier-2 re-exports for query/session/response types | Medium |
| `crates/icydb/src/lib.rs` | facade re-export layer over core/types/macros | Low |

### 1B. Exposure Classification

| Classification | Representative Items | Risk |
| ---- | ---- | ---- |
| Stable API surface | `Query`, `PlanError`, `DbSession`, response types | Low |
| Internal wiring exports | many `pub(crate)` and `pub(in crate::db)` internals | Medium |
| Macro/re-export support | `icydb` facade extra exports | Medium-Low |

### 1C. Public Field Exposure

| Type | Public Fields? | Risk |
| ---- | ---- | ---- |
| `PagedLoadExecution<E>` | no public fields | Low |
| `PagedLoadExecutionWithTrace<E>` | no public fields | Low |
| `InternalError` | public diagnostic fields by design | Medium |

## 2. Subsystem Boundary Mapping

### 2A. Dependency Direction

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| query/plan/cursor | predicate/index/model/types | executor/session | Yes | Medium |
| executor/load/mutation | query plan + index + data + commit | db/session | Mostly | Medium |
| commit/recovery | data/index/relation | executor/db | Mostly | Medium |
| facade crate (`icydb`) | `icydb-core` + build/schema crates | downstream apps | Yes | Low |

### 2B. Circular Dependency Check

| Pair | Cycle Found? | Risk |
| ---- | ---- | ---- |
| query <-> executor | no compile-time cycle observed | Low |
| commit <-> planner | no direct cycle observed | Low |
| relation <-> commit | one-way via prepared ops/helpers | Low-Medium |

### 2C. Implementation Leakage

| Signal | Observation | Risk |
| ---- | ---- | ---- |
| Deep imports outside module boundary | limited; module root re-export discipline generally followed | Low |
| orchestration hub pressure | `db/mod.rs` remains high fan-in | Medium |

## 3. Visibility Hygiene Audit

### 3A. Overexposure

- `pub` declarations in non-test db files: **102**
- restricted visibility declarations (`pub(crate)`, `pub(in crate::db)`, `pub(super)`): **790**

Risk: Moderate. Surface is still mostly contained, but internal visibility footprint is large.

### 3B. Under-Containment Signals

| Signal | Evidence | Risk |
| ---- | ---- | ---- |
| Very large internal modules | `aggregate.rs` (1698 LOC), `route/mod.rs` (1163 LOC) | Medium-High |
| planner/logical hotspots still large | `planner/mod.rs` + `planner/range.rs` + `logical/mod.rs` | Medium |

### 3C. Test Leakage

- Test topology is now directory-module based (`executor/tests/pagination/*`), reducing single-file concentration.
- No public API leak detected from this test split.

## 4. Layering Integrity Validation

| Layer Rule | Status | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| plan should not depend on executor internals | holds | plan modules remain under `query/plan` with typed outputs | Low |
| executor should consume validated plans | holds | executor route/load use executable/logical plan artifacts | Low |
| mutation durability owned by commit/recovery | holds | commit window + marker/replay ownership unchanged | Low |

## 5. Structural Pressure Indicators

| Indicator | Current Value | Risk |
| ---- | ---- | ---- |
| non-test db files >150 LOC | 69 | Medium-High |
| non-test db files >300 LOC | 49 | Medium-High |
| largest non-test db module | `aggregate.rs` 1698 LOC | High |
| pagination test suite total | 7944 LOC | Medium (test maintenance pressure) |

## 6. Encapsulation Risk Index

| Category                  | Risk Index (1-10, lower is better) |
| ------------------------- | ----------------------------------- |
| Public Surface Discipline | 4 |
| Layer Directionality      | 4 |
| Circularity Safety        | 2 |
| Visibility Hygiene        | 6 |
| Facade Containment        | 3 |

### Overall Structural Risk Index (1-10, lower is better)

**5/10**

## 7. Drift Sensitivity Analysis

| Drift Vector | Current Signal | Risk |
| ---- | ---- | ---- |
| aggregate terminal expansion | `0.28.0/0.28.1` added projection terminal family in one large module | Medium-High |
| route hub complexity | high LOC remains in route orchestrator | Medium-High |
| API surface growth | additive load terminals only; no new query mode | Low-Medium |

## 8. Structural Risk Index

- Overall: **5/10**
- Trend vs 2026-02-22: slightly higher structural pressure due concentrated growth in executor aggregate module.
