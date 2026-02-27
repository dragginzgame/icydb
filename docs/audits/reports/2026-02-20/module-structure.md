# Structure / Module / Visibility Audit - 2026-02-20

Scope: layering, exposure boundaries, visibility hygiene, and facade containment.

## 1A. Public Surface Mapping

| Item | Path | Publicly Reachable From Root? | Intended Public API? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `db` module | `crates/icydb-core/src/lib.rs:11` | Yes | Yes | Low |
| `error`, `model`, `types`, `value`, `traits` | `crates/icydb-core/src/lib.rs:12`-`20` | Yes | Yes | Low |
| query/session API types (`Query`, `SessionLoadQuery`, `PagedLoadQuery`) | `crates/icydb-core/src/db/mod.rs:27`-`39` | Yes | Yes | Low |
| execution trace surface (`ExecutionTrace`, `ExecutionOptimization`) | `crates/icydb-core/src/db/mod.rs:22` | Yes | Yes | Low-Medium |
| commit/index/relation internals | module-scoped (`pub(in crate::db)`/`pub(crate)`) | No | Internal | Low |

## 1B. Exposure Classification

| Classification | Representative Items | Risk |
| ---- | ---- | ---- |
| API Surface | query builders, predicate AST, response types | Low |
| Facade-support type | execution trace metadata, storage report | Low-Medium |
| Internal plumbing (not root-exposed) | commit protocol, recovery, executor internals | Low |
| Accidentally exposed | none critical observed | Low |

## 1C. Public Field Exposure

| Type | Public Fields? | Leaks Internal Representation? | Risk |
| ---- | ---- | ---- | ---- |
| `Db<C>` | no public fields | No | Low |
| `EntityRuntimeHooks<C>` | internal visibility fields | No root leak | Low |
| `InternalError` | public fields (`class`, `origin`, `message`, `detail`) | intentional diagnostic surface | Medium |

## 2A. Dependency Direction

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| identity/types | value/serialize (light) | query/index/executor | Yes | Low |
| serialize | types/value | data/index/query/executor | Yes | Low |
| data | identity/serialize | index/executor/commit | Yes | Low |
| index | data/types/query-plan helpers | planner/executor/commit/relation | Mostly | Medium |
| query intent/plan | predicate/index/model | executor/session | Yes | Low-Medium |
| executor | query plan/index/data | db facade | Yes | Medium |
| commit/recovery | data/index/relation/executor-agnostic prepare | db facade + executors | Mostly | Medium |
| facade (`icydb`) | re-exports core | external callers | Yes | Low |

## 2B. Circular Dependency Check

| Subsystem A | Subsystem B | Cycle? | Risk |
| ---- | ---- | ---- | ---- |
| query <-> executor | No direct compile cycle observed | Low |
| commit <-> planner | No | Low |
| index <-> query | one-way helper use (no cycle) | Low-Medium |

## 2C. Implementation Leakage

| Violation | Location | Description | Risk |
| ---- | ---- | ---- | ---- |
| None critical | n/a | no direct planner->executor internals imports detected | Low |
| Minor coupling | `db/mod.rs` orchestration hub | facade wires many internals by design | Medium |

## 3A. Visibility Hygiene (Overexposure Signals)

| Item | Current Visibility | Could Be Narrower? | Risk |
| ---- | ---- | ---- | ---- |
| Many db internals | `pub(crate)` / `pub(in crate::db)` | some possibly narrower over time | Medium |
| Root exports in `db/mod.rs` | `pub use ...` | generally intentional | Low |
| helper constructors on internal errors | `pub(crate)` | ownership-appropriate | Low |

Counts (non-test `db`):
- `pub ` declarations: 99
- restricted visibility (`pub(crate)`, `pub(in crate::db)`, `pub(super)`): 315

## 4A. No Upward References

| Layer | Upward Dependency Found? | Description | Risk |
| ---- | ---- | ---- | ---- |
| data -> planner/executor | No | none observed | Low |
| index -> executor internals | No | none observed | Low |
| recovery -> planner semantics | No direct planner dependency | Low |

## 4B. Plan / Execution Separation

| Property | Status | Risk |
| ---- | ---- | ---- |
| intent depends on execution internals | No | Low |
| planner depends on commit internals | No | Low |
| executor mutates plan definitions | No (revalidation only) | Low |
| cursor protocol owns continuation compatibility | Yes (`cursor_spine`) | Low |

## 4C. Facade Containment

| Facade Item | Leaks Core Internal? | Risk |
| ---- | ---- | ---- |
| `icydb` re-exports domain APIs | No critical leak | Low |
| `__macro` / `__reexports` helper namespaces | intentional macro support surface | Medium-Low |

## 5. Structural Pressure Indicators

| Area | Pressure Type | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| `db/mod.rs` | orchestration hub imports many subsystems | Medium | Medium |
| planner/logical modules | large module surface | High | Medium-High |
| error taxonomy ownership | broad use across layers | Medium | Medium |

## 6. Encapsulation Risk Index

| Category | Risk Index (1-10, lower is better) |
| ---- | ---- |
| Public Surface Discipline | 3 |
| Layer Directionality | 4 |
| Circularity Safety | 2 |
| Visibility Hygiene | 5 |
| Facade Containment | 3 |

Overall Structural Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
