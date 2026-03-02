# Structure / Module / Visibility Discipline Audit - 2026-03-02

Scope: `icydb-core` + facade (`icydb`) public surface, dependency direction, and visibility hygiene.

## 1. Public Surface Map

### 1A. Crate Root Enumeration

| Item | Path | Publicly Reachable From Root? | Intended Public API? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Core root `pub mod` entries (12 + `prelude`) | `crates/icydb-core/src/lib.rs` | Yes | Yes | Medium |
| Core DB root re-export lines (14) | `crates/icydb-core/src/db/mod.rs` | Yes | Yes | Medium |
| Facade root `pub mod` entries (model/obs/base/db/error/traits + hidden support) | `crates/icydb/src/lib.rs` | Yes | Mixed (`__macro`, `__reexports` are hidden support) | Medium |
| Facade DB exports (`Row`, response aliases, session types) | `crates/icydb/src/db/mod.rs` | Yes | Yes | Low |

### 1B. Exposure Classification

| Classification | Representative Items | Risk |
| ---- | ---- | ---- |
| API surface | `DbSession`, `Query`, `PlanError`, response types, aggregate builders | Low |
| Facade-support types | `__macro`, `__reexports`, build/schema re-exports | Medium (intentional hidden exports) |
| Internal plumbing with restricted visibility | `pub(crate)`, `pub(in ...)`, `pub(super)` runtime contracts | Medium |
| Accidental exposure candidates | public-field DTOs in diagnostics/explain/runtime trace surfaces | Medium |

### 1C. Public Field Exposure

| Type | Public Fields? | Leaks Internal Representation? | Risk |
| ---- | ---- | ---- | ---- |
| runtime trace / response DTOs | Yes | No raw store internals exposed | Low-Medium |
| explain/diagnostic DTOs | Yes | No raw storage payload surface by default | Low-Medium |
| internal planning/runtime structs under restricted modules | some public fields (restricted visibility scope) | contained inside crate boundary | Medium |

## 2. Subsystem Dependency Graph

### 2A. Dependency Direction

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `identity` / `types` | foundational types | almost all higher layers | Yes | Low |
| `serialize` / `data` | identity/types/storage contracts | index/executor/commit | Yes | Low-Medium |
| `index` | data + value/key encoding + predicate helpers | planner/executor/commit | Mostly | Medium |
| `query` (intent/plan/explain/fluent) | access + predicate + model + response | executor/session/facade | Yes | Medium |
| `executor` | query plan contracts + index/data + cursor + response | session/query boundary | Mostly (expected plan-consumption coupling) | Medium |
| `commit` / recovery | data/index/relation + marker protocol | db/executor mutation/read boundaries | Yes | Medium |
| facade crate (`icydb`) | `icydb-core` + schema/build crates | downstream users | Yes | Low |

### 2B. Circularity Findings

| Subsystem A | Subsystem B | Cycle? | Risk |
| ---- | ---- | ---- | ---- |
| query | executor | no compile-time cycle observed | Low |
| query | index/data/commit | no upward dependency from lower storage layers | Low |
| commit | planner | no direct planning-logic cycle observed | Low |

### 2C. Implementation Leakage

| Violation | Location | Description | Risk |
| ---- | ---- | ---- | ---- |
| Planner referencing executor internals | none found | non-comment `query/* -> executor/*` references: `0` | Low |
| Index/data/commit referencing query internals | none found | non-comment `index|data|commit -> query` references: `0` | Low |
| Query using runtime kernel symbols directly | none found | non-comment `ExecutionKernel|ExecutionPreparation|LoadExecutor` in `query/*`: `0` | Low |

## 3. Visibility Hygiene Findings

### 3A. Overexposure Metrics (runtime, non-test)

- Runtime Rust files audited: `194`
- Unrestricted `pub` declarations: `507`
- Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`): `1591`
- Public `struct/enum/trait/fn` declarations: `312`
- Public field declarations: `84`

Risk: Medium-High. Surface is intentionally broad, but visibility footprint remains large enough that accidental widening pressure is non-trivial.

### 3B. Under-Containment Signals

| Area | Signal | Risk |
| ---- | ---- | ---- |
| runtime hubs | 13 runtime files >=600 LOC | High |
| largest hubs | `executor/load/mod.rs` (1428), `query/plan/semantics.rs` (1190), `query/plan/validate.rs` (1108), `executor/load/projection.rs` (1076), `query/intent/mod.rs` (1074) | High |
| cross-domain concentration | grouped runtime/policy/continuation paths concentrated in a few modules | Medium-High |

### 3C. Test Leakage

No runtime module imported test-only utilities; checks stayed inside `#[cfg(test)]` boundaries.

## 4. Layering Violations

### 4A. No Upward References

| Layer | Upward Dependency Found? | Description | Risk |
| ---- | ---- | ---- | ---- |
| query -> executor | No | non-comment import/reference matches: `0` | Low |
| index/data/commit -> query | No | non-comment import/reference matches: `0` | Low |
| runtime canonicalization ownership | No | canonicalization implementation remains in `access/canonical.rs` | Low |

### 4B. Plan / Execution Separation

| Rule | Status | Notes |
| ---- | ---- | ---- |
| intent independent of executor internals | Holds | query intent lowers to plan contracts only |
| planner independent of commit internals | Holds | no commit-layer planning ownership leaks |
| executor mutates planner semantics | Not observed | executor consumes validated plans and enforces invariants |

### 4C. Facade Containment

| Facade Item | Leaks Core Internal? | Risk |
| ---- | ---- | ---- |
| `icydb::db` exports | No raw storage internals exposed | Low |
| `icydb::__macro` | intentionally exposes narrow runtime wiring | Medium (intentional hidden surface) |
| `icydb::__reexports` | dependency wiring support only | Medium (intentional hidden surface) |

## 5. Structural Pressure Areas

| Area | Pressure Type | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| access-path fan-out | `AccessPath::` references `187` across `17` runtime files | High | High |
| continuation surface | continuation/anchor mentions `611` across `66` runtime files | High | High |
| grouped policy growth | `GroupPlanError` surface remains broad (19 variants; used across planner+executor boundaries) | Medium-High | Medium-High |
| error mapping spread | `map_err(` appears in `168` callsites across `66` runtime files | Medium-High | Medium-High |

### 5A. Hub Import Pressure (Required)

Method: count subsystem-token imports (`access::`, `cursor::`, etc.) in each hub module.

| Hub Module | Top Imported Subsystems | Unique Sibling Subsystems Imported | Cross-Layer Dependency Count | Delta vs Previous Report | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | `query(13)`, `executor(11)`, `predicate(2)`, `access(2)`, `cursor(1)` | 11 | 10 | unique `+2`, cross-layer `+1` | High |
| `query/intent/mod.rs` | `query(7)`, `access(1)`, `cursor(1)`, `predicate(1)`, `response(1)` | 5 | 4 | unique `+2`, cross-layer `+1` | Medium-High |
| `query/plan/validate.rs` | `access(1)`, `cursor(1)`, `executor(1)`, `predicate(1)`, `query(1)` | 5 | 4 | unique `+2`, cross-layer `+1` | Medium-High |
| `executor/aggregate/mod.rs` | `executor(4)`, `contracts(1)`, `data(1)`, `direction(1)`, `index(1)` | 7 | 6 | unique `+3`, cross-layer `+2` | Medium-High |

Hub Import Pressure Index (`HIP = cross_layer / unique`):
- `executor/load/mod.rs`: `0.91` (high)
- `query/intent/mod.rs`: `0.80` (high)
- `query/plan/validate.rs`: `0.80` (high)
- `executor/aggregate/mod.rs`: `0.86` (high)

## 6. Encapsulation Risk Index

| Category | Risk Index (1-10, lower is better) |
| ---- | ---- |
| Public Surface Discipline | 5 |
| Layer Directionality | 2 |
| Circularity Safety | 2 |
| Visibility Hygiene | 7 |
| Facade Containment | 4 |

### Overall Structural Risk Index (1-10, lower is better)

**6/10**

## 7. Drift Sensitivity Summary

| Growth Vector | Affected Subsystems | Drift Risk |
| ---- | ---- | ---- |
| New `AccessPath` variant | access + planner + executor + cursor + explain + fingerprint | High |
| ORDER/DESC surface growth | query/plan + executor + cursor + index | High |
| Commit marker protocol evolution | commit + executor/mutation + recovery | Medium-High |
| Error taxonomy growth | query + executor + boundary mapping surfaces | Medium |

Summary: layer direction remains clean; main structural pressure is concentrated in large coordination hubs and high HIP ratios.
