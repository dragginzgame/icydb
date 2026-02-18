# Structure / Module / Visibility Audit - 2026-02-18

Scope: `icydb-core` boundary discipline, layering direction, visibility hygiene, and facade containment.

## 1. Public Surface Mapping

### 1A. Crate-root Exposure

| Item | Path | Publicly Reachable From Root? | Intended Public API? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Core subsystem modules (`db`, `error`, `model`, `types`, etc.) | `crates/icydb-core/src/lib.rs:11` | Yes | Yes | Low |
| `db` API re-exports (`Query`, `PlanError`, `Response`, `DataStore`, `IndexStore`) | `crates/icydb-core/src/db/mod.rs:18` | Yes | Yes | Low |
| Commit/recovery internals | `crates/icydb-core/src/db/mod.rs:10` | No (`pub(in crate::db)`) | Internal | Low |
| Executor internals | `crates/icydb-core/src/db/mod.rs:14` | No (`pub(in crate::db)`) | Internal | Low |
| Query deep modules (`plan`, `predicate`, `intent`) | `crates/icydb-core/src/db/query/mod.rs:18` | No (`pub(crate)`) | Internal to crate | Low |

### 1B. Facade Containment (`icydb`)

| Facade Item | Leaks Core Internal? | Risk |
| ---- | ---- | ---- |
| `icydb::db` session/query surface | No | Low |
| `icydb::__macro` wiring exports | Narrow and intentional | Low |
| Raw storage types (`Raw*`) | Not re-exported in facade surface | Low |

Evidence: `crates/icydb/src/lib.rs:119`, `crates/icydb/src/db/mod.rs:1`, `crates/icydb/src/lib.rs:136`.

## 2. Subsystem Boundary Mapping

### 2A. Dependency Direction

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `data` | local key/store codecs | `executor`, `commit`, `relation` | Yes | Low |
| `index` | `identity`, `data` key types | `query`, `executor`, `commit`, `relation` | Yes | Low |
| `query` | `index`, `cursor` plan hooks | `executor`, db root/session | Yes | Low |
| `executor` | `query` plan + `commit` + `index` + `data` | db root | Yes | Medium |
| `commit` | `index`/`data`/`relation`/db hooks | `executor`, db root | Yes | Medium |
| `relation` | `data`/`index` + db hooks | `commit`, db root | Yes | Medium |

Checks for explicit upward leakage:
- No `db::query` usage in `data`, `index`, `commit`, or `relation` from grep sweep.
- No `db::executor` or `db::commit` imports in `query`.

### 2B. Circular Dependency Check

| Subsystem A | Subsystem B | Cycle? | Risk |
| ---- | ---- | ---- | ---- |
| `query` | `executor` | No compile-time cycle detected | Low |
| `commit` | `executor` | No compile-time cycle detected | Low |
| `relation` | `query` | No compile-time cycle detected | Low |

## 3. Visibility Hygiene

### 3A. Overexposure Signals

| Item | Current Visibility | Could Be Narrower? | Risk |
| ---- | ---- | ---- | ---- |
| `Db` runtime/session entrypoints | `pub` | No (facade API) | Low |
| `EntityRuntimeHooks` fields | mostly crate-scoped | No | Low |
| Diagnostics snapshot structs with public fields | `pub` fields | Possibly, but intended report payload | Low |
| `query::plan::Explain*` structs/enums | `pub` fields | Intended explain payload | Low |

### 3B. Test Leakage

No runtime module imports test-only helpers from `#[cfg(test)]` modules were found in this sweep.

## 4. Layering Integrity Validation

| Layer Check | Upward Dependency Found? | Description | Risk |
| ---- | ---- | ---- | ---- |
| Data layer references planner/executor | No | none found by grep | Low |
| Index layer references planner/executor internals | No | none found by grep | Low |
| Query layer references executor/commit internals | No | none found by grep | Low |
| Recovery references planner semantics | No | replay uses commit preparation hooks, not planner AST | Low |

Plan/execution separation status:
- Planner emits `ExecutablePlan` and cursor validation boundaries (`crates/icydb-core/src/db/query/plan/executable.rs:111`).
- Executor re-validates defensively and executes plan shape (`crates/icydb-core/src/db/executor/load/mod.rs:132`).
- Access-path direction is execution data, not AccessPath variant fan-out (`crates/icydb-core/src/db/index/range.rs:81`).

## 5. Structural Pressure Indicators

| Area | Pressure Type | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| `db/mod.rs` | Boundary + runtime-hook hub | Medium | Medium |
| `executor/load/mod.rs` | Multi-path orchestration hub | High | High |
| `query/plan/executable.rs` | Cursor + envelope + boundary enforcement concentration | Medium | Medium |
| `AccessPath` usage across non-test db files | fan-out = 21 | High | High |
| `Direction` references across non-test db files | fan-out = 20 | Medium | Medium |

## Structure Integrity Risk Index

Structure Integrity Risk Index (1-10, lower is better): **4/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Summary:
- Layer boundaries are largely clean and intentional.
- Current risk comes from hub modules and fan-out pressure, not exposure leaks.
