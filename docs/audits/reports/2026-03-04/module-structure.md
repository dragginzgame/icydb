# Structure / Module / Visibility Discipline Audit - 2026-03-04

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Value |
| ---- | ---- |
| Runtime Rust files | 260 |
| `pub` declarations (`pub ...`) | 525 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1918 |
| Public `struct/enum/trait/fn` declarations | 306 |
| Public fields | 108 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| `query/* -> executor/*` non-test runtime references | 0 |
| `index|data|commit/* -> query/*` non-test runtime references | 0 |
| Query runtime symbol leaks from lower layers | 0 |

## Structural Pressure

| Indicator | Current Signal | Risk |
| ---- | ---- | ---- |
| Large runtime modules | 11 files >= 600 LOC | Medium-High |
| Continuation concern spread | 788 mentions across 75 runtime files | High |
| Access-path fan-out | 74 mentions across 10 runtime files | Medium |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/entrypoints.rs` | `executor`, `cursor`, `index`, `response` | 4 | 3 | 0 | Medium-High |
| `executor/route/contracts.rs` | `executor`, `query`, `access`, `direction` | 4 | 3 | N/A (newly tracked module) | Medium |
| `executor/executable_plan.rs` | `query`, `executor`, `cursor`, `access`, `predicate` | 5 | 4 | N/A (newly tracked module) | High |

## Overall Structural Risk Index

**5/10**

Notes:
- The `query/fluent/load.rs` -> `query/fluent/load/*` split reduced one large boundary-spanning façade hotspot.
- Visibility exposure remains manageable, but cross-subsystem continuation and executable-plan hubs still drive structural pressure.
