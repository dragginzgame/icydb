# Structure / Module / Visibility Discipline Audit - 2026-03-03 (Rerun 2)

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Value |
| ---- | ---- |
| Runtime Rust files | 226 |
| `pub` declarations | 516 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1782 |
| Public `struct/enum/trait/fn` declarations | 313 |
| Public fields | 89 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| Upward imports (tracked layer edges) | 0 |
| Cross-layer policy re-derivations | 0 |
| Comparator definitions outside index | 0 |

## Structural Pressure

| Indicator | Current Signal | Risk |
| ---- | ---- | ---- |
| Large runtime modules | 12 files >= 600 LOC | Medium-High |
| Continuation concern spread | 699 mentions across 70 runtime files | High |
| Access-path fan-out | 112 mentions across 12 runtime files | Medium-High |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query(7)`, `access(2)`, `index(2)`, `predicate(2)` | 4 | 3 | `access`: -1 token |
| `executor/route/planner/feasibility.rs` | `executor(5)`, `query(2)`, `cursor(1)` | 3 | 2 | stable |
| `executor/load/entrypoints.rs` | `executor(3)`, `response(1)`, `query(1)`, `index(1)`, `cursor(1)` | 5 | 4 | stable |

## Overall Structural Risk Index

**5/10**
