# Structure / Module / Visibility Discipline Audit - 2026-03-03

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Value |
| ---- | ---- |
| Runtime Rust files | 226 |
| `pub` declarations | 515 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1752 |
| Public `struct/enum/trait/fn` declarations | 312 |
| Public fields | 89 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| `query/* -> executor/*` non-comment refs | 0 |
| `index|data|commit/* -> query/*` non-comment refs | 0 |
| Query runtime symbol leaks | 0 |

## Structural Pressure

| Indicator | Current Signal | Risk |
| ---- | ---- | ---- |
| Large runtime modules | 10 files >= 600 LOC | Medium-High |
| Continuation concern spread | 580 mentions across 67 runtime files | High |
| Access-path fan-out | 117 mentions across 13 runtime files | Medium-High |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query(7)`, `access(2)`, `index(2)`, `predicate(2)` | 4 | 3 | N/A (hub split changed since 2026-03-02) |
| `executor/route/planner/feasibility.rs` | `executor(4)`, `query(2)`, `access(1)`, `cursor(1)` | 4 | 3 | N/A (hub split changed since 2026-03-02) |
| `executor/load/entrypoints.rs` | `executor(3)`, `cursor(1)`, `index(1)`, `response(1)` | 4 | 3 | N/A (entrypoint split in `0.40.6`) |

## Overall Structural Risk Index

**5/10**
