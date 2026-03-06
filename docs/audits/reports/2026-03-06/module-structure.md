# Structure / Module / Visibility Discipline Audit - 2026-03-06

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Previous (2026-03-05) | Current (2026-03-06) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime Rust files | 276 | 280 | +4 |
| Runtime lines | 52,529 | 57,209 | +4,680 |
| `pub` declarations | 2,463 | 2,747 | +284 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1,937 | 2,118 | +181 |
| Public `struct/enum/trait/fn` declarations | 306 | 352 | +46 |
| Public fields | 108 | 162 | +54 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| `query/plan -> executor` direct refs (mechanical token scan) | 1 |
| `executor -> query/plan/validate` direct refs (mechanical token scan) | 1 |
| `index|data|commit -> query` direct refs (mechanical token scan) | 0 |
| `cursor -> executor internals` direct refs (mechanical token scan) | 0 |
| layer-authority invariant check | PASS |
| architecture text-scan invariant check | PASS |

## Structural Pressure

| Indicator | Current Signal | Previous | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Large runtime modules | 15 files >= 600 LOC | 11 | +4 | High |
| Continuation concern spread | 936 mentions across 82 runtime files | 891 / 79 | +45 / +3 | High |
| Access-path fan-out | 116 mentions across 13 runtime files | 116 / 13 | stable | Medium |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor(42)`, `query(6)` | 2 | 1 | stable |
| `executor/load/mod.rs` | `executor(35)`, `query(12)`, `access(8)` | 3 | 2 | slight increase in hub pressure |
| `access/execution_contract.rs` | `access(2)` | 1 | 0 | stable ownership, larger file size pressure |

## Overall Structural Risk Index

**6/10**

Key conclusion:
- Boundary direction checks are still clean, but hub-size and public-surface growth increased structural pressure versus `2026-03-05`.
