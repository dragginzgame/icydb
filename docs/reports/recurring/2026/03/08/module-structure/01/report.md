# Structure / Module / Visibility Discipline Audit - 2026-03-08

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Previous (2026-03-06) | Current (2026-03-08) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime Rust files | 280 | 297 | +17 |
| Runtime lines | 57,209 | 61,217 | +4,008 |
| `pub` declarations | 2,747 | 2,682 | -65 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 2,118 | 2,105 | -13 |
| Public item declarations (`pub struct/enum/trait/fn`, db runtime scan) | 352 | 77 | method scope changed; non-comparable |
| Public fields (db runtime scan) | 162 | 0 | method scope changed; non-comparable |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| `query/* -> executor/*` direct refs (mechanical token scan) | 0 |
| `executor/* -> query/plan` direct refs (mechanical token scan) | 8 (same boundary family as prior `1`) |
| `index|data|commit -> query/*` direct refs | 0 |
| `cursor -> executor internals` direct refs | 0 |
| layer-authority invariant check | PASS |
| architecture text-scan invariant check | PASS |

## Structural Pressure

| Indicator | Current Signal | Previous (2026-03-06) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Large runtime modules | 14 files >= 600 LOC | 15 | -1 | Medium-High |
| Continuation concern spread | 925 mentions across 86 runtime files | 936 / 82 | -11 / +4 | Medium-High |
| Access-path fan-out | 89 mentions across 8 runtime files | 116 / 13 | -27 / -5 | Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor(41)`, `query(6)` | 2 | 1 | stable |
| `executor/load/mod.rs` | `executor(8)` | 1 | 0 | improved (lower sibling pressure) |
| `access/execution_contract/mod.rs` | `access(2)` | 1 | 0 | stable (module split retained ownership) |

## Overall Structural Risk Index

**5/10**

Key conclusion:
- Directionality remains clean and owner boundaries are intact.
- The db runtime grew materially, but hub import pressure and AccessPath spread improved versus the previous run.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
