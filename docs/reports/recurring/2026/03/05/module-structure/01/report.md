# Structure / Module / Visibility Discipline Audit - 2026-03-05

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Value |
| ---- | ---- |
| Runtime Rust files | 267 |
| Runtime lines | 50,722 |
| `pub` declarations | 2,463 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1,937 |
| Public `struct/enum/trait/fn` declarations | 306 |
| Public fields | 108 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| `query/* -> executor/*` non-comment refs | 0 |
| `index|data|commit/* -> query/*` non-comment refs | 0 |
| architecture text-scan invariants | PASS (`[OK] No include_str!-based source text architecture scans detected.`) |

## Structural Pressure

| Indicator | Current Signal | Risk |
| ---- | ---- | ---- |
| Large runtime modules | 10 files >= 600 LOC | Medium-High |
| Continuation concern spread | 790 mentions across 77 runtime files | High |
| Access-path fan-out | 74 mentions across 10 runtime files | Medium |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ---- | ---- | ---- |
| `executor/route/planner/mod.rs` | `executor(42)`, `query(6)` | 2 | 1 | reduced complexity after split from monolithic planner hub |
| `executor/load/entrypoints.rs` | `executor`-dominant with typed entrypoint contracts | 1 visible direct root token | 0 | stable |
| `query/plan/mod.rs` | plan-root re-export boundary, minimal direct cross-layer imports | low direct token fan-out | 0 | stable |

## Overall Structural Risk Index

**5/10**
