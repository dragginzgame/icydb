# Structure / Module / Visibility Discipline Audit - 2026-03-09

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/module-structure.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Previous (2026-03-08) | Current (2026-03-09) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime Rust files | 297 | 310 | +13 |
| Runtime lines | 61,217 | 63,342 | +2,125 |
| `pub` declarations | 2,682 | 3,034 | +352 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 2,105 | 2,440 | +335 |
| Public item declarations (`pub struct/enum/trait/fn`) | 77 | 361 | method-sensitive; inspect with caution |
| Public fields (db runtime scan) | 0 | 0 | stable |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| Upward imports (tracked edges, layer-authority check) | 0 |
| Cross-layer policy re-derivations | 0 |
| Comparator definitions outside index | 0 |
| layer-authority invariant check | PASS |
| architecture text-scan invariant check | PASS |

## Structural Pressure

| Indicator | Current Signal | Previous (2026-03-08) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Large runtime modules | 17 files >= 600 LOC | 14 | +3 | High |
| Continuation concern spread | 1,016 mentions across 89 runtime files | 925 / 86 | +91 / +3 | High |
| Access-path fan-out | 121 mentions across 12 runtime files | 89 / 8 | +32 / +4 | Medium-High |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `executor/route/planner/mod.rs` | `executor(36)`, `query(8)`, `access(1)` | 3 | 2 | increased from 1 to 2 cross-layer imports | High |
| `executor/load/mod.rs` | `executor(6)` | 1 | 0 | stable | Low |
| `access/execution_contract/mod.rs` | `access(1)` | 1 | 0 | stable | Low |

## Overall Structural Risk Index

**6/10**

Key conclusion:
- Directionality checks remain green, but structural pressure increased in size, continuation spread, and AccessPath fan-out.
- Route planner hub import pressure is the main structural containment risk in this run.

## Follow-Up Actions

- owner boundary: `executor/route/planner`; action: move `access`-specific planning references behind route contracts to reduce cross-layer count from 2 to 1; target report date/run: `docs/audits/reports/2026-03/2026-03-12/module-structure.md`
- owner boundary: `db` runtime module owners; action: split or decompose at least two `>=600 LOC` modules and verify large-module count returns to `<=15`; target report date/run: `docs/audits/reports/2026-03/2026-03-12/module-structure.md`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
