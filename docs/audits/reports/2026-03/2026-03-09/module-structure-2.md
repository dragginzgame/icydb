# Structure / Module / Visibility Discipline Audit - 2026-03-09 (Rerun 2)

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-09/module-structure.md`
- code snapshot identifier: `051af8bd` (working-tree rerun)
- method tag/version: `Method V3`
- comparability status: `comparable`

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Baseline (2026-03-09 first run) | Current (2026-03-09 rerun 2) | Delta |
| ---- | ----: | ----: | ----: |
| Runtime Rust files | 310 | 345 | +35 |
| Runtime lines | 63,342 | 63,269 | -73 |
| `pub` declarations | 3,034 | 3,128 | +94 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 2,440 | 2,533 | +93 |
| Public item declarations (`pub struct/enum/trait/fn`) | 361 | 361 | 0 |
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

| Indicator | Current Signal | Baseline (2026-03-09 first run) | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Large runtime modules | 12 files >= 600 LOC | 17 | -5 | Medium |
| Continuation concern spread | 1,059 mentions across 103 runtime files | 1,016 / 89 | +43 / +14 | High |
| Access-path fan-out | 121 mentions across 12 runtime files | 121 / 12 | stable | Medium-High |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Baseline | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |
| `executor/route/planner/mod.rs` | `executor(2)`, `query(1)`, `access(1)` | 1 | 1 | improved from 2 to 1 cross-layer imports | Medium |
| `executor/load/mod.rs` | `executor(6)` | 1 | 0 | stable | Low |
| `access/execution_contract/mod.rs` | `access(1)` | 1 | 0 | stable | Low |

## Overall Structural Risk Index

**5/10**

Key conclusion:
- Directionality checks remain green and route-planner hub pressure improved.
- Large-module concentration has dropped materially versus the day's baseline.
- Remaining structural drag is continuation spread, not layer-direction leakage.

## Follow-Up Actions

- owner boundary: `cursor + route continuation contracts`; action: reduce continuation spread concentration with capability-first helpers and no decision-owner growth; target report date/run: `docs/audits/reports/2026-03/2026-03-12/module-structure.md`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
