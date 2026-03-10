# Structure / Module / Visibility Discipline Audit - 2026-03-10 (Rerun 2)

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/module-structure.md`
- code snapshot identifier: `b456bbc4`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Structural Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports / cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` / `0`) | Low |
| Architecture text-scan invariant | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Runtime compile stability | `cargo check -p icydb-core` | PASS | Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs 2026-03-10 Baseline |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor`, `query`, `access` | 3 | 2 | stable |
| `executor/load/mod.rs` | `executor` | 1 | 0 | stable |
| `access/execution_contract/mod.rs` | `access` | 1 | 0 | stable |

## Overall Structural Risk Index

**5/10**

## Follow-Up Actions

- None required for this rerun.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
