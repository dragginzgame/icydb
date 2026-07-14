# Structure / Module / Visibility Discipline Audit - 2026-03-24

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/module-structure.md`
- code snapshot identifier: `3f453012`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Structural Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports / cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` / `0`) | Low |
| Architecture text-scan invariant | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Runtime compile stability | `cargo check -p icydb-core` | PASS | Medium |
| Recent predicate/explain follow-through preserved subsystem ownership | inspected `db/predicate/capability.rs`, `db/executor/preparation.rs`, `db/executor/explain/descriptor.rs` | PASS | Low |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor`, `query`, `access` | 3 | 2 | stable |
| `executor/runtime_context/load.rs` | `executor` | 1 | 0 | improved (replaces old broader load hub) |
| `access/execution_contract/mod.rs` | none at module root | 0 | 0 | improved |

## Structural Interpretation

- `executor/route/planner/mod.rs` remains the only notable structural hub. It still imports `access`, `query`, and `executor` concerns at the route-planning boundary, but that concentration is stable and expected for the route owner layer.
- The old load-side hub pressure is materially lower now. The current load construction boundary in [`runtime_context/load.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/runtime_context/load.rs) is only `20` lines and imports only executor-local contracts.
- [`access/execution_contract/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/access/execution_contract/mod.rs) is now mostly a boundary/export module, not an import hub.
- No new visibility leak showed up from the predicate capability or explain follow-through. Those changes stayed inside existing `db`-internal module boundaries rather than widening public surface.

## Overall Structural Risk Index

**4/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep `executor/route/planner/mod.rs` from taking on more non-route imports; that remains the main structure-pressure hotspot.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
