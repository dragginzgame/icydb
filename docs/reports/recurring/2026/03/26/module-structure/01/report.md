# Structure / Module / Visibility Discipline Audit - 2026-03-26

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/module-structure.md`
- code snapshot identifier: `a956de44`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Structural Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports / cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` / `0`) | Low |
| Architecture text-scan invariant | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Runtime compile stability | `cargo check -p icydb-core` | PASS | Medium |
| Recent persisted-row cleanup preserved visibility containment | inspected `db/data/persisted_row.rs` helper visibility plus crate-root exports in `lib.rs` / `db/mod.rs` | PASS (`slot_cell_mut`, `required_slot_payload_bytes`, and `encode_slot_payload_from_parts` remain private owner-local helpers; no new `pub` or `pub(crate)` widening`) | Low |
| Route planner hub remains the primary import concentration | inspected `db/executor/route/planner/mod.rs` import surface | PASS | Low-Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/route/planner/mod.rs` | `executor`, `query`, `access` | 3 | 2 | stable |
| `executor/runtime_context/load.rs` | `executor` | 1 | 0 | stable |
| `access/execution_contract/mod.rs` | none at module root | 0 | 0 | stable |

## Structural Interpretation

- `executor/route/planner/mod.rs` remains the only notable structural hub. It still imports `access`, `query`, and `executor` concerns at the route-planning boundary, but that concentration remains stable and expected for the route owner layer.
- [`runtime_context/load.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/runtime_context/load.rs) remains structurally narrow. It is still a small executor-local construction boundary and does not show renewed cross-subsystem fan-in.
- [`access/execution_contract/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/access/execution_contract/mod.rs) remains a boundary/export module rather than an import hub.
- The recent row-path cleanup in [`persisted_row.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/data/persisted_row.rs) stayed owner-local: the new helpers are private, crate-root exports in [`lib.rs`](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs) are unchanged, and no new `db` public re-export was added in [`db/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs).

## Overall Structural Risk Index

**4/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep `executor/route/planner/mod.rs` from taking on more non-route imports; it remains the main structure-pressure hotspot.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
