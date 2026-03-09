# Recurring Audit Summary - 2026-03-09

## Report Preamble

- scope: all recurring audit definitions under `docs/audits/recurring/`, each executed and reported once
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/summary.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity.md` (Risk: 3/10)
2. `contracts/contracts-error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
3. `contracts/contracts-resource-model-compliance` -> `resource-model-compliance.md` (PASS=7, PARTIAL=0, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 7/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation` -> `layer-violation.md` (Risk: 5/10)
7. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 6/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 6/10)
9. `cursor/cursor-ordering` -> `cursor-ordering.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity.md` (Risk: 4/10)
11. `invariants/invariants-invariant-preservation` -> `invariant-preservation.md` (Risk: 4/10)
12. `planner/planner-boundary-semantics` -> `boundary-semantics.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency.md` (Risk: 4/10)
14. `META-AUDIT` -> `meta-audit.md` (Risk: 3/10)

## Global Findings

- Layer-authority and architecture text-scan invariant scripts are green.
- Index-range, field-projection, and memory-id invariant scripts are green.
- Resource-model checklist remains fully compliant (`PASS=7`).
- Structural pressure increased in runtime size, continuation spread, and route-planner hub pressure.
- Cross-layer semantic authority drift remains controlled (no comparator leaks, no policy re-derivation leaks).

## Follow-Up Actions (Required)

- owner boundary: `executor/route` and `query/plan`; action: reduce decision-owner spread and route cross-layer imports called out in `complexity-accretion.md` and `module-structure.md`; target report date/run: `docs/audits/reports/2026-03/2026-03-12/`
- owner boundary: `executor/load`; action: publish and execute modularization cut plan to reduce velocity blast radius from `velocity-preservation.md`; target report date/run: `docs/audits/reports/2026-03/2026-03-12/`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
- `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_plan_having_order_limit_composition_enforces_bounded_policy -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` -> PASS
- `cargo test -p icydb-core route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
