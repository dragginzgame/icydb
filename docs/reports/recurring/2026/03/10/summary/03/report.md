# Recurring Audit Summary - 2026-03-10 (Rerun 3)

## Report Preamble

- scope: rerun of all recurring audit definitions under `docs/audits/recurring/`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/summary.md`
- code snapshot identifier: `b456bbc4`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity-2.md` (Risk: 3/10)
2. `contracts/error-taxonomy` -> `error-taxonomy-2.md` (Risk: 4/10)
3. `contracts/resource-model-compliance` -> `resource-model-compliance-2.md` (PASS=3, PARTIAL=0, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion-3.md` (Risk: 5/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation-3.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation` -> `layer-violation-3.md` (Risk: 4/10)
7. `crosscutting/crosscutting-module-structure` -> `module-structure-3.md` (Risk: 5/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation-3.md` (Risk: 5/10)
9. `executor/cursor-ordering` -> `cursor-ordering-2.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity-2.md` (Risk: 4/10)
11. `integrity/invariant-preservation` -> `invariant-preservation-2.md` (Risk: 4/10)
12. `range/boundary-envelope-semantics` -> `boundary-semantics-2.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency-2.md` (Risk: 4/10)

## Global Findings

- All recurring verification commands used in this run completed with `PASS`.
- Layer authority and cross-layer policy checks remained clean (`0` upward imports, `0` cross-layer policy re-derivations).
- Index-range, memory-id, and field-projection invariant scripts remained clean.
- Targeted replay, anchor/envelope, grouped resource-policy, and route-feature-budget tests all passed.
- No recurring audit reported `PARTIAL`/`FAIL`, and no risk index exceeded `5/10` in this run.

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_plan_having_order_limit_composition_enforces_bounded_policy -- --nocapture` -> PASS
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` -> PASS
- `cargo test -p icydb-core route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
