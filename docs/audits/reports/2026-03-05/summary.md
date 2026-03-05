# Recurring Audit Summary - 2026-03-05

Run scope: all recurring audit definitions under `docs/audits/recurring/` executed and reported individually.

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity.md` (Risk: 3/10)
2. `contracts/contracts-error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
3. `contracts/contracts-resource-model-compliance` -> `resource-model-compliance.md` (PASS=7, PARTIAL=0, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 6/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation` -> `layer-violation.md` (Risk: 5/10)
7. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 5/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 6/10)
9. `cursor/cursor-ordering` -> `cursor-ordering.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity.md` (Risk: 4/10)
11. `invariants/invariants-invariant-preservation` -> `invariant-preservation.md` (Risk: 4/10)
12. `planner/planner-boundary-semantics` -> `boundary-semantics.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency.md` (Risk: 4/10)
14. `follow-up/execution-routing-refactor` -> `execution-routing-refactor-followup.md` (Refactor plan refinement)

## Global Findings

- Layer-authority checks passed with no comparator leakage outside `index/*` and no cross-layer policy re-derivation findings.
- Grouped resource-model compliance is now fully green (`PASS=7`) due explicit grouped `HAVING + ORDER + LIMIT` boundedness coverage.
- Main ongoing pressure remains continuation/anchor coordination spread across runtime files (velocity/complexity concern, not a critical correctness break).
- Boundary semantics edge case `anchor == upper` was explicitly audited; current path short-circuits empty envelopes before range iteration (performance risk `1/10`).
- Route simplification follow-up now includes explicit `RouteShapeKind`, required `AccessRouteClass`, access-owned pushdown/index-range eligibility methods, and parity-shim retirement after soak.
- Development hardening assertion for continuation envelope containment is now present in `resume_bounds_from_refs`.
- Execution-stage routing is now shape-dispatch based in `route/planner/execution/mod.rs`, with local shape builders absorbing prior central branching.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> BLOCKED (`Invalid cross-device link (os error 18)`)
- `cargo fmt --all` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `cargo test -p icydb-core route_capabilities_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_field_extrema_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_load_index_range_ -- --nocapture` -> PASS
- `cargo test -p icydb-core route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths -- --nocapture` -> PASS
- `cargo test -p icydb-core route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows -- --nocapture` -> PASS
- `cargo test -p icydb-core route_plan_mutation_ -- --nocapture` -> PASS

Per repository guidance, cross-filesystem test failures were not retried beyond initial attempts.
