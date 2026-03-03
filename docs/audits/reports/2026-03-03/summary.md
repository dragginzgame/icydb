# Recurring Audit Summary - 2026-03-03

Run scope: all recurring audit definitions under `docs/audits/recurring/` executed and reported individually.

## Audit Run Order and Results

1. `access/access-index-integrity` -> `index-integrity.md` (Risk: 3/10)
2. `contracts/contracts-error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
3. `contracts/contracts-resource-model-compliance` -> `resource-model-compliance.md` (PASS=6, PARTIAL=1, FAIL=0)
4. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 6/10)
5. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 5/10)
6. `crosscutting/crosscutting-layer-violation-results` -> `layer-violation-results.md` (No violations)
7. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 5/10)
8. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 6/10)
9. `cursor/cursor-ordering` -> `cursor-ordering.md` (Risk: 3/10)
10. `executor/executor-state-machine-integrity` -> `state-machine-integrity.md` (Risk: 4/10)
11. `invariants/invariants-invariant-preservation` -> `invariant-preservation.md` (Risk: 4/10)
12. `planner/planner-boundary-semantics` -> `boundary-semantics.md` (Risk: 3/10)
13. `storage/storage-recovery-consistency` -> `recovery-consistency.md` (Risk: 4/10)

## Global Findings

- No strict layer-direction violations detected in this run.
- No critical cursor/envelope correctness regressions detected.
- Grouped resource-model compliance improved; current open item is dedicated composition testing for grouped `HAVING + ORDER + LIMIT` boundedness proof.
- Main ongoing pressure remains continuation and grouped-policy coordination surfaces (velocity/complexity concern, not immediate correctness failure).

## Targeted Test Commands Executed

- `cargo test -p icydb-core grouped_plan_rejects_order_without_limit -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS

