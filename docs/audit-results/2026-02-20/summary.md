# Audit Summary - 2026-02-20

All scores below use a Risk Index (1-10, lower is better).

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index | Score | Run Context |
| ---- | ---- | ---- |
| Invariant Integrity | 4/10 | rerun on current working tree (`invariant-preservation.md`) |
| Recovery Integrity | 4/10 | rerun on current working tree (`recovery-consistency.md`) |
| Cursor/Ordering | 3/10 | rerun on current working tree (`cursor-ordering.md`) |
| Index Integrity | 3/10 | rerun on current working tree (`index-integrity.md`) |
| State-Machine | 4/10 | rerun on current working tree (`state-machine-integrity.md`) |
| Structure Integrity | 4/10 | rerun on current working tree (`module-structure.md`) |
| Complexity | 6/10 | rerun on current working tree (`complexity-accretion.md`) |
| Velocity | 6/10 | rerun on current working tree (`velocity-preservation.md`) |
| DRY | 4/10 | rerun on current working tree (`dry-consolidation.md`) |
| Taxonomy | 4/10 | rerun on current working tree (`error-taxonomy.md`) |

## Risk Index Summary (Vertical Format)

Invariant Integrity
- Score: 4/10
- Run Context: rerun on current working tree (`invariant-preservation.md`)

Recovery Integrity
- Score: 4/10
- Run Context: rerun on current working tree (`recovery-consistency.md`)

Cursor/Ordering
- Score: 3/10
- Run Context: rerun on current working tree (`cursor-ordering.md`)

Index Integrity
- Score: 3/10
- Run Context: rerun on current working tree (`index-integrity.md`)

State-Machine
- Score: 4/10
- Run Context: rerun on current working tree (`state-machine-integrity.md`)

Structure Integrity
- Score: 4/10
- Run Context: rerun on current working tree (`module-structure.md`)

Complexity
- Score: 6/10
- Run Context: rerun on current working tree (`complexity-accretion.md`)

Velocity
- Score: 6/10
- Run Context: rerun on current working tree (`velocity-preservation.md`)

DRY
- Score: 4/10
- Run Context: rerun on current working tree (`dry-consolidation.md`)

Taxonomy
- Score: 4/10
- Run Context: rerun on current working tree (`error-taxonomy.md`)

Codebase Size Snapshot (`scripts/dev/cloc.sh`):
- Rust: files=398, blank=9709, comment=6910, code=57883
- SUM: files=414, blank=9771, comment=6910, code=58120

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 22
- AccessPath references (non-test db files): 210
- PlanError family variants: 29 (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`)
- CursorPlanError variants: 9

## Notable Changes Since Previous Audit

- Completed the full operational audit set for today: boundary, complexity, cursor, DRY, taxonomy, index, invariant, module, recovery, state-machine, velocity.
- Added governance run for the audit suite itself (`meta-audit.md`).
- Cursor continuation protocol gained explicit offset compatibility checks:
  - token now carries `initial_offset`
  - planner validates `ContinuationCursorWindowMismatch`
  - continuation requests apply offset exactly once.
- `PlanError` family grew from 28 -> 29 due `CursorPlanError::ContinuationCursorWindowMismatch`.
- Cursor paging policy variants narrowed from 3 -> 2 after removing offset rejection at policy level.
- Pagination test surface increased materially (`crates/icydb-core/src/db/executor/tests/pagination.rs` now 6218 lines).

## High Risk Areas

- AccessPath growth amplification (new variant work still spans planner + executor + explain + tests).
- Planner/logical/cursor-spine large-module pressure.
- Cursor protocol evolution (token shape + compatibility constraints) remains drift-sensitive.
- Commit/recovery evolution remains high-coordination work because replay equivalence must be preserved.

## Medium Risk Areas

- Defensive duplication around boundary checks and validation ownership (intentional, but message-shape drift is possible).
- Error-origin and classification mapping breadth across layers.
- Hub orchestration pressure in `db/mod.rs` and query-plan modules.

## Drift Signals

- Complexity risk remained at 6/10 (manageable but still elevated in path multiplicity and branch hotspots).
- Velocity risk remained at 6/10 (high-amplification features still dominate iteration cost).
- Cursor/order correctness stayed low-risk (3/10) after offset-window compatibility hardening.
- Structural and invariant/recovery indices stayed stable in the moderate band (4/10).

## Reports Produced This Run

- `docs/audit-results/2026-02-20/boundary-semantics.md`
- `docs/audit-results/2026-02-20/complexity-accretion.md`
- `docs/audit-results/2026-02-20/cursor-ordering.md`
- `docs/audit-results/2026-02-20/dry-consolidation.md`
- `docs/audit-results/2026-02-20/error-taxonomy.md`
- `docs/audit-results/2026-02-20/index-integrity.md`
- `docs/audit-results/2026-02-20/invariant-preservation.md`
- `docs/audit-results/2026-02-20/module-structure.md`
- `docs/audit-results/2026-02-20/recovery-consistency.md`
- `docs/audit-results/2026-02-20/state-machine-integrity.md`
- `docs/audit-results/2026-02-20/velocity-preservation.md`
- `docs/audit-results/2026-02-20/meta-audit.md`
