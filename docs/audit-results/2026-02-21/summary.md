# Audit Summary - 2026-02-21

All scores below use a Risk Index (1-10, lower is better).

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index          | Score | Run Context                                             |
| ------------------- | ----- | ------------------------------------------------------- |
| Invariant Integrity | 4/10  | from 2026-02-20 (`invariant-preservation.md`)          |
| Recovery Integrity  | 4/10  | from 2026-02-20 (`recovery-consistency.md`)            |
| Cursor/Ordering     | 3/10  | from 2026-02-20 (`cursor-ordering.md`)                 |
| Index Integrity     | 3/10  | from 2026-02-20 (`index-integrity.md`)                 |
| State-Machine       | 4/10  | from 2026-02-20 (`state-machine-integrity.md`)         |
| Structure Integrity | 4/10  | from 2026-02-20 (`module-structure.md`)                |
| Complexity          | 6/10  | from 2026-02-20 (`complexity-accretion.md`)            |
| Velocity            | 6/10  | from 2026-02-20 (`velocity-preservation.md`)           |
| DRY                 | 5/10  | run on current working tree (`dry-consolidation.md`)   |
| Taxonomy            | 4/10  | from 2026-02-20 (`error-taxonomy.md`)                  |

## Risk Index Summary (Vertical Format)

Invariant Integrity
- Score: 4/10
- Run Context: from 2026-02-20 (`invariant-preservation.md`)

Recovery Integrity
- Score: 4/10
- Run Context: from 2026-02-20 (`recovery-consistency.md`)

Cursor/Ordering
- Score: 3/10
- Run Context: from 2026-02-20 (`cursor-ordering.md`)

Index Integrity
- Score: 3/10
- Run Context: from 2026-02-20 (`index-integrity.md`)

State-Machine
- Score: 4/10
- Run Context: from 2026-02-20 (`state-machine-integrity.md`)

Structure Integrity
- Score: 4/10
- Run Context: from 2026-02-20 (`module-structure.md`)

Complexity
- Score: 6/10
- Run Context: from 2026-02-20 (`complexity-accretion.md`)

Velocity
- Score: 6/10
- Run Context: from 2026-02-20 (`velocity-preservation.md`)

DRY
- Score: 5/10
- Run Context: run on current working tree (`dry-consolidation.md`)

Taxonomy
- Score: 4/10
- Run Context: from 2026-02-20 (`error-taxonomy.md`)

Codebase Size Snapshot (`scripts/dev/cloc.sh`):
- Non-test snapshot (`=== Non-test files ===`):
  - Rust: files=415, blank=9744, comment=7269, code=57941
  - SUM: files=431, blank=9806, comment=7269, code=58178
- Test snapshot (`=== Test files ===`):
  - Rust: files=7, blank=574, comment=84, code=4821
  - SUM: files=7, blank=574, comment=84, code=4821
- Optional combined total (non-test + test):
  - Rust: files=422, blank=10318, comment=7353, code=62762
  - SUM: files=438, blank=10380, comment=7353, code=62999

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 17
- AccessPath token references (non-test db files): 144
- PlanError family variants: 29 (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`)
- CursorPlanError variants: 9
- Test count (`rg -o '#\\[(tokio::)?test\\]' crates --glob '*.rs' | wc -l`): 827

## Notable Changes Since Previous Audit

- Began 2026-02-21 run set with `dry-consolidation.md`.
- DRY risk increased from 4/10 (2026-02-20) to 5/10, mainly from executor fast-path branch growth and duplicate arity guards between load and aggregate paths.
- Defensive index/cursor boundary duplication remains intact and intentional.

## High Risk Areas

- `crates/icydb-core/src/db/executor/load/aggregate.rs` size and mixed responsibilities increase drift pressure.
- Fast-path routing duplication between aggregate and load execution paths.

## Medium Risk Areas

- Repeated spec-alignment invariant messages across executor modules.
- Overlapping executor input bundle structs (`AccessStreamInputs`, `AccessPlanStreamRequest`, `ExecutionInputs`).

## Drift Signals

- DRY moved up from 4/10 to 5/10 due to routing/guard duplication pressure.
- Other risk indices are carried forward until their audits are rerun for 2026-02-21.

## Reports Produced This Run

- `docs/audit-results/2026-02-21/dry-consolidation.md`
