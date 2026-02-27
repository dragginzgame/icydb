# Audit Summary - 2026-02-22

All scores below use a Risk Index (1-10, lower is better).

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index          | Score | Run Context                                          |
| ------------------- | ----- | ---------------------------------------------------- |
| Invariant Integrity | 4/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Recovery Integrity  | 4/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Cursor/Ordering     | 3/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Index Integrity     | 3/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| State-Machine       | 4/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Structure Integrity | 4/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Complexity          | 6/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| Velocity            | 6/10  | from 2026-02-21 (`summary.md` carry-forward)         |
| DRY                 | 4/10  | run on current working tree (`dry-consolidation.md`) |
| Taxonomy            | 4/10  | from 2026-02-21 (`summary.md` carry-forward)         |

## Risk Index Summary (Vertical Format)

Invariant Integrity
- Score: 4/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Recovery Integrity
- Score: 4/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Cursor/Ordering
- Score: 3/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Index Integrity
- Score: 3/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

State-Machine
- Score: 4/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Structure Integrity
- Score: 4/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Complexity
- Score: 6/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Velocity
- Score: 6/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

DRY
- Score: 4/10
- Run Context: run on current working tree (`dry-consolidation.md`)

Taxonomy
- Score: 4/10
- Run Context: from 2026-02-21 (`summary.md` carry-forward)

Codebase Size Snapshot (`scripts/dev/cloc.sh`):
- Non-test snapshot (`=== Non-test files ===`):
  - Rust: files=415, blank=9890, comment=7289, code=59474
  - SUM: files=431, blank=9952, comment=7289, code=59711
- Test snapshot (`=== Test files ===`):
  - Rust: files=7, blank=574, comment=84, code=4821
  - SUM: files=7, blank=574, comment=84, code=4821
- Optional combined total (non-test + test):
  - Rust: files=422, blank=10464, comment=7373, code=64295
  - SUM: files=438, blank=10526, comment=7373, code=64532

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files, `rg -l "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): 18
- AccessPath token references (non-test db files, `rg -n "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): 160
- PlanError family variants: 29 (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`)
- Test count (`rg -o '#\[(tokio::)?test\]' crates --glob '*.rs' | wc -l`): 856

## Notable Changes Since Previous Audit

- DRY risk decreased from 5/10 (2026-02-21) to 4/10 after route/aggregate hint-channel cleanup and local duplicate-wrapper reductions.
- AccessPath fan-out count increased from 17 to 18.
- AccessPath token references increased from 144 to 160.
- Rust non-test code increased from 57941 to 59474 (+1533).
- Test count increased from 827 to 856 (+29).

## High Risk Areas

- `crates/icydb-core/src/db/executor/load/route.rs` remains large and multi-responsibility, increasing future DRY drift pressure.

## Medium Risk Areas

- Executor-local fast-path arity guard surfaces still split across wrappers.
- Index/spec mismatch invariant strings remain repeated across executor modules.

## Drift Signals

- Continued growth in routing and aggregate-path surface without equivalent reduction in module size.
- DRY pressure improved, but structural growth metrics (token references and code volume) continue upward.

## Reports Produced This Run

- `docs/audits/reports/2026-02-22/dry-consolidation.md`
