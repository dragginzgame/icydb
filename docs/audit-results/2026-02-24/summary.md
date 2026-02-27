# Audit Summary - 2026-02-24

All scores below use a Risk Index (1-10, lower is better).

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index          | Score | Run Context                                 |
| ------------------- | ----- | ------------------------------------------- |
| Invariant Integrity | 4/10  | run on current working tree (`invariant-preservation.md`) |
| Recovery Integrity  | 4/10  | run on current working tree (`recovery-consistency.md`) |
| Cursor/Ordering     | 3/10  | run on current working tree (`cursor-ordering.md`) |
| Index Integrity     | 3/10  | run on current working tree (`index-integrity.md`) |
| State-Machine       | 4/10  | run on current working tree (`state-machine-integrity.md`) |
| Structure Integrity | 5/10  | run on current working tree (`module-structure.md`) |
| Complexity          | 6/10  | run on current working tree (`complexity-accretion.md`) |
| Velocity            | 6/10  | run on current working tree (`velocity-preservation.md`) |
| DRY                 | 5/10  | run on current working tree (`dry-consolidation.md`) |
| Taxonomy            | 4/10  | run on current working tree (`error-taxonomy.md`) |

## Risk Index Summary (Vertical Format)

Invariant Integrity
- Score: 4/10
- Run Context: run on current working tree (`invariant-preservation.md`)

Recovery Integrity
- Score: 4/10
- Run Context: run on current working tree (`recovery-consistency.md`)

Cursor/Ordering
- Score: 3/10
- Run Context: run on current working tree (`cursor-ordering.md`)

Index Integrity
- Score: 3/10
- Run Context: run on current working tree (`index-integrity.md`)

State-Machine
- Score: 4/10
- Run Context: run on current working tree (`state-machine-integrity.md`)

Structure Integrity
- Score: 5/10
- Run Context: run on current working tree (`module-structure.md`)

Complexity
- Score: 6/10
- Run Context: run on current working tree (`complexity-accretion.md`)

Velocity
- Score: 6/10
- Run Context: run on current working tree (`velocity-preservation.md`)

DRY
- Score: 5/10
- Run Context: run on current working tree (`dry-consolidation.md`)

Taxonomy
- Score: 4/10
- Run Context: run on current working tree (`error-taxonomy.md`)

Codebase Size Snapshot (`scripts/dev/cloc.sh`):
- Non-test files (`=== Non-test files ===`): files=425, blank=10962, comment=7882, code=68952
- Test files (`=== Test files ===`): files=7, blank=591, comment=86, code=4993
- Optional combined total (Rust non-test + test): files=432, blank=11553, comment=7968, code=73945

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files, `rg -l "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): 17
- AccessPath token references (non-test db files, `rg -n "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): 163
- PlanError variants: 29 (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`)
- Test count (`rg -o '#\[(tokio::)?test\]' crates --glob '*.rs' | wc -l`): 1029

Notable Changes Since Previous Audit (2026-02-22):
- Completed full audit suite again (all operational audits plus meta-audit) on current tree.
- `0.28.0` and `0.28.1` projection work added `values_by`, `distinct_values_by`, `values_by_with_ids`, `first_value_by`, and `last_value_by` with parity/ordering/scan-budget test locks.
- Added explicit invariant tests that `distinct_values_by(field)` equals first-observed dedup of `values_by(field)` and that `values_by(field).len() >= distinct_values_by(field).len()`.
- Non-test Rust code (split-cloc method) increased from 59474 to 68952 (+9478).
- AccessPath fan-out decreased from 18 to 17; token references increased from 160 to 163.
- Rust test count increased from 856 to 1029 (+173).

High Risk Areas:
- `crates/icydb-core/src/db/executor/load/aggregate.rs` concentration (1698 LOC) now carries aggregate and projection terminal surface in one module.
- `crates/icydb-core/src/db/executor/route/mod.rs` remains a high-coordination route hub (1163 LOC).

Medium Risk Areas:
- Cursor token compatibility rules remain drift-sensitive across decode/spine/executable boundaries.
- Commit/recovery replay-equivalence remains mechanically stable but high-impact if row-op schema evolves.
- DRY pressure increased modestly due additive terminal wrappers and concentrated executor implementation.

Drift Signals:
- Structure risk increased to 5/10 due module concentration, while invariant/recovery/cursor/index stayed stable.
- Complexity and velocity remain at 6/10 (moderate but elevated) because route and aggregate hubs continue to dominate change cost.
- Taxonomy and invariant surfaces remained stable despite projection API growth.

Execution-Contract Crosswalk (mapped to existing audits):
- Phase 1 (canonical execution flow, entrypoints, fast-path bypasses) -> `state-machine-integrity.md`
- Phase 2 (cross-layer invariant matrix) -> `invariant-preservation.md` + `state-machine-integrity.md`
- Phase 3 (temporal coupling / single derivation points) -> `state-machine-integrity.md` + `recovery-consistency.md`
- Phase 4 (shadow-flow equivalence checks) -> `state-machine-integrity.md`
- Phase 5 (failure boundaries / side-effect timing) -> `recovery-consistency.md` + `state-machine-integrity.md`
- Phase 6 (determinism checks) -> `index-integrity.md` + `cursor-ordering.md`
- Phase 7 (performance-vs-correctness inertness) -> `state-machine-integrity.md` + `boundary-semantics.md`

Note:
- Standalone ad-hoc "execution-contract" report was intentionally not retained; findings are mapped to the existing audit taxonomy above.

Reports Produced This Run:
- `docs/audit-results/2026-02-24/invariant-preservation.md`
- `docs/audit-results/2026-02-24/recovery-consistency.md`
- `docs/audit-results/2026-02-24/cursor-ordering.md`
- `docs/audit-results/2026-02-24/boundary-semantics.md`
- `docs/audit-results/2026-02-24/index-integrity.md`
- `docs/audit-results/2026-02-24/state-machine-integrity.md`
- `docs/audit-results/2026-02-24/module-structure.md`
- `docs/audit-results/2026-02-24/complexity-accretion.md`
- `docs/audit-results/2026-02-24/velocity-preservation.md`
- `docs/audit-results/2026-02-24/dry-consolidation.md`
- `docs/audit-results/2026-02-24/error-taxonomy.md`
- `docs/audit-results/2026-02-24/meta-audit.md`
- `docs/audit-results/2026-02-24/summary.md`
