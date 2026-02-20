# Audit Summary - 2026-02-20

All scores below use a Risk Index (1-10, lower is better).
Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index          | Score | Run Context                              |
| ------------------- | ----- | ---------------------------------------- |
| Invariant Integrity | 4/10  | from 2026-02-18 (not rerun today)        |
| Recovery Integrity  | 4/10  | from 2026-02-18 (not rerun today)        |
| Cursor/Ordering     | 3/10  | from 2026-02-18 (not rerun today)        |
| Index Integrity     | 3/10  | from 2026-02-18 (not rerun today)        |
| State-Machine       | 4/10  | from 2026-02-18 (not rerun today)        |
| Structure Integrity | 4/10  | from 2026-02-18 (not rerun today)        |
| Complexity          | 6/10  | rerun on current working tree            |
| Velocity            | 6/10  | from 2026-02-18 (not rerun today)        |
| DRY                 | 4/10  | from 2026-02-19 (not rerun today)        |
| Taxonomy            | 4/10  | from 2026-02-18 (not rerun today)        |

Codebase Size Snapshot (`cd crates && cloc .`):
- Rust: files=395, blank=9527, comment=6823, code=56186
- SUM: files=411, blank=9589, comment=6823, code=56423

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 23
- AccessPath references (non-test db files): 252
- PlanError variants: 28 (PlanError + OrderPlanError + AccessPlanError + PolicyPlanError + CursorPlanError)

Notable Changes Since Previous Audit:
- Re-ran `complexity-accretion` and saved output at `docs/audit-results/2026-02-20/complexity-accretion.md`.
- Complexity score improved from `7/10` to `6/10`, mainly from load-executor decomposition (`load/mod.rs` 655 -> 361 lines).
- AccessPath fan-out stayed at 23 files, but total references increased 251 -> 252.
- `PlanError` family remained at 28 variants.
- Pagination test surface continued to grow (`executor/tests/pagination.rs` 4699 -> 4911 lines).
- Non-test large-file footprint increased slightly (over150: 65 -> 66, over300: 38 -> 39).
- `"executor invariant violated"` usage decreased (31 -> 28), reducing one repeated invariant-string pressure signal.

High Risk Areas:
- Save/load path multiplicity, especially save lane/mode cross-product (9 combinations).
- Planner and commit/index integrity hotspots (`index_range_candidate_for_index`, `validate_unique_constraint`, `prepare_row_commit_for_entity`).
- Cross-cutting boundary and error-origin spread across many modules.

Medium Risk Areas:
- Load fast-path routing remains multi-branch, but now segmented into dedicated modules.
- Bound-conversion semantics and anchor handling still require coordination across multiple layers.
- Key namespace and component-shape enforcement remain partially distributed.

Drift Signals:
- Complexity pressure shifted from one oversized load file into a wider load-module surface (`load/{mod,route,execute,page,pk_stream,secondary_index}.rs`).
- AccessPath fan-out remains elevated and resistant to reduction.
- Test-surface growth remains faster than core orchestration shrinkage.
