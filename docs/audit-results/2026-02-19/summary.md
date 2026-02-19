# Audit Summary - 2026-02-19

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
| Complexity          | 7/10  | rerun on current 0.17/0.18 working tree  |
| Velocity            | 6/10  | from 2026-02-18 (not rerun today)        |
| DRY                 | 4/10  | rerun on current 0.16 working tree       |
| Taxonomy            | 4/10  | from 2026-02-18 (not rerun today)        |

Codebase Size Snapshot (`cd crates && cloc .`):
- Rust: files=392, blank=9509, comment=6792, code=55802
- SUM: files=408, blank=9551, comment=6792, code=56018

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 23
- AccessPath references (non-test db files): 251
- PlanError variants: 28 (PlanError + OrderPlanError + AccessPlanError + PolicyPlanError + CursorPlanError)

Notable Changes Since Previous Audit:
- Re-ran `dry-consolidation` against current 0.16 union-stream working tree.
- Re-ran `complexity-accretion` against current 0.17/0.18 execution state and saved results at `docs/audit-results/2026-02-19/complexity-accretion.md`.
- Composite `Union` execution now uses pairwise stream merge in executor context.
- Composite `Intersection` execution is now stream-native pairwise, removing prior set-materialization asymmetry.
- Merge stream now enforces explicit direction and rejects child stream direction mismatch (`InvariantViolation`).
- Guarded scan-budget execution (`offset + limit + 1`) is now active for proven-safe load shapes, adding one more conditional load path.
- Added high-risk quadrant coverage for `Union x DESC x LIMIT x Continuation` and a three-child union DESC+LIMIT continuation stress test.
- Complexity risk remains `7/10`; growth pressure is concentrated in load orchestration branching and large pagination test surface.
- DRY risk remains stable at `4/10`; pressure shifted from continuation payload fan-out toward distributed direction-handling in key producers.

High Risk Areas:
- Load orchestration branching and flow multiplicity (`pk/secondary/index-range/fallback-budgeted/fallback-unbudgeted`).
- Bound/envelope semantic spread across planner/executor/index modules.
- Save lane/mode cross-product remains high (9 behavioral combinations).

Medium Risk Areas:
- Direction-handling logic remains distributed across access-path stream production and fast-path stream producers.
- Index-range bound encode reason mapping remains duplicated across planner cursor validation and store lookup.
- Relation target-key error vocabulary still spans relation and executor-save layers.

Drift Signals:
- `AccessPath` fan-out widened from previous baseline (21 -> 23 non-test db files).
- `PlanError` family variant surface grew from 24 -> 28.
- `crates/icydb-core/src/db/executor/load/mod.rs` expanded from 363 -> 655 lines since baseline.
- `crates/icydb-core/src/db/executor/tests/pagination.rs` expanded from 3348 -> 4699 lines since baseline.
- `InternalError::new(...)` fan-out remains broad (104 non-test call sites across 33 db files), continuing message-drift pressure.
