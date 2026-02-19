# Audit Summary - 2026-02-19

All scores below use a Risk Index (1-10, lower is better).
Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Invariant Integrity Risk Index: 4/10 (carried from 2026-02-18; not rerun today)
Recovery Integrity Risk Index: 4/10 (carried from 2026-02-18; not rerun today)
Cursor/Ordering Risk Index: 3/10 (carried from 2026-02-18; not rerun today)
Index Integrity Risk Index: 3/10 (carried from 2026-02-18; not rerun today)
State-Machine Risk Index: 4/10 (carried from 2026-02-18; not rerun today)
Structure Integrity Risk Index: 4/10 (carried from 2026-02-18; not rerun today)
Complexity Risk Index: 7/10 (carried from 2026-02-18; not rerun today)
Velocity Risk Index: 6/10 (carried from 2026-02-18; not rerun today)
DRY Risk Index: 4/10 (rerun on current 0.16 working tree)
Taxonomy Risk Index: 4/10 (carried from 2026-02-18; not rerun today)

Codebase Size Snapshot (`cd crates && cloc .`):
- Rust: files=389, blank=9598, comment=6688, code=55583
- SUM: files=405, blank=9640, comment=6688, code=55799

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 23
- AccessPath references (non-test db files): 243
- PlanError variants: 28 (PlanError + OrderPlanError + AccessPlanError + PolicyPlanError + CursorPlanError)

Notable Changes Since Previous Audit:
- Re-ran `dry-consolidation` against current 0.16 union-stream working tree.
- Composite `Union` execution now uses pairwise stream merge in executor context.
- Merge stream now enforces explicit direction and rejects child stream direction mismatch (`InvariantViolation`).
- Added high-risk quadrant coverage for `Union x DESC x LIMIT x Continuation` and a three-child union DESC+LIMIT continuation stress test.
- DRY risk remains stable at `4/10`; pressure shifted from continuation payload fan-out toward distributed direction-handling in key producers.

High Risk Areas:
- No high-risk DRY divergence identified in this rerun.

Medium Risk Areas:
- Direction-handling logic remains distributed across access-path stream production and fast-path stream producers.
- Index-range bound encode reason mapping remains duplicated across planner cursor validation and store lookup.
- Relation target-key error vocabulary still spans relation and executor-save layers.

Drift Signals:
- `AccessPath` fan-out widened from previous baseline (21 -> 23 non-test db files).
- Composite execution now has split behavior (`Union` stream-merged, `Intersection` still materialized), which is safe but increases temporary dual-path maintenance overhead.
- `InternalError::new(...)` fan-out remains broad (104 non-test call sites across 33 db files), continuing message-drift pressure.
