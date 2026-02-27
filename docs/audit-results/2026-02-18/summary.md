# Audit Summary - 2026-02-18

All scores below use a Risk Index (1–10, lower is better).
Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

## Risk Index Summary

| Risk Index          | Score | Run Context               |
| ------------------- | ----- | ------------------------- |
| Invariant Integrity | 4/10  | run on 2026-02-18 baseline |
| Recovery Integrity  | 4/10  | run on 2026-02-18 baseline |
| Cursor/Ordering     | 3/10  | run on 2026-02-18 baseline |
| Index Integrity     | 3/10  | run on 2026-02-18 baseline |
| State-Machine       | 4/10  | run on 2026-02-18 baseline |
| Structure Integrity | 4/10  | run on 2026-02-18 baseline |
| Complexity          | 7/10  | run on 2026-02-18 baseline |
| Velocity            | 6/10  | run on 2026-02-18 baseline |
| DRY                 | 4/10  | run on 2026-02-18 baseline |
| Taxonomy            | 4/10  | run on 2026-02-18 baseline |

Codebase Size Snapshot (`scripts/dev/cloc.sh`):
- Rust: files=390, blank=9417, comment=6629, code=53997
- SUM: files=406, blank=9459, comment=6629, code=54213

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): 21
- PlanError variants: 24

Notable Changes Since Previous Audit:
- No previous `docs/audit-results/*` baseline was found in this repository snapshot.
- Established first dated audit-results baseline at `docs/audit-results/2026-02-18/`.
- Completed `boundary-semantics` audit and recorded one medium drift-sensitive finding.
- Completed `complexity-accretion` baseline audit.
- Completed `cursor-ordering` audit with no critical invariant breaks; current risk index is `3/10` (low risk).
- Completed `dry-consolidation` baseline audit; DRY risk index is `4/10` (moderate pressure), with highest duplication pressure in continuation error construction and distributed `InternalError` message mapping.
- Completed `error-taxonomy` audit; taxonomy risk index is `4/10` with two medium classification-ambiguity findings (commit-marker oversize decode class and unsupported-entity compatibility semantics).
- Re-ran `error-taxonomy` against the current working tree; classification findings and risk index remained stable (`4/10`).
- Fixed one taxonomy ambiguity by classifying oversized persisted commit-marker payloads as `Corruption` at decode/load boundary (`commit/store.rs`).
- Completed `invariant-preservation` audit; invariant integrity risk index is `4/10` with no critical missing invariant, and medium drift pressure concentrated in duplicated planner/executor validation and future DESC-sensitive continuation semantics.
- Completed `recovery-consistency` audit; recovery integrity risk index is `4/10` with strong replay equivalence/idempotence, one medium asymmetry (replay trusting prevalidated relation existence), and one low asymmetry (marker-clear timing) now explicitly bounded by post-`begin_commit` logical infallibility.
- Completed `index-integrity` baseline audit; index integrity risk index is `3/10` with strong ordering/isolation/replay guarantees and low current divergence risk.
- Completed `module-structure` baseline audit; structure integrity risk index is `4/10` with clean layer direction and no deep-public leakage, but moderate hub-module pressure.
- Completed `state-machine-integrity` baseline audit; state-machine risk index is `4/10` with deterministic transition enforcement across planner/executor/commit/recovery boundaries.
- Completed `velocity-preservation` baseline audit; velocity risk index is `6/10`, with change amplification concentrated in `AccessPath` fan-out, cursor continuation, and commit/recovery coordination surfaces.
- Complexity pressure remains concentrated in `PlanError` (24 variants), load execution path fan-out (4 routes), save lane/mode combinations (9), and `AccessPath` references across 21 non-test db files.
- Pre-DESC direction containment is now in place (`Direction`, `resume_bounds`, `anchor_within_envelope`), reducing projected AccessPath/type fan-out risk for future DESC enablement.
- All current weekly audit tracks are now populated for the 2026-02-18 baseline snapshot.

High Risk Areas:
- Access-path fan-out means each new `AccessPath` variant affects many planner/executor surfaces.
- Save/load/index/relation flows already multiply across modes and fast paths.
- `ErrorOrigin` and bound-conversion semantics are still spread across many modules.

Medium Risk Areas:
- Branch-dense functions in commit/index/planner paths raise review and change-safety overhead.
- Multi-layer plan/cursor validation still requires coordinated updates across policy, intent, plan, executable, and executor boundaries.

Drift Signals:
- `AccessPath` is referenced in 21 non-test db modules, so variant growth remains multiplicative.
- Cursor continuation now spans boundary + anchor + direction, with partial centralization of resume and envelope logic.
- Bound conversion logic remains in 16 non-test db modules, so semantic changes still have broad impact.
