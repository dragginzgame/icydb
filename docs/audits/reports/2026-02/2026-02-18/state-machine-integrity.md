# State Machine & Transition Integrity Audit - 2026-02-18

Scope: deterministic transition integrity across plan, execute, commit, cursor continuation, and recovery flows.

## 1. Transition Integrity Table

| Transition | Invariants Checked Before? | Mutation Before Validation? | Risk |
| ---- | ---- | ---- | ---- |
| Planner -> Executable Plan | Yes (`validate_logical_plan_model`, plan/cursor validation) | No | Low |
| Executable Plan -> Executor | Yes (`validate_executor_plan`) | No | Low |
| Save preflight -> commit window | Yes (`preflight_prepare_row_ops`) | No | Low |
| Delete validation -> commit window | Yes (RI + candidate decode/validate) | No | Low |
| Commit marker persisted -> apply | Prevalidated; apply is mechanical | No semantic mutation before preflight | Low |
| Cursor decode -> cursor plan application | Yes (signature, direction, envelope, anchor/boundary checks) | No | Low |
| Recovery marker replay | Yes (row-op prepare checks reused) | No | Low |

Evidence: `crates/icydb-core/src/db/query/plan/executable.rs:111`, `crates/icydb-core/src/db/executor/load/mod.rs:132`, `crates/icydb-core/src/db/executor/mutation.rs:142`, `crates/icydb-core/src/db/executor/delete/mod.rs:89`, `crates/icydb-core/src/db/commit/recovery.rs:94`.

## 2. Partial Mutation Risk Table

| Operation | Partial Mutation Possible? | Protection Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| Save/replace during preflight | No | prepare rollback before marker | Low |
| Delete with relation checks | No | relation validation before commit window | Low |
| Apply phase after marker persist | Not in current implementation | infallible `PreparedRowCommitOp::apply` + marker protocol | Low |
| Recovery replay prepare failure | No lasting divergence | reverse rollback of prepared ops | Low |
| Startup rebuild failure | No lasting divergence | snapshot restore fail-closed | Low |

Evidence: `crates/icydb-core/src/db/commit/apply.rs:40`, `crates/icydb-core/src/db/executor/mutation.rs:155`, `crates/icydb-core/src/db/commit/recovery.rs:101`, `crates/icydb-core/src/db/commit/recovery.rs:122`.

## 3. Plan/Execution Drift Table

| Area | Plan Shape Can Drift? | Executor Can Widen? | Risk |
| ---- | ---- | ---- | ---- |
| Access path selection | No post-plan mutation | No widening path found | Low |
| Cursor continuation | No (signature + direction + boundary checks) | No | Low |
| Index range envelope | No (anchor must remain in envelope) | No | Low |
| Secondary order pushdown | Guarded by validated applicability checks | No silent fallback widening | Low |

Evidence: `crates/icydb-core/src/db/query/plan/continuation.rs:382`, `crates/icydb-core/src/db/query/plan/executable.rs:243`, `crates/icydb-core/src/db/executor/context.rs:89`, `crates/icydb-core/src/db/executor/load/mod.rs:154`.

## 4. Recovery Determinism Table

| Scenario | Deterministic? | Structural Integrity Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| Replay same marker twice | Yes | Yes | Low |
| Interrupted atomic batch replay | Yes | Yes | Low |
| Mixed save/save/delete replay | Yes | Yes | Low |
| Reverse-index replay under FK retarget | Yes | Yes | Low |
| Corrupt marker decode | Fail-closed | Yes | Low |

Evidence: `crates/icydb-core/src/db/commit/tests.rs:332`, `crates/icydb-core/src/db/commit/tests.rs:579`, `crates/icydb-core/src/db/commit/tests.rs:838`, `crates/icydb-core/src/db/executor/tests/semantics.rs:1089`, `crates/icydb-core/src/db/commit/tests.rs:373`.

## 5. Explicit Attack Scenario Results

| Scenario | Invariant Violation Possible? | State Deterministic Afterward? | Risk |
| ---- | ---- | ---- | ---- |
| Failure during index update | Not in normal apply path (mechanical/infallible) | Yes | Low |
| Failure after index update before store update | Guarded by prevalidated, infallible apply path | Yes | Low |
| Failure during delete after index removal | Prevented by commit protocol + replay | Yes | Low |
| Failure during cursor decode | Rejected as invalid cursor | Yes | Low |
| Failure during anchor validation | Rejected as invalid cursor payload | Yes | Low |
| Failure mid-pagination | Cursor continuity remains monotonic and envelope-safe | Yes | Low |
| Replay repeated twice | No-op after marker clear | Yes | Low |
| Planner emits invalid access path | blocked by plan validation | Yes | Low |
| Executor receives corrupted plan/cursor payload | defensive validation rejects | Yes | Low |
| Concurrent logical operations (single-threaded runtime model) | not applicable to current model | N/A | Low |

## Drift Sensitivity

- `PlanError` remains broad (`24` variants) and still carries multiple semantic domains.
- `AccessPath` fan-out remains high (`21` non-test db files), so new plan path types are multiplicative.
- Continuation semantics now centralize bound rewrite and envelope checks, reducing directional drift pressure.

## Overall State-Machine Risk Index

State-Machine Risk Index (1-10, lower is better): **4/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Representative verification run:
- `cargo test -p icydb-core plan_cursor_rejects_index_range_boundary_anchor_mismatch -- --nocapture`
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture`
- `cargo test -p icydb-core load_desc_order_uses_primary_key_tie_break_for_equal_rank_rows -- --nocapture`
