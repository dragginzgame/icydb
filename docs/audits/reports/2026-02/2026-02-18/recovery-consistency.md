# Recovery Consistency Audit - 2026-02-18

Scope: replay equivalence between normal mutation execution and recovery in `icydb-core`.

## 1. Mutation Inventory

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |
| ---- | ---- | ---- |
| Insert | `SaveExecutor::save_entity` (`SaveMode::Insert`) -> `open_commit_window` -> `apply_prepared_row_ops` (`crates/icydb-core/src/db/executor/save/mod.rs:388`) | `ensure_recovered_for_write` -> `replay_recovery_row_ops` -> `db.prepare_row_commit_op` (`crates/icydb-core/src/db/commit/recovery.rs:69`) |
| Replace/Update | `SaveExecutor::save_entity` (`SaveMode::Replace`/`Update`) same commit window path | Same replay path from persisted `CommitRowOp.before/after` |
| Delete | `DeleteExecutor::execute` -> `open_commit_window` -> `apply_prepared_row_ops` (`crates/icydb-core/src/db/executor/delete/mod.rs:77`) | Same replay path from persisted delete row-op (`before=Some, after=None`) |
| Reverse relation update | `prepare_reverse_relation_index_mutations_for_source` during preflight row-op preparation (`crates/icydb-core/src/db/relation/reverse_index.rs:212`) | Same function via `prepare_row_commit_op` during replay/rebuild |
| Index entry creation/removal | `plan_index_mutation_for_entity` + commit ops -> prepared index mutations (`crates/icydb-core/src/db/commit/prepare.rs:95`) | Same planner/preparer path (`prepare_row_commit_op`) |
| Commit marker transitions | `begin_commit` persist marker, `finish_commit` clear marker (`crates/icydb-core/src/db/commit/guard.rs:106`) | `perform_recovery` loads marker, replays row ops, clears marker on success (`crates/icydb-core/src/db/commit/recovery.rs:73`) |
| Startup index rebuild | Not part of normal save/delete path | Recovery-only `rebuild_secondary_indexes_from_rows` (`crates/icydb-core/src/db/commit/recovery.rs:122`) |

## 2. Side-by-Side Flow Comparison

### Insert/Replace/Delete Row-Op Path

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| 1. Pre-mutation invariant checks | save: sanitize/validate/entity invariants + relation checks (`save/mod.rs:404`); delete: plan/post-access validation (`delete/mod.rs:90`) | marker row-op decode + key/entity consistency in `prepare_row_commit_for_entity` (`commit/prepare.rs:40`) | Partially | Medium |
| 2. Referential integrity validation | save pre-checks strong-relation targets (`save/relations.rs:17`), delete validates blocked target deletes (`relation/validate.rs:23`) | replay uses persisted row ops; does not re-run save-side target existence checks | Crash-model equivalent | Medium |
| 3. Unique constraint validation | `plan_index_mutation_for_entity` -> `validate_unique_constraint` before marker persist (`index/plan/unique.rs:25`) | same function path in `prepare_row_commit_for_entity` during replay | Yes | Low |
| 4. Reverse relation mutation | derived in `prepare_reverse_relation_index_mutations_for_source` | same derivation path during replay | Yes | Low |
| 5. Index entry mutation | `PreparedRowCommitOp::apply` writes index ops first | same `PreparedRowCommitOp::apply` call from replay | Yes | Low |
| 6. Store mutation | same `PreparedRowCommitOp::apply` writes row store after index ops (`commit/apply.rs:38`) | same | Yes | Low |
| 7. Commit marker write | `begin_commit` after preflight preparation (`executor/mutation.rs:138`) | marker already persisted; replay consumes persisted marker | Equivalent protocol role | Low |
| 8. Finalization | `finish_commit` clears marker regardless of apply result (`commit/guard.rs:126`) | clear marker only after successful replay (`commit/recovery.rs:77`) | Not strictly identical | Low (safe under post-`begin_commit` infallibility invariant) |

### Commit Marker Corruption Path

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Marker decode/shape checks | Usually not exercised in normal commit apply (marker generated in-process) | strict decode + shape validation before replay (`commit/store.rs`, `commit/validate.rs`) | N/A | Low |
| Classification | N/A | corruption-class failures (`Store`/`Index`/`Serialize`) | N/A | Low |

## 3. Invariant Enforcement Parity

| Invariant | Enforced in Normal | Enforced in Recovery | Enforced at Same Phase? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Expected key == decoded entity key | Yes (`decode_and_validate_entity_key` in save/load/delete) | Yes (`commit/prepare.rs:52`) | Yes | Low |
| Data key namespace/type compatibility | Yes (`DataKey::try_key`) | Yes (same in replay prepare) | Yes | Low |
| Index id/entry decode consistency | Yes in planning/lookup | Yes in replay decode (`commit/decode.rs`) | Yes | Low |
| Unique index enforcement | Yes before marker persist | Yes before replay apply (same path) | Yes | Low |
| Reverse-index symmetry | Yes in preflight prep | Yes in replay prep | Yes | Low |
| Delete strong-relation block | Yes (`relation/validate.rs`) | Preserved after replay via same reverse-index state | Effective parity | Low |
| Save-side strong target existence check | Yes (`save/relations.rs`) | Not rechecked during replay | No | Medium (assumes prevalidated marker) |
| Commit marker no-op row-op rejection | Yes (prepare+validate) | Yes | Yes | Low |

## 4. Ordering Equivalence Table

| Mutation | Normal Order | Recovery Order | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Row-op preparation sequencing | sequential, with rollback on prepare error (`executor/mutation.rs:110`) | sequential, with rollback on prepare error (`commit/recovery.rs:94`) | Yes | Low |
| Index vs store write order | index mutations first, then data store (`commit/apply.rs:38`) | same apply function | Yes | Low |
| Reverse-index mutation placement | included in prepared index ops before data write | same | Yes | Low |
| Marker transition ordering | preflight -> `begin_commit` -> apply -> `finish_commit` clear | load marker -> replay apply -> clear on success | Mostly | Medium |
| Validation-before-mutation guarantee | enforced pre-`begin_commit` in save/delete | enforced pre-`prepared.apply()` in replay loop | Yes | Low |

## 5. Error Classification Equivalence Table

| Failure Scenario | Normal Error Type | Recovery Error Type | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Unique violation | `InternalError(Conflict, Index)` via `index_violation_error` | same (replay uses same unique validator path) | Yes | Low |
| Referential integrity violation (delete blocked) | `InternalError(Unsupported, Executor)` (`relation/validate.rs`) | same once reverse state is replayed | Yes | Low |
| Corrupt commit marker bytes/data key | N/A (normal path creates marker in-process) | `InternalError(Corruption, Store/Index/Serialize)` | N/A | Low |
| Corrupt index entry during mutation planning | `Corruption` (`Index`/`Store`) | same (shared prepare path) | Yes | Low |
| Invalid commit phase (marker already present at begin) | `InvariantViolation/Store` (`commit/guard.rs:108`) | same guard applies to any begin call | Yes | Low |
| Double replay attempt | normal: marker cleared after finish; no reapply | recovery: second `ensure_recovered_for_write` is no-op when marker absent | Yes | Low |

## 6. Divergence Risks

1. Location: `crates/icydb-core/src/db/commit/guard.rs:126` vs `crates/icydb-core/src/db/commit/recovery.rs:73`  
Difference: normal `finish_commit` clears marker even when apply returns `Err`; recovery clears only after successful replay.  
Consequence: asymmetry is currently safe because the post-`begin_commit` apply path is mechanically infallible in current code (see confirmation below).  
Risk Level: Low (would rise immediately if new fallible logic is introduced post-`begin_commit`).

2. Location: `crates/icydb-core/src/db/executor/save/relations.rs:17` vs replay prepare path  
Difference: save-side strong target existence is validated before commit in normal execution, but replay trusts prevalidated marker and does not re-run this check.  
Consequence: equivalence depends on crash-only model (no external state mutation between marker persist and replay).  
Risk Level: Medium-Low.

3. Location: `crates/icydb-core/src/db/commit/recovery.rs:122`  
Difference: startup recovery always includes secondary-index rebuild from rows; normal save/delete does not run global rebuild each mutation.  
Consequence: additional recovery-only repair side effect; semantically convergent but not phase-identical.  
Risk Level: Low.

## 7. Idempotence Verification

| Scenario | Idempotent? | Why / Why Not | Risk |
| ---- | ---- | ---- | ---- |
| Replay same marker twice | Yes | marker cleared after first successful replay; second pass no-op (`commit/tests.rs:332`) | Low |
| Interrupted atomic insert batch replay | Yes | first replay applies rows/indexes; second replay no-op (`commit/tests.rs:579`) | Low |
| Interrupted atomic update batch replay | Yes | old index removed/new index inserted once; second replay stable (`commit/tests.rs:671`) | Low |
| Mixed save/save/delete replay sequence | Yes | final row+index state matches expected sequence (`commit/tests.rs:838`) | Low |
| Reverse-index replay idempotence | Yes | replay/retarget/partial-update scenarios keep expected reverse membership (`executor/tests/semantics.rs:868`, `948`, `1089`, `1316`) | Low |

## 8. Partial Failure Symmetry

| Failure Point | Recovery Outcome | Safe? | Risk |
| ---- | ---- | ---- | ---- |
| After partial reverse-index prep but before full marker prep | rollback restores pre-state (`executor/tests/semantics.rs:1182`) | Yes | Low |
| Between `begin_commit` and apply (interrupted process) | replay completes marker and clears it (`commit/tests.rs:579`, `671`) | Yes | Low |
| Corrupt marker key bytes during replay | replay aborts, marker remains for operator action (`commit/tests.rs:373`) | Yes (fail-closed) | Low |
| Unsupported/miswired entity dispatch during replay | replay aborts without partial row apply; marker remains (`commit/tests.rs:408`, `461`) | Yes | Low |
| Startup rebuild failure on corrupt row | index snapshot restored exactly (`commit/tests.rs:1089`) | Yes | Low |

## 9. Post-begin Infallibility Confirmation

Confirmed for current code path:

- `open_commit_window` does all fallible work (`preflight_prepare_row_ops`) **before** marker persistence; only after success does it call `begin_commit` (`crates/icydb-core/src/db/executor/mutation.rs:138`).
- Post-`begin_commit`, apply uses `PreparedRowCommitOp::apply(self) -> ()` with no `Result` path (`crates/icydb-core/src/db/commit/apply.rs:40`).
- `apply_prepared_row_ops` closure has no domain fallible calls after marker persistence except `CommitApplyGuard::finish()` (`crates/icydb-core/src/db/executor/mutation.rs:155`).
- `CommitApplyGuard::finish()` can only error on an internal double-finish invariant breach (`crates/icydb-core/src/db/commit/guard.rs:45`), and this closure calls it once.

Conclusion:

- Current protocol satisfies: **after `begin_commit`, apply is logically infallible under declared invariants**.
- Therefore marker-clearing asymmetry is presently safe.
- Governance note: any new post-`begin_commit` fallible branch would reopen a recovery-authority risk and should be treated as a blocking audit regression.

## 10. Overall Recovery Risk Index

Recovery Integrity Risk Index (1-10, lower is better): **4/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Rationale:
- Core replay path reuses the same row-op preparation logic as normal execution, which strongly preserves mutation-order and invariant parity.
- Idempotence and reverse-index replay behavior are covered by multiple targeted tests and currently pass.
- Main pressure is limited to two asymmetries: marker-clear timing on unexpected normal-apply errors, and replay trusting prevalidated relation existence rather than re-validating targets.
