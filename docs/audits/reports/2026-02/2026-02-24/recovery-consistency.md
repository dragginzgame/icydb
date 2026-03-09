# Recovery Consistency & Replay Equivalence Audit - 2026-02-24

Scope: equivalence between normal mutation execution and recovery replay.

## 1. Mutation Inventory

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |
| ---- | ---- | ---- |
| Row-op prepare + apply | `crates/icydb-core/src/db/executor/mutation/commit_window.rs:155` | `crates/icydb-core/src/db/commit/recovery.rs:94` |
| Commit marker lifecycle | `crates/icydb-core/src/db/commit/guard.rs:102` / `crates/icydb-core/src/db/commit/guard.rs:124` | `crates/icydb-core/src/db/commit/recovery.rs:49` |
| Index mutations | `crates/icydb-core/src/db/commit/apply.rs:14` | `crates/icydb-core/src/db/commit/recovery.rs:200` |
| Reverse-relation index mutations | `crates/icydb-core/src/db/relation/reverse_index.rs:198` | replayed through same row-op preparation path |

## 2. Side-by-Side Flow Comparison

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| preflight prepare | `prepare_row_commit_for_entity` via commit window | `db.prepare_row_commit_op` during replay loop | Yes | Low |
| apply ordering | index ops then row data | index ops then row data | Yes | Low |
| failure rollback | rollback stack in apply guard | `rollback_prepared_row_ops_reverse` on replay failure | Yes | Low |
| marker clear semantics | clear only on successful `finish_commit` | clear after successful replay | Yes | Low |

## 3. Invariant Enforcement Parity

| Invariant | Enforced in Normal | Enforced in Recovery | Same Phase? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Key decode and key/type integrity | Yes | Yes | Yes | Low |
| Unique index enforcement | Yes | Yes | Yes | Low |
| Reverse relation consistency | Yes | Yes | Yes | Low |
| Marker durability authority | Yes | Yes | Yes | Low |

## 4. Mutation Ordering Verification

| Mutation | Normal Order | Replay Order | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| index mutation + row mutation | deterministic per prepared op | same prepared op order | Yes | Low |
| multi-row marker | marker row order | marker row order | Yes | Low |
| rollback on partial error | reverse order rollback | reverse order rollback | Yes | Low |

## 5. Error Classification Equivalence

| Failure Scenario | Normal Error Class | Recovery Error Class | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| corruption in persisted marker payload | `Corruption` | `Corruption` | Yes | Low |
| unsupported relation target / schema mismatch | `Unsupported` | `Unsupported` | Yes | Medium |
| internal invariant breach | `InvariantViolation` | `InvariantViolation` | Yes | Medium |

## 6. Divergence Detection

| Location | Divergence | Consequence | Risk |
| ---- | ---- | ---- | ---- |
| `rebuild_secondary_indexes_from_rows` startup maintenance | recovery-only maintenance path | bounded by fail-closed snapshot restore behavior | Medium |
| preflight vs replay context source | different entry APIs, shared internals | low if shared helpers remain single-authority | Medium |

## 7. Idempotence Verification

| Scenario | Idempotent? | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| replay same marker twice | Yes | `crates/icydb-core/src/db/commit/tests.rs:294` | Low |
| interrupted atomic batch replay | Yes | `crates/icydb-core/src/db/commit/tests.rs:581` | Low |
| update replay repeat | Yes | `crates/icydb-core/src/db/commit/tests.rs:673` | Low |

## 8. Partial Failure Symmetry

| Scenario | Normal | Replay | Symmetric? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| prepare failure after some row ops | rollback pending ops, marker retained | rollback prepared replay ops, marker retained | Yes | Low |
| apply closure failure | marker retained for recovery | n/a (replay is recovery) | Equivalent durability model | Medium |

## 9. Failure Boundary Map (Execution-Contract Mapping)

| Boundary | Side Effects Before Error? | Recovery Authority | Risk |
| ---- | ---- | ---- | ---- |
| unique validation failure during prepare | No | n/a (marker not persisted) | Low |
| relation integrity failure during delete preflight | No | n/a (marker not persisted) | Low |
| apply failure after marker persist | Yes (partial in-process possible) | marker retained, replay authoritative | Medium |
| replay prepare failure | No durable mutation committed in replay step | rollback + marker retained | Medium |
| marker decode/corruption failure | No replay mutation | marker remains for explicit remediation | Medium |

## 10. Overall Recovery Risk Index (1-10, lower is better)

**4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
