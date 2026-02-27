# Recovery Consistency & Replay Equivalence Audit - 2026-02-20

Scope: correctness equivalence between normal mutation execution and recovery replay.

## 1. Mutation Inventory

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |
| ---- | ---- | ---- |
| Insert/Update/Replace row apply | `crates/icydb-core/src/db/executor/save/mod.rs:390` | `crates/icydb-core/src/db/commit/recovery.rs:94` |
| Delete row apply | `crates/icydb-core/src/db/executor/delete/mod.rs:128` | `crates/icydb-core/src/db/commit/recovery.rs:94` |
| Index entry creation/removal | `crates/icydb-core/src/db/commit/apply.rs:41` | `crates/icydb-core/src/db/commit/apply.rs:41` |
| Reverse relation index mutation | `crates/icydb-core/src/db/commit/prepare.rs:134` | same prepare path during replay | 
| Commit marker transitions | `begin_commit`/`finish_commit` | marker load/replay/clear in recovery |

## 2. Side-by-Side Flow Comparison

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| pre-mutation invariant checks | save/delete validate and preflight before commit | replay validates in prepare phase per row op | Mostly | Medium |
| referential integrity validation | explicit in save/delete before commit | implicit via same prepare hooks when relation fields decoded | Mostly | Medium |
| unique validation | in index plan during prepare | same prepare logic used in replay | Yes | Low |
| reverse relation mutation planning | prepare phase | same prepare phase | Yes | Low |
| index mutation apply | `PreparedRowCommitOp::apply` | same apply method | Yes | Low |
| store mutation apply | `PreparedRowCommitOp::apply` | same apply method | Yes | Low |
| commit marker write | persisted before apply | already persisted; replay consumes marker | Equivalent authority model | Low |
| finalization | `finish_commit` clears marker | recovery clears marker after replay | Yes | Low |

## 3. Invariant Enforcement Parity Table

| Invariant | Enforced in Normal | Enforced in Recovery | Enforced at Same Phase? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| data-key decode validity | Yes | Yes | Yes | Low |
| row key identity match | Yes | Yes | Yes | Low |
| index id/key decode validity | Yes | Yes | Yes | Low |
| unique constraint | Yes | Yes | Yes | Low |
| reverse relation consistency | Yes | Yes | Yes | Low |

## 4. Ordering Equivalence Table

| Mutation | Normal Order | Recovery Order | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| per-row index/data mutation | index ops then data op | index ops then data op | Yes | Low |
| multi-row marker replay | row ops sequence in marker order | same stored order | Yes | Low |
| failure rollback model | apply guard rollback in-process + recovery authority | rollback on prepare failure + replay authority | Equivalent durability contract | Medium |

## 5. Error Classification Equivalence Table

| Failure Scenario | Normal Error Type | Recovery Error Type | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| unique violation | conflict/index violation | same via prepare path | Yes | Low |
| corrupt marker key/entry | corruption | corruption | Yes | Low |
| invalid commit marker shape | corruption/invariant | corruption/invariant | Yes | Low |
| double replay | idempotent no-op after marker clear | idempotent | Yes | Low |

## 6. Divergence Risks

| Location | Difference | Consequence | Risk Level |
| ---- | ---- | ---- | ---- |
| `rebuild_secondary_indexes_from_rows` fallback path | startup rebuild path is recovery-specific maintenance step | not a forward-path mirror, but bounded by snapshot restore | Medium |
| relation checks in delete/save entrypoints vs replay | replay relies on stored ops + prepare invariants rather than high-level query path checks | equivalent for durability; semantics drift possible if prepare hooks diverge | Medium |

## 7. Idempotence Verification

| Scenario | Idempotent? | Why / Why Not | Risk |
| ---- | ---- | ---- | ---- |
| Replay same marker twice | Yes | marker cleared after replay, second pass sees no marker | Low |
| Duplicate index entries on replay | No | apply uses deterministic set semantics over prepared ops | Low |
| Duplicate reverse entries on replay | No | same prepared reverse ops, no marker reapply after clear | Low |

## 8. Overall Recovery Risk Index

Overall Recovery Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
