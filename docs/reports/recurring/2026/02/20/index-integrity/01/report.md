# Index Integrity Audit - 2026-02-20

Scope: correctness-only checks for ordering, namespace/id containment, mutation symmetry, unique enforcement, and replay equivalence.

## Step 0 - Index Invariant Registry

| Invariant | Category | Enforced Where |
| ---- | ---- | ---- |
| Raw key ordering is canonical lexicographic source | Ordering | `crates/icydb-core/src/db/index/store/lookup.rs:136` |
| Prefix/range bounds preserve inclusive/exclusive semantics | Ordering | `crates/icydb-core/src/db/index/range.rs:163` |
| Index id is embedded and validated before trust | Namespace | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:425` |
| Key namespace separation (`User` vs `System`) enforced | Namespace | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:434`, `crates/icydb-core/src/db/relation/reverse_index.rs:83` |
| Component arity must match index model | Structural | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:439` |
| Row op prepare ties row + index + reverse-index mutations | Mutation | `crates/icydb-core/src/db/commit/prepare.rs:79`, `crates/icydb-core/src/db/commit/prepare.rs:134` |
| Unique constraints validated before mutation commit | Mutation | `crates/icydb-core/src/db/index/plan/unique.rs:62`, `crates/icydb-core/src/db/index/plan/unique.rs:166` |
| Replay re-applies row ops deterministically and idempotently | Recovery | `crates/icydb-core/src/db/commit/recovery.rs:94`, `crates/icydb-core/src/db/commit/recovery.rs:110` |

## Step 1A - Encode/Decode Symmetry

| Key Type | Symmetric? | Failure Mode | Risk |
| ---- | ---- | ---- | ---- |
| `IndexKey` <-> raw bytes | Yes (guarded) | decode failure -> corruption error | Low |
| `DataKey` <-> raw bytes | Yes (guarded) | decode failure -> corruption error | Low |
| Reverse-index key (`IndexKeyKind::System`) | Yes (guarded) | relation-target decode mismatch rejected | Low |

## Step 1B - Lexicographic Ordering Proof

| Case | Lexicographically Stable? | Why | Risk |
| ---- | ---- | ---- | ---- |
| Prefix + range bounds | Yes | canonical component encoding then `IndexKey::bounds_for_prefix_component_range` | Low |
| Cursor continuation | Yes | strict excluded anchor rewrite and progression checks | Low |
| Desc traversal | Yes | same raw keys traversed in reverse iterator order | Low |
| Composite component boundaries | Yes | component count is fixed and validated | Low |

## Step 2 - Namespace & Index ID Isolation

| Scenario | Can Cross-Decode? | Prevented Where | Risk |
| ---- | ---- | ---- | ---- |
| Anchor for wrong index id | No | cursor spine id mismatch check | Low |
| Anchor wrong namespace | No | `IndexKeyKind::User` guard | Low |
| Reverse-index key decoded as user key | No | distinct key kind + relation decode context | Low |
| Prefix collision across index ids | No (by id-embedded keyspace) | `IndexId` inside key prefix | Low |

## Step 3 - IndexStore Entry Layout

| Entry Component | Layout Stable? | Decode Safe? | Risk |
| ---- | ---- | ---- | ---- |
| key bytes | Yes | yes (`decode_index_key`) | Low |
| value bytes (`RawIndexEntry`) | Yes | yes (`decode_index_entry`) | Low |
| unique entry cardinality | Yes | explicit `len()==1` guard for unique index | Low |

## Step 4 - Reverse Relation Index Integrity

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |
| ---- | ---- | ---- | ---- |
| Save/replace source relation changes | Yes (`prepare_reverse_relation_index_mutations_for_source`) | Low | Low |
| Delete source entity | Yes (remove reverse memberships) | Low | Low |
| Recovery replay of row ops | Yes (same prepared mutation path) | Low | Medium (depends on marker integrity) |

## Step 5 - Unique Index Enforcement

| Scenario | Unique Enforced? | Recovery Enforced? | Risk |
| ---- | ---- | ---- | ---- |
| conflicting insert | Yes (`index_violation`) | Yes (prepare path reused in replay) | Low |
| stale/corrupt unique entry with >1 key | Yes (classified corruption) | Yes | Low |
| replace same owner/value | Yes (allowed short-circuit) | Yes | Low |
| delete then reinsert | Yes | Yes | Low |

## Step 6 - Row <-> Index Coupling

| Failure Point | Divergence Possible? | Prevented? | Risk |
| ---- | ---- | ---- | ---- |
| index mutation before row mutation | transient in commit apply only | yes (marker + rollback + replay authority) | Medium |
| row mutation without index ops | no for indexed fields | prepare phase computes both together | Low |
| reverse mutation failure mid-prepare | no durable divergence | preflight returns error before commit marker | Low |
| partial commit crash | no permanent divergence | recovery replays persisted marker row ops | Low |

## Step 7 - Recovery Replay Equivalence

| Phase | Normal | Replay | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| pre-apply row op planning | `prepare_row_commit_for_entity` | same path via `db.prepare_row_commit_op` | Yes | Low |
| mutation apply ordering | index ops then data store | same in `PreparedRowCommitOp::apply` | Yes | Low |
| unique and relation checks | during prepare phase | same during replay prepare | Yes | Low |
| commit marker finalization | marker cleared after apply | marker cleared after replay | Yes | Low |

## Step 8 - Explicit Attack Scenarios

| Attack | Outcome | Risk |
| ---- | ---- | ---- |
| key collisions across index ids | blocked by embedded id + decode guards | Low |
| component-arity confusion | blocked by arity checks | Low |
| namespace prefix overlap | blocked by `IndexKeyKind` split | Low |
| partial decode acceptance | decode errors classified as corruption | Low |
| reverse orphan after replay | replay+rebuild paths keep relation indexes aligned | Low-Medium |

## Overall Index Integrity Risk Index

Overall Index Integrity Risk Index (1-10, lower is better): **3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
