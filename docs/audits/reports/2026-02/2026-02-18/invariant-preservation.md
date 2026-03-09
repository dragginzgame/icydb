# Invariant Preservation Audit - 2026-02-18

Scope: explicit invariant registry, enforcement boundaries, and recovery/cursor symmetry in `icydb-core`.

## 1. Invariant Registry

| Invariant | Category | Subsystem(s) Impacted |
| ---- | ---- | ---- |
| Data key entity namespace must match target entity type | Identity | `db/data/key.rs`, `db/executor/*`, `db/commit/prepare.rs` |
| Decoded entity primary key must equal storage key | Identity | `db/entity_decode.rs`, load/save/delete/commit decode paths |
| Index key wire shape (tag/id/segments/sizes) must be valid | Identity | `db/index/key/codec.rs`, `db/commit/decode.rs` |
| Index key namespace (`User` vs `System`) must match use-site contract | Identity | `db/query/plan/executable.rs`, `db/relation/reverse_index.rs` |
| Index component arity must match index model arity | Identity | `db/query/plan/executable.rs`, `db/index/key/codec.rs` |
| Raw index key ordering must be canonical and byte-stable | Ordering | `db/index/key/codec.rs`, `db/index/store/lookup.rs` |
| Cursor continuation must advance strictly past anchor | Ordering | `db/index/store/lookup.rs`, `db/index/range.rs` |
| Cursor anchor must remain inside original envelope | Ordering | `db/query/plan/executable.rs`, `db/index/range.rs` |
| Cursor boundary logical PK must match raw anchor PK | Ordering | `db/query/plan/executable.rs` |
| Cursor signature/direction/arity/type must match executable plan | Ordering | `db/query/plan/continuation.rs`, `db/query/plan/executable.rs` |
| AccessPath shape must be validated before execution | Structural | `db/query/intent/mod.rs`, `db/query/plan/validate/*`, executors |
| Post-access plan order must remain filter -> order -> cursor -> page/limit | Structural | `db/query/plan/logical.rs` |
| Unique index entries must retain cardinality/membership guarantees | Structural | `db/index/plan/mod.rs`, `db/index/store/lookup.rs` |
| Save/delete apply must mutate index + row stores coherently | Mutation | `db/executor/mutation.rs`, `db/commit/apply.rs` |
| Reverse-index mutations must be symmetric for old/new relation targets | Mutation | `db/relation/reverse_index.rs`, `db/commit/prepare.rs` |
| Delete must be blocked when strong relations still reference target | Mutation | `db/relation/validate.rs`, `db/executor/delete/mod.rs` |
| Commit marker row-op shape must encode a concrete mutation | Recovery | `db/commit/validate.rs`, `db/commit/store.rs` |
| Recovery replay must be idempotent and fail-closed | Recovery | `db/commit/recovery.rs`, `db/commit/tests.rs` |
| Recovery must preserve index/data consistency on replay failures | Recovery | `db/commit/recovery.rs`, rollback/snapshot paths |
| Recovery startup rebuild must derive indexes from authoritative rows | Recovery | `db/commit/recovery.rs`, `db/commit/tests.rs` |

## 2. Boundary Map

| Boundary | Input Assumptions | Output Guarantees |
| ---- | ---- | ---- |
| `serialize` -> `deserialize_bounded` (rows/markers/tokens) | Bytes may be malformed/truncated/oversized | Typed value or classified `InternalError`/`PlanError`; no panic on malformed persisted bytes |
| `RawIndexKey` encode -> decode | Raw bytes may be hostile/corrupt | `IndexKey::try_from_raw` validates key kind, id bytes, segment bounds, arity, trailing bytes |
| Identity types -> storage key encoding | Entity/field values may be incompatible | Explicit `Unsupported`/decode failures at encoding boundary (`StorageKeyEncodeError`, `DataKeyEncodeError`) |
| Planner intent -> logical plan | Query may violate policy/schema | `validate_logical_plan_model` enforces shape/order/access before plan leaves planner |
| Logical plan -> executor | Plan may be stale/miswired | `validate_executor_plan` defensively re-checks invariants as `InvariantViolation` |
| Executable plan -> cursor planning | Cursor token may be malformed or foreign | Signature/direction/arity/type/anchor checks before cursor state is accepted |
| Save/delete preflight -> commit window | Row ops may fail decode/index/relation checks | `open_commit_window` only begins commit after preflight preparation succeeds |
| Commit marker -> recovery replay | Marker/data/index bytes may be corrupt | Decode + shape checks; replay rolls back on prepare failure; marker remains authoritative until safe clear |
| Cursor decode -> cursor boundary/anchor semantics | Token may have inconsistent logical/raw continuation state | Boundary-anchor consistency and envelope containment required for index-range continuation |
| Reverse relation mutation prep | Relation value/target keys may be invalid | Symmetric remove/add ops emitted from old/new target-set delta |
| Index store read -> key interpretation | Stored index entries may be corrupt/non-unique | Decode guards enforce key integrity, entry integrity, unique entry size constraints |

## 3. Enforcement Mapping

| Invariant | Assumed At | Enforced At | Exactly Once? | Narrowest Boundary? | Correct Error Class? | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Data key entity namespace match | Row decode/access paths | `DataKey::try_key` | Yes | Yes | Yes (`Corruption`) | Low |
| Decoded entity key equals storage key | load/save/delete/commit decode flows | `decode_and_validate_entity_key` | Yes (central helper) | Yes | Yes (`Corruption`) | Low |
| Index key wire shape | Index lookup/recovery/continuation | `IndexKey::try_from_raw`, commit decode helpers | Yes | Yes | Yes (`Corruption`) | Low |
| Cursor token compatibility (signature/direction/arity/type) | `ExecutablePlan::plan_cursor` | `decode_validated_cursor` | Yes | Yes | Yes (`PlanError` invalid cursor) | Low |
| Cursor anchor index-id/namespace/arity envelope checks | Index-range pagination | `validate_index_range_anchor` | Yes | Yes | Yes (`PlanError`) | Low |
| Boundary PK == anchor PK | Index-range continuation | `validate_index_range_boundary_anchor_consistency` | Yes | Yes | Yes (`PlanError`) | Low |
| Canonical order includes PK tie-break | Planner + executor ordering | `canonicalize_order_spec`, `validate_primary_key_tie_break` | Duplicated | Mostly | Yes | Medium (drift pressure) |
| Plan shape validity (unordered pagination/delete rules) | Planner + post-access execution | `validate_logical_plan_model`, `validate_executor_plan`, `policy::validate_plan_shape` | Duplicated | Not always | Yes | Medium |
| Unique index guarantees | Index mutation planning + lookup decode | `plan_index_mutation_for_entity`, lookup unique entry checks | Duplicated | Yes | Yes (`Conflict` vs `Corruption`) | Low |
| Save/delete mutate index + row coherently | Mutation apply phase | `PreparedRowCommitOp::apply` from preflight-prepared ops | Yes | Yes | N/A | Low |
| Reverse index symmetry | Save/delete/recovery row-op prep | `prepare_reverse_relation_index_mutations_for_source` | Yes | Yes | Yes | Low |
| Strong relation delete gate | Delete execution | `validate_delete_strong_relations_for_source` | Yes | Yes | Yes (`Unsupported` blocked op) | Low |
| Commit marker row-op shape | Marker load + prepare | `validate_commit_marker_shape`, no-op guard in prepare | Duplicated | Yes | Yes (`Corruption`) | Low |
| Recovery idempotence | Startup/write entrypoints | `ensure_recovered` + marker fast check + replay | Yes | Yes | Yes | Low |
| Recovery index rebuild fail-closed | Startup rebuild | snapshot/restore around `rebuild_secondary_indexes_in_place` | Yes | Yes | Yes | Low |
| Direction-aware continuation bounds | Index-range continuation | `resume_bounds` central helper | Yes | Yes | N/A | Medium (DESC not active) |

## 4. Recovery Symmetry Table

| Invariant | Normal Exec | Recovery | Cursor | Reverse Index | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Data key/entity decode invariants | Enforced | Enforced in replay + rebuild | N/A | N/A | Low |
| Entity key match invariant | Enforced in load/save/delete | Enforced in `prepare_row_commit_for_entity` | N/A | Indirect | Low |
| Index key decode/entry integrity | Enforced in lookup/planning | Enforced in marker decode/rebuild | Anchor decode path | Enforced | Low |
| Cursor signature/shape/envelope invariants | Enforced | N/A | Enforced | N/A | Low |
| Boundary-anchor consistency invariant | Enforced | N/A | Enforced | N/A | Low |
| Save/delete index+row coherence | Enforced via prepared apply | Same prepared apply path in replay | N/A | Includes reverse ops | Low |
| Reverse-index symmetry | Enforced in save/delete planning | Enforced because replay uses same row-op prepare path | N/A | Enforced | Low |
| Strong-relation delete blocking | Enforced pre-commit | Preserved after replay because reverse indexes rebuilt/replayed | N/A | Enforced | Low |
| Marker shape/no-op row-op invariant | Enforced on load/prepare | Enforced before replay apply | N/A | N/A | Low |
| Replay idempotence | N/A | Explicitly tested (`recovery_replay_is_idempotent`) | N/A | Preserved | Low |

## 5. High-Risk Focus Areas

### A. Cursor Envelope Safety

- Anchor cannot escape original envelope: enforced by `anchor_within_envelope` in `validate_index_range_anchor`.
- Bound conversion uses `Excluded(anchor)`: centralized in `resume_bounds`.
- Upper bound unchanged for ASC continuation: enforced by `resume_bounds(Direction::Asc, ..)` and test `resume_bounds_asc_rewrites_lower_only`.
- Index id/namespace/arity cannot change across cursor token: enforced in `validate_index_range_anchor`.
- Boundary vs anchor logical/raw drift is rejected: enforced in `validate_index_range_boundary_anchor_consistency` and test `plan_cursor_rejects_index_range_boundary_anchor_mismatch` (passes).

Assessment: structurally strong for active ASC mode.

### B. Index Key Ordering Guarantees

- `IndexKey` documents and enforces ordering parity with byte ordering.
- Ordering parity is covered by focused tests (`index_key_ordering_matches_bytes`, cartesian/randomized semantic-vs-bytes tests).
- Range traversal compares raw keys directly and enforces monotonic continuation.

Assessment: low drift in current ASC traversal.

### C. Reverse Relation Index Correctness

- Reverse-index ops are derived from old/new target-set deltas in one function.
- Save/delete and recovery both use `prepare_row_commit_for_entity`, so reverse-index mutation logic is shared.
- Delete validation cross-checks reverse entry membership against authoritative source rows and fails on orphan pointers.

Assessment: symmetry present; corruption paths are fail-closed.

### D. Recovery Idempotence

- Recovery gate checks marker presence and replays before read/write execution.
- Replay is sequential and rollback-backed on preparation failure.
- Startup rebuild is snapshot-protected: rebuild failure restores pre-rebuild index snapshots.
- Test `db::commit::tests::recovery_replay_is_idempotent` passes in current tree.

Assessment: strong fail-closed behavior.

### E. Expected-Key vs Decoded-Entity Match

- Central helper `decode_and_validate_entity_key` is used across load/save/delete/commit-prep boundaries.
- Mismatch is raised before entities are returned/applied.
- Recovery path reuses the same helper through `prepare_row_commit_for_entity`.

Assessment: centrally enforced and classification-stable (`Corruption`).

## 6. High Risk Invariants

- None at critical severity (no missing invariant with immediate corruption or replay divergence observed).

## 7. Redundant Enforcement

- Plan semantics are validated in both planner and executor boundaries (`validate_logical_plan_model` + `validate_executor_plan` + runtime plan-shape checks).
- Commit marker mutation-shape checks exist at marker-load and row-op preparation boundaries.
- Unique index integrity is checked in mutation planning and during index-entry decode.

Risk: medium drift pressure if one layer changes semantics without synchronized updates.

## 8. Missing Enforcement

- No active DESC invariants are enforced yet; direction-aware helpers currently operate with ASC semantics by design. This is structural containment, not missing ASC enforcement.
- Direct relation-module unit tests are sparse; most relation invariants are exercised via save/commit integration tests.
- No additional missing enforcement was found for current ASC behavior.

## 9. Drift Sensitivity Summary

| Invariant | Sensitive To | Drift Risk |
| ---- | ---- | ---- |
| Cursor envelope + monotonic continuation | DESC activation, bound semantics changes | Medium |
| Boundary-anchor consistency | Cursor token schema evolution | Medium |
| Planner/executor semantic alignment | New plan policy rules, AccessPath additions | Medium |
| Index ordering parity | Index key encoding changes, new value kinds | Medium |
| Reverse-index symmetry | Relation metadata evolution, new relation kinds | Medium |
| Recovery idempotence | Commit marker shape/version changes | Medium |
| Entity key match guarantees | Entity decode or identity codec changes | Low |
| Unique index guarantees | New uniqueness semantics or partial-index support | Medium |

## 10. Overall Invariant Risk Index

Invariant Integrity Risk Index (1-10, lower is better): **4/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Rationale:
- Core invariants are explicit and mostly enforced at narrow decode/plan/commit boundaries.
- Normal execution and recovery share key enforcement paths (notably row-op preparation and decode helpers), which improves symmetry.
- Primary pressure comes from deliberate duplicate validation layers and future DESC-sensitive continuation semantics, not from current missing ASC protections.
