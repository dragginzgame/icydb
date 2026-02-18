# Index Integrity Audit - 2026-02-18

Scope: ordering, namespace isolation, key structural safety, unique enforcement, reverse-index symmetry, and replay idempotence in `icydb-core`.

## 0. Index Invariant Registry

| Invariant | Category | Enforced Where |
| ---- | ---- | ---- |
| Raw key encoding/decoding is bounded and symmetric | Structural | `crates/icydb-core/src/db/index/key/codec.rs:131` |
| Index key byte order preserves canonical component order | Ordering | `crates/icydb-core/src/db/index/key/codec.rs:330`, `crates/icydb-core/src/db/index/key/codec.rs:591` |
| Index id and key namespace are part of key identity | Namespace | `crates/icydb-core/src/db/index/key/codec.rs:109`, `crates/icydb-core/src/db/index/key/codec.rs:165` |
| Invalid/truncated/overlong key payloads fail closed | Structural | `crates/icydb-core/src/db/index/key/codec.rs:426`, `crates/icydb-core/src/db/index/key/codec.rs:785` |
| Row mutations derive index mutations before apply | Mutation | `crates/icydb-core/src/db/commit/prepare.rs:95` |
| Reverse relation index deltas are derived from old/new row views | Mutation | `crates/icydb-core/src/db/relation/reverse_index.rs:212` |
| Unique constraint checks run before commit marker persistence | Mutation | `crates/icydb-core/src/db/index/plan/unique.rs:25`, `crates/icydb-core/src/db/executor/mutation.rs:142` |
| Replay reuses commit preparation path and applies deterministic ops | Recovery | `crates/icydb-core/src/db/commit/recovery.rs:94`, `crates/icydb-core/src/db/commit/prepare.rs:24` |
| Index/store replay is idempotent | Recovery | `crates/icydb-core/src/db/commit/tests.rs:332` |

## 1. Key Encoding and Ordering

| Key Type / Case | Symmetric? | Lexicographically Stable? | Risk |
| ---- | ---- | ---- | ---- |
| Base key encode/decode | Yes | Yes | Low |
| Max-cardinality keys | Yes (`index_key_roundtrip_supports_max_cardinality`) | Yes | Low |
| Mixed composite randomized ordering | Yes | Yes (`semantic_vs_bytes` tests) | Low |
| Corrupted segment boundaries | Rejected | N/A | Low |
| Cross-index isolation under same component bytes | Yes (isolated) | Yes | Low |

Evidence: `crates/icydb-core/src/db/index/key/codec.rs:574`, `crates/icydb-core/src/db/index/key/codec.rs:836`, `crates/icydb-core/src/db/index/key/codec.rs:921`, `crates/icydb-core/src/db/index/key/codec.rs:785`, `crates/icydb-core/src/db/index/key/codec.rs:1427`.

## 2. Namespace and Index Id Isolation

| Scenario | Can Cross-Decode? | Prevented Where | Risk |
| ---- | ---- | ---- | ---- |
| Key from index A interpreted as index B | No | index id in key bytes + decode validation | Low |
| System/User namespace confusion | No | explicit `IndexKeyKind` tag | Low |
| Prefix collisions across index ids | No | index id bytes in prefix bounds | Low |
| Trailing-byte acceptance | No | decode rejects trailing bytes | Low |

Evidence: `crates/icydb-core/src/db/index/key/codec.rs:109`, `crates/icydb-core/src/db/index/key/codec.rs:131`, `crates/icydb-core/src/db/index/key/codec.rs:542`, `crates/icydb-core/src/db/index/key/codec.rs:1427`.

## 3. IndexStore Entry Layout

| Entry Component | Layout Stable? | Decode Safe? | Risk |
| ---- | ---- | ---- | ---- |
| Raw key (`RawIndexKey`) | Yes | Yes | Low |
| Raw entry payload (`RawIndexEntry`) | Yes | Yes | Low |
| Inline fingerprint suffix (16 bytes) | Yes | Yes | Low |

Notes:
- Stored layout is `[RawIndexEntry bytes | 16-byte fingerprint]`.
- Fingerprint is diagnostic only; correctness authority remains row+index commit/replay.

Evidence: `crates/icydb-core/src/db/index/store/mod.rs:72`, `crates/icydb-core/src/db/index/store/mod.rs:88`, `crates/icydb-core/src/db/index/store/mod.rs:103`.

## 4. Reverse Relation Index Integrity

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |
| ---- | ---- | ---- | ---- |
| Save/replace transition | Yes | Low | Low |
| Delete transition | Yes | Low | Low |
| FK retarget update | Yes | Low | Low |
| Replay after interruption | Yes | Low | Low |

Evidence: `crates/icydb-core/src/db/relation/reverse_index.rs:212`, `crates/icydb-core/src/db/executor/tests/semantics.rs:736`, `crates/icydb-core/src/db/executor/tests/semantics.rs:790`, `crates/icydb-core/src/db/executor/tests/semantics.rs:868`, `crates/icydb-core/src/db/executor/tests/semantics.rs:1089`.

## 5. Unique Index Enforcement

| Scenario | Unique Enforced? | Recovery Enforced? | Risk |
| ---- | ---- | ---- | ---- |
| Normal save/replace conflict detection | Yes | Yes | Low |
| Replay of interrupted mutation marker | Yes | Yes | Low |
| Same-value replacement | Yes (no false conflict) | Yes | Low |
| Corrupt unique entry cardinality | Fail-closed as corruption | Yes | Low |

Evidence: `crates/icydb-core/src/db/index/plan/unique.rs:25`, `crates/icydb-core/src/db/commit/recovery.rs:94`, `crates/icydb-core/src/db/index/store/lookup.rs:213`.

## 6. Row-Index Coupling and Replay Equivalence

| Failure Point | Divergence Possible? | Protection Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| Prepare failure before marker | No | preflight prepare + rollback | Low |
| Interrupted commit after marker persist | No | deterministic replay + marker protocol | Low |
| Replay repeated | No | marker clear + idempotence | Low |
| Rebuild failure during startup repair | No | snapshot restore fail-closed | Low |

Evidence: `crates/icydb-core/src/db/executor/mutation.rs:142`, `crates/icydb-core/src/db/commit/recovery.rs:76`, `crates/icydb-core/src/db/commit/tests.rs:332`, `crates/icydb-core/src/db/commit/tests.rs:1089`.

## Overall Index Integrity Risk Index

Index Integrity Risk Index (1-10, lower is better): **3/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

Representative verification run:
- `cargo test -p icydb-core index_key_cross_index_isolation_keeps_ranges_separate -- --nocapture`
- `cargo test -p icydb-core index_key_ordering_randomized_mixed_composite_semantic_vs_bytes -- --nocapture`
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture`
