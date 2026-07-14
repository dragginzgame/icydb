# Index Integrity Audit - 2026-03-03

Scope: index ordering, namespace isolation, unique enforcement, reverse-index symmetry, and replay consistency.

## Invariant Registry

| Invariant | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Key encode/decode is bounded and structural | `crates/icydb-core/src/db/index/key/codec/mod.rs` | PASS | Low |
| Continuation anchor must remain within original envelope | `crates/icydb-core/src/db/index/scan.rs` + `crates/icydb-core/src/db/index/envelope.rs` | PASS | Low |
| Continuation must strictly advance beyond anchor | `crates/icydb-core/src/db/index/scan.rs` (`ensure_continuation_advanced`) | PASS | Low |
| Unique validation happens before commit-op apply | `crates/icydb-core/src/db/index/plan/unique.rs` | PASS | Medium |
| Reverse relation mutations remain symmetric | `crates/icydb-core/src/db/relation/reverse_index.rs` | PASS | Medium |

## Key Encoding and Ordering

| Check | Evidence | Result | Risk |
| ---- | ---- | ---- | ---- |
| Lexicographic ordering contract | `IndexKey::cmp` in `crates/icydb-core/src/db/index/key/codec/mod.rs` | PASS | Low |
| Prefix/range lowering preserves bound semantics | `crates/icydb-core/src/db/index/range.rs` | PASS | Low |
| Resume bounds enforce strict exclusion of anchor | `resume_bounds_from_refs` in `crates/icydb-core/src/db/index/envelope.rs` | PASS | Low |

## Namespace and Index ID Isolation

| Check | Evidence | Result | Risk |
| ---- | ---- | ---- | ---- |
| Index id must match continuation anchor | `validate_index_range_anchor` in `crates/icydb-core/src/db/cursor/anchor.rs` | PASS | Low |
| User/system key namespace enforced | `IndexKeyKind` checks in `crates/icydb-core/src/db/cursor/anchor.rs` + `crates/icydb-core/src/db/relation/reverse_index.rs` | PASS | Low |
| Component arity mismatch rejected | `validate_index_range_anchor` in `crates/icydb-core/src/db/cursor/anchor.rs` | PASS | Low |

## Mutation and Replay Symmetry

| Area | Evidence | Result | Risk |
| ---- | ---- | ---- | ---- |
| Preflight prepare before apply | `open_commit_window` in `crates/icydb-core/src/db/executor/mutation/commit_window.rs` | PASS | Medium |
| Marker authority preserved on failure | `finish_commit` in `crates/icydb-core/src/db/commit/guard.rs` | PASS | Low |
| Replay idempotence and deterministic rebuild | `crates/icydb-core/src/db/commit/recovery.rs` | PASS | Medium |

## Targeted Test Evidence

- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS

## Overall Index Integrity Risk Index

**3/10**
