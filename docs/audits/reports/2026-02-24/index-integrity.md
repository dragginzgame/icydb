# Index Integrity Audit - 2026-02-24

Scope: index key correctness, isolation, row/index coupling, and replay equivalence.

## 1. Index Invariant Registry

| Invariant | Enforced Where | Risk |
| ---- | ---- | ---- |
| Index key encode/decode symmetry | `crates/icydb-core/src/db/index/key/codec.rs:110`, `crates/icydb-core/src/db/index/key/codec.rs:144` | Low |
| Canonical ordered component encoding | `crates/icydb-core/src/db/index/key/ordered.rs:146` | Low |
| Prefix/range raw key bound correctness | `crates/icydb-core/src/db/index/range.rs:122`, `crates/icydb-core/src/db/index/range.rs:149` | Low |
| Index store entry validation at boundary | `crates/icydb-core/src/db/index/store/mod.rs:19`, `crates/icydb-core/src/db/index/store/lookup.rs:141` | Low |
| Unique index enforcement before commit apply | `crates/icydb-core/src/db/index/plan/unique.rs:25` | Medium |
| Reverse relation index mutations remain prepared ops | `crates/icydb-core/src/db/relation/reverse_index.rs:198` | Medium |

## 2. Key Encoding and Ordering Audit

| Check | Result | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| Encode/decode round-trip | Pass | codec encode/decode path + tests in `codec.rs` | Low |
| Lexicographic ordering monotonic | Pass | `IndexKey` ordering implementation in `codec.rs:66` | Low |
| Prefix bound generation consistency | Pass | `bounds_for_prefix_with_kind` via range helper | Low |
| Range component bound generation | Pass | `bounds_for_prefix_component_range` use in `range.rs:149` | Low |

## 3. Namespace and Index ID Isolation

| Isolation Check | Enforcement | Risk |
| ---- | ---- | ---- |
| User/system key-kind separation | `IndexKeyKind` tag + decode checks | Low |
| Index ID mismatch rejection in cursor/index-range path | executable validation tests (`index id mismatch`) | Low |
| Reverse relation uses system-key namespace | `reverse_index_key_for_target_value` uses `IndexKeyKind::System` | Low |

## 4. IndexStore Entry Layout

| Property | Status | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| Key/value validated before insertion | Enforced | `index/store/mod.rs:19` | Low |
| Fingerprint-based debug verification | Enforced | `index/store/mod.rs:114`, `index/store/mod.rs:126` | Low |
| Lookup decode and entity-key extraction guard | Enforced | `index/store/lookup.rs:141` | Low |

## 5. Reverse Relation Index Integrity

| Invariant | Enforcement | Risk |
| ---- | ---- | ---- |
| Target-value -> reverse index key mapping deterministic | `relation/reverse_index.rs:69` | Medium |
| Delete guard uses reverse index truth | `relation/validate.rs:58`, `relation/validate.rs:129` | Medium |
| Replay preserves reverse index membership semantics | `executor/tests/semantics.rs:1662` | Medium |

## 6. Unique Index Enforcement

| Check | Enforcement Point | Equivalent Across Paths? | Risk |
| ---- | ---- | ---- | ---- |
| uniqueness preflight check | `index/plan/unique.rs:25` | Yes | Low |
| uniqueness conflict class mapping | `error.rs` index violation constructor | Yes | Low |

## 7. Row <-> Index Coupling

| Coupling Requirement | Enforcement | Risk |
| ---- | ---- | ---- |
| Row commit prepares matching index ops | `commit/prepare.rs:130` | Low |
| Index apply and row apply stay coupled per prepared row op | `commit/apply.rs:28` | Low |
| Rollback preserves pre-state coupling on failure | `commit/rollback.rs:36` | Medium |

## 8. Recovery Replay Equivalence

| Phase | Normal | Replay | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| prepare row ops | commit window prepare | replay prepare | Yes | Low |
| index mutation apply | apply closure | replay apply | Yes | Low |
| marker-driven completion | finish commit clears marker | replay clears marker | Yes | Low |

## 9. Explicit Attack Scenarios

| Attack | Expected Outcome | Observed Guard | Risk |
| ---- | ---- | ---- | ---- |
| Corrupted raw index key bytes | decode rejection | `IndexKey::try_from_raw` corruption errors | Low |
| Unexpected key kind | decode rejection | `IndexKeyKind::from_tag` guard | Low |
| Cursor anchor for wrong index | plan rejection | executable/cursor validation | Low |
| Unique duplicate insert | conflict error | unique preflight check | Low |

## 10. High Risk Mutation Paths

| Path | Why Risky | Current Risk |
| ---- | ---- | ---- |
| reverse relation mutations in mixed update/delete sequences | multi-step membership transitions | Medium |
| index-range continuation with anchors | bound + id + envelope coupling | Medium |
| commit replay with partial failure rollback | rollback ordering must remain exact | Medium |

## 11. Storage-Layer Assumptions

| Assumption | Validation Status | Risk |
| ---- | ---- | ---- |
| Stable-memory bytes can be hostile | explicit decode guards in key/store/marker paths | Low |
| Store entry bytes are bounded | enforced by `MAX_INDEX_ENTRY_BYTES` and decode checks | Low |
| Index ordering delegated to raw key bytes | explicit comparator + canonical encode | Low |

## 12. Overall Index Risk Index (1-10, lower is better)

**3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## 12A. Determinism Cross-Check (Execution-Contract Mapping)

| Determinism Surface | Status | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| planner candidate ordering | deterministic | index models sorted by name | Low |
| commit/replay store traversal ordering | deterministic | recovery sorts store handles before rebuild | Low |
| index mutation staging order | deterministic | `BTreeMap`/`BTreeSet` staging in index planning | Low |
| registry base container ordering | non-deterministic container, sorted at critical boundaries | `HashMap` in registry; ordering-sensitive flows sort before use | Medium |

## 13. Next-Pass Deep Checks (Requested)

### A. Partial-Update Field Changes Affecting Index Membership (Index Scope)

Add explicit cases:
- old indexed field -> new indexed field
- old `null` -> new value
- old value -> `null`
- update removing entity from one index and adding to another
- unchanged indexed field (must produce no mutation)

Invariant to lock:
- prepared index ops remain minimal, correct, and symmetric.

### B. Mixed Unique + Reverse + Secondary Interaction (Index/Commit Scope)

Add one update that simultaneously:
- violates unique index
- modifies reverse relation
- modifies secondary index

Ordering invariant to lock:
- no side-effectful mutation (or prepared side-effectful mutation) before uniqueness verdict.

### C. Range + Delete Continuation Stability (Cross-Layer Scope)

Track as cross-layer (`executor` + `index`), not index-only:
- delete inside active paginated range
- continuation anchor must not resurrect deleted rows
- continuation anchor must not skip the next row
- continuation anchor must not duplicate rows
