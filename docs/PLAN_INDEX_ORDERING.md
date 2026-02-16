# PLAN_INDEX_ORDERING.md

## Overview

This document defines the canonical index key ordering model introduced in **0.10 (IndexKey v2)**.

IndexKey v2 replaces the prior hash/fingerprint-based format with a **canonical, framed, variable-length, lexicographically ordered key encoding**.

This redesign establishes:

* Deterministic semantic ordering.
* Stable byte-level representation.
* Range-scan capability.
* Composite prefix semantics.
* Canonical cursor continuation stability.

This document defines the **ordering protocol contract** and must remain stable across releases.

---

# 1. Goals

1. Ensure **semantic ordering equivalence** between values and encoded key bytes.
2. Enable efficient **range traversal** via lexicographic B-tree scans.
3. Guarantee **planner determinism**.
4. Guarantee **cursor stability across upgrades**.
5. Eliminate hash/fingerprint ordering artifacts.
6. Prevent future semantic drift in index key encoding.

---

# 2. Scope of Stability

## Internal vs External Stability

Index key byte encoding is:

* Stable within canister/storage boundaries.
* Stable across upgrades.
* Canonical for cursor continuation tokens.

If continuation tokens expose raw key bytes externally, the encoding becomes a **public protocol guarantee** and must never change without migration.

This must be decided explicitly.

---

# 3. Canonical Encoding Law

This is the foundational invariant:

For all indexable values `A` and `B`:

```
semantic_cmp(A, B) ==
byte_lex_cmp(encode(A), encode(B))
```

This must hold for:

* All supported scalar types.
* Composite tuples.
* Prefix subsets.
* PK tie-break ordering.

If this law fails, the entire ordering model is invalid.

---

# 4. IndexKey Structure

IndexKey encodes:

```
(kind, index_id, component_0, ..., component_n, primary_key)
```

### 4.1 Component Rules

* Each component is framed with length + tag.
* Tags are fixed and version-stable.
* Encoding must be total over all indexable types.
* No runtime heuristics or dynamic interpretation allowed.

### 4.2 index_id Rules

* index_id must be stable across upgrades.
* index_id ordering must not affect cross-index isolation.
* index_id must be fixed-width or length-framed consistently.
* index_id ordering must be deterministic.

Changing index_id semantics requires migration.

---

# 5. Ordering Semantics

## 5.1 Composite Ordering

Composite ordering respects left-to-right precedence:

```
(component_0, component_1, ..., component_n, pk)
```

The primary key acts as the final tie-breaker.

This guarantees:

* Strict total ordering.
* No ambiguous equal-key cases.
* Deterministic pagination.

---

## 5.2 Float Handling

Floats must obey:

* NaN is rejected (or canonicalized deterministically).
* +0.0 and -0.0 are encoded identically.
* Ordering reflects numeric semantics.

Float normalization is protocol-level and frozen.

---

## 5.3 Enum / Tagged Types

Enum ordering must be defined as:

* Stable discriminant order.
* Followed by canonical payload ordering (if any).

Changing enum ordering breaks index stability.

---

# 6. Prefix Semantics

## 6.1 Prefix Equivalence Law

Prefix scan correctness requires:

```
semantic_prefix_match(A, prefix) ==
byte_prefix_match(encode(A), encode(prefix))
```

These must be equivalent.

---

## 6.2 Unique Constraint Enforcement

Uniqueness is enforced via prefix scan over:

```
(component_0, ..., component_n)
```

If another row exists with identical non-PK prefix but different PK, uniqueness violation occurs.

This eliminates hash collision risk from prior fingerprint model.

---

# 7. Cursor & Pagination Invariants

## 7.1 Cursor Stability Law

Continuation tokens derived from index keys must satisfy:

* Byte-stable across upgrades.
* Deterministic resume position.
* Equivalent ordering comparison to canonical sort.

Cursor comparison must use the same ordering law as index sorting.

---

## 7.2 Page Boundary Invariant

Given pages P1 and P2:

* No row appears twice.
* No row is skipped.
* Strict monotonic ordering across pages.

---

# 8. Range Query Semantics

With canonical ordering:

* `>=`, `>`, `<`, `<=`, `BETWEEN`
  are implemented via bounded traversal.

Lower and upper bounds must be constructed using canonical encoded components.

---

# 9. Planner Contract

Planner eligibility checks for:

* ORDER BY pushdown
* Prefix filtering
* Range compatibility

must rely exclusively on canonical encodability and prefix membership.

No fingerprint heuristics allowed.

---

# 10. Non-Goals

This design does not include:

* Cost-based optimization.
* Multi-index intersection.
* Bitmap indexes.
* Compression strategies.
* Statistics-based selection.

---

# 11. Mandatory Test Matrix

Before implementation is considered stable:

### 11.1 Canonical Ordering Tests

* Numeric ascending/descending equivalence.
* Negative values.
* Mixed-length text ordering.
* Enum discriminant ordering.
* Composite ordering across types.
* Boundary values (min/max).
* Zero-length components.
* Large payload components.

### 11.2 Byte vs Semantic Equivalence Tests

Assert:

```
encode(A) < encode(B)
iff
semantic_cmp(A, B) == Less
```

Across randomized samples.

---

### 11.3 Prefix Bound Tests

* Lower-bound inclusive.
* Lower-bound exclusive.
* Upper-bound inclusive.
* Upper-bound exclusive.
* Composite prefix truncation.
* PK tie-break behavior.

---

### 11.4 Cursor Continuation Tests

* Resume after exact boundary.
* Resume inside composite prefix.
* Resume across page boundaries.
* Upgrade-stability test (encode/decode roundtrip).

---

### 11.5 Cross-Index Isolation

* Keys from different index_id must never collide.
* Range scans must not bleed into adjacent index namespace.

---

# 12. Pre-Implementation Checklist

Before writing encoder logic:

1. Define canonical Value ordering exhaustively.
2. Define tag space for all indexable types.
3. Freeze float normalization rules.
4. Decide NaN behavior explicitly.
5. Decide index_id encoding width and framing.
6. Confirm PK encoding canonicalization.
7. Confirm whether raw key bytes are public API.
8. Document migration strategy (if needed).

---

# 13. Migration Considerations

IndexKey v2 is incompatible with v1 hash-based keys.

Options:

* Full re-index migration.
* Dual-read compatibility layer.
* Versioned key namespace separation.

Migration must not silently alter ordering semantics.

---

# 14. Failure Modes to Guard Against

* Drift between semantic_cmp and byte_cmp.
* Prefix mismatch between semantic and byte interpretation.
* Cursor instability after upgrade.
* Index_id encoding change altering traversal ordering.
* Accidental Value variant expansion without encoder update.

---

# 15. Architectural Freeze Clause

IndexKey encoding, tag ordering, and canonical comparison rules are protocol-level and must not change without:

* Explicit version increment.
* Migration path.
* Backward-compatibility plan.

---

# Final Assessment

You are ready to begin implementation once:

* Canonical Value ordering is fully defined.
* All encoding invariants are explicitly written.
* Migration strategy is decided.
* Tests are scaffolded first.
