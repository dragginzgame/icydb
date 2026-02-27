# Boundary + Envelope Semantics Audit - 2026-02-20

Scope: correctness-only verification of envelope containment, inclusive/exclusive handling, monotonic continuation, and AccessPath immutability.

## 1. Invariant Registry

| Invariant | Enforced Where | Structural or Implicit? |
| ---- | ---- | ---- |
| Resume lower/upper edge rewrites are strict `Excluded(anchor)` by direction | `crates/icydb-core/src/db/index/range.rs:103`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:143` | Structural |
| Anchor must stay inside original envelope | `crates/icydb-core/src/db/index/range.rs:122`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:454` | Runtime guard |
| No upper-bound widening on continuation | `crates/icydb-core/src/db/index/range.rs:109`, `crates/icydb-core/src/db/index/range.rs:111` | Structural |
| Bound inclusivity/exclusivity is preserved end-to-end | `crates/icydb-core/src/db/index/range.rs:163`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:193` | Structural |
| Raw key ordering is canonical ordering source | `crates/icydb-core/src/db/index/store/lookup.rs:136`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:132` | Structural |
| Cursor cannot mutate access-path shape/index id/namespace/arity | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:414`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:429`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:439` | Runtime guard |

## 2. Bound Transformation Proof Table

| Location | Transformation | Invariant Preserved | Enforcement Type | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/index/range.rs:103` | `(lower, upper, anchor) -> resume_bounds` | Strict monotonic continuation | Structural | Low |
| `crates/icydb-core/src/db/query/plan/cursor_spine.rs:143` | Direction-aware `apply_anchor` (`Asc` lower excluded, `Desc` upper excluded) | No anchor re-inclusion | Structural | Low |
| `crates/icydb-core/src/db/index/range.rs:163` | `Bound<Value> -> Bound<Vec<u8>>` encode | Inclusive/exclusive parity | Structural | Low |
| `crates/icydb-core/src/db/index/range.rs:178` | `Bound<IndexKey> -> Bound<RawIndexKey>` | Raw-envelope equivalence | Structural | Low |
| `crates/icydb-core/src/db/query/plan/cursor_spine.rs:192` | `contains` check for candidate anchor | Envelope containment | Runtime guard | Low |
| `crates/icydb-core/src/db/index/store/lookup.rs:140` | continuation advancement gate while scanning | Strict forward-only progression | Runtime guard | Low |

## 3. Envelope Attack Matrix

| Scenario | Structural Prevention? | Runtime Guard? | Test Only? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Anchor == lower (Included) | No | Yes (`contains`) | No | Low |
| Anchor == lower (Excluded) | No | Yes (`contains`) | No | Low |
| Anchor == upper (Included) | No | Yes (`contains`) | No | Low |
| Anchor == upper (Excluded) | No | Yes (`contains`) | No | Low |
| Anchor just below lower | No | Yes (`contains`) | No | Low |
| Anchor just above upper | No | Yes (`contains`) | No | Low |
| Empty range | Yes (`is_empty`) | Yes | No | Low |
| Single-element range | No | Yes (`contains` + strict excluded resume) | No | Low |
| Unbounded range | Partial | Yes (`continuation_advanced`) | No | Medium |
| Composite/mutated AccessPath | No | Yes (unexpected anchor rejected) | No | Low |

## 4. Upper Bound Immutability

| Code Path | Upper Modified? | Proven Immutable? | Risk |
| ---- | ---- | ---- | ---- |
| `resume_bounds(Direction::Asc, ...)` | No | Yes (`upper` returned unchanged) | Low |
| `resume_bounds(Direction::Desc, ...)` | Yes (tightened only to `Excluded(anchor)`) | Yes (never widened) | Low |
| Cursor spine anchor validation | No | Yes (checks against original `range_end`) | Low |
| Store traversal with continuation | No | Yes (uses resolved bounds only) | Low |

## 5. Ordering Alignment

| Layer | Ordering Source | Divergence Possible? | Risk |
| ---- | ---- | ---- | ---- |
| Raw index traversal | `BTreeMap` key order | No | Low |
| Continuation advancement | `KeyEnvelope::continuation_advanced` | Low (single comparator authority) | Low |
| Cursor boundary comparison | canonical order slots + direction | Low | Low |
| Post-access cursor filter | strict `>` against boundary | Low | Low |

## 6. Anchor/Boundary Consistency

| Issue | Structural? | Guarded? | Drift-Sensitive? | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Boundary/anchor PK mismatch | No | Yes (`validate_index_range_boundary_anchor_consistency`) | Medium | Medium |
| Anchor index id mismatch | No | Yes | Low | Low |
| Anchor namespace mismatch | No | Yes | Low | Low |
| Anchor component-arity mismatch | No | Yes | Low | Low |

## 7. Composite Containment

| Property | Mutable? | Prevention Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| IndexRange -> composite path conversion | No | Access-path-kind gate + signature checks | Low |
| Index id mutation | No | decoded id must match planned id | Low |
| Predicate widening | No | continuation signature validation | Medium |
| Upper bound mutation | No | immutable range envelope checks | Low |

## 8. Duplication/Omission Proof

| Mechanism | Duplication Possible? | Omission Possible? | Risk |
| ---- | ---- | ---- | ---- |
| Strict excluded resume | No | Low | Low |
| Store monotonic progression gate | No | Low | Low |
| Post-access cursor filtering | Low | Low | Low |
| Offset-once continuation handling | No (explicit `effective_page_offset`) | Low | Low |

## 9. Drift Sensitivity

| Drift Vector | Impacted Invariant | Risk |
| ---- | ---- | ---- |
| Adding more continuation token fields | cursor compatibility constraints | Medium |
| Future DESC expansion in additional paths | ordering-alignment invariant | Medium |
| New AccessPath variants carrying anchors | composite containment invariant | Medium |
| Additional envelope helpers outside `KeyEnvelope` | single-authority boundary semantics | Medium |

## 10. Overall Envelope Risk Index

Overall Boundary/Envelope Risk Index (1-10, lower is better): **3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
