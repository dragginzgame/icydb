# Boundary + Envelope Semantics Audit - 2026-02-24

Scope: envelope containment, bound semantics, and continuation monotonicity.

## 1. Invariant Registry

| Invariant | Enforced Where | Structural or Runtime |
| ---- | ---- | ---- |
| Directional continuation edge rewrite is strict excluded anchor | `crates/icydb-core/src/db/query/cursor/spine.rs:177`, `crates/icydb-core/src/db/index/range.rs:171` | Structural |
| Anchor must stay inside original envelope | `crates/icydb-core/src/db/query/cursor/spine.rs:187`, `crates/icydb-core/src/db/index/range.rs:197` | Runtime guard |
| Continuation candidate must advance | `crates/icydb-core/src/db/query/cursor/spine.rs:202`, `crates/icydb-core/src/db/index/store/lookup.rs:111` | Runtime guard |
| Boundary/anchor coherence for index-range cursor | `crates/icydb-core/src/db/query/cursor/anchor.rs:105`, `crates/icydb-core/src/db/query/cursor/spine.rs:350` | Runtime guard |
| Raw envelope emptiness remains direction-agnostic | `crates/icydb-core/src/db/query/cursor/spine.rs:208`, `crates/icydb-core/src/db/index/range.rs:227` | Structural |

## 2. Bound Transformation Proof Table

| Location | Transformation | Invariant Preserved | Enforcement Type | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `index/range.rs:171` | resume bounds with anchor | strict-after continuation | Structural | Low |
| `cursor/spine.rs:177` | `apply_anchor` edge rewrite | no boundary re-inclusion | Structural | Low |
| `index/range.rs:197` | envelope containment check | no out-of-range anchor | Runtime guard | Low |
| `index/store/lookup.rs:35` | lookup bounds rewritten per continuation | continuation monotonicity | Structural | Low |

## 3. Envelope Containment Attack Matrix

| Scenario | Structural Prevention? | Runtime Guard? | Test Coverage? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Anchor below lower bound | No | Yes (`contains`) | Yes | Low |
| Anchor above upper bound | No | Yes (`contains`) | Yes | Low |
| Wrong index id in anchor | No | Yes (anchor consistency) | Yes | Low |
| Wrong boundary arity | No | Yes (`ContinuationCursorBoundaryArityMismatch`) | Yes | Low |
| Unbounded envelope with malformed cursor | No | Yes (decode + signature + direction + offset) | Yes | Medium |

## 4. Upper Bound Immutability Verification

| Code Path | Upper Modified? | Proven Immutable/Widen-Free? | Risk |
| ---- | ---- | ---- | ---- |
| `resume_bounds(Direction::Asc, ...)` | No | Yes | Low |
| `resume_bounds(Direction::Desc, ...)` | Tightened only | Yes (never widened) | Low |
| Cursor spine validation | No | Yes | Low |
| Lookup traversal | No | Yes | Low |

## 5. Raw vs Logical Ordering Alignment

| Layer | Ordering Source | Divergence Possible? | Risk |
| ---- | ---- | ---- | ---- |
| Index traversal | raw `BTreeMap` key order | Low | Low |
| Continuation advancement check | directional comparator in envelope | Low | Low |
| Post-access filtering | logical order cursor phase | Low | Low |

## 6. Anchor/Boundary Consistency Check

| Issue | Guarded? | Drift-Sensitive? | Risk |
| ---- | ---- | ---- | ---- |
| boundary/anchor arity mismatch | Yes | Medium | Medium |
| boundary/anchor field-type mismatch | Yes | Medium | Medium |
| non-index-range access with anchor | Yes | Low | Low |

## 7. Composite AccessPath Containment

| Property | Mutable? | Prevention Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| Access plan kind under continuation | No | cursor support gate in access plan | Low |
| index-range anchor use outside eligible paths | No | `CursorSupport::IndexRangeAnchor` gate | Low |
| path signature mutation | No | continuation signature verification | Medium |

## 8. Duplication / Omission Guarantee

| Mechanism | Duplication Possible? | Omission Possible? | Risk |
| ---- | ---- | ---- | ---- |
| strict excluded continuation edge | No | Low | Low |
| continuation advancement gate | No | Low | Low |
| pagination window offset on continuation | Low | Low | Low |

## 9. Drift Sensitivity Analysis

| Drift Vector | Impacted Invariant | Risk |
| ---- | ---- | ---- |
| Additional continuation token fields | compatibility gates must grow in lockstep | Medium |
| New access-path continuation support | anchor/boundary containment | Medium |
| Boundary helper duplication outside envelope core | semantic drift risk | Medium |

## 10. Overall Envelope Risk Index (1-10, lower is better)

**3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
