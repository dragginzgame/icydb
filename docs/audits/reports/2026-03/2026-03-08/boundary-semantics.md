# Boundary + Envelope Semantics Audit - 2026-03-08

Scope: bound transformations, envelope containment, and continuation monotonicity.

## Bound Transformation Proof Table

| Location | Transformation | Invariant Preserved | Enforcement Type | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `db/index/envelope.rs` (`resume_bounds_from_refs`) | lower-edge rewrite to `Bound::Excluded(anchor)` | strict continuation; no duplication | structural helper + runtime guards | Low |
| `db/index/scan.rs` (`ensure_anchor_within_envelope`) | pre-scan containment gate | no envelope escape | runtime guard | Low |
| `db/index/scan.rs` (`ensure_continuation_advanced`) | strict post-anchor progression gate | monotonic pagination | runtime guard | Low |
| `db/cursor/anchor.rs` (`validate_index_range_anchor`) | index-id/namespace/arity anchor checks | access-path immutability | runtime guard | Low |

## Envelope Attack Matrix

| Scenario | Structural Prevention? | Runtime Guard? | Test Coverage? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Anchor below lower bound | No | Yes | Yes | Low |
| Anchor above upper bound | No | Yes | Yes | Low |
| Anchor == upper | No | Yes (empty-envelope short-circuit after rewrite) | Yes | Low |
| Anchor with wrong namespace/index | No | Yes | Yes | Low |
| Mutated/composite path cursor attempt | Structural + validation rejection | Yes | Yes | Low |

## Upper Bound Immutability

| Code Path | Upper Modified? | Proven Immutable? | Risk |
| ---- | ---- | ---- | ---- |
| continuation rewrite in `index/envelope.rs` | No | Yes (lower-only rewrite) | Low |
| traversal in `index/scan.rs` | No | Yes (upper forwarded unchanged) | Low |

## Overall Envelope Risk Index

**3/10**
