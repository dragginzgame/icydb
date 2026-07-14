# Boundary + Envelope Semantics Audit - 2026-03-03

Scope: bound transformations, envelope containment, and continuation monotonicity.

## Bound Transformation Proof Table

| Location | Transformation | Invariant Preserved | Risk |
| ---- | ---- | ---- | ---- |
| `db/index/envelope.rs` (`resume_bounds_from_refs`) | rewrites one edge to `Bound::Excluded(anchor)` | strict continuation/no duplication | Low |
| `db/cursor/anchor.rs` | semantic range lowering + envelope containment check | no envelope escape | Low |
| `db/index/scan.rs` | anchor-within-envelope + strict-advance guards | fail-closed scan behavior | Low |

## Envelope Attack Matrix (Current Outcome)

| Scenario | Structural Prevention | Runtime Guard | Risk |
| ---- | ---- | ---- | ---- |
| anchor below lower bound | no | yes (`contains`) | Low |
| anchor above upper bound | no | yes (`contains`) | Low |
| wrong index id / namespace | no | yes (cursor-anchor checks) | Low |
| correct shape but no advancement | no | yes (`ensure_continuation_advanced`) | Low |

## Duplication/Omission Guarantees

| Mechanism | Duplication Risk | Omission Risk |
| ---- | ---- | ---- |
| excluded-anchor resume rewrite | Low | Low |
| continuation strict-advance guard | Low | Low |
| boundary-based post-access filtering | Low | Low |

## Overall Envelope Risk Index

**3/10**
