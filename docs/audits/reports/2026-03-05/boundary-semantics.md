# Boundary + Envelope Semantics Audit - 2026-03-05

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

## Upper-Bound Immutability

| Code Path | Upper Modified? | Proven Immutable? | Risk |
| ---- | ---- | ---- | ---- |
| continuation resume in `index/envelope.rs` | no | yes (lower-only rewrite) | Low |
| scan traversal in `index/scan.rs` | no | yes (passes original upper bound) | Low |
| cursor application in `cursor/anchor.rs` | no | yes | Low |

## Edge Case: `anchor == upper` (Performance)

Case audited:
- input envelope includes `upper`
- continuation anchor equals `upper`
- ASC resume rewrite yields `(upper, upper]`

Verification:
- `resume_bounds_from_refs` rewrites to `Bound::Excluded(anchor)` on lower edge.
- `resolve_data_values_in_raw_range_limited` calls `envelope_is_empty(&start_raw, &end_raw)` immediately after rewrite.
- empty envelope returns `Ok(Vec::new())` before any `self.map.range((start_raw, end_raw))` iteration.
- regression coverage added in `crates/icydb-core/src/db/index/scan.rs`:
  `anchor_equal_to_upper_resumes_to_empty_envelope`.

Conclusion:
- no accidental `[upper, upper]` treatment detected.
- fast empty short-circuit exists in current control flow.
- this is performance-only (not correctness) and currently low pressure.

Edge-case risk index: **1/10**

## Development Hardening Assertions

- `resume_bounds_from_refs` now includes:
  - `debug_assert!(envelope.contains(anchor), "cursor anchor escaped envelope")`
  - ordered-bounds assertion before rewrite (`lower_key <= upper_key` when bounded)
- This hardens continuation-envelope invariants during development without changing release behavior.

## Duplication/Omission Guarantees

| Mechanism | Duplication Risk | Omission Risk |
| ---- | ---- | ---- |
| excluded-anchor resume rewrite | Low | Low |
| continuation strict-advance guard | Low | Low |
| boundary-based post-access filtering | Low | Low |

## Overall Envelope Risk Index

**3/10**
