# Cursor + Ordering Correctness Audit - 2026-03-05

Scope: continuation correctness, boundary monotonicity, and envelope containment.

## Invariant Table

| Area | Evidence | Verified | Risk |
| ---- | ---- | ---- | ---- |
| Signature compatibility | `validate_cursor_signature` in `crates/icydb-core/src/db/cursor/spine.rs` | Yes | Low |
| Direction and initial-offset compatibility | `validate_cursor_direction` + `validate_cursor_window_offset` in `spine.rs` | Yes | Low |
| Index-range anchor structural checks (id, namespace, arity) | `validate_index_range_anchor` in `crates/icydb-core/src/db/cursor/anchor.rs` | Yes | Low |
| Envelope containment | `KeyEnvelope::contains` and anchor checks in `index/envelope.rs` + `cursor/anchor.rs` | Yes | Low |
| Strict advancement past anchor | `continuation_advanced` in `index/envelope.rs` and `ensure_continuation_advanced` in `index/scan.rs` | Yes | Low |
| Resume bound rewrite uses excluded anchor | `resume_bounds_from_refs` in `crates/icydb-core/src/db/index/envelope.rs` | Yes | Low |

## Failure-Mode Classification

| Failure Type | Actual Error Surface | Correctness |
| ---- | ---- | ---- |
| Unsupported token version | `CursorPlanError::ContinuationCursorVersionMismatch` | Correct |
| Boundary arity mismatch | `CursorPlanError::ContinuationCursorBoundaryArityMismatch` | Correct |
| PK boundary type mismatch | `CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch` | Correct |
| Anchor out of envelope | payload invalidation in cursor anchor validation + index invariant in scan guard | Correct |

## Targeted Test Evidence

- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> BLOCKED in that environment by a local test-execution issue

## Overall Cursor/Ordering Risk Index

**3/10**
