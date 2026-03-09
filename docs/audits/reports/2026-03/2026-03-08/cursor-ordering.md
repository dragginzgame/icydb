# Cursor + Ordering Correctness Audit - 2026-03-08

Scope: continuation correctness, ordering monotonicity, and envelope containment.

## Invariant Table

| Area | Evidence | Verified | Risk |
| ---- | ---- | ---- | ---- |
| Cursor signature and shape compatibility | `crates/icydb-core/src/db/cursor/spine.rs` (`validate_cursor_signature` and compatibility checks) | Yes | Low |
| Anchor index-id/namespace/arity checks | `crates/icydb-core/src/db/cursor/anchor.rs` (`validate_index_range_anchor`) | Yes | Low |
| Resume rewrite excludes anchor | `crates/icydb-core/src/db/index/envelope.rs` (`resume_bounds_from_refs`) | Yes | Low |
| Strict advancement after anchor | `ensure_continuation_advanced` in `db/index/scan.rs` | Yes | Low |
| Envelope containment is enforced before traversal | `ensure_anchor_within_envelope` in `db/index/scan.rs` | Yes | Low |

## Attack Scenario Outcome (Sampled)

| Scenario | Guarded? | Guard Type | Risk |
| ---- | ---- | ---- | ---- |
| Anchor out of envelope | Yes | runtime guard + invariant error | Low |
| Anchor equals upper bound | Yes | resume rewrite + empty-envelope short-circuit | Low |
| Wrong index id / namespace | Yes | cursor anchor validation | Low |
| No strict advance after anchor | Yes | continuation-advanced guard | Low |

## Verification Readout

- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS

## Overall Cursor/Ordering Risk Index

**3/10**
