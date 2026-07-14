# Access Stream Anchor Interpretation Follow-Up (2026-03-05)

## Scope

Strict scope only:

- Goal: stream layer should not interpret anchors.
- Non-goal: rewrite stream execution architecture.

## Changes Applied

- Added one conversion helper on access-stream continuation input:
  - `AccessScanContinuationInput::index_scan_continuation()`
- Updated physical index-range resolver wiring to consume index-layer continuation
  contract directly, instead of reading anchor in stream physical resolver:
  - removed `continuation.anchor()` usage from
    `executor/stream/access/physical.rs`
  - `resolve_index_range` now accepts `IndexScanContinuationInput<'_>` directly.
- Removed stream-binding anchor exposure and migrated remaining stream fast-path
  callsites to continuation-input wiring:
  - removed `AccessStreamBindings::index_range_anchor()` usage at load callsites
  - `try_execute_index_range_limit_pushdown_stream(...)` now accepts
    `AccessScanContinuationInput<'_>` rather than raw `anchor + direction`
  - aggregate fast-path index-range branch now passes
    `AccessScanContinuationInput::new(None, direction)` explicitly.
- Removed dead anchor-era helper APIs from stream access surface:
  - removed `AccessStreamBindings::with_index_range(...)`
  - removed `AccessScanContinuationInput::anchor()`.
- Collapsed remaining stream-owned anchor state in continuation carrier:
  - `AccessScanContinuationInput` now stores one
    `IndexScanContinuationInput` directly rather than separate
    `anchor + direction` fields.
  - `direction()` and `index_scan_continuation()` now delegate to the
    stored index-layer continuation contract.

## Behavioral Contract

No behavior changes:

- same direction and anchor values are passed through
- same index scan codepath and limits are used
- stream execution topology is unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.
- `cargo test -p icydb-core load_execute_stage_order_matches_linear_contract -- --nocapture` passed.
