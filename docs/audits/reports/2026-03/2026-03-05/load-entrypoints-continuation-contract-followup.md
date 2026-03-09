# Load Entrypoints Continuation Contract Follow-Up (2026-03-05)

## Concern

`executor/load/entrypoints.rs` was acting as a continuation orchestration hub by
directly unpacking cursor boundary/anchor token internals to build runtime scan
inputs and continuation bindings.

## Change Applied

- Added `ResolvedScalarContinuationContext` in:
  - `crates/icydb-core/src/db/executor/continuation/mod.rs`
- Added engine resolver:
  - `ContinuationEngine::resolve_scalar_context(cursor, continuation_signature)`
- Migrated scalar load entrypoint path to consume the resolved contract:
  - `crates/icydb-core/src/db/executor/load/entrypoints.rs`

## What Moved Out of `load/entrypoints.rs`

- Direct anchor extraction from range token:
  - removed `index_range_token.map(range_token_anchor_key)`
- Direct continuation binding assembly from primitives:
  - removed local `ScalarContinuationBindings::new(...)` with primitive pieces
- Direct access-scan continuation input assembly from primitives:
  - removed local `AccessScanContinuationInput::new(previous_anchor, direction)`

These now flow through:

- `ResolvedScalarContinuationContext::bindings(direction)`
- `ResolvedScalarContinuationContext::access_scan_input(direction)`
- `ResolvedScalarContinuationContext::debug_assert_route_continuation_invariants(...)`
  (strict-advance + effective-offset debug contract checks)
- `ScalarContinuationBindings::validate_load_scan_budget_hint(...)`
  (continuation-owned scan-budget precondition checks formerly in load page helper)
- `ScalarContinuationBindings::{post_access_cursor_boundary, continuation_applied, effective_keep_count_for_limit}`
  plus `ScalarContinuationContext::{has_cursor_boundary, has_index_range_anchor, effective_page_offset_for_plan}`
  (replaces direct cursor-boundary protocol reads in load/kernel/route-mode callsites)
- `ScalarContinuationContext::{route_continuation_mode, route_window_projection_for_plan}`
  plus `ScalarRouteWindowProjection`
  (route/mode now consumes continuation-owned continuation-shape/window projection inputs,
  including precomputed keep/fetch window counts, instead of deriving them in router code)
- `ScalarRouteContinuationInvariantProjection`
  (scalar continuation debug assertion path now consumes a minimal DTO rather than
  the full `RouteContinuationPlan` contract type)

## Behavior

No behavior changes:

- route plan still derives direction and continuation policy
- effective-offset and strict-advance invariant checks remain
- cursor validation/revalidation remains unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
