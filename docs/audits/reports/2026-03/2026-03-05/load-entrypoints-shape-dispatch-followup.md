# Load Entrypoints Shape Dispatch Follow-Up (2026-03-05)

## Concern

`executor/load/entrypoints.rs` still derived mode/order compatibility locally and
matched ordering + cursor variants in execution staging, which kept load shape
routing logic spread across entrypoint code.

## Change Applied

- Added continuation-owned requested shape contract:
  - `RequestedLoadExecutionShape`
- Extended continuation resolver boundary:
  - `ContinuationEngine::resolve_load_cursor_context(plan, cursor, requested_shape)`
- Kept compatibility + cursor revalidation inside continuation resolver:
  - mode/order compatibility
  - scalar/grouped cursor shape compatibility
  - scalar/grouped cursor revalidation
- Simplified load entrypoint context build:
  - `build_execution_context` now asks for resolved cursor context and no longer
    matches execution ordering locally.
- Simplified grouping/projection stage dispatch:
  - dispatch now matches only resolved cursor shape (`PreparedLoadCursor`) and
    no longer carries ordering mismatch error branches.

## Behavior

No behavior changes:

- grouped-vs-scalar mode mismatch invariants remain enforced
- cursor shape mismatch invariants remain enforced
- cursor revalidation remains mandatory

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.
