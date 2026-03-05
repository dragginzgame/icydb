# Load Entrypoints Final Surface Follow-Up (2026-03-05)

## Concern

`apply_paging` and `materialize_surface` still carried repeated payload-shape
mismatch branches (`Scalar` vs `Grouped`) in load entrypoint orchestration.

## Change Applied

- Replaced payload+trace bundle output with typed final surface contract:
  - `LoadExecutionSurface`
  - variants:
    - `ScalarRows`
    - `ScalarPage`
    - `ScalarPageWithTrace`
    - `GroupedPageWithTrace`
- Updated `execute_load` to return `LoadExecutionSurface`.
- Simplified `apply_paging`:
  - mode-based normalization only
  - centralized scalar/grouped extraction helpers
- Simplified `materialize_surface`:
  - mode-based surface emission
  - no repeated cross-product mismatch match arms

## Behavior

No behavior changes:

- scalar rows still suppress continuation cursor
- scalar paged traced/non-traced surfaces remain distinct
- grouped paged trace surface remains unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.
