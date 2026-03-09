# Load Entrypoints Wrapper Typed-Surface Follow-Up (2026-03-05)

## Concern

Public load entrypoint wrappers still called shared surface conversion helpers
(`into_*`) that performed wrapper-level output shape checks.

## Change Applied

- Removed `LoadExecutionSurface` conversion helper methods (`into_scalar_rows`,
  `into_scalar_page`, `into_scalar_page_with_trace`,
  `into_grouped_page_with_trace`).
- Added mode-typed wrapper materialization helpers in `entrypoints.rs`:
  - `execute_load_scalar_rows`
  - `execute_load_scalar_page`
  - `execute_load_scalar_page_with_trace`
  - `execute_load_grouped_page_with_trace`
- Updated public wrappers to call mode-typed helpers directly.

## Effect

- Public wrappers now return mode-typed outputs directly from typed helper
  boundaries.
- Wrapper-level surface conversion checks are removed.
- Any mismatch classification now remains internal to private load helpers.

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.
