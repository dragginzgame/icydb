> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-entrypoints-wrapper-typed-surface-followup/01/report.md).
