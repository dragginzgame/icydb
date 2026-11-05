> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-entrypoints-final-surface-followup/01/report.md).
