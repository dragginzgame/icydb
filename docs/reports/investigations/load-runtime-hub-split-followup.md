> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Load Runtime Hub Split Follow-Up (2026-03-05)

## Concern

`executor/load/mod.rs` still carried grouped runtime context/window types in one
large hub file, which slowed scanability and concentrated unrelated concerns.

## Change Applied

- Split grouped runtime context/window types into a dedicated module:
  - `crates/icydb-core/src/db/executor/load/grouped_runtime.rs`
- Moved these type definitions out of `load/mod.rs`:
  - `GroupedPaginationWindow`
  - `GroupedContinuationContext`
  - `GroupedRuntimeProjection`
  - `GroupedExecutionContext`
- Kept the external `load` boundary stable by re-exporting those types from
  `load/mod.rs`.
- Added a structural guard test:
  - `route_layer_does_not_compute_page_window_directly` in
    `crates/icydb-core/src/db/executor/tests/load_structure.rs`
  - The test scans `src/db/executor/route/**/*.rs` and fails if any file
    contains `compute_page_window(`.

## Behavior

No behavior changes:

- grouped runtime execution flow is unchanged
- grouped pagination/continuation contracts are unchanged
- route/load execution outputs are unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_execute_stage_order_matches_linear_contract -- --nocapture` passed.
- `cargo test -p icydb-core route_layer_does_not_compute_page_window_directly -- --nocapture` passed.
- `cargo test -p icydb-core db::executor::tests::route::load:: -- --nocapture` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-runtime-hub-split-followup/01/report.md).
