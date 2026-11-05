> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Load Entrypoints Cursor Resolution Follow-Up (2026-03-05)

## Concern

`executor/load/entrypoints.rs` still owned cursor/order compatibility and cursor
revalidation branching, which kept continuation protocol decisions spread in the
load orchestration layer.

## Change Applied

- Added continuation-owned cursor resolution contracts in:
  - `crates/icydb-core/src/db/executor/continuation/mod.rs`
  - `LoadCursorInput`
  - `PreparedLoadCursor`
  - `ResolvedLoadCursorContext`
- Added resolver API:
  - `ContinuationEngine::resolve_load_cursor_context(plan, cursor)`
- Migrated load entrypoint context build to use resolver output:
  - `crates/icydb-core/src/db/executor/load/entrypoints.rs`

## What Moved Out of `load/entrypoints.rs`

- `plan.execution_ordering()` lookup
- `plan.supports_execution_cursor()` lookup
- scalar/grouped cursor-vs-ordering compatibility branching
- scalar/grouped cursor revalidation calls

`load/entrypoints` now consumes:

- `resolved_cursor.ordering()`
- `resolved_cursor.supports_cursor()`
- `resolved_cursor.into_cursor()`

## Behavior

No behavior changes:

- mode/order compatibility checks still enforced
- cursor revalidation still mandatory before execution
- grouped/scalar mismatch invariants unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-entrypoints-cursor-resolution-followup/01/report.md).
