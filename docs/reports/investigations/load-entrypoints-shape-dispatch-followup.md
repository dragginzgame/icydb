> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-entrypoints-shape-dispatch-followup/01/report.md).
