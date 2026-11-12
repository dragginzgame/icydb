> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Load Entrypoints Module Split Follow-Up (2026-03-05)

## Concern

`executor/load/entrypoints.rs` remained a large orchestration hub despite prior
continuation cleanup, concentrating both scalar and grouped execution paths in
one file.

## Change Applied

- Split `entrypoints` into a directory module:
  - `crates/icydb-core/src/db/executor/load/entrypoints/mod.rs`
  - `crates/icydb-core/src/db/executor/load/entrypoints/scalar.rs`
  - `crates/icydb-core/src/db/executor/load/entrypoints/grouped/mod.rs`
- Moved scalar-specific entrypoint materialization + scalar execution spine into
  `scalar.rs`.
- Moved grouped-specific entrypoint materialization + grouped execution spine into
  `grouped.rs`.
- Kept `mod.rs` as the orchestration root for shared stage wiring and stage-order
  contract guards.

## Behavior

No behavior changes:

- stage order remains unchanged
- scalar/grouped execution contracts remain unchanged
- continuation flow and pagination behavior remain unchanged

## Verification

- `cargo fmt --all` passed.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core load_execute_stage_order_matches_linear_contract -- --nocapture` passed.
- `cargo test -p icydb-core load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary -- --nocapture` passed.
- `cargo test -p icydb-core grouped_fluent_execute_supports_cursor_continuation -- --nocapture` passed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/05/load-entrypoints-module-split-followup/01/report.md).
