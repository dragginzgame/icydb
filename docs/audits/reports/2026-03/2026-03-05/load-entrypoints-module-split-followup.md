# Load Entrypoints Module Split Follow-Up (2026-03-05)

## Concern

`executor/load/entrypoints.rs` remained a large orchestration hub despite prior
continuation cleanup, concentrating both scalar and grouped execution paths in
one file.

## Change Applied

- Split `entrypoints` into a directory module:
  - `crates/icydb-core/src/db/executor/load/entrypoints/mod.rs`
  - `crates/icydb-core/src/db/executor/load/entrypoints/scalar.rs`
  - `crates/icydb-core/src/db/executor/load/entrypoints/grouped.rs`
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
