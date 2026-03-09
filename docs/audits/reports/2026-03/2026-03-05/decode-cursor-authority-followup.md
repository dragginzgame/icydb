# Decode Cursor Authority Follow-Up (2026-03-05)

## Rule

No module outside cursor runtime may call `decode_cursor`.

## Change Applied

- Added cursor-runtime decode boundary:
  - `decode_optional_cursor_token` in `crates/icydb-core/src/db/cursor/mod.rs`.
- Replaced direct session decode usage:
  - `crates/icydb-core/src/db/session.rs` now calls `decode_optional_cursor_token`.
- Kept session-level error mapping unchanged:
  - decode failures still map to `QueryError::Plan(PlanError::Cursor(...))`.
- Moved PK fast-path boundary decode authority out of load hub:
  - `load/mod.rs` no longer decodes PK cursor boundaries directly.
  - `ScalarContinuationContext::validate_pk_fast_path_boundary` in
    `crates/icydb-core/src/db/executor/continuation/mod.rs` now owns the
    `decode_pk_cursor_boundary` call.
  - route planner now gates PK fast-path shape and delegates boundary decode
    to continuation runtime context.

## Verification

- `rg -n "decode_cursor\\(" crates/icydb-core/src/db --glob '!**/codec/cursor.rs'`
  - Only match: `crates/icydb-core/src/db/cursor/mod.rs`.
- `rg -n "decode_pk_cursor_boundary" crates/icydb-core/src/db`
  - Runtime decode use now occurs in continuation runtime context, not load hub.
- `rg -n "validate_pk_fast_path_boundary_if_applicable" crates/icydb-core/src/db`
  - No remaining matches.
- `cargo check -p icydb-core` passed.
- `cargo test -p icydb-core --lib cursor::tests` passed.

## Net Effect

- Closed the `session.rs` continuation decode authority leak.
- Removed remaining load-hub PK boundary decode leak.
- Cursor token decode authority is now runtime-centralized in `db/cursor`.
