# icydb-schema-tests

Integration and design test suite for IcyDB schemas and generated behavior.

This crate validates schema contracts, macro-driven entities, query semantics, and end-to-end paths used by the workspace.

Notes:
- `publish = false` (test-only crate)
- includes integration tests under `tests/` and scenario modules under `src/`
- commonly run with `cargo test -p icydb-schema-tests`
