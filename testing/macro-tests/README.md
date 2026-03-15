# icydb-schema-tests

Macro/schema contract test suite for generated IcyDB behavior.

This crate is test-only (`publish = false`) and validates schema contracts and generated behavior across workspace scenarios.
Shared fixture definitions live in `schema/fixtures`.
Pocket-IC canister integration tests live in `testing/pocket-ic`.

Typical command:

```bash
cargo test -p icydb-schema-tests --locked
```

References:

- Testing strategy: `TESTING.md`
- Workspace overview: `README.md`
