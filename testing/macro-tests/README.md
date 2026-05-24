# icydb-schema-tests

Macro/schema contract test suite for generated IcyDB behavior.

This crate is test-only (`publish = false`) and validates schema contracts and generated behavior across workspace scenarios.
Shared fixture definitions live in `schema/test/fixtures`.
Canister integration tests live in `testing/integration`.

Typical command:

```bash
cargo test -p icydb-schema-tests --locked
```

References:

- Testing strategy: `TESTING.md`
- Workspace overview: `README.md`
