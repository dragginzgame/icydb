# icydb-schema-tests

Integration and design test suite for schema/macro-generated IcyDB behavior.

This crate is test-only (`publish = false`) and validates schema contracts, query semantics, and generated-system behavior across workspace scenarios.

Typical command:

```bash
cargo test -p icydb-schema-tests --locked
```

References:

- Testing strategy: `TESTING.md`
- Workspace overview: `README.md`
