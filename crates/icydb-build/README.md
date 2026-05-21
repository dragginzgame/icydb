# IcyDB Build

Build-time helpers for generated canister wiring, query/export surfaces, and
observability endpoints.

This crate generates the actor glue used by `icydb::start!()`, including
store/session wiring and config-gated `__icydb_*` endpoints for SQL, DDL,
fixtures, schema reports, snapshots, and metrics. Endpoint emission is driven
by `BuildOptions`, normally produced from `icydb.toml` by `icydb-config-build`.

Generated build failures remain a codegen boundary: invalid schema metadata or
options panic during generation instead of being exposed as a runtime API.

This crate is usually consumed transitively through `icydb` and is published to
support downstream dependency resolution.

References:

- Workspace overview: `README.md`
- Release notes: `CHANGELOG.md`
