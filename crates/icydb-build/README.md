# IcyDB Build

Build-time helpers for generated canister wiring, query/export surfaces, and metrics endpoints.

This crate also generates canister-local SQL routing glue (`sql_dispatch`) that maps
runtime SQL entity identifiers to concrete typed entity execution surfaces.
That routing boundary is actor/facade-owned and intentionally not part of
`icydb-core` SQL semantics.

This crate is usually consumed transitively through `icydb` and is published to support downstream dependency resolution.

References:

- Workspace overview: `README.md`
- Release notes: `CHANGELOG.md`
