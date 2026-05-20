# icydb-primitives

Shared primitive metadata and scalar capability classification for IcyDB.

This crate defines canonical scalar registry data used by schema and runtime layers (for example `ScalarKind`, `ScalarMetadata`, and coercion-family metadata).

Use this crate when tooling or internal components need stable scalar capability information.

## Boundary

`icydb-primitives` owns scalar capability metadata that must stay shared across schema and runtime layers: scalar kind identity, coercion family, numeric-coercion support, arithmetic/equality/ordering support, query keyability, and storage-key encodability.

Schema-only trait-generation policy stays outside this crate. Decisions such as which generated Rust traits a schema wrapper derives, whether a primitive supports `%`, `Copy`, or `Hash`, and how schema macro inputs map onto concrete Rust types remain owned by `icydb-schema` / `icydb-schema-derive` until they are explicitly promoted into shared scalar metadata.

References:

- Workspace overview: `README.md`
- Release notes: `CHANGELOG.md`
