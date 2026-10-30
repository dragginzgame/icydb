# IcyDB

Public runtime facade for IcyDB.

This crate exposes accepted-schema database sessions, structural writes,
optional SQL and typed reads, shared runtime types, and the host build facade
used by generated canister actor glue.

Public IC endpoint exports belong to the separate `canic-icydb` adapter, with
explicit declarations and IC Wire payloads. This library can be used without
mounting endpoints; enabling a feature does not export a public method.

Ordinary database-owning canisters depend on `icydb` alone. Author declarations
through `icydb::model::prelude::*` and use this facade's runtime, scalar types
and build surfaces. The runtime-only `#[icydb::request_execution]` and
`#[icydb::test]` entry attributes are exposed directly.

Lightweight model or shared-contract crates may instead depend on `icydb-model`
alone: declarations use `icydb_model::prelude::*`, and schema-owned scalar
types are available through re-exports such as `icydb_model::schema::Ulid`.
Scalar/proposal-only tooling may use `icydb-schema` alone when model macros
are unnecessary. These are supported narrower dependency choices; disabling
the facade's default features still includes the database runtime.

Database writes use only the accepted schema snapshot for admission. They do
not invoke `icydb-model` application validators or normalizers. Applications
that want those Rust behaviors must call `normalize`, `validate`, or
`normalize_and_validate` explicitly before constructing a write.

For full setup, examples, and release notes:

- Workspace README: `../../README.md`
- [Dependency and installation guide](../../INSTALLING.md)
- Changelog: `../../CHANGELOG.md`
