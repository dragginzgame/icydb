# IcyDB Core

Runtime engine for IcyDB.

`icydb-core` contains execution, planning, storage-facing runtime logic, SQL
lowering and execution, schema catalog handling, diagnostics, and observability
plumbing used by the public `icydb` crate.

Canonical scalar representation and metadata come from `icydb-schema`;
`icydb-core` owns their runtime value operations, coercion policy, codecs, and
storage behavior.

Most applications should depend on `icydb` directly. Use `icydb-core` only when
you explicitly need internal runtime surfaces.

Current runtime behavior is governed by the accepted schema catalog, the query
contracts, and the SQL subset contract. Historical design notes are archived
under `docs/design/archive/`; they are not the current execution baseline.

References:

- Workspace overview: `../../README.md`
- Query contract: `../../docs/contracts/QUERY_CONTRACT.md`
- SQL subset contract: `../../docs/contracts/SQL_SUBSET.md`
- Release notes: `../../CHANGELOG.md`
