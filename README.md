![Dependency MSRV](https://img.shields.io/badge/dependency%20MSRV-1.88.0-blue.svg)
![Internal Toolchain](https://img.shields.io/badge/internal%20rustc-1.98.1-4c1.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It provides typed entities, durable stable-memory storage, indexes,
bounded typed and dynamic queries, a single-entity SQL frontend, explicit
schema migrations, and generated operational endpoints.

Current workspace version: `0.261.11`

IcyDB is pre-1.0. Incompatible internal format changes require recreation or
reinstall; schema migration operates only within the current supported format.
Read the [release notes](CHANGELOG.md) before upgrading.

## Add IcyDB

Use the same release for runtime and host-side model generation:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.261.11" }

[build-dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.261.11" }
```

The default feature set includes structural, typed, and dynamic reads and writes.
Optional features are `sql` for session SQL and generated SQL endpoints,
`metrics` for heap-only entity hit/instruction reports, and `migration` for
explicit schema-migration operations. Features compile capabilities; public
endpoints require explicit source declarations.

Runtime-enabled crates author schemas through `icydb::model`. Standalone
schema-only tooling may depend on `icydb-model` instead. The public dependency
path supports Rust `1.88.0`; workspace development uses pinned Rust `1.98.1`,
with a `1.96.0` declared floor for other workspace packages.

See [installation](INSTALLING.md) for feature and endpoint setup, local tools,
validation, and troubleshooting.

## Start From A Compiled Example

The maintained [single-package example](testing/model-facade-only/) is compiled
by the workspace. Its native test exercises generated startup, a typed insert,
and a typed read; its separate PocketIC test covers install and same-code
upgrade.

| File | Responsibility |
| --- | --- |
| [Cargo.toml](testing/model-facade-only/Cargo.toml) | Runtime and build dependencies, with SQL disabled |
| [design/mod.rs](testing/model-facade-only/src/design/mod.rs) | Canister namespace, journaled store key, record, and entity |
| [build.rs](testing/model-facade-only/src/build.rs) | Generate the actor from the shared schema module |
| [lib.rs](testing/model-facade-only/src/lib.rs) | Host memory grant, lifecycle wiring, typed writes and reads |
| [Canister test](testing/integration/tests/model_facade.rs) | Install, write, read, and upgrade qualification |

The fixture deliberately renames its IcyDB dependency to `runtime_api` to
exercise Cargo dependency resolution. Applications can use the ordinary
`icydb` name from the dependency example above. Its test endpoints are
demonstrations; application endpoints must enforce their own authorization.

Keep schema declarations in a module shared by the build script and canister.
Declare a permanent database `memory_namespace` and journaled store `key`;
grant the corresponding `icydb.<namespace>` memory pool in the canister host
with `icydb::ic_memory_range!` and `mode = Allowed`. Persisted logical keys
own allocation identity; Rust names and declaration order do not.

[Schema authoring](docs/guides/schema-authoring.md) covers scalar/composite
primary keys, generated identities, relations, named values, managed timestamps,
host grants, and memory profiles. [Startup readiness](docs/guides/startup-readiness.md)
covers composed lifecycle hooks and readiness before restoring application
timers or caches.

## Runtime Contracts

Accepted schema snapshots are the runtime authority. Generated declarations
propose schema and supply typed adapters; query planning, admission, storage,
and recovery consume accepted metadata.

- Journaled stores publish durable batches and converge them through the
  existing replicated recovery driver. Heap stores are intentionally volatile.
- Ordinary typed/dynamic reads use bounded public admission. A limit alone does
  not make an unsafe scan admissible. Trusted methods require application-owned
  authorization.
- Manual endpoints, timers, and background entries use
  `#[icydb::request_execution]` so nested database calls share request budgets.
  Generated IcyDB endpoints establish their scope automatically.
- Declared relations enforce target existence and delete restrictions.
  Collection relations to composite targets remain outside the supported scope.
- Named-record scalar paths support their accepted query/index capabilities.
  Lists, sets, and maps remain owner-local values with whole-field replacement.
- Same-store mutation batches can be atomic across at most 64 accepted entities.
  Separate writes remain separate commits; returning `Err` from application
  code does not undo earlier successful writes.
- Source-versioned schema migration is explicit. Supported entity renames retain
  accepted identity and data; physical transformations use bounded advancement,
  recovery, and terminal receipts.

Use the [public facade guide](docs/guides/public-facade-api.md) for maintained
query/write examples, [read intent](docs/guides/read-intent.md) for caller-facing
endpoints, and [schema migrations](docs/guides/schema-migrations.md) for deployment.

## SQL And Observability

The optional SQL frontend supports single-entity reads, mutations, aggregates,
grouping, introspection, and accepted-catalog DDL. It excludes joins, subqueries,
CTEs, window functions, and transaction blocks. The
[SQL subset contract](docs/contracts/SQL_SUBSET.md) owns the exact supported
syntax and semantics.

Generated SQL reads are controller-gated by default; an explicit synchronous
application guard can replace that authorization. Generated updates and DDL
remain administrative surfaces. Prefer typed endpoints for narrowly scoped
caller-facing reads.

Typed query explanations and SQL `EXPLAIN` describe planning. Optional metrics
report update-side entity hits and instructions; query calls cannot retain
heap counters. Storage snapshots and schema inspection have separate APIs.
[Diagnostics](docs/guides/diagnostics.md) explains compact E-codes.

## Development

Start with [INSTALLING.md](INSTALLING.md) and [local command safety](SECURITY.md).
Use focused tests while iterating; the complete validation workflow belongs to
release preparation. Raw non-gzipped Wasm bytes, IC cycles, and instruction counts
are the performance measures.

The public facade is in `crates/icydb`; runtime internals are in
`crates/icydb-core`. Model authoring lives in `icydb-model` and
`icydb-model-macros`, proposal/scalar contracts in `icydb-schema`, diagnostics
in `icydb-diagnostic-code`, and the CLI in `icydb-cli`.
`schema/`, `canisters/`, and `testing/` hold compiled examples and qualification
fixtures.

## Documentation

Guides explain usage; contracts define maintained behavior. Design reports and
release notes retain historical evidence and do not replace current contracts.

- [Durability operations](docs/operations/DURABILITY_GUIDE.md),
  [durability](docs/contracts/DURABILITY.md),
  [atomicity](docs/contracts/ATOMICITY.md), and
  [transaction semantics](docs/contracts/TRANSACTION_SEMANTICS.md)
- [Query contract](docs/contracts/QUERY_CONTRACT.md),
  [predicate semantics](docs/contracts/QUERY_PRACTICE.md),
  [cursors](docs/contracts/CURSOR.md), and
  [resource bounds](docs/contracts/RESOURCE_MODEL.md)
- [Read admission](docs/contracts/READ_ADMISSION.md),
  [write admission](docs/contracts/WRITE_ADMISSION.md),
  [relations](docs/contracts/REF_INTEGRITY.md),
  [identity](docs/contracts/IDENTITY_CONTRACT.md), and
  [nested storage](docs/contracts/NESTED_STORAGE.md)
- [Persisted-format policy](docs/contracts/PERSISTED_FORMAT_POLICY.md) and
  [durable-surface inventory](docs/contracts/PERSISTED_FORMAT_INVENTORY.md)
- [Multi-canister workflows](docs/guides/multi-canister-workflows.md) and
  [foundations](docs/FOUNDATIONS.md)
- [1.0 feature contract](docs/1.0-FEATURES.md) and
  [1.0 readiness](docs/1.0-TODO.md)

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option.
