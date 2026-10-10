<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-readme-header.svg" alt="IcyDB — Store, organize and query data inside Internet Computer apps" width="100%">
</p>

<!-- helper-navigation:start -->
<p align="center">
  <a href="https://github.com/dragginzgame/canic"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/canic.svg" width="18" height="18" alt=""> <strong>canic</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/icydb"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/icydb.svg" width="18" height="18" alt=""> <strong>icydb</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-timers"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-timers.svg" width="18" height="18" alt=""> <strong>ic-timers</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-memory"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-memory.svg" width="18" height="18" alt=""> <strong>ic-memory</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-query"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-query.svg" width="18" height="18" alt=""> <strong>ic-query</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-backup"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-backup.svg" width="18" height="18" alt=""> <strong>ic-backup</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-blob-storage"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-blob-storage.svg" width="18" height="18" alt=""> <strong>ic-blob-storage</strong></a>
  &nbsp;&middot;&nbsp;
  <a href="https://github.com/dragginzgame/ic-testkit"><img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icons/ic-testkit.svg" width="18" height="18" alt=""> <strong>ic-testkit</strong></a>
</p>
<!-- helper-navigation:end -->

![Dependency MSRV](https://img.shields.io/badge/dependency%20MSRV-1.88.0-blue.svg)
![Internal Toolchain](https://img.shields.io/badge/internal%20rustc-1.99.0-4c1.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

IcyDB is a database toolkit for applications running on the Internet Computer.
It helps developers describe the information their application stores, save
that information inside a canister, and find or update records without building
a database layer from scratch.

On the Internet Computer, applications run in programs called **canisters**.
Canisters can hold both application code and data. IcyDB is embedded in a Rust
canister and uses the canister's own memory, so it does not require a separate
database server.

IcyDB is designed for structured information such as users, products, game
records, marketplace listings and relationships between records. Its queries
are deliberately predictable and resource-limited to fit the Internet
Computer's execution environment.

Current workspace version: `0.270.1`

> **Before using IcyDB:** IcyDB is still before version 1.0. Ordinary supported
> schema changes can use explicit migrations, but some IcyDB upgrades may change
> its internal storage format and require the database to be recreated or the
> canister to be reinstalled. Always read the [release notes](CHANGELOG.md)
> before upgrading an application that contains important data.

## At A Glance

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-at-a-glance.svg" alt="IcyDB at a glance: an embedded canister database for structured records, typed and bounded access, application-owned authorization and a deliberately smaller scope than PostgreSQL" width="800">
</p>

## When Might IcyDB Be Useful?

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-decision-guide.svg" alt="Decision guide for whether an Internet Computer application with structured records, upgrade persistence and predictable single-entity queries is a good fit for IcyDB" width="800">
</p>

## Key Ideas In Plain Language

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-terminology.svg" alt="Plain-language definitions of schema, entity, stable memory, index, typed API, bounded query and single-entity SQL" width="800">
</p>

## How It Works

1. The developer describes the application's records, fields, identities,
   indexes and relationships in a schema.
2. IcyDB generates typed Rust interfaces from that shared schema.
3. The canister accepts the schema as runtime metadata and uses it to validate
   storage and query behavior.
4. Application endpoints authorize callers, then use typed APIs or the optional
   restricted SQL frontend to read and change data.
5. IcyDB plans and executes admitted work within explicit resource limits and
   stores durable records through journaled stable-memory operations.

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-how-it-works.svg" alt="How a developer's data blueprint becomes typed Rust interfaces used by an application canister with stable records, indexes and bounded query planning" width="800">
</p>

## Important Limits

- IcyDB is embedded in an Internet Computer canister; it is not an external
  hosted database service.
- Queries operate on one entity type at a time. Joins, subqueries, common table
  expressions and window functions are not supported.
- Application code owns caller authorization. Enabling a capability does not
  automatically make a public endpoint safe.
- Separate writes are separate commits unless the application uses an explicitly
  supported bounded atomic batch.
- Returning `Err` from application code does not undo writes that already
  succeeded.
- Atomic behavior does not extend automatically across messages, stores or
  canisters.

## Add IcyDB

Use the same release for runtime and host-side model generation:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.270.1" }

[build-dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.270.1" }
```

The default feature set includes structural, typed, and dynamic reads and writes.
Optional features are `sql` for session SQL and generated SQL endpoints,
`metrics` for heap-only entity hit/instruction reports, and `migration` for
explicit schema-migration operations. Features compile capabilities; public
endpoints require explicit source declarations.

Runtime-enabled crates author schemas through `icydb::model`. Standalone
schema-only tooling may depend on `icydb-model` instead. The public dependency
path supports Rust `1.88.0`; workspace development uses pinned Rust `1.99.0`,
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
define the host `icydb_memory_pool` provider with a matching owner/namespace grant
in one shared allocation pool. Components request permanent keys; the host owns
physical exclusions and allocation authority. Existing persisted
logical keys own allocation identity; Rust names and declaration order do not.

[Schema authoring](docs/guides/schema-authoring.md) covers scalar/composite
primary keys, generated identities, relations, named values, managed timestamps,
host grants, and memory profiles. [Startup readiness](docs/guides/startup-readiness.md)
covers composed lifecycle hooks and readiness before restoring application
timers or caches.

## Runtime Contracts

Accepted schema snapshots are the runtime authority. Generated declarations
propose schema and supply typed adapters; query planning, admission, storage,
and recovery consume accepted metadata.

In practical terms, the schema is not only build-time documentation. The
accepted runtime form controls which records, fields, queries, indexes and
relationships the database may use.

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-query-journey.svg" alt="Query journey from application-owned authorization through schema validation and deterministic bounded planning to indexed or admitted record access and bounded results" width="800">
</p>

| Storage | Intended use | Durability |
| --- | --- | --- |
| Journaled stable storage | Important application records | Publishes durable batches and participates in replicated recovery |
| Heap storage | Temporary caches or deliberately volatile state | Live only; it has no stable allocation identity or durable recovery path |

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

<p align="center">
  <img src="https://raw.githubusercontent.com/dragginzgame/shared-assets/main/icydb/icydb-schema-lifecycle.svg" alt="Schema lifecycle from declaration and accepted runtime schema through stored records, supported schema changes and explicit bounded migration, with pre-1.0 internal format changes shown as a separate recreation or reinstall boundary" width="800">
</p>

> **Write behavior:** Separate successful writes remain committed even if later
> application code returns `Err`. Use the supported bounded same-store mutation
> batch when several admitted entity changes must commit atomically.

Use the [public facade guide](docs/guides/public-facade-api.md) for maintained
query/write examples, [read intent](docs/guides/read-intent.md) for caller-facing
endpoints, and [schema migrations](docs/guides/schema-migrations.md) for deployment.

## SQL And Observability

The optional SQL frontend operates on one entity type at a time:

| Supported | Not supported |
| --- | --- |
| Filtering and projection | Joins |
| Sorting and pagination | Subqueries |
| Grouping and aggregates | Common table expressions |
| Selected mutations | Window functions |
| Schema inspection and accepted-catalog DDL | PostgreSQL-style transaction blocks |

The [SQL subset contract](docs/contracts/SQL_SUBSET.md) owns the exact supported
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

| Topic | Start here |
| --- | --- |
| Installation and endpoint setup | [Installing IcyDB](INSTALLING.md) |
| Schema and generated Rust models | [Schema authoring](docs/guides/schema-authoring.md) |
| Typed reads and writes | [Public facade API](docs/guides/public-facade-api.md) |
| Durability and transactions | [Durability operations](docs/operations/DURABILITY_GUIDE.md), [durability contract](docs/contracts/DURABILITY.md), [atomicity](docs/contracts/ATOMICITY.md) and [transaction semantics](docs/contracts/TRANSACTION_SEMANTICS.md) |
| Queries and resource limits | [Query contract](docs/contracts/QUERY_CONTRACT.md), [predicate semantics](docs/contracts/QUERY_PRACTICE.md), [cursors](docs/contracts/CURSOR.md) and [resource bounds](docs/contracts/RESOURCE_MODEL.md) |
| Read and write safety | [Read admission](docs/contracts/READ_ADMISSION.md) and [write admission](docs/contracts/WRITE_ADMISSION.md) |
| Relations, identity and nested data | [Relations](docs/contracts/REF_INTEGRITY.md), [identity](docs/contracts/IDENTITY_CONTRACT.md) and [nested storage](docs/contracts/NESTED_STORAGE.md) |
| Stored-format compatibility | [Persisted-format policy](docs/contracts/PERSISTED_FORMAT_POLICY.md) and [durable-surface inventory](docs/contracts/PERSISTED_FORMAT_INVENTORY.md) |
| Architecture and multi-canister use | [Foundations](docs/FOUNDATIONS.md) and [multi-canister workflows](docs/guides/multi-canister-workflows.md) |
| Path to 1.0 | [Feature contract](docs/1.0-FEATURES.md) and [readiness tracker](docs/1.0-TODO.md) |

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option.
