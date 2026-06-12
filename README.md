![Dependency MSRV](https://img.shields.io/badge/dependency%20MSRV-1.88.0-blue.svg)
![Internal Toolchain](https://img.shields.io/badge/internal%20rustc-1.96.0-4c1.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It gives Rust canisters typed entities, stable-memory storage,
accepted schema catalogs, indexes, fluent queries, a reduced single-entity SQL
surface, pagination, grouped aggregates, DDL-backed catalog mutation, and
generated observability endpoints.

Current workspace version: `0.181.18`

IcyDB's dependency-facing minimum supported Rust version is `1.88.0` for the
public `icydb` crate path. Repository development, formatting, Clippy, tests,
CI, and release builds use Rust `1.96.0`.

For local development setup, test prerequisites, and troubleshooting, see
[INSTALLING.md](INSTALLING.md). Safety notes for host-touching commands live in
[SECURITY.md](SECURITY.md).

## Current Shape

- Schema macros declare canisters, stores, entities, fields, indexes, records,
  enums, collection types, validators, sanitizers, and explicit relations.
- Accepted schema snapshots are the runtime authority for row layouts, index
  catalogs, schema reconciliation, SQL DDL, and observability.
- Primary keys can be scalar or composite. Composite keys use ordered
  components and generated key structs.
- Strong relations are explicit and validated at write time. Scalar and
  composite target identities use full accepted primary-key metadata; collection
  relations to composite targets remain intentionally deferred.
- SQL is intentionally single-entity. It is useful for canister-local reads,
  writes, aggregates, introspection, and accepted-catalog DDL, not joins or
  general relational workloads.

## Use IcyDB

Pin IcyDB by tag in downstream canisters:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.181.18" }
```

SQL is enabled by default. For typed/fluent-only builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.181.18", default-features = false }
```

Canisters normally call `icydb::start!()` in `src/lib.rs` and use a build
script to generate actor glue with `icydb::build::BuildOptions`.

## Minimal Schema

Schema definitions normally live in a small schema crate used by the canister:

```rust
use icydb::design::prelude::*;

#[canister(
    memory_namespace = "app",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103
)]
pub struct AppCanister {}

#[store(
    ident = "APP_STORE",
    store_name = "main",
    canister = "AppCanister",
    storage(stable(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
    ))
)]
pub struct AppStore {}

#[entity(
    store = "AppStore",
    version = 1,
    pk(field = "id"),
    index(field = "name"),
    index(fields = ["active", "score"]),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "score", value(item(prim = "Decimal", scale = 3)))
    )
)]
pub struct User {}

```

The main branch also accepts strict scalar shorthand such as `pk(field = "id")`
and `index(field = "name")`. Composite keys use ordered field lists such as
`pk(fields = ["tenant_id", "local_id"])`.

## Storage Modes

Stores choose one explicit storage contract:

- `storage(stable(...))`: durable stable-memory BTrees for data, index, and
  schema roles. This is the direct stable backend.
- `storage(heap())`: volatile Rust `BTreeMap` storage. It is useful for live
  in-process state and tests, but rows and indexes are not recovered across
  upgrade/reinitialization.
- `storage(journaled(...))`: journaled cached-stable storage. Reads use live
  Rust BTree projections, writes publish marker-bound journal batches, and fold
  applies committed journal records into canonical stable data/index/schema
  BTrees.

Journaled stores use four memory IDs: `data_memory_id`, `index_memory_id`,
`schema_memory_id`, and `journal_memory_id`. The first three are the canonical
stable source-of-truth roles; the fourth is the durable journal tail. Journaled
storage is durable, but it is not the same contract as direct `stable(...)`
storage, and it does not make `heap()` durable.

## Query From Rust

Use the runtime prelude from canister code:

```rust
use icydb::prelude::*;

pub fn top_users() -> Result<Vec<User>, icydb::Error> {
    db!()
        .load::<User>()
        .filter_eq("active", true)
        .order_desc("score")
        .limit(10)
        .entities()
}

pub fn rename_user(id: Ulid, name: String) -> Result<User, icydb::Error> {
    let patch = db!().structural_patch::<User, _, _>([(
        "name",
        InputValue::Text(name),
    )])?;

    db!().mutate_structural::<User>(id, patch, icydb::db::MutationMode::Update)
}
```

With the default `sql` feature, the same entity can be queried or mutated
through reduced single-entity SQL:

```rust
use icydb::prelude::*;

let rows = db!().execute_sql_query::<User>(
    "SELECT id, name, score FROM User WHERE score >= 100 ORDER BY score DESC LIMIT 10",
)?;

let updated = db!().execute_sql_update::<User>(
    "UPDATE User SET name = 'Ada' WHERE id = '01J...' RETURNING id, name",
)?;

let ddl = db!().execute_sql_ddl::<User>(
    "CREATE INDEX IF NOT EXISTS user_score_idx ON User (score)",
)?;
```

## SQL Scope

IcyDB supports a focused, canister-friendly SQL subset:

- `SELECT`, `EXPLAIN`, `DESCRIBE`, `SHOW ENTITIES`, `SHOW STORES`,
  `SHOW MEMORY`, `SHOW COLUMNS`, and `SHOW INDEXES`
- `INSERT`, `UPDATE`, and `DELETE`, including supported `RETURNING` shapes
- `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE INDEX IF NOT EXISTS`,
  `DROP INDEX`, and `DROP INDEX IF EXISTS`
- `ALTER TABLE ... ADD COLUMN`, `ALTER COLUMN ... SET/DROP DEFAULT`,
  `ALTER COLUMN ... SET/DROP NOT NULL`, `RENAME COLUMN`, and retained-slot
  `DROP COLUMN`
- `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, projection aliases, `DISTINCT`,
  aggregates, grouped aggregates, `HAVING`, searched `CASE`, and common
  scalar/numeric/text functions
- field-path indexes, multi-field indexes, unique indexes, filtered indexes,
  and deterministic `LOWER`/`UPPER`/`TRIM` expression indexes

Out of scope by design: joins, subqueries, CTEs, quoted identifiers, window
functions, cursor pagination in scalar SQL, and broad unbounded pattern
matching.

Detailed SQL contract: [docs/contracts/SQL_SUBSET.md](docs/contracts/SQL_SUBSET.md)

## Local Development

Repository setup, local SQL demo commands, generated endpoint config, CLI
usage, IC test prerequisites, and wasm report commands live in
[INSTALLING.md](INSTALLING.md).

## Repository Map

- `crates/icydb` - public API crate and facade.
- `crates/icydb-core` - runtime, planner, executor, persisted rows, stores,
  SQL, schema catalog, and metrics internals.
- `crates/icydb-build` - generated canister actor glue.
- `crates/icydb-config` - host-side `icydb.toml` parsing for build
  scripts and CLI checks.
- `crates/icydb-derive` - public derive helpers.
- `crates/icydb-schema-derive` and `crates/icydb-schema` - schema macros and
  schema AST.
- `crates/icydb-cli` - developer CLI for local SQL, config checks, canister
  lifecycle helpers, and observability reports.
- `schema/*` - demo, audit, and test schemas.
- `canisters/*` - demo, audit, and integration canisters.
- `testing/*` - macro, wasm, and IC testkit support.
- `docs/contracts/*` - behavior contracts.
- `docs/changelog/*` - detailed release notes.

## More Docs

- [INSTALLING.md](INSTALLING.md)
- [SECURITY.md](SECURITY.md)
- [CHANGELOG.md](CHANGELOG.md)
- [docs/contracts/QUERY_CONTRACT.md](docs/contracts/QUERY_CONTRACT.md)
- [docs/contracts/QUERY_PRACTICE.md](docs/contracts/QUERY_PRACTICE.md)
- [docs/contracts/SQL_SUBSET.md](docs/contracts/SQL_SUBSET.md)
- [docs/contracts/REF_INTEGRITY.md](docs/contracts/REF_INTEGRITY.md)
- [docs/contracts/RESOURCE_MODEL.md](docs/contracts/RESOURCE_MODEL.md)
- [docs/contracts/TRANSACTION_SEMANTICS.md](docs/contracts/TRANSACTION_SEMANTICS.md)
- [docs/ROADMAP.md](docs/ROADMAP.md)

## License

Licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
