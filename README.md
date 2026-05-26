![Declared MSRV](https://img.shields.io/badge/declared%20MSRV-1.88.0-blue.svg)
![Internal Toolchain](https://img.shields.io/badge/internal%20rustc-1.95.0-4c1.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It gives Rust canisters typed entities, stable-memory storage,
accepted schema catalogs, indexes, fluent queries, a reduced single-entity SQL
surface, pagination, grouped aggregates, DDL-backed catalog mutation, and
generated observability endpoints.

Current workspace version: `0.164.5`

IcyDB's declared minimum supported Rust version is `1.88.0`. Repository
development, formatting, Clippy, CI, and release builds use Rust `1.95.0`.

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
- Strong relations are explicit and validated at write time. Relation runtime
  metadata is moving toward composite target support, but user-facing composite
  relation declarations are still in progress.
- SQL is intentionally single-entity. It is useful for canister-local reads,
  writes, aggregates, introspection, and accepted-catalog DDL, not joins or
  general relational workloads.

## Use IcyDB

Pin IcyDB by tag in downstream canisters:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.164.5" }
```

SQL is enabled by default. For typed/fluent-only builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.164.5", default-features = false }
```

Canisters normally call `icydb::start!()` in `src/lib.rs` and use a build
script to generate actor glue with `icydb::build::BuildOptions`. Local
canisters in this repository load those switches from `icydb.toml` through
`icydb-config-build`.

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
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102
)]
pub struct AppStore {}

#[entity(
    store = "AppStore",
    pk(fields = ["id"]),
    index(fields = ["name"]),
    index(fields = ["active", "score"]),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "score", value(item(prim = "Decimal", scale = 3)))
    )
)]
pub struct User {}

#[entity(
    store = "AppStore",
    pk(fields = ["tenant_id", "local_id"]),
    index(fields = ["status"]),
    fields(
        field(ident = "tenant_id", value(item(prim = "Text", max_len = 64))),
        field(ident = "local_id", value(item(prim = "Nat64"))),
        field(ident = "status", value(item(prim = "Text", max_len = 32)))
    )
)]
pub struct Account {}
```

The main branch also accepts strict scalar shorthand such as `pk(field = "id")`
and `index(field = "name")`; both forms normalize to the same ordered
field-list model.

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

- `SELECT`, `EXPLAIN`, `DESCRIBE`, `SHOW ENTITIES`, `SHOW TABLES`,
  `SHOW COLUMNS`, and `SHOW INDEXES`
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

## Generated Endpoints

Generated canister glue uses fixed `__icydb_*` Rust/export names. Endpoint
families are enabled through `icydb.toml` and checked by the CLI before calls:

```toml
[canisters.demo_rpg.sql]
readonly = true
ddl = true
fixtures = true

[canisters.demo_rpg.metrics]
enabled = true
reset = true

[canisters.demo_rpg.snapshot]
enabled = true

[canisters.demo_rpg.schema]
enabled = true
```

Current generated surfaces:

- `__icydb_query` for controller-gated read SQL
- `__icydb_ddl` for supported accepted-catalog SQL DDL
- `__icydb_fixtures_reset` and `__icydb_fixtures_load` for local fixture flows
- `__icydb_snapshot` for storage inventory and stable allocation metadata
- `__icydb_schema` and `__icydb_schema_check` for accepted schema diagnostics
- `__icydb_metrics` and `__icydb_metrics_reset` for runtime metrics

Fixture loading calls a plain non-exported user hook when present:

```rust
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    Ok(())
}
```

## Local SQL Demo

The repository includes a demo RPG canister with SQL-visible `character` and
`grid` entities. `character` has a scalar primary key; `grid` uses a composite
`(x, y)` primary key.

```bash
scripts/dev/sql-start-demo
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT x, y, terrain FROM grid ORDER BY danger_level DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SHOW TABLES"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "CREATE INDEX IF NOT EXISTS character_renown_idx ON character (renown)"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DROP INDEX IF EXISTS character_renown_idx ON character"
```

`sql` keeps an explicit `--canister/-c` flag because it also accepts trailing
SQL text. Target-style commands such as `snapshot`, `schema show`,
`schema check`, `metrics`, and `canister refresh` take the canister as a
required positional argument.

All canister-targeting commands default the ICP environment to `demo`, or use
`ICP_ENVIRONMENT` when it is set:

```bash
cargo run -q -p icydb-cli -- canister list
cargo run -q -p icydb-cli -- canister list --environment test
```

`icydb sql` only queries the current canister state. It does not create or load
demo data automatically. Use `canister refresh` for the destructive local reset
flow for the selected ICP canister; it clears that canister's stable memory,
then calls `__icydb_fixtures_load` when the fixture endpoint is configured.

## CLI Commands

Install the local CLI:

```bash
make install
```

Common command shapes:

```bash
icydb config init --canister demo_rpg --all
icydb config show --environment demo
icydb config check --environment demo

icydb sql --canister demo_rpg --sql "SELECT COUNT(*) FROM character"
icydb sql -e test -c demo_rpg --sql "SHOW TABLES"

icydb canister list
icydb canister deploy demo_rpg
icydb canister refresh demo_rpg
icydb canister upgrade demo_rpg
icydb canister status demo_rpg

icydb snapshot demo_rpg
icydb schema show demo_rpg
icydb schema check demo_rpg
icydb metrics demo_rpg
icydb metrics demo_rpg --window-start-ms <timestamp>
icydb metrics demo_rpg --reset
```

`config init` writes `icydb.toml` at the visible workspace root by default.
Readonly SQL is enabled unless `--no-readonly` is passed; `--all` enables every
currently supported generated endpoint family. `config show` prints the
resolved config visible from the current directory. Add `--environment <name>`
to compare configured canister names with the local ICP environment, and use
`config check --environment <name>` in scripts when that mismatch should fail.

## Repository Map

- `crates/icydb` - public API crate and facade.
- `crates/icydb-core` - runtime, planner, executor, persisted rows, stores,
  SQL, schema catalog, and metrics internals.
- `crates/icydb-build` - generated canister actor glue.
- `crates/icydb-config-build` - host-side `icydb.toml` parsing for build
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
