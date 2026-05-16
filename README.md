![MSRV](https://img.shields.io/badge/rustc-1.95.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It gives Rust canisters typed entities, stable-memory storage,
indexes, fluent queries, reduced SQL, pagination, aggregate/grouped execution,
and explain/metrics surfaces.

Current workspace version: `0.156.7`

For local development setup, test prerequisites, and troubleshooting, see
[INSTALLING.md](INSTALLING.md). Safety notes for host-touching commands live in
[SECURITY.md](SECURITY.md).

## Why Use It?

- **Typed data model:** entities, fields, indexes, relations, validation, and
  persistence are generated from schema macros.
- **One query spine:** fluent Rust queries and reduced SQL lower into the same
  planner, prepared-plan cache, and executor terminals.
- **Stable-memory durability:** writes are committed through guarded row/index
  paths built for canister upgrades and recovery.
- **Operational visibility:** SQL/fluent explain output, attribution counters,
  storage snapshots, and metrics are available for debugging and audits.

## Use IcyDB

Pin IcyDB by tag in downstream canisters:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.156.7" }
```

SQL is enabled by default. For typed/fluent-only builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.156.7", default-features = false }
```

## Minimal Shape

Schema definitions normally live in a small schema crate used by the canister:

```rust
use icydb::design::prelude::*;

#[canister(memory_min = 10, memory_max = 20, commit_memory_id = 12)]
pub struct AppCanister {}

#[store(
    ident = "APP_STORE",
    canister = "AppCanister",
    data_memory_id = 10,
    index_memory_id = 11
)]
pub struct AppStore {}

#[entity(
    store = "AppStore",
    pk(field = "id"),
    index(fields = "name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "score", value(item(prim = "Decimal", scale = 3)))
    )
)]
pub struct User {}
```

Use the runtime prelude from canister code:

```rust
use icydb::prelude::*;

pub fn top_users() -> Result<Vec<User>, icydb::Error> {
    db!()
        .load::<User>()
        .filter_eq(User::ACTIVE, true)
        .order_desc(User::SCORE)
        .limit(10)
        .entities()
}

pub fn rename_user(id: Ulid, name: String) -> Result<User, icydb::Error> {
    let patch = icydb::db::StructuralPatch::new()
        .set_field(User::MODEL, "id", InputValue::Ulid(id))?
        .set_field(User::MODEL, "name", InputValue::Text(name))?;

    db!().mutate_structural::<User>(id, patch, icydb::db::MutationMode::Update)
}
```

With the default `sql` feature, the same entity can be queried through reduced
single-entity SQL:

```rust
use icydb::prelude::*;

let rows = db!().execute_sql_query::<User>(
    "SELECT id, name, score FROM User WHERE score >= 100 ORDER BY score DESC LIMIT 10",
)?;

let updated = db!().execute_sql_update::<User>(
    "UPDATE User SET name = 'Ada' WHERE id = '01J...' RETURNING id, name",
)?;
```

## Reduced SQL Scope

IcyDB supports a focused, canister-friendly SQL subset:

- `SELECT`, `EXPLAIN`, `DESCRIBE`, `SHOW TABLES`, `SHOW COLUMNS`, `SHOW INDEXES`
- `INSERT`, `UPDATE`, `DELETE`, including supported `RETURNING` shapes
- `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, projection aliases, `DISTINCT`,
  aggregates, grouped aggregates, and common scalar/numeric functions
- single-entity execution only

Out of scope by design: joins, subqueries, CTEs, quoted identifiers, window
functions, and broad unbounded pattern matching.

Detailed SQL contract: [docs/contracts/SQL_SUBSET.md](docs/contracts/SQL_SUBSET.md)

## Local SQL Demo

The repository includes a demo RPG canister with a `Character` table.

```bash
scripts/dev/sql-start-demo
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --canister demo_rpg --sql "SHOW TABLES"
```

`sql` keeps an explicit `--canister/-c` flag because it also accepts trailing
SQL text. Target-style commands such as `canister refresh`, `snapshot`,
`schema`, and `metrics` take the canister as a required positional argument.
All of them default the ICP environment to `demo`. To inspect local canister IDs:

```bash
cargo run -q -p icydb-cli -- canister list
cargo run -q -p icydb-cli -- canister list --environment test
```

`icydb sql` only queries the current canister state. It does not create or load
demo data automatically. Use `canister refresh` for the destructive local reset
flow for the selected ICP canister; it clears that canister's stable
memory, not host disk contents, then calls `__icydb_fixtures_load` when the
canister exports the configured fixture endpoint.

Read SQL is sent through the canister's standard controller-gated
`__icydb_query` endpoint, which returns the shell's perf footer
payload. SQL DDL uses the canister's `__icydb_ddl` update endpoint for supported
`CREATE INDEX` commands.

Canisters opt into DB endpoint surfaces through `icydb.toml`. `readonly = true`
generates the controller-gated `__icydb_query` endpoint. `ddl = true`
generates the `__icydb_ddl` update endpoint. `fixtures = true` generates the
`__icydb_fixtures_reset` and `__icydb_fixtures_load` update endpoints.
`schema.enabled = true` generates the `__icydb_schema` query endpoint for
accepted live schema metadata. The generated canister glue routes each SQL
statement to the matching accepted entity:

The CLI checks this config before calling generated endpoint families. If a
surface is disabled for the selected canister, the command fails locally with
the config key to enable instead of waiting for a replica method-not-found
error.

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

```rust
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    Ok(())
}
```

```bash
cargo run -q -p icydb-cli -- config init --canister demo_rpg --ddl --fixtures --metrics --metrics-reset --snapshot --schema
cargo run -q -p icydb-cli -- config show
cargo run -q -p icydb-cli -- config check -e demo
cargo run -q -p icydb-cli -- canister refresh demo_rpg -e demo
```

Interactive shell:

```bash
cargo run -q -p icydb-cli -- sql --canister demo_rpg
```

Installed CLI:

```bash
make install
icydb sql --canister demo_rpg --sql "SELECT COUNT(*) FROM character"
icydb sql -e test -c demo_rpg --sql "SHOW TABLES"
icydb canister refresh demo_rpg
icydb snapshot demo_rpg
icydb schema demo_rpg
icydb metrics demo_rpg
icydb metrics demo_rpg --reset
```

## Observability

Generated canisters can expose:

- `__icydb_snapshot()` for configured current storage inventory
- `__icydb_schema()` for configured accepted live schema metadata
- `__icydb_metrics(window_start_ms: Option<u64>)` for configured metrics
- `__icydb_metrics_reset()` to clear configured in-memory metrics

Example:

```bash
icydb snapshot <canister>
icydb schema <canister>
icydb metrics <canister>
icydb metrics <canister> --window-start-ms <timestamp>
icydb metrics <canister> --reset
```

## Repository Map

- `crates/icydb` — public API crate.
- `crates/icydb-core` — runtime, planner, executor, persisted rows, stores.
- `crates/icydb-derive` — public derive helpers.
- `crates/icydb-schema-derive` and `crates/icydb-schema` — schema macros and AST.
- `crates/icydb-cli` — local SQL shell.
- `schema/*` — demo, audit, and test schemas.
- `canisters/*` — demo, audit, and integration canisters.
- `testing/*` — macro, wasm, and Pocket-IC test support.
- `docs/contracts/*` — behavior contracts.
- `docs/changelog/*` — detailed release notes.

## Development

Local workstation setup, common checks, PocketIC test setup, wasm audit commands,
and troubleshooting are in [INSTALLING.md](INSTALLING.md).

## More Docs

- [INSTALLING.md](INSTALLING.md)
- [SECURITY.md](SECURITY.md)
- [CHANGELOG.md](CHANGELOG.md)
- [docs/contracts/QUERY_CONTRACT.md](docs/contracts/QUERY_CONTRACT.md)
- [docs/contracts/QUERY_PRACTICE.md](docs/contracts/QUERY_PRACTICE.md)
- [docs/contracts/RESOURCE_MODEL.md](docs/contracts/RESOURCE_MODEL.md)
- [docs/contracts/TRANSACTION_SEMANTICS.md](docs/contracts/TRANSACTION_SEMANTICS.md)
- [docs/ROADMAP.md](docs/ROADMAP.md)

## License

Licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
