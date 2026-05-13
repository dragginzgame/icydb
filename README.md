![MSRV](https://img.shields.io/badge/rustc-1.95.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It gives Rust canisters typed entities, stable-memory storage,
indexes, fluent queries, reduced SQL, pagination, aggregate/grouped execution,
and explain/metrics surfaces.

Current workspace version: `0.154.4`

## Why Use It?

- **Typed data model:** entities, fields, indexes, relations, validation, and
  persistence are generated from schema macros.
- **One query spine:** fluent Rust queries and reduced SQL lower into the same
  planner, prepared-plan cache, and executor terminals.
- **Stable-memory durability:** writes are committed through guarded row/index
  paths built for canister upgrades and recovery.
- **Operational visibility:** SQL/fluent explain output, attribution counters,
  storage snapshots, and metrics are available for debugging and audits.

## Install

Use the Rust toolchain pinned by this workspace:

```bash
rustup toolchain install 1.95.0
```

Pin IcyDB by tag in downstream canisters:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.154.4" }
```

SQL is enabled by default. For typed/fluent-only builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.154.4", default-features = false }
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
        .filter(FilterExpr::gt("score", Decimal::from_i128_with_scale(100_000, 3)))
        .order_desc("score")
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
cargo run -q -p icydb-cli -- demo fresh
cargo run -q -p icydb-cli -- sql --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --sql "SHOW TABLES"
```

The `sql`, `canister`, and `demo` commands default to the `demo_rpg` canister
in the `demo` ICP environment when `--canister` and `--environment` are
omitted. To inspect local canister IDs:

```bash
cargo run -q -p icydb-cli -- canister list
cargo run -q -p icydb-cli -- canister list --environment test
```

`icydb sql` only queries the current canister state. It does not create or load
demo data automatically. Use `demo fresh` for a fresh reinstall and seed, or
`demo reload` when the canister already exists and should keep its installed
wasm:

```bash
cargo run -q -p icydb-cli -- demo reload
```

Interactive shell:

```bash
cargo run -q -p icydb-cli -- sql
```

Installed CLI:

```bash
make install
icydb sql --canister demo_rpg --sql "SELECT COUNT(*) FROM character"
icydb sql --environment test --canister demo_rpg --sql "SHOW TABLES"
```

## Observability

Generated canisters expose:

- `icydb_snapshot()` for current storage shape
- `icydb_metrics(window_start_ms: Option<u64>)` for metrics
- `icydb_metrics_reset()` to clear in-memory metrics

Example:

```bash
icp canister call <canister> icydb_snapshot '()' --environment demo
icp canister call <canister> icydb_metrics '(null)' --environment demo
icp canister call <canister> icydb_metrics_reset '()' --environment demo
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

```bash
make check      # type-check workspace
make clippy     # lint with warnings denied
make test       # unit + integration tests
make fmt        # format workspace
make build      # release build
```

Useful audit commands:

```bash
make wasm-size-report
make wasm-audit-report
```

## More Docs

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
