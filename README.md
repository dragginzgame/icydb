![MSRV](https://img.shields.io/badge/rustc-1.95.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/>

IcyDB is a schema-first persistence and query runtime for Internet Computer
canisters. It gives Rust canisters typed entities, stable-memory storage,
indexes, fluent queries, reduced SQL, pagination, aggregate/grouped execution,
and explain/metrics surfaces.

Current workspace version: `0.156.1`

Local development commands that install tools, download binaries, or reset
canister state are documented in [SECURITY.md](SECURITY.md).

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

### Development Prerequisites

Install system prerequisites with your normal package manager before running the
repo's local targets. On Ubuntu, the packages are:

```bash
build-essential cmake curl wget gzip libssl-dev pkg-config ripgrep python3 python-is-python3
```

Canister development also needs:

```bash
binaryen wabt jq
```

Use the Rust toolchain pinned by this workspace:

```bash
rustup toolchain install 1.95.0
rustup target add wasm32-unknown-unknown
```

Local ICP workflows require the current Canic ICP tools with `icp` on `PATH`.
Install those tools through the Canic ICP distribution you normally use.

Optional canister-operation utilities should also be installed explicitly when
you need them, rather than through a repo bootstrap script:

- `didc` from DFINITY Candid releases.
- `idl2json` and `yaml2candid` from DFINITY idl2json releases.
- `quill` from DFINITY Quill releases.

For local repo maintenance after prerequisites are installed:

```bash
make update-dev            # check local prerequisites and fetch locked deps
make install-canister-deps # install wasm target plus cargo canister tools
make install-hooks         # opt into repo git hooks
```

This repository's local Make targets do not install OS packages or run `sudo`.

Pin IcyDB by tag in downstream canisters:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.156.1" }
```

SQL is enabled by default. For typed/fluent-only builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.156.1", default-features = false }
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
scripts/dev/sql-start-demo
cargo run -q -p icydb-cli -- sql --sql "SELECT name, charisma FROM character ORDER BY charisma DESC LIMIT 5"
cargo run -q -p icydb-cli -- sql --sql "DESCRIBE character"
cargo run -q -p icydb-cli -- sql --sql "SHOW TABLES"
```

The `sql` and `canister` commands default to the `demo_rpg` canister in the
`demo` ICP environment when `--canister` and `--environment` are omitted. To
inspect local canister IDs:

```bash
cargo run -q -p icydb-cli -- canister list
cargo run -q -p icydb-cli -- canister list --environment test
```

`icydb sql` only queries the current canister state. It does not create or load
demo data automatically. Use `canister refresh` for a generic destructive
rebuild/reinstall of the selected ICP canister; it clears that canister's stable
memory, not host disk contents. Any fixture loading is a canister-specific API
call, not an IcyDB CLI command:

Read SQL is sent through the canister's standard controller-gated
`icydb_admin_sql_query` endpoint, which returns the shell's perf footer
payload. SQL DDL uses the canister's `ddl` update endpoint for supported
`CREATE INDEX` commands.

Canisters opt into SQL surfaces through `icydb.toml`. `readonly = true`
generates the controller-gated `icydb_admin_sql_query` endpoint. `ddl = true`
generates the `ddl`, `fixtures_reset`, and `fixtures_load_default` update
endpoints. The generated canister glue routes each SQL statement to the
matching accepted entity:

```toml
[canisters.demo_rpg.sql]
readonly = true
ddl = true
```

```rust
fn icydb_admin_sql_load_default() -> Result<(), icydb::Error> {
    Ok(())
}
```

```bash
cargo run -q -p icydb-cli -- config show
cargo run -q -p icydb-cli -- config check --environment demo
cargo run -q -p icydb-cli -- canister refresh --canister demo_rpg
icp canister call demo_rpg fixtures_load_default '()' --environment demo
```

Interactive shell:

```bash
cargo run -q -p icydb-cli -- sql --canister demo_rpg
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
make test       # unit + integration tests; PocketIC tests need a binary
make fmt        # format workspace
make build      # release build
```

PocketIC-backed tests use `POCKET_IC_BIN` when it points at an executable
binary. To let the test helper download the pinned PocketIC release instead,
set `ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1`; set `POCKET_IC_SERVER_SHA256` as well
when you want checksum verification for the provided, cached, or downloaded
binary.

Useful audit commands:

```bash
make wasm-size-report
make wasm-size-report SIZE_REPORT_ARGS="--profile wasm-release --canister minimal"
make wasm-audit-report
make wasm-audit-report AUDIT_REPORT_ARGS="--profile wasm-release --canister minimal"
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
