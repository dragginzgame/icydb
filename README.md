![MSRV](https://img.shields.io/badge/rustc-1.94.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB — Query Execution Engine + Typed Models for Internet Computer Canisters

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/> <img src="assets/swampfree.png" alt="100% Certified Swamp-Free" width="120"/>

> Schema-first entity modeling plus a deterministic query execution engine for IC canisters.
> Built for [Dragginz](https://dragginz.io/), now open to everyone.

---

## SQL Quickstart

Use this when working inside this repo with the test SQL canister.

1. Initialize the SQL harness (deploy + erase fixtures + load defaults):

```bash
scripts/dev/sql.sh --init
```

2. Run one query:

```bash
scripts/dev/sql.sh "select name, charisma from character order by charisma desc limit 5"
```

3. Command split:

```bash
scripts/dev/sql.sh --deploy   # deploy canister only
scripts/dev/sql.sh --reset    # destructive: erase fixtures + reload defaults
scripts/dev/sql.sh --init     # deploy + destructive reset + reload
```

---

## What Is IcyDB?

**IcyDB** is an embedded Rust runtime for canister data with a typed query planner/executor.

You get:

- typed entities and indexes from macros,
- fluent query APIs (`load`, `delete`, grouped/aggregate terminals),
- reduced SQL entrypoints that lower into the same planner/executor,
- stable-memory persistence with guarded recovery for interrupted writes,
- and explain/metrics surfaces for execution observability.

If you are new to this space: think "database-like query execution and safety" while still coding with normal Rust types.

---

## Current Line

- Workspace version on `main`: `0.55.7`
- Latest tagged release in this repo: `v0.55.7`
- Changelog: `CHANGELOG.md`
- Detailed `0.55.x` notes: `docs/changelog/0.55.md`

---

## 0.55 Highlights

- Expression indexes are now part of the core planning/runtime path for supported deterministic key expressions.
- SQL dispatch routing is entity-keyed and deterministic, with one generated `sql_dispatch` surface for canister endpoints.
- Continuation and load-pipeline containment work reduced mixed-concern executor hubs while preserving staged planner -> route -> executor contracts.

---

## Why Use It?

- **Real query execution engine**: intent -> planner -> executor, not just macro-generated structs.
- **Deterministic pagination**: cursor tokens are forward-only and bound to canonical query shape.
- **Fluent + SQL surfaces**: use typed Rust builders or reduced SQL, both routed through one runtime.
- **Stable-memory durability**: data survives upgrades and write interruption recovery is explicit.
- **Execution observability**: `EXPLAIN`, trace metadata, and row-flow counters support debugging.

---

## Library Quick Start

### 1. Toolchain

- Rust `1.94.0` (edition 2024)

```bash
rustup toolchain install 1.94.0
```

### 2. Add IcyDB

Use a pinned git tag so builds are repeatable:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.55.7" }
```

---

## Example

### Define an entity

```rust
use icydb::prelude::*;

#[entity(
    pk(field = "id", source = "internal"), // use "external" if IDs come from callers
    fields(
        field(ident = "id", value(item(prim = "Ulid"))),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "description", value(item(prim = "Text"))),
    ),
)]
pub struct User;
```

### Run a typed fluent query

```rust
use icydb::prelude::*;

pub fn users_named_ann() -> Result<Vec<View<User>>, icydb::Error> {
    let views = db!()
        .load::<User>()
        .filter_expr(FilterExpr::eq(User::NAME, "ann"))?
        .order_by("name")
        .offset(100)
        .limit(50)
        .views()?;

    Ok(views)
}
```

### Explain one query before execution

```rust
use icydb::prelude::*;

pub fn explain_users_named_ann() -> Result<String, icydb::Error> {
    let explain = db!()
        .load::<User>()
        .filter_expr(FilterExpr::eq(User::NAME, "ann"))?
        .order_by("name")
        .limit(25)
        .explain_execution_verbose()?;

    Ok(explain)
}
```

### Execute reduced SQL (same planner/executor path)

```rust
use icydb::prelude::*;

let projected = db!().execute_sql_projection::<User>(
    "SELECT id, name FROM User WHERE name = 'ann' ORDER BY id LIMIT 25",
)?;

let grouped = db!().execute_sql_grouped::<User>(
    "SELECT name, COUNT(id) FROM User GROUP BY name ORDER BY name LIMIT 10",
    None,
)?;
```

### Expose SQL endpoints in your canister (generated dispatch)

`icydb::start!()` generates a `sql_dispatch` module for your canister schema.
Use it to expose a small SQL API without hand-written per-entity routing:

```rust
use ic_cdk::query;
use icydb::db::sql::SqlQueryRowsOutput;

icydb::start!();

#[query]
fn sql_entities() -> Vec<String> {
    sql_dispatch::entities()
}

#[query]
fn query(sql: String) -> Result<Vec<String>, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

#[query]
fn query_rows(sql: String) -> Result<SqlQueryRowsOutput, icydb::Error> {
    sql_dispatch::query_rows(sql.as_str())
}
```

What each endpoint returns:

- `sql_entities`: supported SQL entity names for this canister.
- `query`: shell-friendly output lines (good for `dfx canister call` and logs), including `EXPLAIN` output.
- `query_rows`: structured projection rows (`entity`, `columns`, `rows`, `row_count`) for programmatic clients.

Dispatch behavior:

- Routing is keyed by the parsed SQL entity name.
- Unknown entities fail immediately with one deterministic unsupported-entity error listing supported entities.
- `EXPLAIN` follows execution parity: invalid/non-executable queries are rejected (for example unordered `LIMIT/OFFSET`).

Example calls:

```bash
dfx canister call <canister> sql_entities
dfx canister call <canister> query '("SELECT id, name FROM User ORDER BY id LIMIT 5")'
dfx canister call <canister> query_rows '("SELECT id, name FROM User ORDER BY id LIMIT 5")'
dfx canister call <canister> query '("EXPLAIN SELECT id, name FROM User ORDER BY id LIMIT 5")'
```

---

## Query Engine Notes

- `db!().load::<User>()` and `db!().delete::<User>()` build typed query intent.
- Planning validates fields/operators/coercions, then chooses a valid access strategy.
- Execution performs defensive boundary validation and fail-closed cursor checks.
- Cursor pagination requires ordered queries and appends primary-key tie-break ordering when needed.
- Grouped execution is explicit and bounded by runtime resource guards.

For contract-level behavior:

- `docs/contracts/QUERY_CONTRACT.md`
- `docs/contracts/QUERY_PRACTICE.md`
- `docs/contracts/RESOURCE_MODEL.md`
- `docs/contracts/SQL_SUBSET.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`

### Aggregate terminals

```rust
use icydb::prelude::*;

let median_rank_id = db!()
    .load::<User>()
    .filter_expr(FilterExpr::eq(User::GROUP, 7))?
    .order_by("id")
    .median_by("rank")?;

let distinct_ranks = db!()
    .load::<User>()
    .filter_expr(FilterExpr::eq(User::GROUP, 7))?
    .order_by("id")
    .count_distinct_by("rank")?;

let min_max_rank_ids = db!()
    .load::<User>()
    .filter_expr(FilterExpr::eq(User::GROUP, 7))?
    .order_by("id")
    .min_max_by("rank")?;
```

### Batch writes: choose your lane

IcyDB has two explicit batch-write behaviors:

- `*_many_atomic`: all-or-nothing for a **single entity type per call**
- `*_many_non_atomic`: fail-fast, earlier items may commit before a later error

```rust
use icydb::prelude::*;

// Single-entity-type atomic batch:
// either all User rows commit, or none do.
let users = vec![user_a, user_b, user_c];
let _saved = db!().insert_many_atomic::<User>(users)?;

// Non-atomic batch:
// earlier rows may already be committed if a later row fails.
let _maybe_partial = db!().insert_many_non_atomic::<User>(more_users)?;
```

`*_many_atomic` is not a multi-entity transaction API. Coordinating `User` and `Order`
in one atomic transaction is out of scope for the current surface.

---

## Reduced SQL Scope (Current 0.55 Line)

Executable SQL entrypoints:

- `execute_sql` for entity-shaped `SELECT`/`DELETE`
- `execute_sql_projection` for projection-shaped `SELECT`
- `execute_sql_grouped` for constrained grouped aggregates
- `execute_sql_aggregate` for constrained global aggregates
- `explain_sql` for `EXPLAIN` wrappers over executable reduced SQL

Out of scope and fail-closed by design:

- `INSERT`, `UPDATE`
- joins/subqueries/CTEs
- table aliases
- quoted identifiers
- window functions

---

## Project Layout

- `crates/icydb` — public API crate.
- `crates/icydb-core` — runtime, planner, executor, stores.
- `crates/icydb-derive` — derive macros and helper codegen surfaces.
- `crates/icydb-primitives` — shared primitive/domain types.
- `crates/icydb-schema-derive` — procedural macros for schema/types.
- `crates/icydb-schema` — schema AST and validation.
- `crates/icydb-build` — build-time codegen for canister wiring.
- `canisters/minimal` — minimal SQL canister harness for wasm footprint auditing.
- `canisters/quickstart` — SQL quickstart canister harness for onboarding and integration flows.
- `schema/quickstart` — SQL quickstart canister schema fixtures.
- `schema/minimal` — minimal schema fixtures for lightweight wasm audits.
- `schema/test` — shared schema fixtures for macro/e2e test harnesses.
- `testing/macro-tests` — macro and schema contract tests.
- `testing/pocket-ic` — Pocket-IC integration tests for canister flows.
- `assets`, `scripts`, `Makefile` — docs, helpers, workspace commands.

---

## Schema Crates

IcyDB keeps schema definitions in dedicated crates so canister builds only link
the schema surface they need.

- `schema/quickstart` holds the SQL quickstart canister schema surface.
- `schema/test` holds shared schema fixtures used by macro/e2e test harnesses.
- `canisters/quickstart/src/seed` holds deterministic quickstart fixture datasets.
- `schema/minimal` holds a tiny single-entity schema used by the
  `canisters/minimal` wasm footprint baseline.

This split keeps the wasm audit baseline from absorbing unrelated fixture schema
weight while preserving full-featured fixtures for test harnesses.

---

## Observability Endpoints

IcyDB generates these canister methods:

- `icydb_snapshot()` -> current storage report
- `icydb_metrics(window_start_ms: Option<u64>)` -> metrics window filter
- `icydb_metrics_reset()` -> clears in-memory metrics

Example:

```bash
dfx canister call <canister> icydb_snapshot
dfx canister call <canister> icydb_metrics '(null)'
dfx canister call <canister> icydb_metrics '(opt 1735689600000)'
dfx canister call <canister> icydb_metrics_reset
```

---

## Local Development

```bash
make check      # type-check workspace
make clippy     # lint (warnings denied)
make test       # unit + integration tests
make fmt        # format workspace
make build      # release build
make wasm-size-report   # build/report minimal canister wasm size
make wasm-audit-report  # write dated wasm+twiggy audit report under docs/audits/reports
```

Pre-commit hooks run:

```bash
cargo fmt --all -- --check
cargo sort --check
cargo sort-derives --check
```

---

## Versioning and Security

- Tags are treated as immutable.
- Pin to a specific tag in production.
- Avoid floating branches for production deployments.

Check tags:

```bash
git ls-remote --tags https://github.com/dragginzgame/icydb.git
```

---

## Current Focus

- Continue expression-index and reduced-SQL hardening with fail-closed behavior.
- Keep RouteShape/AccessPath authority boundaries stable while reducing executor branching pressure.
- Continue pipeline containment and continuation/cursor boundary cleanup without widening ownership surfaces.
- Preserve deterministic local SQL harness flows (`scripts/dev/sql.sh`) and CI parity.
- Track active work in `docs/ROADMAP.md` and current design docs under `docs/design/`.

---

## License

Licensed under either:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)

at your option.
