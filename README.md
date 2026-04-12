![MSRV](https://img.shields.io/badge/rustc-1.94.1-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB — Query Execution Engine + Typed Models for Internet Computer Canisters

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/> <img src="assets/swampfree.png" alt="100% Certified Swamp-Free" width="120"/>

> Schema-first entity modeling plus a deterministic query execution engine for IC canisters.
> Built for [Dragginz](https://dragginz.io/), now open to everyone.

---

## Local SQL Demo

Use this when working inside this repo against the demo SQL canister surface.
The demo code now lives under `canisters/demo/rpg`, but the default local DFX
canister name is still `demo_rpg`, which is what `scripts/dev/sql.sh` talks
to unless you override `--canister`. The demo SQL endpoint is intentionally
hard-bound to the `Character` entity; it does not route SQL text across
multiple entity types.

1. Initialize the SQL harness (deploy + erase fixtures + load defaults):

```bash
scripts/dev/sql.sh --init
```

2. Run one query:

```bash
scripts/dev/sql.sh "select name, charisma from character order by charisma desc limit 5"
```

3. Run one DESCRIBE:

```bash
scripts/dev/sql.sh "describe character"
```

4. Show supported entities (or use the SQL-style alias):

```bash
scripts/dev/sql.sh "show entities"
scripts/dev/sql.sh "show tables"
```

5. Show indexes for one entity:

```bash
scripts/dev/sql.sh "show indexes character"
```

6. Show columns for one entity:

```bash
scripts/dev/sql.sh "show columns character"
```

7. Command split:

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

- Workspace version on `main`: `0.76.13`
- Latest tagged release in this repo: `v0.76.13`
- Current branch work in progress: `0.76.14`
- Changelog: `CHANGELOG.md`
- Detailed `0.76.x` notes: `docs/changelog/0.76.md`
- Pre-`1.0.0` internal protocol policy: keep one active internal format/version only; do not preserve parallel `v1`/`v2` compatibility paths for superseded internal protocols.

---

## Recent Highlights

- Current branch work on the `0.76` line now splits write results on one clearer rule: `SELECT` and every admitted row-producing mutation surface share the same row payload family, while non-returning typed create/insert/update/replace/delete helpers share one mutation-result family.
- `0.76.12` adds a separate authored typed write shape per entity, so generated fields and managed timestamps are structurally absent from the authored create payload instead of being rejected only after a full entity value is built.
- `0.76.11` hardens insert-generated field ownership on the admitted authored write lanes, so typed-dispatch SQL and public structural writes now reject explicit values for `generated(insert = \"...\")` fields instead of letting caller-authored values compete with system-owned synthesis.
- `0.76.10` keeps reduced SQL defaults explicit by widening schema-owned `generated(insert = \"...\")` only to a small allowlist, so typed-dispatch inserts can synthesize `Timestamp::now` as well as `Ulid::generate` while ordinary `default = ...` values still stay a typed-Rust construction concern.
- `0.76.14` admits `INSERT ... RETURNING`, `UPDATE ... RETURNING`, and `DELETE ... RETURNING` on the unified dispatch lane, keeps bare dispatch mutations count-first, and then extends that same row family to fluent delete returning plus typed `create_returning...`, `insert_returning...`, and `update_returning...` helpers.
- `0.76.7` adds narrow typed-dispatch `INSERT ... SELECT` for the same entity lane, but keeps that copy-insert surface intentionally bounded: scalar source only, field-only or admitted scalar computed projection only, deterministic primary-key-backed ordering, and no grouped or aggregate source admission.
- `0.76.6` widens the reduced SQL write lane with ordered-window `UPDATE`, write-lane aliases, and generated-key `Ulid` inserts while keeping mutation ownership on typed dispatch.
- `0.76.5` broadens the reduced SQL write lane so typed-dispatch `UPDATE ... WHERE ...` can target rows selected by the admitted reduced predicate surface, and single-table aliases now work on that narrowed `UPDATE` path.
- SQL remains default-on. Disable default features to compile out the public SQL APIs and generated canister `sql_dispatch` glue while keeping the typed runtime/query path.

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

- Rust `1.94.1` (edition 2024)

```bash
rustup toolchain install 1.94.1
```

### 2. Add IcyDB

Use a pinned git tag so builds are repeatable. SQL is enabled by default:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.76.13" }
```

Compile out the SQL frontend if you only use typed Rust APIs:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.76.13", default-features = false }
```

With `default-features = false`, `db::sql::*`, SQL session helpers, and generated
`sql_dispatch` modules are not available.

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

pub fn users_named_ann() -> Result<Vec<User>, icydb::Error> {
    let users = db!()
        .load::<User>()
        .filter_expr(FilterExpr::eq(User::NAME, "ann"))?
        .order_by("name")
        .offset(100)
        .limit(50)
        .entities()?;

    Ok(users)
}
```

### Apply one mutation

```rust
use icydb::prelude::*;
use icydb::db::{MutationMode, UpdatePatch};

pub fn rename_user(user_id: Ulid, new_name: String) -> Result<(), icydb::Error> {
    let patch = UpdatePatch::new()
        .set_field(User::MODEL, "id", Value::Ulid(user_id))?
        .set_field(User::MODEL, "name", Value::Text(new_name))?;

    db!().mutate_structural::<User>(user_id, patch, MutationMode::Update)?;

    Ok(())
}
```

Mode semantics:

- `Insert` requires one full after-image patch and fails if the row already exists.
- `Update` applies one patch over one existing row and fails if the row is missing.
- `Replace` requires one full after-image patch, rebuilds from an empty row image, and inserts if the row is missing.

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
    "SELECT id, name FROM User WHERE LOWER(name) LIKE 'ann%' ORDER BY id LIMIT 25",
)?;

let grouped = db!().execute_sql_grouped::<User>(
    "SELECT name, COUNT(id) FROM User GROUP BY name ORDER BY name LIMIT 10",
    None,
)?;
```

### Reduced SQL In Rust

With the `sql` feature enabled, IcyDB keeps a narrow typed SQL surface for:

- `query_from_sql(...)` when you want to lower one SQL `SELECT` or typed
  `DELETE` intent into the canonical query model
- `parse_sql_statement(...)` and `sql_statement_route(...)` when you need
  route metadata
- `execute_sql(...)` for scalar `SELECT`
- `execute_sql_grouped(...)` for grouped `SELECT`
- `execute_sql_aggregate(...)` for constrained global aggregates

Typed/fluent APIs own public mutation behavior. There is no generated canister
`sql_dispatch` module anymore.

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

## Reduced SQL Scope (Current 0.77 Line)

Executable SQL entrypoints:

- `execute_sql` for entity-shaped `SELECT`
- `execute_sql_grouped` for constrained grouped aggregates
- `execute_sql_aggregate` for constrained global aggregates
- `parse_sql_statement` / `sql_statement_route` for route metadata only

Public mutation shapes are typed/fluent, not SQL-dispatch:

- `create(...)`, `insert(...)`, `update(...)`, `replace(...)`
- `create_returning...`, `insert_returning...`, `update_returning...`
- `delete::<E>()`
- `delete::<E>().returning...`

Single-table aliases are admitted on the reduced SQL lane for:

- `SELECT`

Dedicated typed/session introspection helpers:

- `describe_entity::<E>()`
- `show_indexes::<E>()`
- `show_columns::<E>()`
- `show_entities()`

Out of scope and fail-closed by design:

- joins/subqueries/CTEs
- quoted identifiers
- window functions
- public SQL mutation execution
- `LIKE` patterns outside bounded trailing-wildcard prefix forms (`field LIKE 'prefix%'`, `LOWER(field) LIKE 'prefix%'`, `UPPER(field) LIKE 'prefix%'`)

---

## Project Layout

- `crates/icydb` — public API crate.
- `crates/icydb-core` — runtime, planner, executor, stores.
- `crates/icydb-derive` — derive macros and helper codegen surfaces.
- `crates/icydb-primitives` — shared primitive/domain types.
- `crates/icydb-schema-derive` — procedural macros for schema/types.
- `crates/icydb-schema` — schema AST and validation.
- `crates/icydb-build` — build-time codegen for canister wiring.
- `canisters/audit/*` — SQL canister harnesses used for wasm footprint auditing across small and larger audit fixture sets.
- `canisters/demo/rpg` — the broad SQL RPG demo plus perf/integration canister surface.
- `canisters/test/sql` — the lightweight SQL smoke-test canister surface.
- `canisters/test/sql_parity` — the broad SQL-vs-typed/fluent parity and explain canister surface.
- `schema/audit/*` — matching audit schema fixtures used by the wasm footprint matrix.
- `schema/demo/rpg` — the broad demo canister schema surface.
- `schema/test/fixtures` — shared schema fixtures for macro/e2e test harnesses.
- `schema/test/sql` — the lightweight SQL smoke-test fixture surface.
- `schema/test/sql_parity` — the broad SQL parity test-canister fixture surface.
- `testing/macro-tests` — macro and schema contract tests.
- `testing/pocket-ic` — Pocket-IC integration tests for canister flows.
- `testing/wasm-helpers` — shared generated-surface assertions and helpers for the wasm audit canisters.
- `assets`, `scripts`, `Makefile` — docs, helpers, workspace commands.

---

## Schema Crates

IcyDB keeps schema definitions in dedicated crates so canister builds only link
the schema surface they need.

- `schema/demo/rpg` holds the broad demo canister schema surface.
- `schema/test/fixtures` holds shared schema fixtures used by macro/e2e test harnesses.
- `schema/test/sql` holds the lightweight SQL smoke-test fixture surface.
- `schema/test/sql_parity` holds the broad SQL parity test-canister fixture surface.
- `schema/demo/rpg/src/fixtures` holds deterministic RPG fixture datasets shared by demo and test canisters.
- `schema/audit/minimal`, `schema/audit/one_simple`, `schema/audit/one_complex`, `schema/audit/ten_simple`, and `schema/audit/ten_complex` hold the audit fixture families used by the corresponding wasm footprint canisters.
- `testing/wasm-helpers` holds shared generated actor / `sql_dispatch` assertions used across those audit canisters.

This split keeps the wasm audit baseline from absorbing unrelated fixture schema
weight while preserving full-featured fixtures for test harnesses.

---

## Observability Endpoints

IcyDB generates these canister methods for every canister:

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
make test-sql-parity  # broad SQL parity canister only
make fmt        # format workspace
make build      # release build
make wasm-size-report   # build/report wasm sizes for minimal + one/ten simple/complex audit canisters
make wasm-audit-report  # write dated wasm+twiggy audit reports for the audit canisters under docs/audits/reports
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

- Keep the new planner-proven and witness-backed covering routes fail-closed while extending measured executor wins.
- Keep demo, test, and audit canister surfaces separated so smoke tests, parity tests, and wasm audits do not compete for the same binary.
- Preserve deterministic local SQL harness flows (`scripts/dev/sql.sh`), wasm audit baselines, and CI parity.
- Keep `CandidType` wire-surface comments as plain `//` comments instead of `///` doc comments so normal canister builds do not retain those strings in wasm.
- Track active work in `docs/ROADMAP.md` and current design docs under `docs/design/`.

---

## License

Licensed under either:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)

at your option.
