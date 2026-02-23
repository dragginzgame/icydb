![MSRV](https://img.shields.io/badge/rustc-1.93.1-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB — Data Models for Internet Computer Canisters

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/> <img src="assets/swampfree.png" alt="100% Certified Swamp-Free" width="120"/>

> Schema-first data models for Internet Computer canisters.
> Built for [Dragginz](https://dragginz.io/), now open to everyone.

---

## What Is IcyDB?

**IcyDB** is a Rust framework that helps you:

- define your canister data as typed Rust entities,
- query that data with a fluent API,
- store data in stable memory,
- and recover safely if a write is interrupted.

If you are new to this space: think of IcyDB as a way to get "database-like" structure and safety while still writing normal Rust code.

---

## Current Release

- Workspace version: `0.24.7`
- Changelog: `CHANGELOG.md`

---

## Why Use It?

- **Less boilerplate**: generate common data model code with macros.
- **Typed queries**: work with `User`, `Order`, etc. directly instead of loose maps.
- **Stable-memory persistence**: data survives upgrades.
- **Predictable behavior**: query and write paths are validated and tested.
- **Built-in diagnostics**: metrics and storage snapshot endpoints are generated for you.

---

## Quick Start

### 1. Toolchain

- Rust `1.93.1` (edition 2024)

```bash
rustup toolchain install 1.93.1
```

### 2. Add IcyDB

Use a pinned git tag so builds are repeatable:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.24.7" }
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

### Query data

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

---

## Helpful Notes

- `db!().load::<User>()` gives you a typed load query.
- `db!().delete::<User>()` gives you a typed delete query.
- IDs are typed as `Id<E>` for better safety.
- Planning/execution internals stay inside the framework; the public API stays focused and ergonomic.

For deeper rules and behavior:

- `docs/contracts/QUERY_CONTRACT.md`
- `docs/contracts/QUERY_PRACTICE.md`
- `docs/contracts/IDENTITY_CONTRACT.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`

### Execution & Aggregate Guarantees (0.25 milestone line)

- Aggregate terminals include field-based operations (`min_by`, `max_by`, `nth_by`, `sum_by`, `avg_by`) with explicit capability boundaries.
- Field-extrema tie-break behavior is deterministic: `(field_value, primary_key_asc)`.
- Field terminal continuation behavior is explicit: non-paged terminals reject cursor tokens.
- DISTINCT behavior is explicit per terminal, with canonical fallback where field-extrema fast paths are ineligible.

Reference docs:

- `docs/design/0.25-aggregate-expansion.md`
- `docs/status/0.25-status.md`

### Batch Writes: Choose Your Lane

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

## Project Layout

- `crates/icydb` — public API crate.
- `crates/icydb-core` — runtime, query engine, stores.
- `crates/icydb-derive` — derive macros and helper codegen surfaces.
- `crates/icydb-primitives` — shared primitive/domain types.
- `crates/icydb-schema-derive` — procedural macros for schema/types.
- `crates/icydb-schema` — schema AST and validation.
- `crates/icydb-build` — build-time codegen for canister wiring.
- `crates/icydb-schema-tests` — integration/design tests.
- `assets`, `scripts`, `Makefile` — docs, helpers, workspace commands.

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
```

Pre-commit hooks run:

- `cargo fmt --all -- --check`
- `cargo sort --check`
- `cargo sort-derives --check`

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

- Finalize `0.25.0` aggregate-expansion release alignment (version cut + publish metadata).
- Keep aggregate terminal parity/consistency coverage green as follow-up patches land.
- Continue docs consolidation and runnable examples.
- Track upcoming work in `docs/ROADMAP.md` and active design docs under `docs/design/`.

---

## License

Licensed under either:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)

at your option.
