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
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
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
        .filter_expr(FilterExpr::eq("name", "ann"))?
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

- `docs/QUERY_CONTRACT.md`
- `docs/QUERY_PRACTICE.md`
- `docs/IDENTITY_CONTRACT.md`

---

## Project Layout

- `crates/icydb` — public API crate.
- `crates/icydb-core` — runtime, query engine, stores.
- `crates/icydb-schema-derive` — procedural macros for schema/types.
- `crates/icydb-schema` — schema AST and validation.
- `crates/icydb-build` — build-time codegen for canister wiring.
- `crates/icydb-schema-tests` — integration/design tests.
- `assets`, `scripts`, `Makefile` — docs, helpers, workspace commands.

---

## Observability Endpoints

IcyDB generates these canister methods:

- `icydb_snapshot()` -> current storage report
- `icydb_metrics(since_ms: Option<u64>)` -> metrics since a timestamp
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

- `cargo fmt -- --check`
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

- Better docs and runnable examples.
- More test coverage across query/index behavior.
- Better production metrics and storage visibility.
- Smaller generated Wasm output from `icydb_build`.
- Future transaction work (see `docs/ROADMAP.md`).

---

## License

Licensed under either:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)

at your option.
