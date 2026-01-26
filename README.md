![MSRV](https://img.shields.io/badge/rustc-1.93.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)

# IcyDB — Data Model Framework for the Internet Computer

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/> <img src="assets/swampfree.png" alt="100% Certified Swamp-Free" width="120"/>

> Battle-tested, schema-first data models for Internet Computer canisters.
> Built for [Dragginz](https://dragginz.io/), now open to everyone.

---

## 👋 Overview

**IcyDB** is a Rust framework for building **strongly-typed, queryable data models**
inside Internet Computer canisters.

It provides:
- declarative entity definitions,
- typed query intent with explicit semantics,
- deterministic planning and execution,
- and stable-memory–backed storage with predictable behavior.

IcyDB is designed for **single-message atomicity**, explicit correctness guarantees,
and mechanical enforcement of architectural boundaries.

---

## ✨ Highlights

- **Entity macros** — define schema-first entities declaratively.
- **Typed query intent** — build queries as `Query<E>` with explicit semantics.
- **Deterministic planning** — validated, executor-safe plans only.
- **Stable storage** — B-tree–backed stable memory via CanIC structures.
- **Path dispatch** — `icydb_build` generates internal routing helpers.
- **Observability endpoints** — `icydb_snapshot`, `icydb_metrics`, `icydb_metrics_reset`.
- **IC integration** — ergonomic `icydb::start!` and `icydb::build!` macros.
- **Testability** — fixtures, predicate validation, index testing utilities.

---

## ⚡ Quick Start

### Toolchain

- **Rust 1.93** (edition 2024)
- Install with:
  ```bash
  rustup toolchain install 1.93

### Add IcyDB

Use a pinned git tag for reproducible builds:

```toml
[dependencies]
icydb = { git = "https://github.com/dragginzgame/icydb.git", tag = "v0.0.1" }
```

---

## 🚀 Example

### Define an entity

```rust
use icydb::prelude::*;

#[entity(
    sk(field = "id"),
    fields(
        field(ident = "id", value(item(is = "types::Ulid"))),
        field(ident = "name", value(item(is = "text::Name"))),
        field(ident = "description", value(item(is = "text::Description"))),
    ),
)]
pub struct User {}
```

---

### Build and execute a query

Queries are built as **typed intent**, explicitly planned, and then executed.

```rust
use icydb::prelude::*;

#[query]
pub fn users_named_ann() -> Result<Vec<UserView>, icydb::Error> {
    let query = Query::<User>::new(ReadConsistency::MissingOk)
        .filter(eq("name", "ann"))
        .order_by("name")
        .page(50, 100);

    let plan = query.plan()?;
    let rows = db!().load::<User>().execute(plan)?;

    Ok(rows.views())
}
```

Key properties:

* Entity type is fixed at construction (`Query<User>`).
* Missing-row behavior is explicit (`ReadConsistency`).
* Executors only accept validated, executable plans.

---

## 🏗️ Project Layout

* `icydb/` — meta crate re-exporting the public API.
* `crates/icydb-core` — runtime (entities, traits, query engine, stores).
* `crates/icydb-schema-derive` — proc-macros for schema, traits, and views.
* `crates/icydb-schema` — schema AST, builder, and validation.
* `crates/icydb-build` — build-time codegen for actors, queries, metrics.
* `crates/test`, `crates/test_design` — integration and design tests.
* `assets/`, `scripts/`, `Makefile` — docs, helpers, workspace tasks.

---

## 📟 Observability & Tooling

The following endpoints are generated automatically:

* `icydb_snapshot()` → live `StorageReport`
* `icydb_metrics()` → metrics since a given timestamp
* `icydb_metrics_reset()` → clears metrics state

Example usage:

```bash
dfx canister call <canister> icydb_snapshot
dfx canister call <canister> icydb_metrics
dfx canister call <canister> icydb_metrics_reset
```

---

## 🧑‍💻 Local Development

Workspace commands (see `Makefile`):

```bash
make check      # type-check workspace
make clippy     # lint (warnings denied)
make test       # unit + integration tests
make fmt        # format workspace
make build      # release build
```

Pre-commit hooks run:

* `cargo fmt -- --check`
* `cargo sort --check`
* `cargo sort-derives --check`

---

### Style & Conventions

* Prefer typed errors (`thiserror`) over panics in library code.
* Keep functions small and single-purpose.
* Use explicit semantics over implicit defaults.
* Co-locate unit tests; integration tests live under `crates/test*`.
* No backward-compatibility guarantee yet — breaking changes are documented.

---

## 🔒 Versioning & Security

* Git tags are treated as **immutable by project policy**.
* Production users should always pin to a specific tag.
* Floating branches are not recommended for production.

Verify available tags:

```bash
git ls-remote --tags https://github.com/dragginzgame/icydb.git
```

---

## 📊 Current Focus

* Expanding documentation and runnable examples.
* Increasing test coverage across query and index paths.
* Tracking memory usage and store statistics in production.
* Reducing WASM size produced by `icydb_build`.

---

## 📄 License

Licensed under either of:

* Apache License, Version 2.0 (`LICENSE-APACHE`)
* MIT License (`LICENSE-MIT`)

at your option.
