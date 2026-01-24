
![MSRV](https://img.shields.io/badge/rustc-1.93.0-blue.svg)
[![CI](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml/badge.svg)](https://github.com/dragginzgame/icydb/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-APACHE)
[![Crate](https://img.shields.io/crates/v/icydb.svg)](https://crates.io/crates/icydb)
[![Docs](https://img.shields.io/docsrs/icydb)](https://docs.rs/icydb)

# IcyDB — Data Model Framework for the Internet Computer

<img src="assets/icydblogo.svg" alt="IcyDB logo" width="220"/> <img src="assets/swampfree.png" alt="100% Certified Swamp-Free" width="120"/>

> Battle-tested, schema-first data models for Internet Computer canisters. Built for [Dragginz](https://dragginz.io/), now open to everyone.

## 👋 Overview

**IcyDB** is a Rust framework for building strongly-typed, queryable data models on the [Internet Computer](https://internetcomputer.org).

---

## ✨ Highlights

- **Entity macros** – define entities declaratively with schema attributes.
- **Query builder** – type-safe filters, sorting, offsets, limits.
- **Stable storage** – B-Tree-backed stable memory via `canic` structures with predictable costs.
- **Path dispatch** – `icydb_build` generates internal dispatch helpers so you can map paths to entity types without exposing global endpoints.
- **Observability endpoints** – `icydb_snapshot`, `icydb_metrics`, `icydb_metrics_reset` ship automatically.
- **Integration with IC canisters** – ergonomic `icydb::start!` and `icydb::build!` macros.
- **Testability** – fixtures, query validation, index testing utilities.

---

## ⚡ Quickstart

1. **Install Rust 1.93.0** (workspace uses edition 2024).
2. **Add IcyDB** to your `Cargo.toml` using the latest tag:
   ```toml
   [dependencies]
  icydb = { git = "https://github.com/dragginzgame/icydb.git" }
   ```
3. **Declare an entity** with the `#[entity]` macro and a primary key.
4. **Query your data** via `db!().load::<Entity>()...`.

See [INTEGRATION.md](INTEGRATION.md) for pinning strategies, feature flags, and troubleshooting tips.

---

## 🚀 Example

### Define an entity

```rust
/// Rarity
/// Affects the chance of an item dropping or an event occurring.
#[entity(
    store = "GameStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(is = "text::Name"))),
        field(ident = "description", value(item(is = "text::Description"))),
        field(ident = "order", value(item(is = "game::Order"))),
        field(ident = "color", value(item(is = "types::color::RgbHex"))),
    ),
)]
pub struct Rarity {}
```

### Query entities

```rust
#[query]
pub fn rarities() -> Result<Vec<RarityView>, icydb::Error> {
    let query = icydb::db::query::load()
        .filter(|f| {
            // (level >= 2 AND level <= 4) OR (name CONTAINS "ncon")
            (f.gte("level", 2) & f.lte("level", 4)) | f.contains("name", "ncon")
        })
        .sort(|s| s.desc("level"))
        .limit(100);

    let rows = db!().debug().load::<Rarity>().execute(query)?;
    Ok(rows.views())
}
```

---

## 🏗️ Project Layout

- `icydb/` — meta crate re-exporting everything for downstream users.
- `crates/icydb-core` — runtime (entities, traits, filters, query engine, stores).
- `crates/icydb-macros` — proc-macros that generate schema, traits, and views.
- `crates/icydb-schema` — schema AST, builder, and validation.
- `crates/icydb-build` — build-time codegen for actors/queries/metrics.
- `crates/icydb/src/base` — built-in design types, sanitizers, and validators.
- `crates/test` and `crates/test_design` — integration and design tests.
- `assets/`, `scripts/`, `Makefile` — docs, helper scripts, and workspace tasks.

---

## 📟 Observability & Tooling

- `icydb_snapshot()` → live `StorageReport` with data/index/state breakdowns.
- `icydb_metrics()` → `EventReport` for counters since `since_ms`.
- `icydb_metrics_reset()` → clears metrics state.

Examples:
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
make clippy     # lint with warnings denied
make test       # run all unit + integration tests
make fmt        # format the workspace (or fmt-check to verify)
make build      # release build
```

Pre-commit hooks run `cargo fmt -- --check`, `cargo sort --check`, and `cargo sort-derives --check`. Run any of the `make fmt*`, `make clippy`, or `make check` targets once to auto-install and enable them.

### Style & conventions

- Prefer `?` + typed errors (`thiserror`) instead of panics in library code.
- Keep functions focused; extract helpers when logic grows.
- Import ergonomically: group paths per crate (e.g., `use crate::{db, design};`).
- Use saturating arithmetic for counters and totals.
- Co-locate small unit tests; integration/design tests live in `crates/test` and `crates/test_design`.
- No backward-compatibility promise yet—document breaking changes in the changelog.

---

## 🤝 Contributing & Support

We welcome issues, discussions, and pull requests now that the repository is public. To contribute:

1. Fork and clone the repo.
2. Install the toolchain (`rustup toolchain install 1.93.0`).
3. Run `make fmt-check && make clippy && make check && make test` before opening a PR.
4. Document user-visible changes in [CHANGELOG.md](CHANGELOG.md) under the latest heading.

Need help? Start with [INTEGRATION.md](INTEGRATION.md), [VERSIONING.md](VERSIONING.md), or open a GitHub issue.

---

## 📊 Current Focus

- Expanding documentation and runnable examples.
- Deepening test coverage across entity indexes and query paths.
- Tracking store statistics & memory usage in production deployments.
- Reducing WASM size produced by `icydb_build`.

---

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)

at your option.
