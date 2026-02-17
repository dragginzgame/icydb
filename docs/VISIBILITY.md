# VISIBILITY.md

## Purpose

This document defines the **mandatory visibility architecture** for `icydb-core`.

It is not advisory.

It is the authoritative rule for:

* Public API surface
* Internal layering
* Subsystem boundaries
* Re-export structure

Any change that violates this document is an architectural regression.

---

# 1. Crate-Level API Rule

`icydb-core` exposes a **structured public API**.

It does **not** flatten types or functions into the crate root.

The crate root must remain minimal and namespace-oriented.

---

# 2. Only These Modules Are Public at Crate Root

In `lib.rs`, the only allowed `pub mod` declarations are:

```rust
pub mod types;
pub mod value;
pub mod traits;
pub mod db;
pub mod error;
```

No other `pub mod` declarations are permitted at crate root.

Specifically forbidden:

```rust
pub mod model;
pub mod patch;
pub mod sanitize;
pub mod validate;
pub mod visitor;
pub mod obs;
pub mod serialize;
```

These must not be publicly exposed at crate root.

---

# 3. No Root-Level Flattening

The following pattern is prohibited in `lib.rs`:

```rust
pub use some_module::SomeType;
```

Unless `SomeType` is explicitly part of the documented root API (currently only `error` items if needed).

Specifically forbidden examples:

```
icydb_core::Ulid
icydb_core::Session
icydb_core::VisitorContext
icydb_core::deserialize
```

Correct usage:

```
icydb_core::types::Ulid
icydb_core::db::Session
icydb_core::value::Value
```

Root namespace must not become a type dump.

---

# 4. Second-Level Flattening Rule (Mandatory)

Inside each public top-level module:

* Submodules must be private (`mod`, not `pub mod`)
* Public items must be re-exported one level up

Example:

### Correct (`types/mod.rs`)

```rust
mod ulid;
mod timestamp;
mod account;

pub use ulid::Ulid;
pub use timestamp::Timestamp;
pub use account::Account;
```

### Forbidden

```rust
pub mod ulid;
pub mod timestamp;
```

No deep module trees may be public.

---

# 5. Subsystem Boundary Rule (db Example)

Inside `db/mod.rs`:

```rust
mod session;
mod query;
mod executor;
mod index;
mod store;

pub use session::Session;
pub use query::{Query, QueryError};
```

Strictly forbidden:

```rust
pub mod query;
pub use query::*;
```

Wildcard re-exports are prohibited.

Deep internal modules must remain sealed:

```
db::query::plan
db::executor::context
db::index::lookup
```

These must not be reachable from outside the crate.

---

# 6. Internal Infrastructure Modules

Some modules are shared internally but not part of public API.

These must be declared:

```rust
pub(crate) mod sanitize;
pub(crate) mod validate;
pub(crate) mod patch;
pub(crate) mod visitor;
pub(crate) mod serialize;
pub(crate) mod obs;
pub(crate) mod model;
```

Rules:

* They must NOT be `pub mod`
* They must NOT be re-exported at crate root
* They must NOT be reachable from external crates

They are internal engine infrastructure only.

---

# 7. Deep Modules Must Never Be Public

The following patterns are permanently disallowed:

```
query::plan::*
query::validate::*
index::store::*
executor::*
diagnostics::*
access::*
logical::*
```

If a type from these areas is needed externally, it must be:

1. Re-exported at the subsystem boundary (e.g., `db`)
2. Not exposed via its deep path

---

# 8. Sibling Crate Rule

`icydb` and `icydb-schema-derive` must never rely on deep internal paths.

Forbidden:

```
icydb_core::db::query::plan::...
icydb_core::model::entity::...
```

Allowed:

```
icydb_core::db::QueryError
icydb_core::types::Ulid
```

If a sibling crate requires something:

* It must be publicly re-exported at the correct boundary.
* Core internals must not be widened just to satisfy it.

---

# 9. Re-Export Discipline

Re-exports must occur only at:

* Crate root (top-level namespaces only)
* Subsystem boundary modules (`types`, `db`, etc.)

Never re-export from deep modules directly.

Correct:

```rust
pub use merge::MergePatchError;
```

Incorrect:

```rust
pub use merge::error::MergePatchError;
```

Flatten one level at a time.

---

# 10. unreachable_pub Policy

After module sealing is complete:

```rust
#![warn(unreachable_pub)]
```

is mandatory.

Purpose:

* Detect stray `pub` items that are no longer reachable.
* Enforce minimal external API surface.

It is not required that all internal `pub(crate)` be eliminated.
Only `pub` visibility is policed.

---

# 11. Internal Import Rule

Inside the crate:

* Use full internal paths for implementation dependencies:

  ```
  crate::db::query::plan::Planner
  ```

* Use boundary paths only when depending on subsystem API:

  ```
  crate::db::QueryError
  ```

Do not use public re-exports to hide internal coupling.

---

# 12. Architectural Intent

`icydb-core` is an engine crate.

It must:

* Expose stable subsystem boundaries.
* Seal planner, executor, and index internals.
* Allow refactors of internal modules without external breakage.
* Prevent namespace pollution at crate root.

This visibility structure is part of the engine contract.

