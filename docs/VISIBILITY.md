# VISIBILITY.md

## Purpose

This document defines the mandatory visibility architecture for `icydb-core`.

It is authoritative.

Any change that violates these rules is an architectural regression.

---

# Core Model

`icydb-core` uses a **wide namespace, shallow API** model.

* **Tier 1 — Crate Root:** Namespace only.
* **Tier 2 — Subsystem Roots:** Public API boundaries.
* **Tier 3+ — Implementation Layers:** Internal unless explicitly re-exported by Tier 2.

Public surface is shallow.
Namespaces may be wide.

---

# 1. Crate Root (Tier 1)

## 1.1 Root Is Namespace Only

The crate root defines subsystem namespaces.

Example:

```rust
pub mod db;
pub mod error;
pub mod model;
pub mod obs;
pub mod patch;
pub mod sanitize;
pub mod serialize;
pub mod traits;
pub mod types;
pub mod validate;
pub mod value;
pub mod visitor;
```

Root modules may be `pub` to support:

* Macros
* Derive expansions
* Stable namespace paths

This does **not** mean all contents are public API.

---

## 1.2 No Root Flattening

The crate root must not become a type surface.

Forbidden:

```rust
pub use some_module::Type;
```

Unless explicitly intended as part of documented public API.

Correct usage:

```
icydb_core::db::Session
icydb_core::types::Ulid
```

Incorrect usage:

```
icydb_core::Session
icydb_core::Ulid
```

Root is a namespace index only.

---

# 2. Subsystem Boundaries (Tier 2)

Each root module defines its own public surface.

Only Tier-2 modules define external API.

Each Tier-2 module owns its subtree and is responsible for:

Re-export discipline

Preventing deep pub

Maintaining API stability

---

## 2.1 Module Root Defines API

Inside a subsystem:

```rust
mod child_a;
mod child_b;

pub use child_a::TypeA;
pub use child_b::TypeB;
```

* Child modules must not be `pub mod`.
* All public API must be re-exported at the subsystem root.
* No wildcard re-exports.

Forbidden:

```rust
pub mod child_a;
pub use child_a::*;
```

---

## 2.2 External Imports Must Use Tier 2

External crates must import only from subsystem roots.

Allowed:

```rust
icydb_core::db::QueryError
```

Forbidden:

```rust
icydb_core::db::query::plan::Planner
```

Deep paths must not form part of public API.

---

# 3. Internal Implementation (Tier 3+)

Modules below Tier 2 are implementation detail.

---

## 3.1 Tier-3 Modules

Inside Tier-2 modules:

```rust
pub(crate) mod child;
```

Not:

```rust
pub mod child;
```

Tier-3 modules must not define public API.

---

## 3.2 Tier-3 Re-Exports

Within Tier-3 modules:

```rust
pub(crate) use child::Type;
```

Never `pub use`.

Only Tier-2 may expose types publicly.

---

## 3.3 Deep Modules Must Never Be Public

The following patterns must never be publicly reachable:

```
db::query::plan::*
db::query::validate::*
db::executor::*
db::index::store::*
db::index::key::*
diagnostics::*
access::*
logical::*
```

If required publicly, re-export at Tier-2.

---

# 4. Import Discipline

## 4.1 Inside the Crate

Implementation code may use deep imports within its subtree.

Example (inside `db`):

```rust
use super::query::plan::Planner;
```

Across subsystems, use Tier-2 boundary paths.

Correct:

```rust
use crate::db::QueryError;
```

Incorrect:

```rust
use crate::db::query::plan::QueryError;
```

---

## 4.2 Sibling Crates

Sibling crates (`icydb`, `icydb-schema-derive`) must not rely on deep paths.

If a type is required:

* Re-export it intentionally at Tier-2.
* Do not widen deep module visibility.

---

# 5. Infrastructure Modules

Infrastructure modules (e.g., `sanitize`, `validate`, `patch`, `visitor`, etc.):

* May be `pub mod` at root for namespace stability.
* Must not expose deep internal modules.
* Must follow Tier-2 / Tier-3 rules internally.

Namespace visibility does not equal API exposure.

---

# 6. Re-Export Discipline

Re-exports are allowed only at:

* Tier 2 (subsystem root)
* Rarely at Tier 1 (explicitly documented)

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

# 7. Lint Enforcement

The crate must enable:

```rust
#![warn(unreachable_pub)]
```

This ensures:

* No accidental deep public items.
* No unintended API leakage.

`pub(crate)` in Tier-3 is intentional and allowed.

---

# 8. Architectural Guarantees

This model guarantees:

* Stable namespace paths (macro-safe).
* Shallow, controlled public API.
* No deep public module trees.
* Internal refactor freedom.
* Clear subsystem ownership.
* Minimal accidental coupling.

---

# Absolute Rule

Root modules may be public namespaces.

Only Tier-2 modules define public API.

Tier-3 and below never define public API.

Everything else is implementation detail.

