# VISIBILITY.md

## Purpose

This document defines the visibility and export architecture for `icydb-core`.

It is authoritative.

Any change that widens visibility or couples callers to deep implementation
paths without an intentional module-boundary decision is an architectural
regression.

---

# Core Model

`icydb-core` does **not** use a rigid "Tier 2 only" public-surface rule.

Instead, it uses a **module-owned boundary** model:

- the crate root defines top-level namespaces
- each module root defines the public surface for its own children
- deep modules are internal by default unless their parent module root
  intentionally re-exports them
- restricted visibility (`pub(crate)`, `pub(in crate::db)`, etc.) is used to
  keep implementation seams narrow

Depth alone does not decide whether something is part of the API.
Intentional re-export at the owning module root does.

---

# 1. Crate Root

## 1.1 Root Is Primarily Namespace

The crate root establishes stable top-level namespaces such as:

```rust
pub mod db;
pub mod error;
pub mod metrics;
pub mod model;
pub mod sanitize;
pub mod serialize;
pub mod traits;
pub mod types;
pub mod validate;
pub mod value;
pub mod visitor;
```

These `pub mod` declarations provide stable paths for:

- downstream imports
- macro expansion targets
- derive-generated code
- explicit subsystem ownership

This does **not** mean every descendant of those namespaces is public API.

## 1.2 Root Flattening Must Stay Rare

The crate root should not become an indiscriminate type dump.

Rare root-level re-exports are acceptable only when they are intentional and
documented as part of the crate surface.

Correct:

```rust
icydb_core::db::DbSession
icydb_core::types::Ulid
```

Not preferred as a default pattern:

```rust
icydb_core::DbSession
icydb_core::Ulid
```

---

# 2. Module Boundary Rule

## 2.1 Every Module Owns Its Boundary

If a module has children, its root file is the only place that should define
the public surface for those children.

Example:

```rust
mod child_a;
mod child_b;

pub use child_a::TypeA;
pub use child_b::TypeB;
```

This rule applies at every level:

- crate root
- subsystem roots such as `db`
- nested directory-module roots such as `db/data/persisted_row/mod.rs`
- nested planner/executor roots such as
  `db/query/plan/access_choice/mod.rs`

## 2.2 External Callers Import From Module Roots

Callers outside a module subtree should import from the owning module root, not
from deep implementation paths.

Correct:

```rust
use crate::db::Predicate;
```

Incorrect:

```rust
use crate::db::predicate::parser::some_internal_helper;
```

If a nested item needs to be public, re-export it at the relevant module root
first.

## 2.3 Deep Modules Are Internal By Default

A deep file or child module is implementation detail unless a parent module
root intentionally re-exports it.

This means:

- no accidental deep `pub` trees
- no caller dependency on storage/planner/executor leaf files
- internal refactors can move code without widening API

---

# 3. Visibility Levels

## 3.1 Use the Narrowest Visibility That Matches Ownership

`icydb-core` intentionally uses several visibility levels:

- `pub` for intentional module-root API
- `pub(crate)` for crate-internal surfaces
- `pub(in crate::db)` for `db`-owned internal seams
- `pub(in crate::db::...)` for narrower subsystem-local seams when needed

Examples from the current tree:

- crate-root namespaces in `crates/icydb-core/src/lib.rs`
- crate-internal wiring in `crates/icydb-core/src/db/mod.rs`
- `db`-local helper ownership across query/executor/data/index boundaries

## 3.2 Do Not Widen Visibility to Avoid Re-Exports

If a caller needs an item, do not make a deep child `pub mod` just to make the
import convenient.

Instead:

1. decide which module owns that surface
2. re-export the item there
3. keep the child module private or restricted if possible

## 3.3 Restricted Visibility Is Part of the Architecture

Patterns such as:

```rust
pub(crate) mod query;
pub(in crate::db) mod executor;
pub(in crate::db) use describe::describe_entity_model;
```

are intentional architectural boundaries, not temporary hacks.

They communicate:

- who owns the seam
- who may depend on it
- where public surface formation is allowed

---

# 4. Directory Modules

When a module is split into multiple files, convert it into a directory module
with `mod.rs` as the root.

Examples in the current tree include:

- `crates/icydb-core/src/db/data/persisted_row/mod.rs`
- `crates/icydb-core/src/db/query/plan/access_choice/mod.rs`
- `crates/icydb-core/src/db/executor/explain/descriptor/mod.rs`

Rules:

- module wiring belongs in `mod.rs`
- re-exports belong in `mod.rs`
- child files stay below that root
- `#[path = "..."]` wiring is prohibited

This keeps module ownership explicit and makes the boundary file easy to audit.

---

# 5. Import Discipline

## 5.1 Inside the Owning Subtree

Implementation code may use deeper imports inside its own subtree when that
stays within the boundary owner.

For example, code inside `db::query` may depend on deeper query-plan helpers
that are not part of the external `db` surface.

## 5.2 Across Subsystem Boundaries

Across subsystem boundaries, import from the owning module root instead of
reaching into leaf files.

Correct:

```rust
use crate::db::QueryError;
```

Incorrect:

```rust
use crate::db::query::intent::QueryError;
```

## 5.3 Sibling Crates Must Not Rely on Deep Internals

Sibling crates such as `icydb` must not bind themselves to deep `icydb-core`
implementation paths.

If a type is needed externally:

- re-export it intentionally
- keep the deep owner private or restricted
- avoid turning implementation layout into downstream contract

---

# 6. Re-Export Discipline

Re-exports should happen only at the module root that owns the boundary being
formed.

Correct:

```rust
mod merge;

pub use merge::MergePatchError;
```

Incorrect:

```rust
mod merge;

pub use merge::error::MergePatchError;
```

Flatten one owned boundary at a time.

Do not skip intermediate boundaries and expose leaf-layout details directly.

---

# 7. Lint and Review Enforcement

`icydb-core` enables:

```rust
#![warn(unreachable_pub)]
```

This is a useful guard, but it is not the entire policy.

Architectural review must still enforce:

- no accidental deep public modules
- no dependency on private layout from sibling crates
- no visibility widening used as a shortcut around boundary design
- no `#[path]` module wiring

---

# 8. Architectural Guarantees

This model guarantees:

- stable namespace paths for macros and downstream users
- explicit boundary ownership at every module root
- shallow caller-facing APIs relative to owned module boundaries
- internal refactor freedom inside each subtree
- narrower coupling through restricted visibility
- clearer review of export decisions after module splits

---

# Absolute Rule

Visibility follows ownership, not file depth.

Each module root defines the public surface for its children.

Anything not intentionally re-exported at the owning module root is
implementation detail.
