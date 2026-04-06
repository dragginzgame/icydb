# Codebase Hygiene Standard

## Purpose

This document defines source consistency and readability standards used across `icydb-core`.
The goal is to keep the codebase easy to navigate, maintain, and evolve as the system grows.

This document governs file structure, imports, documentation, and organization of code inside modules.

## 1. Import Organization

Imports should be grouped and ordered as follows:

- `crate` imports
- `std` imports
- External crate imports

Example:

```rust
use crate::{
    db::{
        access::{AccessPlan, AccessPath},
        executor::ExecutionTrace,
    },
    value::Value,
};
use std::collections::BTreeMap;
use thiserror::Error;
```

Rules:

- Avoid `super::super::` paths.
- `super::` usage is allowed; reserve it for local module-relative references.
- Prefer grouped `crate::{...}` imports.
- Avoid inline `crate::long::path::...` usage inside functions.
- When deriving or implementing `Display`, prefer `use std::fmt::{self, Display};` for consistency with adjacent formatting impls.

Required top-of-file sequence (module files):

1. `mod ...;` declarations
2. one blank line
3. `use ...;` imports
4. one blank line
5. `pub use ...;` / `pub(crate) use ...;` / `pub(in ...) use ...;` re-exports
6. one blank line
7. constants, type declarations, and functions

This includes test-only re-exports/imports:

- `#[cfg(test)] pub use ...;` still belongs in the re-export block.
- `#[cfg(test)] mod tests;` belongs with the `mod ...;` declarations.

Example:

```rust
mod alpha;
mod beta;
#[cfg(test)]
mod tests;

use crate::something::Thing;
use std::collections::BTreeMap;

pub use alpha::Alpha;
pub(crate) use beta::Beta;
#[cfg(test)]
pub(crate) use beta::beta_for_test;

const LIMIT: usize = 128;
```

## 2. Module Header Comments

Every module should begin with a module-level documentation header describing ownership and boundaries.

Example:

```rust
//! Module: executor::route::planner
//! Responsibility: route eligibility and capability classification.
//! Does not own: planner semantics or execution logic.
//! Boundary: converts planner output into executable route plans.
```

This prevents architectural drift and clarifies module responsibilities.

## 3. Struct and Enum Documentation

All public structs and enums should include documentation describing:

- What the type represents
- Where it is used
- Which layer owns it

Spacing rule for documented type declarations (`struct`, `enum`, `trait`):

- Leave one blank line before the doc comment block.
- Leave one blank line after the doc comment block and before the type declaration.
- When attributes are present (for example `#[derive(...)]`), keep that same blank line between the
  doc block and the first attribute; this applies equally to structs, enums, and traits.
- Apply this consistently so type docs are visually scannable in large files.

Error-enum variant formatting:

- Keep one blank line between variant blocks (attribute + variant).
- Keep a blank line after the last variant block before the closing `}`.
- Prefer alphabetical variant order by variant name for readability and low-friction diffs.

Example:

```rust

///
/// RouteContinuationPlan
///
/// Runtime continuation contract derived from planner window projections.
/// Owns resolved continuation windows but does not own planner window semantics.
///

```
(note blank lines before and after)

## 4. Function Documentation

Functions should include documentation when they:

- Enforce invariants
- Perform planner checks
- Derive execution contracts
- Contain non-obvious logic

Ordering rule for documented items with attributes:

- Write doc/comments first, then attributes, then the item declaration.
- Example order: `/// ...` -> `#[must_use]` -> `pub fn ...`.
- If adjacent comments for one item are mixed (`//` and `///`) from prior edits, normalize them to the current standard:
  use `///` for item documentation and `//` only for inline/non-item notes.

Example:

```rust
/// Determine whether secondary index ordering is deterministic.
///
/// Requires ORDER BY to include primary key as the final tie-break.
/// Prevents unstable pagination anchors.
```

## 5. Code Section Banners

Large modules should group related functions with section separators.

Example:

```rust
// -----------------------------------------------------------------------------
// Access-path classification
// -----------------------------------------------------------------------------

fn classify_access_path(...) { ... }

// -----------------------------------------------------------------------------
// Pushdown eligibility
// -----------------------------------------------------------------------------

fn secondary_order_contract_is_deterministic(...) { ... }
```

Use separators only when grouping multiple related functions.

## 6. Function Ordering

Functions should appear in the following order:

- Public API
- Constructors
- Core logic
- Helper functions
- Internal utilities
- Tests

When a type and its `impl` live in the same file:

- Place the inherent `impl TypeName { ... }` block immediately below the type definition when feasible.
- Keep related trait impls adjacent to the type/inherent impls unless a clear module-structure reason requires separation.
- If a type has many impl blocks, prefer a stable alphabetical ordering (typically by trait name) to reduce scan friction and merge churn.

## 7. Function Length

Functions longer than approximately 80 lines should be reviewed for decomposition.

- Split functions by semantic stage when possible.
- Avoid deeply nested logic blocks.

## 8. Visibility Rules

Visibility should follow layer boundaries:

| Component | Visibility |
| --- | --- |
| executor internals | `pub(in crate::db::executor)` |
| db-layer DTOs | `pub(in crate::db)` |
| public APIs | `pub` |

Visibility should be minimized wherever possible.

## 9. Invariant Handling

Use invariant helpers rather than `panic!` or `unwrap()` for internal correctness checks.

Examples:

- `executor_invariant(...)`
- `planner_invariant(...)`
- `cursor_invariant(...)`

## 10. Naming Consistency

Common terms should remain consistent across modules.

Examples:

- `page_limit`
- `fetch_limit`
- `rows_materialized`
- `rows_scanned`
- `rows_filtered`
- `rows_aggregated`
- `rows_emitted`

Avoid introducing new names for existing concepts.

## 11. Match Expression Hygiene

Large `match` blocks should dispatch to helper functions rather than embedding large logic bodies.

Example:

```rust
match path.kind() {
    AccessPathKind::ByKey => handle_by_key(...),
    AccessPathKind::IndexRange => handle_index_range(...),
}
```

## 12. Slice Shape And Change Coupling

Routine feature work should preserve delivery velocity by keeping slices narrow.

See [velocity-preservation.md](/home/adam/projects/icydb/docs/governance/velocity-preservation.md)
for the current enforced rules, including:

- slice-shape file-count limits
- cross-domain touch limits
- `Slice-Override` / `Slice-Justification` escape hatch requirements
- guarded root-module growth checks for parser and session SQL roots

When a change spans parser, lowering/session, build/canister, or integration
surfaces together, treat that as an exception that must be justified explicitly.

## 13. Test Placement

If tests are split into a separate file, declare `mod tests;` at the top with other module declarations.
If tests are inline, keep `#[cfg(test)] mod tests { ... }` at the bottom of the module.
For inline tests, the `///`, `/// TESTS`, `///` banner must have exactly one blank line before it and one blank line after it.

Imported module example (top of file):

```rust
mod decode;
mod encode;

#[cfg(test)]
mod tests;
```

Inline module example (bottom of file):

```rust
///
/// TESTS
///

#[cfg(test)]
mod tests {

}
```

Tests should cover invariants and planner eligibility rules.

## 14. Redundant Code Removal

During maintenance passes, remove:

- Duplicate helpers
- Dead code
- Legacy compatibility logic
- Multi-version internal protocol support kept only for pre-`1.0.0` compatibility
- Outdated comments

## 15. Formatting

All code should pass the following before committing:

- `cargo fmt --all`
- `cargo clippy`

## Why This Is Valuable

Following this standard ensures:

- Consistent code structure
- Easier onboarding for contributors
- Safer large refactors
- Clearer architectural boundaries

## One Extra Tip

When doing this hygiene pass, commit per module, not one massive commit.

Examples:

- `cleanup: normalize imports and documentation in executor/route`
- `cleanup: reorder helpers and banners in planner/pushdown`
- `cleanup: tighten visibility in index/storage`

This keeps history readable.
