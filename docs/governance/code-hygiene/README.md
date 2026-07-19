# Codebase Hygiene Standard

## Purpose

This document defines source consistency and readability standards used across `icydb-core`.
The goal is to keep the codebase easy to navigate, maintain, and evolve as the system grows.

This directory governs file structure, imports, documentation, and organization
of code inside modules. The standard lives in this README; copyable example
modules live under
[`example-crate/`](/home/adam/projects/icydb/docs/governance/code-hygiene/example-crate).

This is not the module hardening audit. Use
`docs/audits/targeted/modules/module-surface-hardening.md` when the task is to justify
retained surface, remove stale complexity, or evaluate cleanup against hot-path
and wasm-sensitive runtime shape. Use
`docs/audits/targeted/modules/module-cleanup-runner.md` when the task is to patch a named
module using that policy.

## Example Crate

The `example-crate/` tree is documentation-only Rust that models the preferred
IcyDB crate and module shape. It intentionally has no `Cargo.toml`, is outside
the Cargo workspace, and must not own package metadata or version numbers.

```text
example-crate/
└── src/
    ├── lib.rs
    ├── catalog/
    │   ├── admission.rs
    │   ├── mod.rs
    │   ├── snapshot.rs
    │   └── tests.rs
    ├── diagnostic.rs
    └── plan/
        ├── mod.rs
        └── route.rs
```

The example demonstrates module-level ownership headers, top-of-file ordering,
grouped imports, narrow visibility, public item docs, invariant-bearing
constructors, typed diagnostics, leaf-local inline tests, and boundary-level
`tests.rs`.

When copying from it:

- Copy structure and ordering, not the example domain names.
- Keep runtime authority in the owning module; do not reconstruct accepted
  runtime state from generated model conveniences.
- Use scoped visibility before widening a symbol to `pub`.
- Put cross-module tests in the owner boundary instead of burying them in a
  leaf module.
- Keep examples formatted with `rustfmt`.

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
- Avoid `super::` outside tests unless narrowly justified. When justified, keep
  it to local module-relative references.
- Prefer grouped `crate::{...}` imports.
- Group imports by root instead of scattering repeated paths through the file.
- Avoid inline `crate::long::path::...` usage inside functions.
- Keep normal imports, re-exports, and module declarations in their own blocks;
  do not mix `use` and `pub use` lines to save vertical space.
- Keep `#[cfg(...)]` imports in the same conceptual block they would occupy
  without the `cfg`.
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
//! Module: executor::planning::route::planner
//! Responsibility: route eligibility and capability classification.
//! Does not own: planner semantics or execution logic.
//! Boundary: converts planner output into executable route plans.
```

This prevents architectural drift and clarifies module responsibilities.

## Documentation And Wasm Size

Do not remove public API docs for raw wasm-size reasons alone. Rust doc comments
are metadata for rustdoc and are not expected to be retained in emitted wasm
objects under release/stripped builds. If a generated-doc path is suspected of
affecting raw wasm bytes, prove it with a direct raw `.wasm` comparison before
changing style policy.

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
- If a multi-line item doc block uses the house style with standalone `///`
  lines before and after the body, leave a real blank line after the closing
  standalone `///` before any attributes or item declaration.
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
- Can panic, either directly or through an intentional `expect`, `unwrap`, or
  caller-triggered assertion exposed through a public API

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

Public APIs with reachable panic paths must include a `# Panics` section naming
the exact condition. Prefer returning a typed error when the caller can
reasonably recover.
Internal invariant failures should use the local invariant helper for that
subsystem instead of naked `panic!`, `unwrap`, or `expect`; if such an
invariant failure remains visible through a public API, document it as an
invariant violation rather than a normal caller contract. Tests may still use
`expect` when it improves failure messages.

Production executor code has the stricter canister-runtime rule: no panicking
`panic!`, `assert!`, `.unwrap()`, or `.expect()` in runtime paths. Return
`InternalError` or a more specific typed error, and reserve `debug_assert!` for
documenting invariants that are already enforced by fallible code. Test and
benchmark-only code may still use panicking assertions for clarity.

Workspace lint policy for panic docs is tracked in
[`panic-docs-clippy-lint.md`](/home/adam/projects/icydb/docs/design/ideas/panic-docs-clippy-lint.md).

Comments should state intent, ownership, invariants, or non-obvious tradeoffs.
Remove comments that restate the next line, describe obsolete behavior, or name
historical implementation steps without explaining a current constraint.

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

- Place the inherent `impl TypeName { ... }` block immediately below the type
  definition when feasible.
- Follow the inherent impl with trait impls for that same type, ordered
  alphabetically by trait name unless a stronger local convention exists.
- Keep related trait impls adjacent to the type/inherent impls unless a clear
  module-structure reason requires separation.
- If a file contains multiple types, keep each type family together: type,
  inherent impl, then trait impls for that type.
- Keep inline `#[cfg(test)] mod tests { ... }` as the final item in the file.
  It must not split type families, helper sections, or later runtime items.

Example:

```rust
pub struct RoutePlan {
    // fields
}

impl RoutePlan {
    // constructors and inherent methods
}

impl Display for RoutePlan {
    // formatting
}

impl TryFrom<RouteInput> for RoutePlan {
    // conversion
}
```

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
In production executor code, use fallible invariant helpers or typed
`InternalError` returns rather than panicking assertions or unwrap/expect calls.

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
for the current review guidance, including:

- keeping routine feature slices narrow
- keeping cross-domain changes explicit
- avoiding large parser and session SQL root-module growth

When a change spans parser, lowering/session, build/canister, or integration
surfaces together, call that out explicitly in the PR summary.

## 13. Test Placement

If tests are split into a separate file, declare `mod tests;` at the top with other module declarations.
If tests are inline, keep `#[cfg(test)] mod tests { ... }` at the bottom of the module, after all
runtime types, impls, helpers, and utilities.
For inline tests, the `///`, `/// TESTS`, `///` banner must have exactly one blank line before it and one blank line after it.
Use leaf-local `tests.rs` only for tests that stay within that module's own boundary.
If a test exercises subsystem behavior across sibling modules, shared fixtures, orchestration layers, or boundary contracts, move it to a subsystem-level `tests/` directory owned by that boundary instead of adding another leaf `tests.rs`.
When a subsystem already has an owner suite such as `executor/tests/`, prefer extending that suite over creating new cross-module `tests.rs` files in leaves.

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

During active development, run:

- `cargo fmt --all`

This should be the default formatter command after edits; it fixes formatting directly instead of producing a failure that then needs another command. Reserve `cargo fmt --all --check` for final non-mutating release/readiness verification or CI parity.

Before committing, code should also pass:

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

- `cleanup: normalize imports and documentation in executor/planning/route`
- `cleanup: reorder helpers and banners in planner/pushdown`
- `cleanup: tighten visibility in index/storage`

This keeps history readable.
