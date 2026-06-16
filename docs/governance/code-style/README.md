# IcyDB Code Style Examples

This directory contains documentation-only Rust examples that model the
preferred IcyDB crate and module shape. Treat
`docs/governance/code-hygiene.md` as the source of truth if guidance here ever
conflicts with the hygiene standard.

The sample code is intentionally outside the Cargo workspace and has no
`Cargo.toml`; it should not own package metadata or version numbers. Use it as a
copyable shape for new crates, modules, and review comments.

## Example Tree

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

## What This Demonstrates

- module-level ownership headers
- `mod`, `use`, and `pub use` ordering
- grouped `crate::{...}` imports
- narrow visibility with owner-local re-exports
- public type and function documentation
- invariant-bearing constructors that return typed diagnostics
- leaf-local inline tests and boundary-level `tests.rs`

## Copying Rules

- Copy structure and ordering, not the example domain names.
- Keep runtime authority in the owning module; do not reconstruct accepted
  runtime state from generated model conveniences.
- Use scoped visibility before widening a symbol to `pub`.
- Put cross-module tests in the owner boundary instead of burying them in a
  leaf module.
- Keep examples formatted with `rustfmt`.
