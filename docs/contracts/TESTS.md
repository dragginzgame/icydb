# Testing Layout Contract (IcyDB)

This document defines **where tests live** and **how to choose the right test home** in `crates/icydb-core/src/db/`.
The goal is to keep tests **sustainable**, **discoverable**, and **architecturally aligned** with subsystem ownership.

This is not a “LOC instrumentation” policy. The layout rules exist to:
- preserve subsystem boundaries,
- keep production modules readable,
- keep tests near the invariants they protect,
- avoid test scaffolding leaking into production code,
- scale as modules grow.

---

## Terminology

### Unit tests
Tests that validate **local invariants** of a module/subsystem and may depend on crate-private details.

### Integration tests (internal)
Tests that validate **cross-module behavior within the crate** (planner → route → kernel → cursor, etc.).
These often require fixtures and multi-step orchestration.

### External integration tests
Black-box tests under `crates/<crate>/tests/` that treat the crate as an API boundary.

---

## North Star Rules

### Rule 1 — Tests must live at the owning boundary
A test should live in the lowest layer that **promises** the behavior being asserted.

- If the assertion is a cursor contract → cursor owns the test.
- If the assertion is a route policy matrix → route owns the test.
- If the assertion is end-to-end execution behavior → executor integration harness owns the test.

### Rule 2 — Prefer module-co-located tests, not inline tests
Unit tests should be “near” the code they validate but should not drown production files.

Default:
- Directory module → `tests.rs`
- Leaf file module → `*_tests.rs` sibling

Inline `#[cfg(test)] mod tests { ... }` is allowed only for **small micro-tests** (see Rule 4).

### Rule 3 — Cross-subsystem tests live in an explicit harness
Cross-subsystem behavior belongs in a dedicated test harness directory (example: `db/executor/tests/**`).

Do not spread cross-cutting tests across many leaf modules. That becomes unmaintainable.

### Rule 4 — Inline tests are a privilege (small, local, no scaffolding)
Inline tests are permitted only when all are true:
- Small (< ~100 lines total for the tests module)
- No shared fixtures/helpers
- Validates a local helper/invariant
- Reading the test in-place improves understanding of the code immediately above it

When inline tests grow beyond this, they must be moved to `tests.rs` or `*_tests.rs`.

### Rule 5 — Test-only scaffolding must not leak into production APIs
If tests need helpers:
- keep helpers in `tests.rs` / `*_tests.rs` / `tests/` harness files, or
- keep helpers behind `#[cfg(test)]` and **crate-private** visibility

Avoid adding public exports solely to satisfy tests.

---

## Canonical Locations (IcyDB DB Tree)

This section maps test types to concrete locations that match the existing repository structure.

### A) Subsystem unit tests: `tests.rs` in the subsystem directory
Use when validating invariants within a subsystem consisting of multiple files.

Examples (recommended / preferred):
- `crates/icydb-core/src/db/cursor/tests.rs`
- `crates/icydb-core/src/db/executor/route/tests.rs`
- `crates/icydb-core/src/db/executor/kernel/tests.rs`
- `crates/icydb-core/src/db/executor/aggregate/tests.rs`
- `crates/icydb-core/src/db/commit/tests.rs` (already exists)

Pattern:

```rust
// subsystem/mod.rs
#[cfg(test)]
mod tests;