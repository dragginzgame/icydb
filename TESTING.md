# Testing Strategy

This document defines **all test categories in IcyDB**, what each category is responsible for, where it lives in the repository, and—most importantly—**what it is allowed to depend on**.

The goal is to keep tests:

* Architecturally honest
* Maintainable over time
* Clear about intent and scope

---

## Test Taxonomy (Authoritative)

IcyDB uses **five distinct classes of tests**. Each class has a clear purpose and a clear home.

| Test Type              | Purpose                                   | Lives In                        | May Touch Internals | Runs On IC |
| ---------------------- | ----------------------------------------- | ------------------------------- | ------------------- | ---------- |
| Unit Tests             | Local invariants of a single module       | Same crate as code              | Yes                 | No         |
| Integration Tests      | Cross-module correctness inside one crate | `crates/*/tests` or `mod tests` | Limited             | No         |
| Schema / Planner Tests | Declarative schema → planning semantics   | `icydb-core`                    | Yes                 | No         |
| End-to-End (E2E) Tests | Full system behavior                      | `tests/e2e`                     | No                  | Yes        |
| Regression Tests       | Lock in previously-broken behavior        | Wherever the bug lived          | Yes                 | Depends    |

---

## 1. Unit Tests

### Purpose

Validate **local invariants** of a single module or type.

Examples:

* Ordering invariants
* Boundary conditions
* Error classification
* Small pure functions

### Characteristics

* Very fast
* Very focused
* Minimal setup

### Location

* Inline `#[cfg(test)] mod tests` next to the code

### Allowed Dependencies

* Same module or sibling modules
* Private helpers

### Forbidden

* Cross-crate behavior assertions
* Schema-level reasoning
* Persistence

---

## 2. Integration Tests (Intra-crate)

### Purpose

Verify **multiple modules within the same crate** work together correctly.

Examples:

* Query planner + predicate normalization
* Index model + schema validation
* Commit ops + executor ordering

### Location

Either:

* `#[cfg(test)] mod tests` using internal APIs, or
* `crates/<crate>/tests/*.rs`

### Allowed Dependencies

* Internal (non-public) APIs
* Real data structures

### Forbidden

* IC execution
* Canister lifecycle

---

## 3. Schema / Planner Tests (Critical Category)

### Purpose

Assert **semantic correctness** of:

* Schema models
* Index selection
* Access path planning
* Predicate normalization
* Determinism guarantees

These tests are *pure logic* and are the primary guardrail for query correctness.

### Location

**`icydb-core`** (intentionally)

These tests live in core because they must:

* Call non-public planner functions
* Assert on internal types (`AccessPlan`, `AccessPath`)
* Avoid public "test hatches"

### Key Rule

> Planner logic must depend on **models**, not runtime entities.

Tests in this category should:

* Construct `EntityModel`, `IndexModel`, and `SchemaInfo` directly
* Avoid `EntityKind`, derives, or runtime glue

### Model Construction Rules (Tests)

When a test needs a manual model:

* Use the legacy helper `LegacyTestEntityModel` in `crates/icydb-core/src/test_fixtures.rs`
* Add a short comment explaining why typed entities are not used
* Do **not** inline `EntityModel { ... }` in test modules

When a typed entity already exists in the test:

* Use `E::MODEL`
* Do not recreate the model manually

### Forbidden

* Canisters
* Persistence
* Serialization
* Proc-macro reliance

---

## 4. End-to-End (E2E) Tests

### Purpose

Validate **real-world behavior** from a user perspective:

* Schema declaration
* Deployment
* Reads and writes
* Referential integrity
* Recovery behavior

These tests answer: *"Does the system actually work when deployed?"*

### Location

`tests/e2e`

### Characteristics

* Slow
* Expensive
* High confidence

### Allowed Dependencies

* Public API only
* Real canisters
* Real storage

### Forbidden

* Internal planner assertions
* White-box checks

---

## 5. Regression Tests

### Purpose

Ensure **specific previously-broken behavior never regresses**.

Regression tests are not a separate location—they are a **label and discipline**.

### Rules

* Live next to the code that was fixed
* Reference the bug or invariant in comments
* Prefer minimal reproductions

Example:

```rust
// Regression: ORDER BY on non-orderable field must error (0.5.25)
```

---

## What We Explicitly Do NOT Have

* ❌ Macro DSL tests
* ❌ `__internal` test-only APIs
* ❌ Dual schema languages ("real" vs "test")
* ❌ Inline `EntityModel { ... }` in test modules
* ❌ Unlabeled manual models (use `LegacyTestEntityModel`)

If a test requires one of these, the architecture is wrong.

---

## Design Principles (Non-Negotiable)

1. **Models over runtime types**
   Planner logic depends on schema models, not `EntityKind`.

2. **Black-box vs white-box clarity**
   E2E tests are black-box. Core tests are white-box. Never mix them.

3. **No test-only public APIs**
   Tests live where internals already exist.

4. **Determinism is testable**
   Planner output must be stable across runs and input ordering.

---

## When Adding a New Test, Ask

1. Am I testing *local logic* or *system behavior*?
2. Do I need internals, or only public APIs?
3. Can this be expressed purely in terms of models?

If the answers are unclear, stop and reassess.

---

## Summary

* Unit tests protect invariants
* Integration tests protect module composition
* Schema / planner tests protect correctness
* E2E tests protect reality
* Regression tests protect history

Each class has a home. Each home has rules.

Violating those boundaries is how test suites rot.
