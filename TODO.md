````md
# TODO — Model vs Typed API Cleanup

## Goal

Move icydb to a **typed-entity–first architecture**, where:

- Typed entities (`EntitySchema`, `EntityKind`, derives/macros) are the **single source of truth**
- The structural model layer (`EntityModel`, `EntityFieldModel`) remains as an **internal representation**
- Humans do **not** manually construct models except in narrowly justified cases

The model layer should exist to serve planning, validation, and introspection — not as a primary API.

---

## Current State

- Typed queries (`Query<T>`) lower into model-based plans (`QueryModel`, `LogicalPlan`)
- A parallel model-based API still exists for:
  - intent validation
  - planner logic
  - equivalence testing
- Some tests manually construct `EntityModel` instances using:
  - ad-hoc helpers
  - leaked boxed slices
  - stringly-typed field definitions

This is functional but no longer aligned with the desired abstraction boundary.

---

## Direction

### 1. Typed entities are canonical

- Typed entities (via derives / `EntitySchema`) should be the **authoritative definition**
- `EntityModel` should be:
  - derived from typed entities
  - cached / static
  - reused consistently

Manual construction of `EntityModel` should be considered legacy.

---

### 2. Quarantine the model layer

- `EntityModel`, `EntityFieldModel`, etc. remain valid **internals**
- They should be:
  - consumed by planners
  - used in migrations / introspection
  - compared against typed plans in tests
- They should **not** be hand-authored in new code

---

### 3. Reduce manual model construction in tests

Current pattern to eliminate over time:

```rust
fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel
````

Preferred direction:

* Tests should reference `T::MODEL` wherever possible
* If a model-only test is required, provide:

  * a shared helper
  * or a derived model from a minimal typed entity

Rule of thumb:

> No new tests should manually assemble `EntityModel` unless strictly necessary.

---

### 4. Preserve typed ↔ model equivalence tests

Tests asserting that:

* typed queries
* and model-based queries

produce identical `LogicalPlan`s are **valuable and should remain**.

However:

* Both sides should ideally originate from the same typed entity definition
* The model side should not drift or be redefined independently

---

## Action Items

* [ ] Identify tests that can switch from manual models to `T::MODEL`
* [ ] Introduce a helper for model-only tests that derives from a typed entity
* [ ] Add guidance (or a lint) discouraging new manual `EntityModel` construction
* [ ] Document the model layer as *internal / derived*, not user-facing
* [ ] Gradually migrate legacy tests to typed-first definitions

---

## Non-Goals (for now)

* Removing the model layer entirely
* Rewriting the planner to be purely generic over typed entities
* Breaking existing intent / plan validation logic

The focus is **directional cleanup**, not a big-bang rewrite.

```
