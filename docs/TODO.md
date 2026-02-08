# TODO — Typed-Entity–First API Cleanup

## Goal

Move IcyDB toward a **typed-entity–first architecture**, where:

- Typed entities (`EntitySchema`, `EntityKind`, derive macros) are the **canonical source of truth**
- The structural model layer (`EntityModel`, `EntityFieldModel`) exists as a **derived, internal representation**
- Humans **do not manually author models** except in narrowly scoped, explicitly justified cases

The model layer should serve **planning, validation, and introspection**, not act as a primary authoring API.

This is a **directional cleanup**, not a rewrite.
This work supports the long-term direction described in ROADMAP.md.

---

## Current State

- Typed queries (`Query<T>`) lower into model-based plans (`QueryModel`, `LogicalPlan`)
- A parallel, model-oriented API still exists for:
  - intent validation
  - planner logic
  - equivalence testing
- Some tests manually construct `EntityModel` instances using:
  - ad-hoc helpers
  - leaked boxed slices
  - stringly-typed field definitions

This is functional, but it violates the intended abstraction boundary and causes drift
between typed entities and their runtime representation.

---

## Direction

### 1. Typed entities are canonical

- Typed entities defined via derives and `EntitySchema` are the **authoritative definition**
- `EntityModel` must be:
  - derived from typed entities
  - static or cached
  - reused consistently across planner, validator, and executor

Manual construction of `EntityModel` is considered **legacy** and should not be the default.

---

### 2. Quarantine the model layer

- `EntityModel`, `EntityFieldModel`, and related structures remain valid **internal machinery**
- They may be:
  - consumed by planners
  - used for migrations and introspection
  - compared against typed plans in tests
- They should **not** be hand-authored in new application or test code

The model layer is **derived**, not user-facing.

---

### 3. Reduce manual model construction in tests

Pattern to eliminate over time:

```rust
fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel
```

Preferred patterns:

- Tests should reference `T::MODEL` wherever possible
- If a model-only test is required, provide:
  - a shared helper that derives a model from a minimal typed entity, or
  - a deliberately marked *invalid-schema helper* for negative tests

Rule of thumb:

> No new tests should manually assemble `EntityModel` unless the test is explicitly about invalid schemas.

---

### 4. Preserve typed ↔ model equivalence tests

Tests asserting that:

- typed queries
- and model-based queries

produce identical `LogicalPlan`s are **valuable and should remain**.

However:

- Both sides should originate from the **same typed entity definition**
- The model representation must not drift or be independently redefined

The goal is **equivalence verification**, not parallel authoring.

---

## Action Items

- [x] Identify tests that can switch from manual models to `T::MODEL`
- [x] Introduce helpers for model-only tests that derive from typed entities
- [x] Clearly document manual `EntityModel` construction as legacy
- [x] Reserve manual model construction only for invalid-schema tests
- [x] Gradually migrate legacy tests to typed-first definitions

---

## Additional TODO

- [ ] Implement `saturating_sub` in the inherent trait for numeric newtypes that require it

---

## Explicit Non-Goals (for now)

- Removing the model layer entirely
- Rewriting the planner to be fully generic over typed entities
- Breaking existing intent or plan validation logic

This effort is about **clarifying authority and boundaries**, not destabilizing core systems.
