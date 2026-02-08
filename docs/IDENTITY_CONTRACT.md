# Identity and Primary Key Contract (0.7.0)

This document freezes identity and primary-key semantics for IcyDB 0.7.x.
It is a source-of-truth boundary: anything not stated here is not guaranteed.

## 1. Definition of `Id<E>`

`Id<E>` is a typed primary-key value. It is not a capability and not an authorization token.

`Id<E>` exists to:
- bind a primitive key type to an entity at compile time
- prevent accidental mixing of keys across entities
- improve API and schema correctness

`Id<E>` does not:
- grant access
- imply permission
- represent authority
- enforce security boundaries

## 2. Construction Rules for `Id<E>`

The system enforces the following construction rules.

Explicit construction is allowed:
- creating `Id<E>` from a known primitive key (for example `Ulid` or `Principal`) via an explicit constructor is valid

Implicit construction is forbidden:
- `Id<E>` must not be created implicitly via deserialization
- `Id<E>` must not be created implicitly via trait-based coercion
- `Id<E>` must not be created implicitly via inference from unrelated types
- `Id<E>` must not be created implicitly via relation metadata

This preserves intentional, auditable identity construction. It is not a security isolation boundary.

## 3. Declared Type Authority

For all entities:
- the declared field type is authoritative for storage and identity
- primary-key types must be derived only from the declared PK field
- relation metadata must never influence storage shape or PK type derivation
- illegal schemas (for example, ambiguous relation-typed PK declarations) must fail at compile time, not runtime

## 4. Relations vs Storage

`rel = "Entity"` expresses semantic relationship only.

`prim = "Type"` expresses storage representation.

These concerns are intentionally separated.

Primary keys may also be foreign keys, provided their primitive storage type is explicit.

This supports patterns such as:
- 1-to-1 extension tables
- sidecar metadata tables
- counters and aggregates

## 5. Runtime Safety

For any legal schema:
- generated code must not panic at runtime
- identity accessors (for example `entity.id()`) must be infallible
- schema validation must occur at derive/compile time

## 6. Conceptual Layers (Naming Boundary)

IcyDB keeps identity and key semantics split across explicit layers.

- Schema/domain layer: fields express relations semantically (`customer`, `owner`, `invoice`), not storage syntax.
- Identity layer: `Id<E>` (and `self.id()`) expresses entity identity at the type level.
- Storage layer: primitive keys (`Ulid`, `Principal`, `u64`, etc.) are raw key material.
- Query layer: predicates compare explicit key values; relation meaning is schema metadata, not inferred at runtime.

## 7. Naming Conventions

Use names that match the layer and call site purpose.

Schema fields use relation semantics:

```rust
struct Order {
    customer: Ulid,
}
```

Not:

```rust
struct Order {
    customer_id: Ulid,
}
```

Loading an entity by its own key uses `*_id`:

```rust
fn load_customer(customer_id: Ulid) { /* ... */ }
```

Filtering other entities by a relation key uses `*_key`:

```rust
fn orders_for_customer(customer_key: Ulid) { /* ... */ }
```

Inside entity methods, use typed identity:

```rust
fn audit_label(&self) -> String {
    format!("{:?}", self.id())
}
```

## 8. Rationale

`Id<E>` and primitive keys are not interchangeable concepts.

- `Id<E>` is an entity-typed identity handle.
- Primitive keys are storage/domain values used for persistence and explicit comparisons.

Many ORMs hide this distinction by collapsing relation naming, identity, and key transport into one concept. IcyDB keeps them separate so code can state intent precisely:

- relation meaning in schema names
- identity meaning in `Id<E>`
- storage meaning in primitive key values

This separation improves correctness, prevents accidental cross-entity key mixing, and makes query boundaries auditable.

## 9. Do Not Do This

- Do not rename schema relation fields to `*_id` as a style default.
- Do not pass `Id<E>` casually across API boundaries where a primitive key is the real contract.
- Do not collapse identity (`Id<E>`) and storage key (`Ulid`/`Principal`/etc.) into one naming convention.
