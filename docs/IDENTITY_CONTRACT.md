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
