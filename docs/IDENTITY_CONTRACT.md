# Identity and Primary Key Contract (0.8.x)

This document freezes identity and primary-key semantics for IcyDB 0.8.x.
It is a source-of-truth boundary: anything not stated here is not guaranteed.

## 1. Threat Model Assumptions

Identity values in IcyDB are **public inputs**.

- `Id<E>` values are non-secret and may be logged, serialized, deserialized, and transmitted.
- Any external `Id<E>` input may be adversarial.
- No identity type implies trust by itself.
- Correctness comes from explicit verification, not possession of an identifier.

## 2. Definition of `Id<E>`

`Id<E>` is a typed primary-key value. It is not a capability and not an authorization token.

`Id<E>` exists to:
- bind a primitive key type to an entity at compile time
- prevent accidental mixing of keys across entities
- improve API and schema correctness

`Id<E>` does not:
- grant access
- imply permission
- represent authority
- prove ownership
- prove existence
- enforce security boundaries

`Id<E>` warning:
"`Id<E>` is a public identifier. It is not a secret and must never be treated as proof of authorization, existence, or ownership."

## 3. Construction and Input Rules for `Id<E>`

Explicit construction is allowed:
- creating `Id<E>` from a known primitive key (for example `Ulid` or `Principal`) via an explicit constructor is valid

Deserialization from untrusted input is allowed:
- `Id<E>` may appear in DTO/API payloads
- accepting and deserializing IDs from external callers is expected
- deserializing an ID does not validate trust, authority, or existence

Implicit semantic inference is forbidden:
- `Id<E>` meaning must not be inferred from unrelated types
- `Id<E>` meaning must not be inferred from relation metadata
- coercions that hide entity-kind boundaries are not part of the contract

This preserves auditable identity flow without treating identity as a security primitive.

## 4. Declared Type Authority

For all entities:
- the declared field type is authoritative for storage and identity shape
- primary-key types must be derived only from the declared PK field
- relation metadata must never influence storage shape or PK type derivation
- illegal schemas (for example, ambiguous relation-typed PK declarations) must fail at compile time, not runtime

## 5. Verification Model (Required)

Every use of external identity must verify context explicitly.

- Existence checks are explicit lookups.
- Authorization checks are explicit policy decisions.
- Ownership checks are explicit domain checks.
- Cryptographic checks (when required) are explicit verification steps.

No layer may assume trust from `Id<E>` type alone.

## 6. Identity Projection Semantics

Identity projection is a one-way, mechanical derivation from canonical key bytes.

Projection exists for:
- external system compatibility
- deterministic mapping
- correlation avoidance

Projection does not provide:
- secrecy
- authentication
- authorization
- capability semantics
- proof of ownership or existence

Projected identifiers are public, non-authoritative values and must be treated as untrusted input.

## 7. Relations vs Storage

`rel = "Entity"` expresses semantic relationship only.

`prim = "Type"` expresses storage representation.

These concerns are intentionally separated.

Primary keys may also be foreign keys, provided their primitive storage type is explicit.

This supports patterns such as:
- 1-to-1 extension tables
- sidecar metadata tables
- counters and aggregates

## 8. Conceptual Layers (Naming Boundary)

IcyDB keeps identity and key semantics split across explicit layers.

- Schema/domain layer: fields express relations semantically (`customer`, `owner`, `invoice`), not storage syntax.
- Identity layer: `Id<E>` (and `self.id()`) expresses entity identity at the type level.
- Storage layer: primitive keys (`Ulid`, `Principal`, `u64`, etc.) are raw key material.
- Query layer: predicates compare explicit key values; relation meaning is schema metadata, not inferred at runtime.
- Security/policy layer: trust decisions are explicit and contextual.

## 9. Naming Conventions

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

## 10. Rationale

`Id<E>` and primitive keys are not interchangeable concepts.

- `Id<E>` is an entity-typed identity handle.
- Primitive keys are storage/domain values used for persistence and explicit comparisons.

IcyDB keeps relation naming, identity typing, and key transport separate so code can state intent precisely:
- relation meaning in schema names
- identity meaning in `Id<E>`
- storage meaning in primitive key values
- trust meaning in explicit verification logic

This separation improves correctness, prevents accidental cross-entity key mixing, and makes identity flows auditable.

## 11. Do Not Do This

- Do not treat `Id<E>` as a capability, session token, or proof object.
- Do not assume authorization, ownership, or existence from possession of an ID.
- Do not rename schema relation fields to `*_id` as a style default.
- Do not collapse identity (`Id<E>`) and storage key (`Ulid`/`Principal`/etc.) into one naming convention.
