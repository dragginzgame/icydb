# Referential Integrity (RI)

## Status (as of 0.7.x)

Save-time referential integrity is enforced for **strong** relations only. References are typed primary-key values; existence checks run only where the schema explicitly declares strength.

Key points:
* `Id<T>` is a typed primary-key value used for entity-kind correctness
* `RelationStrength::Strong` is enforced at save time for `Id<T>`, `Option<Id<T>>`,
  and collections of `Id<T>` (e.g. `List<Id<T>>`, `Set<Id<T>>`)
* `RelationStrength::Weak` is **not validated** and is purely semantic
* Strength is **explicit** schema intent (no inference from type shape or cardinality)
* Locality is enforced at `DbSession<C>` via `E: EntityKind<Canister = C>`
* Schema metadata is emitted via associated constants; no runtime schema registry

This document defines the **referential integrity model** for IcyDB.

It is **normative**: it specifies what guarantees exist, what is explicitly not guaranteed, and where future extensions may occur. It is not a feature roadmap.

This document applies to **IcyDB 0.7**.

---

## 1. Scope and intent

Referential integrity (RI) in IcyDB is a **save-time validation rule**, not a query feature and not a relational system.

RI exists to ensure that certain references declared in the schema point to existing entities at the time of mutation, **without introducing relational semantics** such as joins, cascades, or reverse lookups.

IcyDB remains a **typed key/value database** with explicit invariants.

---

## 2. What a reference is

A reference is a typed primary-key value for another entity:

```
Id<T>
```

A reference:

* identifies an entity by key
* does not imply ownership
* does not imply lifecycle coupling
* does not imply joins or relational traversal semantics

References are **not joins** and do not participate in query planning.

`Id<T>` is a typed primary-key value and is **not automatically RI-validated** except where the schema declares a strong relation.

Collection fields that contain `Id<T>` are treated as references for RI when the field is marked strong.

---

## 3. Reference discovery (schema-driven)

RI is **schema-driven** and **field-scoped**.

Only entity fields declared as relations in the schema are considered for save-time enforcement. There is **no traversal beyond the field boundary** (no nested discovery inside records, enums, tuples, maps, or arbitrary containers), and **no inference** from type shape, cardinality, or field names.

---

## 4. Strong vs weak references

IcyDB distinguishes between **strong** and **weak** references. The distinction controls **validation**, not representation.

Strength is declared explicitly in the schema DSL:

* `item(rel = "EntityA", strong)`
* `item(rel = "EntityA", weak)`
* `item(rel = "EntityA")` (defaults to `weak`)

---

### 4.1 Strong references (validated)

Strong references are **validated at save time**.

Strong reference rules:

* Strength is **explicit** in schema metadata
* Validation occurs **pre-commit**
* Validation checks that the referenced entity exists
* Validation failure aborts the mutation
* No durable state is mutated on failure
* No cascading inserts or deletes are performed

Supported strong shapes:

* `Id<T>`
* `Option<Id<T>>`
* Collections of `Id<T>` (e.g. `List<Id<T>>`, `Set<Id<T>>`)

Collection kinds enforced in 0.7.x:
* relation lists (`many` list cardinality)
* relation sets (`many` set cardinality, typically `IdSet<T>`)

Collection enforcement is **aggregate**:

* Every referenced target must exist
* Empty collections are valid
* Any missing target causes the save to fail

Strength is **not inferred** from cardinality or container shape.

---

### 4.2 Weak references (allowed, not validated)

Weak references are **not validated for existence**.

Weak reference rules:

* Strength is **explicit** in schema metadata
* Values are type-checked and serialized normally
* They do **not** participate in save-time RI enforcement
* Missing targets do **not** cause errors
* They do **not** affect atomicity

Weak references make **no correctness guarantees** beyond typing.

---

## 5. Save-time enforcement model

### 5.1 When RI runs

RI enforcement:

* Runs **before the commit boundary**
* Occurs during mutation pre-commit
* Is synchronous and bounded
* Does not rely on traps or recovery

### 5.2 What is enforced

Only **strong references** are enforced.

For collection fields, enforcement is element-wise and bounded; a single missing
target fails the save. Empty collections are valid.

Weak references are allowed but not validated; there is no recursive discovery
or inference.

RI is skipped when:
* relation strength is `weak`
* a relation value is explicitly unset (`None`)
* a collection entry is explicitly `None`
* the field is not a schema-declared relation field
* the relation is nested beyond the field boundary (records/enums/tuples/maps)

### 5.3 What is not enforced

IcyDB explicitly does **not** enforce:

* Delete-side referential integrity
* Cascading deletes
* Reverse reference checks
* Read-time validation
* Deferred or lazy validation
* Cross-mutation or cross-message constraints

---

## 6. Atomicity compatibility

Referential integrity is designed to be fully compatible with the IcyDB atomicity model.

* All validation is pre-commit
* Apply phase remains mechanical and infallible
* No partial state visibility is possible
* Weak references do not weaken atomicity guarantees

RI enforcement does **not** depend on traps, recovery timing, or read behavior.

---

## 7. Error classification

Validation failures for strong references are reported as **write-time validation errors**.

They surface as `InternalError` with `ErrorClass::InvariantViolation` and
`ErrorOrigin::Executor`. They are **not** corruption.

---

## 8. Explicit non-goals (0.7)

The following are **out of scope** for IcyDB 0.7:

* Many-to-many relations
* Recursive existence validation
* Delete-side RI enforcement
* Cascading behavior
* Query-time relation semantics
* Joins or relational algebra

Introducing any of these requires a new RI specification.

---

## 9. Future extension points (non-binding)

The following extensions are explicitly reserved for the future:

* Cardinality-aware many-relations
* Static guarantees for entity–store ownership
* Tooling for reference diagnostics and visualization

Any extension **must preserve**:

* bounded pre-commit validation
* single-message atomicity
* executor simplicity
* explicit, opt-in semantics

---

## 10. Summary

IcyDB’s RI model is:

* **Explicit**
* **Bounded**
* **Save-time only**
* **Schema-driven**
* **Non-relational**

Strong references provide correctness where it is safe and bounded.

Weak references provide flexibility where correctness cannot be enforced without violating IcyDB’s design goals.

This balance is intentional and foundational.
