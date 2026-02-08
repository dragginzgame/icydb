# Referential Integrity (RI)

## Status (IcyDB 0.7.x)

IcyDB enforces **save-time referential integrity** for **explicitly declared strong relations only**.

References are stored as **typed primary-key values**. Existence checks are performed **only** when the schema declares a relation as strong.

This document is **normative**. It defines:

* what guarantees exist,
* what is explicitly *not* guaranteed,
* and where future extensions may occur.

It is not a roadmap.

This specification applies to **IcyDB 0.7**.

---

## 1. Scope and intent

Referential integrity (RI) in IcyDB is a **bounded, save-time validation rule**.

It exists to ensure that certain schema-declared references point to existing entities **at the moment of mutation**, without introducing relational database semantics.

IcyDB is **not** a relational system. It does not support:

* joins
* cascades
* reverse lookups
* delete-side enforcement
* query-time relation semantics

RI is intentionally narrow, explicit, and opt-in.

---

## 2. What a reference is

A reference is a **typed primary-key value** identifying another entity:

```rust
Id<T>
```

A reference:

* identifies an entity by key
* does **not** imply ownership
* does **not** imply lifecycle coupling
* does **not** imply traversal, joins, or relational semantics

References are **identity values**, not relationships in the relational sense.

`Id<T>` is a *boundary type* used for entity-kind correctness. It is **not** automatically validated for existence.

Existence validation occurs **only** where the schema explicitly declares a strong relation.

---

## 3. Schema-driven discovery

Referential integrity is **schema-driven and field-scoped**.

Only fields explicitly declared as relations in the schema participate in RI enforcement.

There is:

* no inference from type shape or cardinality
* no discovery inside nested structures
* no traversal beyond the field boundary

RI applies **only** to top-level entity fields declared as relations.

---

## 4. Relation strength

IcyDB distinguishes between **strong** and **weak** relations.

Strength controls **validation behavior**, not representation.

Strength is declared explicitly in the schema DSL:

```text
item(rel = "EntityA", strong)
item(rel = "EntityA", weak)
item(rel = "EntityA")        // defaults to weak
```

Strength is **never inferred**.

---

### 4.1 Strong relations (validated)

Strong relations are **validated at save time**.

Rules:

* Strength is explicit schema intent
* Validation runs **before commit**
* The referenced entity **must exist**
* Any failure aborts the mutation
* No partial state is written
* No cascading inserts or deletes occur

Supported strong shapes in 0.7.x:

* `Id<T>`
* `Option<Id<T>>`
* Collections of `Id<T>`

Supported collection forms:

* relation lists (`many` list cardinality)
* relation sets (`many` set cardinality, e.g. `IdSet<T>`)

Collection validation is **aggregate**:

* every referenced target must exist
* empty collections are valid
* a single missing target fails the save

---

### 4.2 Weak relations (not validated)

Weak relations are **not validated for existence**.

Rules:

* Strength is explicit schema intent
* Values are type-checked and serialized normally
* Missing targets do **not** cause errors
* Weak relations do **not** affect atomicity

Weak relations provide **no correctness guarantees** beyond type safety.

---

## 5. Save-time enforcement model

### 5.1 When enforcement runs

RI enforcement:

* runs during mutation pre-commit
* completes before the commit boundary
* is synchronous and bounded
* does not rely on traps or recovery

### 5.2 What is enforced

Only **strong relations** are enforced.

For collections, validation is element-wise and bounded.

RI enforcement is skipped when:

* the relation strength is `weak`
* the value is explicitly absent (`None`)
* the field is not a schema-declared relation
* the reference is nested beyond the field boundary
  (records, enums, tuples, maps, etc.)

There is no recursive discovery.

### 5.3 What is not enforced

IcyDB explicitly does **not** enforce:

* delete-side referential integrity
* cascading deletes or updates
* reverse reference checks
* read-time validation
* deferred or lazy validation
* cross-mutation or cross-message constraints

---

## 6. Atomicity compatibility

Referential integrity is designed to preserve IcyDB’s atomicity model.

* All validation occurs pre-commit
* The apply phase remains mechanical and infallible
* No partial state visibility is possible
* Weak relations do not weaken atomicity guarantees

RI enforcement does **not** depend on traps, recovery timing, or read behavior.

---

## 7. Error classification

Strong-relation failures surface as **write-time validation errors**.

They are reported as:

* `ErrorClass::InvariantViolation`
* `ErrorOrigin::Executor`

They indicate invalid input, **not** corruption.

---

## 8. Explicit non-goals (0.7)

The following are **out of scope** for IcyDB 0.7:

* many-to-many relations
* recursive existence validation
* delete-side RI enforcement
* cascading behavior
* query-time relation semantics
* joins or relational algebra

Any addition requires a new RI specification.

---

## 9. Reserved extension points (non-binding)

The following extensions are explicitly reserved:

* cardinality-aware many-relations
* stronger static guarantees for entity–store locality
* tooling for reference diagnostics and visualization

Any extension must preserve:

* bounded pre-commit validation
* single-message atomicity
* executor simplicity
* explicit, opt-in semantics

---

## 10. Summary

IcyDB’s referential integrity model is:

* **explicit**
* **schema-driven**
* **save-time only**
* **bounded**
* **non-relational**

Strong relations provide correctness where it is safe and enforceable.

Weak relations provide flexibility where enforcement would violate IcyDB’s design goals.

This balance is intentional and foundational.
