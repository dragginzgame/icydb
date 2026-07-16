# Referential Integrity (RI)

## Status

IcyDB enforces referential integrity for schema-declared relations by default.
Only relations explicitly configured as **unchecked** opt out.

References are stored as **typed primary-key values**. Enforced relations trigger
save-time target-existence checks and delete-time source-reference checks.
The surrounding row strictness and ingress rules are defined in
`docs/contracts/WRITE_ADMISSION.md`.

This document is **normative**. It defines:

* what guarantees exist,
* what is explicitly *not* guaranteed,
* and where future extensions may occur.

It is not a roadmap.

This specification reflects the current shipped contract; the baseline
originated in the `0.10` line.

---

## 1. Scope and intent

Referential integrity (RI) in IcyDB is a **bounded pre-commit validation
rule**.

It exists to ensure that certain schema-declared references point to existing entities **at the moment of mutation**, without introducing relational database semantics.

IcyDB is **not** a relational system. It does not support:

* joins
* cascades
* public reverse traversal queries
* query-time relation semantics

RI is intentionally narrow, schema-driven, and enabled by default for declared
relations.

---

## 2. What a reference is

A reference is a **typed primary-key value** identifying another entity:

```rust
Id<T>
```

A reference:

* identifies an entity by key
* is a public, non-secret identifier value
* does **not** imply ownership
* does **not** imply authorization
* does **not** imply lifecycle coupling
* does **not** imply traversal, joins, or relational semantics

References are **identity values**, not relationships in the relational sense.

`Id<T>` is a *boundary type* used for entity-kind correctness. It is **not** automatically validated for existence.
`Id<T>` values may be deserialized from untrusted input; validation is explicit and contextual.

Existence validation occurs wherever the schema declares a relation unless that
relation explicitly sets `enforcement = "unchecked"`.

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

## 4. Relation enforcement

IcyDB distinguishes between **enforced** and **unchecked** relations.

Enforcement controls **validation behavior**, not representation.

Relations are enforced by default. The schema DSL uses one enum-valued
`enforcement` setting and permits an explicit unchecked opt-out:

```text
item(rel = "EntityA", prim = "Ulid")
item(rel = "EntityA", prim = "Ulid", enforcement = "enforced")
item(rel = "EntityA", prim = "Ulid", enforcement = "unchecked")
```

Unchecked behavior is **never inferred**.

---

### 4.1 Enforced relations (validated)

Enforced relations are validated on both save and delete paths.

Rules:

* Enforced is the default schema intent
* Validation runs **before commit**
* The referenced entity **must exist**
* Any failure aborts the mutation
* No partial state is written
* No cascading inserts or deletes occur

Supported enforced shapes in the current contract:

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

### 4.2 Unchecked relations (not validated)

Unchecked relations are **not validated for existence**.

Rules:

* The opt-out must be explicit schema intent
* Values are type-checked and serialized normally
* Missing targets do **not** cause errors
* Unchecked relations do **not** affect atomicity

Unchecked relations provide **no referential-integrity guarantees**.

---

## 5. Enforcement model

### 5.1 When enforcement runs

RI enforcement:

* is part of the write-admission pre-commit defined in
  `docs/contracts/WRITE_ADMISSION.md`
* is synchronous and bounded
* does not rely on traps or recovery
* applies to both save-time target existence checks and delete-time source checks

### 5.2 What is enforced

Only relations whose enforcement is **enforced** receive RI guarantees.

For collections, validation is element-wise and bounded.

RI enforcement is skipped when:

* the relation enforcement is `unchecked`
* the value is explicitly absent (`None`)
* the field is not a schema-declared relation
* the reference is nested beyond the field boundary
  (records, enums, tuples, maps, etc.)

There is no recursive discovery.

### 5.3 What is not enforced

IcyDB explicitly does **not** enforce:

* cascading deletes or updates
* query-time reverse traversal semantics
* read-time validation
* deferred or lazy validation
* cross-mutation or cross-message constraints

---

## 6. Atomicity compatibility

Referential integrity is designed to preserve IcyDB’s atomicity model.

* Relation validation completes under the write-admission contract
* The apply phase follows `docs/contracts/ATOMICITY.md`
* Unchecked relations do not weaken atomicity guarantees

RI enforcement does **not** depend on traps, recovery timing, or read behavior.

---

## 7. Error classification

Enforced-relation failures surface as **write-time validation errors**.

They are reported as:

* `ErrorClass::InvariantViolation`
* `ErrorOrigin::Executor`

They indicate invalid input, **not** corruption.

---

## 8. Explicit non-goals

The following are out of scope for the current RI contract:

* many-to-many relations
* recursive existence validation
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
* an explicit opt-out for non-enforcing relations

---

## 10. Summary

IcyDB’s referential integrity model is:

* **schema-driven**
* **enforced by default with an explicit unchecked opt-out**
* **save-time and delete-time for enforced relations**
* **bounded**
* **non-relational**

Enforced relations provide correctness where it is safe and enforceable.

Unchecked relations provide flexibility where dangling references are an
intentional part of the domain model.

This balance is intentional and foundational.
