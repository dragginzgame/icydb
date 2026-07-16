# Referential Integrity (RI)

## Status

IcyDB enforces referential integrity for every schema-declared relation.

References are stored as **typed primary-key values**. Declared relations trigger
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

Existence validation occurs wherever the schema declares a relation. A field
that may contain a missing or stale identifier must be declared as an ordinary
key-typed field rather than a relation.

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

## 4. Declared relations

The presence of `rel` is the complete enforcement declaration:

```text
item(rel = "EntityA", prim = "Ulid")
item(prim = "Ulid") // ordinary identifier with no relation guarantee
```

There is no weak, unchecked, or non-enforcing relation mode. Target metadata is
retained only for fields that accept the full relation contract.

---

### 4.1 Relation guarantees

Relations are validated on both save and delete paths.

Rules:

* Declaring `rel` opts into the complete relation contract
* Validation runs **before commit**
* The referenced entity **must exist**
* Any failure aborts the mutation
* No partial state is written
* No cascading inserts or deletes occur

Supported relation shapes in the current contract:

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

## 5. Enforcement model

### 5.1 When enforcement runs

RI enforcement:

* is part of the write-admission pre-commit defined in
  `docs/contracts/WRITE_ADMISSION.md`
* is synchronous and bounded
* does not rely on traps or recovery
* applies to both save-time target existence checks and delete-time source checks

### 5.2 What is enforced

Every schema-declared relation receives RI guarantees.

For collections, validation is element-wise and bounded.

RI enforcement is skipped when:

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

RI enforcement does **not** depend on traps, recovery timing, or read behavior.

---

## 7. Error classification

Relation failures surface as **write-time validation errors**.

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
* the distinction between relations and ordinary identifier fields

---

## 10. Summary

IcyDB’s referential integrity model is:

* **schema-driven**
* **always enforced for declared relations**
* **save-time and delete-time**
* **bounded**
* **non-relational**

Ordinary key-typed fields remain available when dangling identifiers are an
intentional part of the domain model. The difference is explicit and
foundational: a relation always carries referential-integrity guarantees.
