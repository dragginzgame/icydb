# Referential Integrity (RI)

## Status

IcyDB enforces referential integrity for every schema-declared relation.

Accepted relation edges are the sole live relation authority. References are
stored as **typed primary-key values**. Declared relations trigger save-time
target-existence checks and delete-time source-reference checks.
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

A scalar reference is a **typed primary-key value** identifying another entity:

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

A composite relation uses an explicitly declared ordered set of top-level local
fields that exactly matches the target's accepted composite primary-key
components. Collection relations to composite targets are not part of the
current contract.

`Id<T>` is a *boundary type* used for entity-kind correctness. It is **not** automatically validated for existence.
`Id<T>` values may be deserialized from untrusted input; validation is explicit and contextual.

Existence validation occurs wherever the schema declares a relation. A field
that may contain a missing or stale identifier must be declared as an ordinary
key-typed field rather than a relation.

---

## 3. Schema-driven discovery

Referential integrity is **schema-driven and field-scoped**.

Only accepted relation edges explicitly admitted from schema declarations
participate in RI enforcement. Generated relation metadata is proposal input;
runtime save, reverse-index, and delete paths do not infer a missing edge from
raw generated field kinds.

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
* explicitly declared ordered top-level fields matching a composite target key

Supported collection forms:

* relation lists (`many` list cardinality)
* relation sets (`many` set cardinality, e.g. `IdSet<T>`)

Collection validation is **aggregate**:

* every referenced target must exist
* empty collections are valid
* a single missing target fails the save

### 4.2 Relation activation

Adding a relation when historical rows exist publishes a planner-invisible
candidate edge and an accepted activation. Relevant future source writes
validate targets and maintain the isolated reverse generation immediately.
The bounded Forward/Verify job then proves historical target existence and
complete reverse state under stable source and target revisions.

Until atomic promotion, the candidate generation is not accepted reverse-index
authority. Target deletes are conservatively blocked for the affected target
entity so incomplete reverse state cannot authorize deletion. Promotion moves
the exact edge and reserved constraint identity into accepted relation state;
abort removes the activation/job and makes its candidate generation
unreachable.

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

Pending relation activation is not a weak relation mode. Its new-write gate is
already authoritative, while historical findings remain migration evidence
rather than accepted-state corruption.

### 5.3 What is not enforced

IcyDB explicitly does **not** enforce:

* cascading deletes or updates
* query-time reverse traversal semantics
* read-time validation
* deferred checking of a newly authored relation value
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

Accepted and pending relations retain their stable constraint ID and name in
typed runtime or validation diagnostics. Historical activation findings are
reported through the bounded validation response; they are not collapsed into
an ordinary write error.

---

## 8. Explicit non-goals

The following are out of scope for the current RI contract:

* implicit junction-table or relational many-to-many traversal
* recursive existence validation
* cascading behavior
* deferred constraint checking
* `ON DELETE SET NULL` or `ON DELETE SET DEFAULT`
* query-time relation semantics
* joins or relational algebra

Any addition requires a new RI specification.

---

## 9. Reserved extension points (non-binding)

The following extensions are explicitly reserved:

* collection relations to composite target keys
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
