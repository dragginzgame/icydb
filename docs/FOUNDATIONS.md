# IcyDB Foundations

This document describes the **conceptual foundations and mental model** of IcyDB.

It explains *why* the system is structured the way it is, what assumptions it makes,
and which constraints guide design decisions.

This document is **interpretive, not normative**.

> Where this document overlaps with formal contracts
> (atomicity, referential integrity, query semantics, identity and PK invariants),
> those contracts take precedence.

Normative specifications live in:
- `docs/ATOMICITY.md`
- `docs/REF_INTEGRITY.md`
- `docs/QUERY_CONTRACT.md`
- `docs/IDENTITY_CONTRACT.md`

---

## 1. What IcyDB is (and is not)

IcyDB is a **typed, embedded key/value database engine**.

It is not:
- an ORM
- a SQL engine
- a relational algebra system

IcyDB provides:
- typed entities with explicit identity
- declarative query intent
- deterministic planning
- mechanical execution
- index maintenance
- explicit atomicity and recovery rules
- optional write-time integrity checks

IcyDB intentionally does **not** provide:
- SQL syntax
- joins or relational traversal
- aggregation or expression evaluation
- cascading behavior
- implicit cross-entity semantics

Design decisions prioritize:
**semantic clarity, correctness, bounded execution, and predictability**.

---

## 2. Intent → Plan → Execute separation

IcyDB enforces a strict, layered separation between **intent**, **planning**, and **execution**.

This separation is foundational and non-negotiable.

---

### 2.1 Query intent

Query intent is:
- declarative
- schema-aware
- free of execution strategy

Intent describes *what* is requested, never *how* it is retrieved.

Examples:
- predicates
- ordering intent
- pagination intent
- consistency mode

Intent must be valid independently of indexes or access paths.

---

### 2.2 Logical planning

The planner:
- validates intent against schema
- normalizes predicates
- selects access paths
- enforces schema-level constraints

The result is a **LogicalPlan** that is:
- fully specified
- deterministic
- schema-validated
- free of ambiguity

The planner is the **only layer** allowed to interpret schema semantics.

---

### 2.3 Execution

The executor:
- executes a validated plan
- performs no schema reasoning
- enforces execution-time invariants
- operates deterministically

Any valid plan must be executable without fallible logic beyond mechanical data access.


---

## 3. Typed schema vs runtime values

IcyDB distinguishes between **query-visible schema** and **runtime payload values**.

---

### 3.1 Query-visible schema

The schema defines:
- fields allowed in predicates
- fields allowed in access paths
- fields eligible for indexing
- fields eligible for integrity checks

Schema fields are exhaustively validated during planning.

---

### 3.2 Runtime-visible values

Entities may carry values that:
- are not part of the schema
- are not indexable
- are not filterable

These values:
- may be preserved in views
- may participate in ordering
- are treated as opaque for comparison

This mirrors the separation between logical schema and physical tuple payloads in
traditional database engines.

---

### 3.3 Schema Validation vs Compile-Time Boilerplate

IcyDB prefers a single validation boundary in the schema pass.

Policy:
- if an invariant can be enforced in a few lines in schema validation (`validate` / `fatal_errors`), enforce it there
- do not add large codegen boilerplate solely to force extra compile-time assertion errors
- use generated compile-time assertions only for invariants that cannot be expressed in the schema pass without comparable complexity

This keeps validation centralized and avoids superlinear maintenance cost as entity count grows.

---

### 3.4 Identity Trust Model

`Id<E>` values are typed identifiers, not capabilities.

- IDs are public and non-secret.
- IDs may be deserialized from untrusted input.
- Possession of an ID does not prove authorization, ownership, correctness, or existence.
- Trust decisions are explicit checks performed after lookup and policy verification.

---

## 4. Unsupported and opaque values

`Value::Unsupported` represents runtime values that:

- must not participate in planning
- must not be indexed
- must not be compared for equality or ordering

The executor:
- preserves these values
- treats comparisons involving them as incomparable
- maintains stable ordering when required

This prevents crashes while preserving determinism.

---

## 5. Ordering semantics

Ordering in IcyDB follows these principles:

- filtering occurs before ordering
- ordering occurs before pagination
- ordering is stable
- incomparable values compare as equal
- stable ordering preserves input order for incomparable values

These rules guarantee deterministic results even under partial ordering.

---

## 6. `IN` semantics

`IN` expresses **set membership only**.

It does not encode:
- execution strategy
- index hints
- scan vs lookup behavior

All execution decisions are planner-controlled.

This preserves logical equivalence across execution strategies.

---

## 7. Unit keys and singleton entities

Entities with `PrimaryKey = ()` represent **existence**, not a value domain.

For such entities:
- the primary key has no scalar domain
- `Value::Unit` represents presence
- executor validation must not treat `Unit` as a mismatch

This aligns with relational theory while remaining compatible with key/value storage.

---

## 8. Collections and cardinality (conceptual)

Collection cardinality in IcyDB is semantic, not structural.

| Kind | Semantics |
| --- | --- |
| Single | exactly one value |
| Optional | zero or one value |
| List | ordered sequence with duplicates |
| Set | membership semantics with uniqueness |

For many-valued fields:
- relation fields use set semantics (`IdSet<E>`) with key-based normalization
- non-relation fields use list semantics (`OrderedList<T>`) with insertion-order preservation

Views and update views are transport surfaces (`Vec<T::ViewType>`, patches), not domain semantics.

Normalization happens only at explicit boundaries:
- `IdSet<E>` deduplicates and canonicalizes by key on construction/deserialization
- `OrderedList<T>` preserves order and duplicates

Collection predicates (`In`, `NotIn`, `Contains`, `IsEmpty`) express membership/cardinality, not ordering.

Non-goals include implicit list deduplication, hidden ordering guarantees for sets,
cascade/ownership inference from collection shape, and relation discovery in nested structures.

---

## 9. Referential integrity (conceptual overview)

Referential integrity in IcyDB is a **write-time validation rule**, not a query feature.

Key characteristics:
- schema-driven
- explicit
- bounded
- non-relational

Only references explicitly declared in the schema participate in enforcement.

> **Normative definition:** see `docs/REF_INTEGRITY.md`.

---

## 10. Reference shape constraints (conceptual)

To preserve atomicity and bounded execution, IcyDB constrains where relations may appear.

Conceptually:
- direct `Id<T>` shapes may be validated
- nested or inferred relations are not discovered
- reference validation never implies traversal or joins

These constraints exist to preserve:
- bounded validation cost
- deterministic commits
- simple recovery semantics

> **Normative rules:** see `docs/REF_INTEGRITY.md`.

---

## 11. Error classification (conceptual)

IcyDB distinguishes between broad classes of failure:
- corruption
- invariant violation
- conflict
- validation / plan errors

Classification is part of the correctness model.

> **Normative definitions:** see `docs/QUERY_CONTRACT.md` and executor error docs.

---

## 12. Recovery and read safety (conceptual)

IcyDB relies on:
- explicit commit markers
- deterministic recovery
- recovery-before-read

Reads do not perform recovery checks after startup; a post-startup trap may expose
partial state until recovery is triggered by a write or restart.

> **Normative guarantees:** see `docs/ATOMICITY.md`.

---

## 13. Design goals (non-features)

IcyDB optimizes for:
- correctness over convenience
- explicit invariants
- planner/executor clarity
- bounded execution cost
- predictable behavior

It explicitly does not optimize for:
- SQL compatibility
- expressive query syntax
- implicit schema behavior
- automatic derivations

---

## 14. Non-goals

The following are intentional non-features:
- relational joins
- cascading semantics
- expression evaluation
- implicit cross-entity behavior
- hidden execution logic

Any future additions must preserve the intent/plan/execute separation.

---

## 15. Summary

IcyDB is a **small, principled key/value database engine**.

Its constraints are intentional:
- integrity checks prevent invalid persisted state
- planning rigor ensures determinism
- recovery rules preserve atomicity

This document exists to keep those assumptions deliberate, visible, and stable as the
system evolves.
