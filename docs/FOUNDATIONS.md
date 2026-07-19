# IcyDB Foundations

This document describes the **conceptual foundations and mental model** of IcyDB.

It explains *why* the system is structured the way it is, what assumptions it makes,
and which constraints guide design decisions.

This document is **interpretive, not normative**.

> Where this document overlaps with formal contracts
> (atomicity, referential integrity, query semantics, identity and PK invariants),
> those contracts take precedence.

Normative specifications live under [`contracts/`](contracts/), including:

- [query semantics](contracts/QUERY_CONTRACT.md),
  [read admission](contracts/READ_ADMISSION.md),
  [cursor semantics](contracts/CURSOR.md), and
  [SQL scope](contracts/SQL_SUBSET.md);
- [write admission](contracts/WRITE_ADMISSION.md),
  [atomicity](contracts/ATOMICITY.md), and
  [transaction semantics](contracts/TRANSACTION_SEMANTICS.md);
- [durability](contracts/DURABILITY.md),
  [persisted-format policy](contracts/PERSISTED_FORMAT_POLICY.md), and the
  [persisted-format inventory](contracts/PERSISTED_FORMAT_INVENTORY.md); and
- [referential integrity](contracts/REF_INTEGRITY.md),
  [resource bounds](contracts/RESOURCE_MODEL.md), and
  [identity](contracts/IDENTITY_CONTRACT.md).

Architecture terminology lives in [TERMINOLOGY.md](architecture/TERMINOLOGY.md)
and [NAMING.md](architecture/NAMING.md).

---

## 0. Execution Environment Assumptions

IcyDB is designed for deterministic, single-threaded execution environments.

In particular, it assumes:

* Wasm-based execution
* Single-threaded actors
* Deterministic replay
* Explicit stable-memory persistence
* No shared memory across instances
* No background threads or asynchronous maintenance tasks
* Message-bound execution with bounded instruction limits

These assumptions influence:

* The strict separation of intent → plan → execute
* The absence of relational joins
* The requirement for bounded validation cost
* The use of explicit commit markers
* The avoidance of implicit traversal or cascading semantics
* The deterministic recovery model

Where substrate constraints conflict with traditional database expectations,
substrate constraints take precedence.

---

## 1. What IcyDB is (and is not)

IcyDB is a **typed, embedded key/value database engine** with a bounded
single-entity analytical query layer.

It is not:
- an ORM
- a general-purpose SQL engine
- a multi-entity relational algebra system

IcyDB provides:
- typed entities with explicit identity
- declarative query intent
- deterministic planning
- mechanical execution
- index maintenance
- explicit atomicity and recovery rules
- optional write-time integrity checks
- reduced SQL parsing and execution for admitted single-entity shapes
- bounded scalar expressions, grouped queries, and aggregates

IcyDB intentionally does **not** provide:
- broad SQL compatibility
- joins or relational traversal
- unbounded expression evaluation
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
- enforces admitted query semantics and schema-level constraints

The result is a **LogicalPlan** that is:
- fully specified
- deterministic
- schema-validated
- free of ambiguity
- independent of a chosen physical access path

The planner is the **only layer** allowed to interpret schema semantics.

---

### 2.3 Access and route planning

Physical planning:

- derives access capabilities from the logical plan and accepted catalog;
- selects one deterministic access plan;
- derives an executor route and its eligibility facts; and
- carries explicit fallback or downgrade reasons.

These phases choose *how* to satisfy already-validated semantics. They do not
reinterpret the query or upgrade its admitted capabilities.

---

### 2.4 Execution

The executor:
- executes a validated plan
- consumes accepted authority carried by the plan rather than reconstructing
  schema semantics
- enforces execution-time invariants
- operates deterministically

Execution remains fallible for bounded persisted decoding, corruption checks,
resource limits, and revalidation of planner-proposed route facts. Those checks
must not become a second schema interpreter or an authority upgrade.


---

## 3. Accepted fields and query capabilities

Every persisted top-level row slot belongs to accepted schema. Accepted schema
also declares which query operations each exact field kind supports.

---

### 3.1 Accepted field authority

Accepted schema defines:

- every persisted field and physical slot;
- the exact current persisted field kind and absence policy;
- fields admitted in predicates and access paths;
- fields eligible for comparison, ordering, grouping, or indexing; and
- fields participating in integrity checks.

Generated models may propose or reconcile these facts, but they are not runtime
fallback authority.

---

### 3.2 Accepted but non-queryable fields

An accepted field may deliberately have no predicate, ordering, grouping, or
index capability. Its value is still declared by accepted schema, decoded under
its exact accepted kind, and preserved by normal row admission.

Unsupported operations on such a field fail closed. IcyDB does not preserve an
out-of-schema opaque payload as a compatibility fallback.

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

### 3.5 Write Strictness

IcyDB has no non-strict entity or table mode. Accepted schema owns the exact
persisted kind and absence behavior for every field, and every supported row
mutation ingress must satisfy that authority before commit.

Trusted/admin exposure changes who may invoke a write; it does not bypass row
admission. Declared relations participate in the complete referential-integrity
contract.

> **Normative definition:** see `docs/contracts/WRITE_ADMISSION.md`.

---

## 4. Unsupported values and operations

IcyDB has no maintained `Value::Unsupported` catch-all representation.

- A query operation on a field without the required accepted capability is
  rejected during admission or planning.
- An incoming value incompatible with the accepted field kind is rejected
  during write admission.
- A persisted value that fails bounded current-form decoding is corruption.

Unsupported values are not silently preserved, compared as equal, or routed
through a legacy decoder.

---

## 5. Ordering semantics

Ordering in IcyDB follows these principles:

- filtering occurs before ordering
- ordering occurs before pagination
- admitted ordering uses only accepted orderable domains
- non-orderable fields fail closed rather than becoming incomparable values
- explicit `ORDER BY` owns direction and null ordering
- pagination uses the complete deterministic order, including its canonical
  primary-key tie-breaker
- without explicit `ORDER BY`, result order is not a public guarantee, although
  one unchanged plan over unchanged state remains deterministic

These rules keep ordering deterministic without inventing a partial-order
fallback.

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
- always enforced for declared relations
- bounded
- non-relational

Only references explicitly declared in the schema participate in enforcement;
ordinary key-typed fields carry no target-existence or delete-safety guarantee.

> **Normative definition:** see `docs/contracts/REF_INTEGRITY.md`.

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

> **Normative rules:** see `docs/contracts/REF_INTEGRITY.md`.

---

## 11. Error classification (conceptual)

IcyDB distinguishes between broad classes of failure:
- corruption
- invariant violation
- conflict
- validation / plan errors

Classification is part of the correctness model.

> **Normative definitions:** see `docs/contracts/QUERY_CONTRACT.md` and executor error docs.

---

## 12. Recovery and read safety (conceptual)

IcyDB relies on:
- explicit commit markers
- deterministic recovery
- recovery-before-read

Guarded read and write entrypoints perform the required marker check before
operation-specific execution. If recovery cannot complete, the guarded
operation fails rather than proceeding on partial state.

> **Normative guarantees:** see `docs/contracts/ATOMICITY.md` and
> `docs/contracts/DURABILITY.md`.

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
- broad query syntax
- implicit schema behavior
- automatic derivations

---

## 14. Non-goals

The following are intentional non-features:
- relational joins
- cascading semantics
- unbounded expression evaluation
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
