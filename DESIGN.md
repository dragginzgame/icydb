# icydb — Design Notes

This document describes the **architectural and semantic foundations** of icydb.
It is not a feature roadmap and not a user guide. Its purpose is to make explicit the
**invariants, boundaries, and theoretical assumptions** that the system relies on.

---

## 1. What icydb is (and is not)

icydb is a **typed, embedded key/value database engine**, not an ORM and not a SQL system.

It provides:

* explicit key/value storage with typed entities
* declarative query intent
* explicit planning
* executor-level semantics
* index maintenance
* atomicity and recovery guarantees
* optional write-time integrity checks

It intentionally does **not** provide:

* SQL syntax
* joins, aggregation, or relational algebra
* expression evaluation
* automatic derived fields
* implicit cross-entity behavior

Design decisions are evaluated primarily on **correctness, semantic clarity, and predictability**, not feature breadth.

---

## 2. Core architectural separation

icydb enforces a strict separation between **query intent**, **planning**, and **execution**.

### 2.1 Query intent

Query intent is:

* declarative
* schema-aware
* free of execution strategy

Examples:

* predicates
* ordering intent
* pagination intent
* consistency mode

Intent describes *what rows are desired*, not *how to fetch them*.

---

### 2.2 Logical planning

Logical planning:

* validates intent against the schema
* normalizes predicates
* selects access paths
* enforces plan-level invariants

The output of planning is a **LogicalPlan** that is:

* fully specified
* schema-validated
* free of ambiguity

The planner is the **only layer allowed to reason about schema semantics or field meaning**.

---

### 2.3 Execution

The executor:

* runs a validated LogicalPlan
* performs no schema reasoning beyond plan validation
* enforces execution invariants
* tolerates runtime-only values

The executor must be able to execute *any valid plan deterministically*.

---

## 3. Schema vs runtime values

icydb distinguishes between:

### 3.1 Query-visible schema

The schema defines:

* fields visible to planning
* fields allowed in predicates
* fields allowed in access paths
* fields eligible for indexing
* fields eligible for integrity checks

Schema fields are validated strictly and exhaustively.

---

### 3.2 Runtime-visible values

Entities may carry values that are:

* not part of the schema
* not indexable
* not filterable
* not type-validated

These values:

* may participate in ordering
* may appear in views or diagnostics
* are treated as **opaque / incomparable**

This distinction mirrors the separation between logical schema and physical tuple payloads in traditional database engines.

---

## 4. Unsupported and opaque values

`Value::Unsupported` represents values that:

* must not participate in planning
* must not be indexed
* must not be compared for equality or ordering

The executor:

* carries these values through execution
* treats comparisons involving them as incomparable
* preserves stable ordering when they appear in ORDER BY

This prevents crashes while preserving correctness.

---

## 5. Ordering semantics

Ordering in icydb follows these rules:

* ORDER BY is applied **after filtering**
* Pagination (LIMIT/OFFSET) is applied **after ordering**
* Ordering is **stable**
* Incomparable values are treated as equal for ordering purposes
* Stable ordering preserves input order for incomparable values

These rules ensure deterministic behavior even with partial orders.

---

## 6. `IN` semantics

`IN` expresses **set membership**, and nothing more.

It does not encode:

* execution strategy
* access-path hints
* union vs scan semantics

All execution decisions are planner-controlled.

This preserves logical equivalence across execution strategies.

---

## 7. Unit keys and singleton entities

Entities with `PrimaryKey = ()` represent **singleton existence**, not a per-row primary-key value.

For such entities:

* the primary key has no value domain
* `Value::Unit` represents existence, not a scalar
* executor-side type validation must not treat `Unit` as a mismatch

This matches relational theory for relations with zero attributes while remaining compatible with key/value storage.

---

## 8. Referential integrity (RI)

Referential integrity in icydb is a **write-time validation rule**, not a query feature.
Referential integrity enforcement is schema-driven and may be selectively relaxed,
but reference discovery is always structural and deterministic.

icydb supports:

* existence checks for **strong** references (`Id<T>`, `Option<Id<T>>`, and collections of `Id<T>`)
* validation during save/update **before the commit boundary**

icydb explicitly does **not** support:

* joins or reference traversal
* cascading deletes
* reverse reference tracking
* deferred constraint checks
* reference-based query planning

Referential integrity:

* is enforced only during mutation
* requires no graph traversal
* performs key existence checks for strong references (including collections)
* does not alter execution or query semantics

RI exists to prevent invalid persisted state, not to enable relational querying.

---

## 9. Reference shape constraints

To preserve key/value semantics and atomicity guarantees, icydb constrains where relations may appear.

Strong reference shapes (validated):

* `Id<T>`
* `Option<Id<T>>`
* collections of `Id<T>` (e.g. `List<Id<T>>`, `Set<Id<T>>`)

Weak reference shapes (allowed, not validated):

* `Map<_, Id<T>>`
* nested references inside records, enums, tuples, or collections
* implicit or inferred relations are never introduced automatically

These constraints ensure:

* bounded validation cost
* predictable atomicity
* mechanical commit application
* simple recovery semantics

Future expansion of reference shapes, if any, must preserve these properties and will be considered explicitly.

---

## 10. Error classification

icydb distinguishes between:

* **Corruption**: persisted data is invalid or inconsistent
* **InvariantViolation**: violated internal contract or impossible state
* **Conflict**: legitimate write-time conflict
* **Validation / Plan errors**: invalid intent

Error classification is part of the correctness model and must not change silently across layers.

---

## 11. Recovery and read safety

Reads are guarded by startup recovery, but do not perform marker checks after
startup. A post-startup trap may leave partial state visible to reads until a
write triggers recovery or the process restarts.

icydb enforces:

* recovery-before-read
* authoritative commit markers
* atomic visibility of writes

Any API that allows reads must either:

* perform recovery
* or be explicitly restricted to internal or diagnostic use

Recovery is a correctness mechanism, not an optimization.

---

## 12. Design goals (non-features)

icydb optimizes for:

* semantic correctness
* explicit invariants
* planner/executor clarity
* predictable behavior
* bounded execution cost

It explicitly does **not** optimize for:

* SQL compatibility
* expressive query syntax
* automatic derivations
* implicit cross-entity behavior

---

## 13. Non-goals

* Expression evaluation
* Arbitrary computed fields
* Relational joins
* Cascading semantics
* Implicit schema extension
* Hidden execution behavior

If any of these are added in the future, they must respect the existing intent/plan/execute separation and preserve key/value semantics.

---

## 14. Summary

icydb is designed as a **small, principled key/value database engine**.

Integrity checks, planning rigor, and recovery guarantees exist to:

* prevent invalid persisted state
* preserve atomicity
* ensure deterministic execution

Any behavior that appears permissive or constrained is **intentional**, not accidental.

This document exists to ensure those constraints remain explicit, deliberate, and stable.
