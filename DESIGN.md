# icydb — Design Notes

This document describes the **architectural and semantic foundations** of icydb.
It is not a feature roadmap and not a user guide. Its purpose is to make explicit the
**invariants, boundaries, and theoretical assumptions** that the system relies on.

---

## 1. What icydb is (and is not)

icydb is a **typed, embedded database engine**, not an ORM and not a SQL system.

It provides:

* declarative query intent
* explicit planning
* executor-level semantics
* index maintenance
* recovery and atomicity guarantees

It intentionally does **not** provide:

* SQL syntax
* joins or aggregation
* expression evaluation
* automatic derived fields

Design decisions are evaluated primarily on **correctness and semantic clarity**, not feature breadth.

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

The planner is the **only layer allowed to reason about schema semantics**.

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

Schema fields are validated strictly.

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

This distinction is intentional and mirrors the separation between
logical schema and physical tuple payloads in traditional database engines.

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

Entities with `PrimaryKey = ()` represent **singleton existence**, not identity.

For such entities:

* the primary key has no value domain
* `Value::Unit` represents existence, not a scalar
* executor-side type validation must not treat `Unit` as a mismatch

This matches relational theory for relations with zero attributes.

---

## 8. Error classification

icydb distinguishes between:

* **Corruption**: persisted data is invalid or inconsistent
* **InvariantViolation**: logical impossibility or violated contract
* **Conflict**: legitimate write-time conflict
* **Validation / Plan errors**: invalid intent

Error classification is part of the correctness model and must not change silently across layers.

---

## 9. Recovery and read safety

Reads must never observe partial commit state.

icydb enforces:

* recovery-before-read
* commit marker validation
* atomic visibility of writes

Any API that allows reads must either:

* perform recovery
* or be restricted to internal use

---

## 10. Design goals (non-features)

icydb optimizes for:

* semantic correctness
* explicit invariants
* planner/executor clarity
* predictable behavior

It explicitly does **not** optimize for:

* SQL compatibility
* expressive query syntax
* automatic derivations
* implicit behavior

---

## 11. Non-goals

* Expression evaluation
* Arbitrary computed fields
* User-defined execution hints
* Implicit schema extension

If these are added in the future, they must respect the existing intent/plan/execute separation.

---

## 12. Summary

icydb is designed as a **small, principled database engine**.

Many behaviors that appear “permissive” at runtime are:

* intentionally constrained
* planner-isolated
* execution-safe

This document exists to ensure those behaviors remain **intentional**, not accidental.

