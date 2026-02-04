# Referential Integrity (RI) — v2 Contract

## Status (as of 0.6.x)

Save-time referential integrity is currently **disabled**. References are identity-only and may be dangling; existence is checked at point of use.

Key points:
* `Ref<T>` is **identity only**
* There is **no save-time RI**
* Locality is enforced at `DbSession<C>` via `E: EntityKind<Canister = C>`
* Strong/weak classification is removed pending a future two-pass schema

This document defines the **referential integrity model** for IcyDB.

It is **normative**: it specifies what guarantees exist, what is explicitly not guaranteed, and where future extensions may occur. It is not a feature roadmap.

This document applies to **IcyDB 0.6**.

---

## 1. Scope and intent

Referential integrity (RI) in IcyDB is a **save-time validation rule**, not a query feature and not a relational system.

RI exists to ensure that certain references declared in the schema point to existing entities at the time of mutation, **without introducing relational semantics** such as joins, cascades, or reverse lookups.

IcyDB remains a **typed key/value database** with explicit invariants.

---

## 2. What a reference is

A reference is a typed pointer to another entity’s primary key:

```
Ref<T>
```

A reference:

* identifies an entity by key
* does not imply ownership
* does not imply lifecycle coupling
* does not imply query-time semantics

References are **not joins** and do not participate in query planning.

---

## 3. Reference discovery (structural)

### 3.1 Structural discovery rule

All references are **structurally discoverable**.

That is:

> Any `Ref<T>` present anywhere in an entity’s value graph may be discovered deterministically by traversing the value structure.

References may appear in:

* entity fields
* records
* enums (variant payloads)
* tuples
* newtypes
* lists, sets, and maps

Discovery is **structural**, not semantic.
Discovery does **not** imply enforcement.

---

## 4. Strong vs weak references

IcyDB distinguishes between **strong** and **weak** references.

This distinction controls **validation**, not representation.

---

### 4.1 Strong references (validated)

Strong references are **validated at save time**.

In IcyDB 0.6, the following references are strong by default:

* Direct entity fields of type:

  * `Ref<T>`
  * `Option<Ref<T>>`
* With cardinality:

  * `One`
  * `Opt`

Strong reference rules:

* Validation occurs **pre-commit**
* Validation checks that the referenced entity exists
* Validation failure aborts the mutation
* No durable state is mutated on failure

Strong references are **bounded and deterministic**.

---

### 4.2 Weak references (allowed, not validated)

Weak references are **not validated for existence**.

In IcyDB 0.6, the following references are weak by default:

* References nested inside:

  * records
  * enums
  * tuples
  * newtypes
* References inside collections:

  * `Vec<Ref<T>>`
  * `Set<Ref<T>>`
  * `Map<_, Ref<T>>`
  * any nested combination thereof

Weak reference rules:

* They are type-checked and serialized normally
* They are preserved through views and updates
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

Weak references are allowed but not validated; no recursive discovery is
performed for enforcement in 0.6.

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

They are **not** corruption and **not** invariant violations.

Schema-level disallowed constructs (if any) must fail **at schema validation time**, not at runtime.

Runtime invariant violations must never be used to signal unsupported reference shapes.

---

## 8. Explicit non-goals (0.6)

The following are **out of scope** for IcyDB 0.6:

* Strong reference collections
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

* Schema-level `weak` / `strong` annotations
* Opt-in strong reference collections with bounded validation
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
