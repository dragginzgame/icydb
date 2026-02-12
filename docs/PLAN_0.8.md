
## Near-Term Roadmap (0.8.x)

The 0.8 series focuses on **completing core database correctness features**
that are expected of a production-grade typed datastore, while **preserving
the 0.7.x contract**.

No multi-entity transactions or implicit behavior are introduced.

---

### 1. Complete Collection Semantics (Maps / Keyed Collections)

IcyDB will complete support for **schema-declared keyed collections**.

**Goals**

* First-class map / keyed-list semantics
* Key-based identity for collection entries
* Deterministic ordering and patch application
* Explicit failure on conflicting key mutations

**Outcomes**

* Lists are no longer overloaded to behave like maps
* Patch semantics are key-driven, not index-driven
* Identical inputs produce identical mutation results

**Non-Goals**

* Relational joins
* Query-time map operators
* Implicit conversions from lists

---

### 2. Enforced Uniqueness Constraints

IcyDB will enforce **schema-declared uniqueness constraints** at save time.

**Goals**

* Unique indexes are enforced, not advisory
* Inserts and updates fail deterministically on violation
* Clear, explicit error reporting

**Outcomes**

* Correctness for identity and deduplication use-cases
* Removal of application-level uniqueness guards

**Non-Goals**

* Deferred constraint checking
* Cross-entity or transactional uniqueness

---

### 3. Stable Pagination and Cursor Semantics

IcyDB will provide **stable, deterministic pagination** over ordered queries.

**Goals**

* Cursor-based pagination tied to explicit ordering
* No skipped or duplicated rows across pages
* Deterministic iteration guarantees

**Outcomes**

* Safe API pagination
* Reliable batch processing
* Predictable query behavior
* Structural execution guards that verify post-access phase application (filter, order, then pagination) so planner/executor contract regressions are caught early.

**Non-Goals**

* Snapshot isolation
* Transactional consistency across pages

---

## Explicit Non-Goals (0.8.x)

The following remain out of scope:

* Multi-entity transactions
* Cascading deletes
* Delete-side referential integrity enforcement
* Partial update semantics
* Authorization or capability-based identity models
* Relational query planning

---

## Summary

0.8.x is a **structural correctness release**.

It completes collection semantics, enforces schema-declared constraints,
and adds stable pagination — all without expanding the atomicity or
transactional contract.
