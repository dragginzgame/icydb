
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

**Non-Goals**

* Snapshot isolation
* Transactional consistency across pages

---

### 4. Strong Referential Integrity — Delete-Time Validation (Constrained)

IcyDB will complete **strong referential integrity enforcement** by adding
**delete-time validation**.

**Goals**

* Deletes that would violate a strong relation are rejected
* Enforcement is symmetric with insert/update validation
* Checks are deterministic and fail-fast

**Characteristics**

* Implemented via internal reverse-lookup metadata
* Validation only; no additional mutations are performed

**Explicit Non-Goals**

* Cascading deletes
* Automatic cleanup of dependent entities
* Weak-relation enforcement
* Cross-entity transactional behavior

---

## Explicit Non-Goals (0.8.x)

The following remain out of scope:

* Multi-entity transactions
* Cascading deletes
* Partial update semantics
* Authorization or capability-based identity models
* Relational query planning

---

## Summary

0.8.x is a **structural correctness release**.

It completes collection semantics, enforces schema-declared constraints,
adds stable pagination, and closes remaining strong-RI gaps — all without
expanding the atomicity or transactional contract.

