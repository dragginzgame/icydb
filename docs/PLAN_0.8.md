
## Near-Term Roadmap (0.8.x)

The 0.8 series focuses on **completing core database correctness features**
that are expected of a production-grade typed datastore, while **preserving
the 0.7.x contract**.

No multi-entity transactions or implicit behavior are introduced.

---

### 1. Complete Collection Semantics (List / Set / Map)

IcyDB collection semantics in 0.8 are constrained to three schema-level collection types:
`List` (`Vec`), `Set` (`BTreeSet`), and `Map` (`BTreeMap`).

**Goals**

* Keep list/set/map semantics explicit and non-overloaded
* Preserve deterministic ordering and patch application
* Explicit failure on conflicting key mutations

**Outcomes**

* List semantics remain index-based
* Set semantics remain value-based
* Map semantics remain key-based
* Identical inputs produce identical mutation results

**Non-Goals**

* Relational joins
* Query-time map operators
* New collection kinds beyond `Vec` / `BTreeSet` / `BTreeMap`

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
* Deterministic iteration over a total canonical ordering
* Strict forward-only continuation
* Cursor invalidation when canonical query semantics change
* Live-state continuation semantics (no snapshot isolation)

**Outcomes**

* Safe API pagination
* Reliable batch processing for fixed query shapes under stable ordered keys
* Predictable query behavior with signature-bound continuation tokens
* Structural execution guards that verify post-access phase application (filter, order, then pagination) so planner/executor contract regressions are caught early.
* Explicitly documented drift behavior under concurrent writes that reorder rows between page requests

**Non-Goals**

* Snapshot isolation
* Transactional consistency across pages

---

### 4. Strong Referential Integrity — Delete-Time Validation (Post-0.8.0)

IcyDB will add **delete-time referential integrity enforcement** for **strong relations**
in a later **0.8.x** milestone.
This is explicitly **not** part of `0.8.0`.

**Goals**

* Reject deletes that would leave dangling strong references
* Perform validation deterministically before commit
* Preserve explicit weak-relation behavior (no existence validation)
* Keep semantics validation-only (no implicit cascades)

**Outcomes**

* Strong relation correctness holds for both save and delete paths
* Dangling-reference creation is blocked by executor validation
* RI behavior remains explicit and schema-driven

**Non-Goals**

* Cascading deletes
* Implicit graph traversal or relational query behavior

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
and adds stable pagination in `0.8.0`.
Delete-time strong-relation RI is targeted for a later `0.8.x` release,
without expanding the transactional contract.
