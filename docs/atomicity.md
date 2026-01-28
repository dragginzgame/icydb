
# IcyDB Atomicity Model (Single-Message)

This document defines the atomicity and write-safety contract for IcyDB under
**single-message update execution** on the Internet Computer.

It is a **constraint on future changes**, not an implementation plan.

**Assumption:** All mutation entrypoints complete within a single IC update call
and perform no `await`, yield, or re-entrancy after any durable state mutation.

---

## 1. Definitions

* **Single-message atomicity**
  Within a single update call, either all intended writes become visible at the
  end of the call, or the call traps and **no user-visible mutation is committed**.
  This relies solely on IC rollback semantics.

* **Commit window**
  The phase of a mutation after all fallible work has completed and durable
  state mutation begins.

* **Best-effort operations**
  Multi-entity batch helpers may partially succeed; earlier successful writes
  are retained if later items fail.

---

## 2. Commit Discipline (Marker-Optional)

IcyDB enforces atomicity through **execution discipline**, not multi-phase
recovery.

If a commit marker is used, it is **diagnostic only** and must not be relied on
for correctness.

### Required discipline

* All fallible work (validation, decoding, planning, index derivation) must
  complete **before** the commit window.
* After the commit window begins:

  * no fallible operations may be performed
  * any invariant violation must trap
* No `await`, yield, or re-entrancy is permitted once durable mutation begins.

### Marker rules (if present)

* Marker must be empty before mutation begins.
* Marker may be written at the start of the commit window.
* Marker must be cleared before returning from the update call.
* Marker must never be observable across messages.

---

## 3. Executor Guarantees

* **Save (single entity)**
  Atomic within a single update call. All validation and planning occurs before
  the commit window.

* **Delete (single entity or planner-based)**
  Atomic within a single update call. Scan and planning are completed before
  mutation; apply phase is infallible or traps.

* **Upsert (single entity via unique index)**
  Atomic; implemented as a Save.

### Best-effort by design

* Batch helpers (`insert_many`, `update_many`, `replace_many`) are **not atomic
  as a group**. Partial success is preserved if later items fail.

---

## 4. Explicit Non-Goals

* No multi-message commit or forward recovery protocol.
* No durability of partial progress across traps.
* No batch atomicity across multiple entities.
* No read-time recovery or gating logic.

If any of these are introduced in the future, this document must be revised.

---

## 5. Invariants (Must Never Be Broken)

* No fallible work after the commit window begins.
* No `await`, yield, or re-entrancy after any durable mutation.
* All durable mutations for an operation occur within a single update call.
* Executors must not rely on commit markers for correctness.
* Mutation entrypoints must enforce the commit discipline before writes.

---

## 6. Consequences

* Atomicity is guaranteed by IC execution semantics, not recovery logic.
* Traps leave no partially committed state.
* Commit markers, if present, are informational and must not affect behavior.
* Introducing async or multi-message mutation **invalidates this contract**.

---

## Design Note (Non-Binding)

If IcyDB ever introduces multi-message commits or awaits in mutation entrypoints,
a new atomicity model must be specified, including:

* recovery semantics
* read behavior during in-flight commits
* index/data ordering guarantees

Until then, this model is authoritative.

---

## Why this is the right replacement

* **Internally consistent** with “single message only”
* Does not claim forward recovery you do not support
* Keeps your “no fallible work after commit” discipline intact
* Makes future async work an explicit contract break, not a silent regression
* Aligns cleanly with your executor/query-facade design philosophy

