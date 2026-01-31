
# IcyDB Atomicity Model (Explicit, Single-Message)

This document defines the **atomicity, write-safety, and invariant contract** for
IcyDB mutations executed within a **single Internet Computer update call**.

It is a **normative constraint on future changes**, not an implementation plan.

**Scope:**
This model applies to all IcyDB mutation executors in the current architecture.
It assumes no `await`, yield, or re-entrancy during mutation execution.

---

## 1. Core Principle

**Atomicity is enforced by IcyDB itself, not delegated to IC trap semantics.**

Within a single mutation operation:

> Either all intended durable mutations are committed as a unit,
> or no durable mutation is made visible.

This guarantee must hold **even if execution does not trap**.

---

## 2. Definitions

### Atomic mutation

A mutation whose effects are applied entirely or not at all, as defined by
IcyDB’s own commit discipline.

### System recovery step

A **system recovery step** is a synchronous, unconditional operation that restores
global database invariants (e.g. completing or rolling back a previously started
commit) before any new mutation is planned or validated.

System recovery:
* Executes before mutation pre-commit begins
* Leaves the database in a fully consistent state
* Is not part of the current mutation’s atomicity scope
* Is not observable by reads as partial state
* Is not read-time recovery

### Commit boundary

The explicit boundary after which durable state mutation occurs.
This boundary is **structural and enforced**, not implicit.

### Apply phase

The phase in which prevalidated, infallible operations are mechanically applied
to durable state.

### Commit marker

A persisted representation of intended mutations used to enforce atomicity and
support deterministic application. Commit markers are **semantically meaningful**
and must be correct.

---

## 3. Commit Discipline (Required)

IcyDB enforces atomicity via a **two-phase discipline** within a single update
call.

Before any mutation’s pre-commit phase begins, the system may perform a mandatory
**system recovery step** to restore global invariants from prior incomplete commits.

This recovery step is conceptually separate from the current mutation and must
complete successfully before mutation planning or validation begins.

### Phase 1 — Pre-commit (Fallible)

All fallible work **must complete before any durable mutation**, including:

* validation
* decoding
* schema checks
* query planning
* index derivation
* uniqueness resolution
* mutation planning

If any step fails, **no durable state is mutated**.

---

### Phase 2 — Apply (Infallible)

After the commit boundary:

* All durable mutations are applied mechanically
* No fallible operations are permitted
* No validation, decoding, or planning occurs
* Any invariant violation is a **logic error** (bug), not a recoverable condition

The apply phase must be correct by construction.

---

## 4. Commit Markers

Commit markers are **authoritative**, not diagnostic.

### Required properties

* Marker content fully describes the intended durable mutations
* Marker is validated completely before application
* Marker application is deterministic and infallible
* Marker application alone is sufficient to produce a correct final state

### Visibility rules

* Markers may be persisted during execution
* Markers must not be observable as committed state
* Markers must not be required for read-time logic in the current model

---

## 5. Executor Guarantees

### Save (single entity)

* Fully atomic
* All fallible work occurs pre-commit
* Apply phase replays validated marker ops only

### Delete (single entity or planner-based)

* Fully atomic
* Scan and planning are pre-commit
* Apply phase performs only raw removals and index updates

### Upsert (unique index)

* Fully atomic
* Implemented as a validated save with uniqueness resolution

---

## 6. Explicit Non-Goals (0.6 Contract)

The following are **explicitly out of scope**:

* Multi-message commits
* Async or awaited mutation paths
* Forward recovery after process crash
* Read-time recovery, read gating, or lazy repair of partial commits
* Atomicity across independent mutation calls
* Distributed or cross-canister transactions

Introducing any of these **requires a new atomicity specification**.

---

## 7. Invariants (Must Hold in Release Builds)

The following invariants are **mandatory and non-negotiable**:

* No durable mutation before pre-commit completes successfully
* No fallible work after the commit boundary
* Apply phase must be infallible by construction
* Commit marker application must not depend on IC trap rollback
* Executors must not rely on traps for correctness
* Mutation correctness must not depend on recovery occurring after the commit
  boundary or at read time.
* System recovery, if present, must complete before mutation pre-commit and must
  not be interleaved with mutation planning or apply phases.
* No `await`, yield, or re-entrancy during mutation execution

Violating any invariant is a **bug**, not an acceptable failure mode.

---

## 8. Consequences

* Atomicity is an **IcyDB guarantee**, independent of IC rollback semantics
* Traps are treated as catastrophic failures, not control flow
* Partial state visibility is structurally impossible
* Release builds enforce invariants explicitly (no `debug_assert!` reliance)

---

## Design Note (Non-Binding)

If IcyDB later introduces:

* async mutation entrypoints
* multi-message commits
* durable recovery protocols

Then a new atomicity model must define:

* recovery semantics
* read behavior during in-flight commits
* ordering and visibility guarantees

System recovery, if implemented, is expected to run synchronously at mutation
entrypoints or initialization boundaries and is not a substitute for atomic
apply-phase correctness.

Until then, this document is authoritative.

---

## Why This Replaces the Old Model

* Removes implicit reliance on IC traps
* Matches the current executor + commit-marker architecture
* Makes invariants explicit and enforceable
* Prevents silent regression via async or refactors
* Sets a clean, honest contract for 0.6+
