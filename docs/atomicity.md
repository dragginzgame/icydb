
# IcyDB Atomicity Model (Explicit, Single-Message)

This document defines the **atomicity, write-safety, and invariant contract** for
IcyDB mutations executed within a **single Internet Computer update call**.

It is a **normative constraint on future changes**, not an implementation plan.

**Scope:**
This model applies to all IcyDB mutation executors in the current architecture.
It assumes no `await`, yield, or re-entrancy during mutation execution.

---

## 1. Core Principle

**Atomicity is enforced by IcyDB’s commit discipline; IC trap rollback is not
relied upon for correctness.**

Within a single mutation operation:

> Either all intended durable mutations are committed as a unit,
> or the operation returns an error and no partial durable mutation is made visible.

IC traps may still occur, but traps are treated as catastrophic failures, not a
correctness mechanism. This guarantee must hold **even if execution does not trap**.

---

## 2. Definitions

### Atomic mutation

A mutation whose effects are applied entirely or not at all, as defined by
IcyDB’s own commit discipline.

### System recovery step

A **system recovery step** is a synchronous, unconditional operation that restores
global database invariants (e.g. completing or rolling back a previously started
commit) before operations proceed.

System recovery:
* Executes at startup before the first read or mutation pre-commit begins
* Leaves the database in a fully consistent state
* Is not part of the current mutation’s atomicity scope
* Is not observable by reads as partial state
* Is idempotent, bounded, and deterministic; if it cannot complete, the entrypoint
  must fail and must not proceed to reads or mutation planning

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

Before the first read or mutation’s pre-commit phase begins, the system performs
a mandatory **system recovery step** to restore global invariants from prior
incomplete commits. Write entrypoints also perform a cheap marker check and
replay recovery if a marker is present.

This recovery step is conceptually separate from the current mutation and must
complete successfully before any read execution, planning, or validation begins.

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
* Markers must not be observable as committed application state
* Markers are applied during the apply phase after they are persisted
* The system recovery step handles markers left behind by interrupted commits
* Read entrypoints perform startup recovery before accessing durable stores
* Write entrypoints perform a marker check and recovery before accessing durable stores;
  reads must not branch on marker presence outside recovery
* Read entrypoints do not perform marker checks after startup; a post-startup
  trap may leave partial state visible to reads until a write triggers recovery
  or the process restarts

---

## 5. Executor Guarantees

### Save (single entity)

* Fully atomic
* All fallible work occurs pre-commit
* Apply phase replays validated marker ops only

### Batch writes (insert_many / update_many / replace_many)

Batch write helpers are **fail-fast and non-atomic**. Partial successes may be
committed before an error is returned, and there is no transactional rollback
across the batch. These helpers exist for convenience, not for multi-entity
atomicity.

### Delete (single entity or planner-based)

* Fully atomic
* Scan and planning are pre-commit
* Apply phase performs only raw removals and index updates

### Upsert (unique index)

* Fully atomic
* Implemented as a validated save with uniqueness resolution

---

## 6. Explicit Non-Goals (0.7 Contract)

The following are **explicitly out of scope**:

* Multi-message commits
* Async or awaited mutation paths
* Forward recovery after process crash
* Lazy or deferred recovery during read execution (reads do not perform marker checks)
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
* All mutation entrypoints must perform write-side recovery (marker check + replay) before pre-commit
* Mutation correctness must not depend on recovery occurring after the commit
  boundary.
* Startup recovery must complete before any read pre-commit, and write-side recovery
  must complete before mutation pre-commit; recovery must not be interleaved with
  mutation planning or apply phases.
* No `await`, yield, or re-entrancy during mutation execution

Violating any invariant is a **bug**, not an acceptable failure mode.

---

## 8. Consequences

* Atomicity is an **IcyDB guarantee**, independent of IC rollback semantics
* Traps are treated as catastrophic failures, not control flow
* Partial state visibility is prevented for writes; reads may observe partial state
  after a post-startup trap until a write triggers recovery or the process restarts
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

System recovery is expected to run synchronously at startup (before the first
read or mutation) and is not a substitute for atomic apply-phase correctness.
Write entrypoints also perform a cheap marker check and replay if needed.

Until then, this document is authoritative.

---

## Why This Replaces the Old Model

* Removes implicit reliance on IC traps
* Matches the current executor + commit-marker architecture
* Makes invariants explicit and enforceable
* Prevents silent regression via async or refactors
* Sets a clean, honest contract for 0.7+
