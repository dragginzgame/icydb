# IcyDB Atomicity Model (Explicit, Single-Message)

This document defines the **atomicity, write-safety, and invariant contract** for
IcyDB mutations executed within a **single Internet Computer update call**.
The operator-facing durability summary lives in
`docs/contracts/DURABILITY.md`.
The row-content and mutation-ingress requirements that must complete before the
commit boundary live in `docs/contracts/WRITE_ADMISSION.md`.

It is the normative runtime contract, not an implementation plan.

**Scope:**
This model applies to all IcyDB mutation executors in the current architecture.
It assumes no `await`, yield, or re-entrancy during mutation execution.

This is not a Postgres-style transaction model. It does not make an entire
canister update method transactional, and it does not roll back prior state
changes when application code returns `Err`. A returned `Err` is just a response
value; only a trap/panic participates in IC message rollback semantics, and
IcyDB must not rely on that rollback for database correctness.

Internet Computer update calls for one canister are serialized. If two clients
submit updates concurrently, the canister observes them one at a time. For
example, if only one mint budget remains, the first update that consumes it can
commit and the later update can observe the exhausted budget and return an
error.

---

## 1. Core Principle

**Atomicity is enforced by IcyDB’s commit discipline; IC trap rollback is not
relied upon for correctness.**

Within a single mutation operation:

> Either all intended durable mutations are committed as a unit,
> or the operation returns an error and no partial durable mutation is made visible.

This statement is scoped to that one IcyDB mutation operation. It does not undo
earlier successful IcyDB operations or unrelated application state changes in
the same canister update method.

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

* Executes at startup before the first guarded operation and is re-enforced by
  guarded operation boundaries through a fast persisted-marker check
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

A persisted representation of current journal batches used to enforce
atomicity and support deterministic recovery. Commit markers are
**semantically meaningful** and must be correct.

---

## 3. Commit Discipline (Required)

IcyDB enforces atomicity via a **two-phase discipline** within a single update
call.

Before the first read or mutation’s pre-commit phase begins, the system performs
a mandatory **system recovery step** to restore global invariants from prior
incomplete commits. Guarded read and write entrypoints both perform a cheap
marker check and journal publication/fold recovery if a marker is present.

This recovery step is conceptually separate from the current mutation and must
complete successfully before any read execution, planning, or validation begins.

### Phase 1 — Pre-commit (Fallible)

All fallible work **must complete before any durable mutation**, including:

* accepted-schema write admission
* validation
* decoding
* schema checks
* query planning
* index derivation
* uniqueness resolution
* mutation planning

If any step fails, **no durable state is mutated**.

`docs/contracts/WRITE_ADMISSION.md` defines the exact field, absence, key,
relation, and supported-ingress guarantees included in write admission.

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

* Marker content fully describes the intended durable journal batches
* Marker is validated completely before application
* Marker application is deterministic and infallible
* Marker publication plus the current journal fold is sufficient to produce a
  correct final state

### Visibility rules

* Markers may be persisted during execution
* Markers must not be observable as committed application state
* Marker-bound journal batches are appended during the apply phase after the
  marker is persisted
* The system recovery step handles markers left behind by interrupted commits
* Read entrypoints enforce recovery before accessing durable stores
* Write entrypoints perform a marker check and recovery before accessing durable
  stores; reads must not branch on marker presence outside recovery
* Guarded reads and writes both perform marker checks at entry; if a marker is
  present, recovery publishes and folds its journal batches before read or
  mutation execution proceeds

---

## 5. Executor Guarantees

### Save (single entity)

* Fully atomic
* Every row after-image satisfies `docs/contracts/WRITE_ADMISSION.md`
* All fallible admission and preparation work occurs pre-commit
* Apply phase appends marker-bound journal batches and applies prevalidated row
  operations only

### Batch writes (single entity, explicit semantics)

IcyDB now exposes two explicit batch lanes:

* `insert_many_atomic` / `update_many_atomic` / `replace_many_atomic` are
  atomic within a single entity type. If any item fails pre-commit
  validation, the whole batch fails and no row from that batch is persisted.
  This is not multi-entity transaction support.
* `insert_many_non_atomic` / `update_many_non_atomic` /
  `replace_many_non_atomic` are fail-fast and non-atomic. Partial successes
  may commit before an error is returned.

Detailed batch-lane behavior and edge cases are defined in
`docs/contracts/TRANSACTION_SEMANTICS.md`; per-item row strictness remains
governed by `docs/contracts/WRITE_ADMISSION.md` in both lanes.

### Delete (single entity or planner-based)

* Fully atomic
* Scan and planning are pre-commit
* Apply phase performs only raw removals and index updates

---

## Startup Recovery Window (Operational Guidance)

At process startup, there is a bounded **recovery window** before the first
successful guarded read/write call. During this window, a leftover commit marker
from a previous trap may still represent partially applied durable state.

Unsafe during this window:
* Direct store/index access that bypasses `ensure_recovered` /
  `ensure_recovered_for_write`
* Any tooling or helper that reads durable rows without going through guarded
  query/session entrypoints

Fully consistent point:
* The system is fully consistent immediately after the first guarded recovery
  pass completes successfully (startup read-side recovery or write-side marker
  check plus journal publication/fold).

---

## 6. Explicit Non-Goals

The following are **explicitly out of scope**:

* Multi-message commits
* Async or awaited mutation paths
* Forward recovery after process crash
* Lazy or deferred recovery interleaved with read execution logic; recovery
  runs at guarded entry boundaries before read execution
* Atomicity across independent mutation calls
* Distributed or cross-canister transactions

Introducing any of these **requires a new atomicity specification**.

---

## 7. Invariants (Must Hold in Release Builds)

The following invariants are **mandatory and non-negotiable**:

* No durable mutation before pre-commit completes successfully
* No commit marker publication before applicable write admission and commit
  preparation complete
* No fallible work after the commit boundary
* Apply phase must be infallible by construction
* Commit marker application must not depend on IC trap rollback
* Executors must not rely on traps for correctness
* All mutation entrypoints must perform write-side recovery (marker check plus
  journal publication/fold) before pre-commit
* Mutation correctness must not depend on recovery occurring after the commit
  boundary
* Startup recovery must complete before read execution, and read/write
  entrypoint marker checks must complete before operation-specific execution;
  recovery must not be interleaved with mutation planning or apply phases
* No `await`, yield, or re-entrancy during mutation execution

Violating any invariant is a **bug**, not an acceptable failure mode.

---

## 8. Consequences

* Atomicity is an **IcyDB guarantee**, independent of IC rollback semantics
* Traps are treated as catastrophic failures, not control flow
* Partial state visibility is prevented for writes and for guarded reads.
  Direct durable reads that bypass guarded entrypoints are out of contract and
  may observe transient partial state.
* Release builds enforce invariants explicitly (no `debug_assert!` reliance)

---

System recovery is expected to run synchronously at startup (before the first
read or mutation) and is not a substitute for atomic apply-phase correctness.
Guarded read and write entrypoints also perform a cheap marker check and
journal publication/fold recovery if needed.

## Current Contract Rationale

* Removes implicit reliance on IC traps
* Matches the current executor + commit-marker architecture
* Makes invariants explicit and enforceable
* Prevents silent regression via async or refactors
* Defines one explicit atomicity contract for the current release line
