# Weekly Audit: State Machine & Transition Integrity

## Scope

This audit verifies that all execution flows preserve invariants across state transitions:

* Plan → Execute
* Execute → Commit
* Save lifecycle
* Delete lifecycle
* Cursor continuation lifecycle
* Recovery lifecycle

Do NOT discuss:

* Performance
* Refactoring
* Architectural improvements

Only correctness of state transitions.

---

## Current Adjacency And Merge Decision

This audit is intentionally retained as a standalone audit. It overlaps with
storage recovery, cursor ordering, invariant preservation, and schema transition
audits, but it owns a different question:

> Can one state boundary be entered, widened, skipped, or published out of
> order?

Do not turn this into a deep replay-equivalence audit. That belongs to
`storage/storage-recovery-consistency.md`.

Do not turn this into a cursor comparison-order audit. That belongs to
`executor/cursor-ordering.md`.

Do not turn this into a broad invariant inventory. That belongs to
`integrity/invariant-preservation.md`.

This audit must instead sample transition gates across those domains and verify
that state ownership remains explicit and fail-closed.

---

## Current Source Map

Every run must re-check this map and update the report when ownership moved.

| State Boundary | Current Owner Paths | Adjacent Audit |
| -------------- | ------------------- | -------------- |
| schema transition admission | `crates/icydb-core/src/db/schema/transition.rs`, `crates/icydb-core/src/db/schema/reconcile.rs` | canonical semantic authority |
| schema mutation runner publication | `crates/icydb-core/src/db/schema/reconcile.rs`, `crates/icydb-core/src/db/schema/mutation/field_path/*`, `crates/icydb-core/src/db/schema/mutation/runner.rs` | invariant preservation |
| route-plan validation handoff | `crates/icydb-core/src/db/executor/planning/route/*`, `crates/icydb-core/tests/write_boundary_guards.rs` | layer violation |
| commit-window open/apply/finish | `crates/icydb-core/src/db/executor/mutation/commit_window.rs`, `crates/icydb-core/src/db/commit/guard.rs` | recovery consistency |
| SQL/fluent write transition barrier | `crates/icydb-core/src/db/session/sql/*`, `crates/icydb-core/src/db/executor/tests/mutation_save.rs` | completeness |
| recovery write gate handoff | `crates/icydb-core/src/db/commit/recovery.rs`, `crates/icydb-core/src/db/mod.rs` | recovery consistency |

---

## Required Modern Transition Samples

Every run must include at least one concrete evidence row for each of these
families. A row may be source-audit evidence, a focused test, or both.

| Family | Required Question | Minimum Evidence |
| ------ | ----------------- | ---------------- |
| schema mutation runner | Can staged physical work or accepted-after schema publish before validation, runtime invalidation, rebuild-gate revalidation, physical-store publication, final physical-store revalidation, and accepted snapshot handoff? | focused schema mutation runner/reconciliation test or source guard |
| schema transition barrier | Can unsupported accepted-schema drift reach read/write staging? | focused session/executor transition-barrier test |
| route-plan handoff | Can executor route construction bypass validated planner output? | focused route structural guard |
| commit-window lifecycle | Can apply/finish occur without a persisted marker-backed commit window? | commit guard or commit-window test |
| recovery handoff | Are writes blocked or rebuilt before recovery completion? | focused recovery gate test or source guard |

If one family has no live evidence, the report must mark it `PARTIAL` and name
the missing probe.

---

# Ground Truth Specification

The database must behave as a deterministic state machine.

At every transition boundary:

1. All invariants must hold before proceeding.
2. No partial invariant violation may be externally visible.
3. Errors must not leave mutated state.
4. Planner decisions must not be reinterpreted differently at execution time.
5. Execution must not widen or alter plan shape.
6. Recovery must restore exact structural invariants.

---

# Execution State Model (Mandatory)

Every run must declare the explicit execution-state model before transition
analysis.

Produce:

| State | Owner | Entry Condition | Exit Condition | Notes |
| ----- | ----- | --------------- | -------------- | ----- |

Required minimum state families (rename allowed if equivalent and explicit):

* unplanned / accepted-intent
* planned
* executing
* commit-window-open
* commit-marker-persisted
* applied
* recovered

State-model invariants to verify:

* states are mutually exclusive for a given execution context
* entry/exit conditions are explicit and testable
* no implicit transitional state is relied on without declaration

Any newly introduced state must be listed explicitly and linked to owner
authority.

---

# State Exclusivity Verification (Mandatory)

Every run must explicitly verify that incompatible states cannot coexist.

Produce:

| State Pair | Can Coexist? | Expected Result | Observed | Risk |
| ---------- | ------------ | --------------- | -------- | ---- |

Required minimum exclusivity pairs:

* executing / commit-window-open -> No
* commit-marker-persisted / recovered -> No
* executing / recovered -> No
* commit-window-open / applied -> No

---

# Transition Completeness Check (Mandatory)

Every run must verify that each declared state has legal exits and is not
accidentally terminal or unreachable.

Produce:

| State | Legal Outgoing Transitions | Missing Transition? | Unreachable? | Risk |
| ----- | -------------------------- | ------------------- | ------------ | ---- |

Required minimum transitions:

* unplanned -> planned
* planned -> executing
* executing -> commit-window-open / cursor-continuation
* commit-window-open -> commit-marker-persisted
* commit-marker-persisted -> applied / replayed-apply
* applied -> marker-cleared
* recovered -> writes-allowed

---

# Required Legal Transitions To Audit

## A. Planner → Executable Plan

Verify:

* Plan shape cannot mutate after validation.
* AccessPath cannot widen.
* Ordering cannot change.
* Envelope cannot change.
* Execute path is unreachable without validated plan.

---

## B. Executable Plan → Executor

Verify:

* Executor uses exactly the planned access path.
* No fallback broad scan occurs silently.
* No widening of bounds.
* No change of index id.
* No predicate reinterpretation.
* Unvalidated route/load handoff is rejected.

---

## C. Save Lifecycle

Validate sequence:

1. Validation
2. Index mutation
3. Store mutation
4. Commit

Verify:

* Invariants validated before mutation.
* Unique constraints validated before commit.
* No mutation occurs before validation completes.
* Failure at any step does not leave inconsistent state.
* Mutation path is unreachable without commit window.

---

## D. Delete Lifecycle

Validate:

1. Existence check
2. Referential integrity validation
3. Index removal
4. Store removal

Verify:

* Strong RI checked before mutation.
* Index and store removal are consistent.
* No orphaned index entries.
* No orphaned data rows.
* Delete mutation path is unreachable without commit window.

---

## E. Cursor Continuation Lifecycle

Validate:

1. Decode
2. Validation
3. Plan application
4. Bound substitution
5. Execution

Verify:

* Cursor cannot mutate plan shape.
* Cursor cannot mutate predicate.
* Cursor cannot mutate index id.
* Bound substitution is monotonic.
* Envelope preserved.
* Invalid cursor/anchor transition is rejected before execution.

---

## F. Recovery Lifecycle

Validate:

* Replay does not alter ordering.
* Replay does not widen envelope.
* Index/store consistency restored deterministically.
* No duplicate entries created.
* No index drift.
* Write paths are blocked until recovery completion.

---

# Required Illegal Transition Rejection Checks

Every run must include illegal transition probes and expected fail-closed
behavior.

Required illegal transitions:

* execute without validated plan -> reject
* apply mutation without commit window -> reject
* write before recovery completed -> reject
* cursor resume with invalid anchor/envelope -> reject

Produce:

| Illegal Transition | Expected Rejection Gate | Observed Behavior | Risk |
| ------------------ | ----------------------- | ----------------- | ---- |

---

# Transition Authority Ownership Checks

Each transition must have a single authority boundary.

Produce:

| Transition | Authority Module | Alternate Path Exists? | Result | Risk |
| ---------- | ---------------- | ---------------------- | ------ | ---- |

Flag any transition with multiple authorities or bypass paths.

---

# Execution/Replay Equivalence Checks

Idempotence alone is insufficient. Verify execution/replay state equivalence.

Required invariant:

* `state_after_execute(commit) == state_after_replay(commit)`

Required equivalence checks:

* `execute(commit)` final state equals `replay(commit)` final state
* replay retries remain idempotent after partial apply
* live apply and replay apply preserve identical index/store invariants
* execution and replay produce identical index/store ordering

Produce:

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

---

# Commit Marker Authority Check

Verify commit marker remains the sole durable handoff authority between execute
and replay paths.

Produce:

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

Required checks:

* marker persistence occurs before mutation visibility
* marker absence prevents replay path activation
* marker ownership is confined to commit/recovery authority boundary

---

# Partial-Execution Failure-Point Safety

Validate deterministic state ownership at failure cut points.

Required failure cut points:

* before marker persistence
* after marker persistence before full apply
* mid-apply
* during delete mutation

Produce:

| Failure Point | Expected Durable State | Recovery Owner | Result | Risk |
| ------------- | ---------------------- | -------------- | ------ | ---- |

---

# Mutation Entrypoint Coverage Check

Verify all mutation entrypoints route through the same commit-window protocol.

Produce:

| Mutation Entrypoint | Routes Through Commit Window? | Shared Transition Path | Result | Risk |
| ------------------- | ----------------------------- | ---------------------- | ------ | ---- |

---

# Logical Concurrency Safety (Required)

Even in single-threaded execution, logically overlapping operations must remain
deterministic and non-divergent.

Produce:

| Scenario | Deterministic Ordering? | State Divergence Possible? | Risk |
| -------- | ----------------------- | -------------------------- | ---- |

Required scenarios:

* overlapping save operations
* save + delete on same entity
* cursor continuation during mutation lifecycle

---

# Explicit Attack Scenarios

You must reason through:

1. Failure during index update.
2. Failure after index update but before store update.
3. Failure during delete after index removal.
4. Failure during cursor decode.
5. Failure during anchor validation.
6. Failure mid-pagination.
7. Recovery replay repeated twice.
8. Planner emits invalid access path.
9. Executor receives corrupted plan.
10. Concurrent logical operations (even if single-threaded).
11. Mutation entrypoint bypass attempt around commit window.
12. Write attempted before recovery gate completion.
13. Overlapping save operations.
14. Save + delete on same entity.
15. Cursor continuation during mutation.

For each, state:

* Can invariant be violated?
* Can partial mutation occur?
* Can index/store divergence occur?
* Is error classification correct?
* Is state deterministic afterward?

---

# Required Output Format

## 0. Run Metadata + Comparability Note

- compared baseline report path (daily baseline rule: first run of day compares
  to latest prior comparable report or `N/A`; same-day reruns compare to that
  day's `state-machine-integrity.md` baseline)
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

---

## 1. Execution State Model Table

| State | Owner | Entry Condition | Exit Condition | Notes |
| ----- | ----- | --------------- | -------------- | ----- |

---

## 2. State Exclusivity Verification Table

| State Pair | Can Coexist? | Expected Result | Observed | Risk |
| ---------- | ------------ | --------------- | -------- | ---- |

---

## 3. Transition Completeness Table

| State | Legal Outgoing Transitions | Missing Transition? | Unreachable? | Risk |
| ----- | -------------------------- | ------------------- | ------------ | ---- |

---

## 4. Transition Integrity Table

| Transition | Invariants Checked Before? | Mutation Before Validation? | Risk |
| ---------- | -------------------------- | --------------------------- | ---- |

---

## 5. Illegal Transition Rejection Table

| Illegal Transition | Expected Rejection Gate | Observed Behavior | Risk |
| ------------------ | ----------------------- | ----------------- | ---- |

---

## 6. Transition Authority Table

| Transition | Authority Module | Alternate Path Exists? | Result | Risk |
| ---------- | ---------------- | ---------------------- | ------ | ---- |

---

## 7. Execution/Replay Equivalence Table

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

---

## 8. Commit Marker Authority Table

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

---

## 9. Failure-Point Safety Table

| Failure Point | Expected Durable State | Recovery Owner | Result | Risk |
| ------------- | ---------------------- | -------------- | ------ | ---- |

---

## 10. Mutation Entrypoint Coverage Table

| Mutation Entrypoint | Routes Through Commit Window? | Shared Transition Path | Result | Risk |
| ------------------- | ----------------------------- | ---------------------- | ------ | ---- |

---

## 11. Logical Concurrency Safety Table

| Scenario | Deterministic Ordering? | State Divergence Possible? | Risk |
| -------- | ----------------------- | -------------------------- | ---- |

---

## 12. Plan/Execution Drift Table

| Area | Plan Shape Can Drift? | Executor Can Widen? | Risk |
| ---- | --------------------- | ------------------- | ---- |

---

## 13. Recovery Determinism Table

| Scenario | Deterministic? | Structural Integrity Preserved? | Risk |
| -------- | -------------- | ------------------------------- | ---- |

---

## 14. Drift Sensitivity

Identify:

* Implicit invariants.
* Areas without structural enforcement.
* Areas relying on ordering assumptions.
* Areas without failure tests.

---

## 15. Optional Transition Graph Snapshot

Include a compact transition graph when useful for reviewer comparability.

Example shape:

* query -> plan -> execute -> open commit window -> persist marker -> apply -> clear marker
* startup -> ensure recovered -> replay marker -> apply -> clear marker

---

## Overall State-Machine Risk Index (1–10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

## Verification Readout

Include command outcomes using `PASS` / `FAIL` / `BLOCKED`.
