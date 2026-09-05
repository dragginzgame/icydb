# Recurring Audit: State Machine & Transition Integrity

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers)
to all inventories, checks, and output sections below. Record selected and
excluded obligations before analysis; broad coverage requires a requested baseline.

Current method tag: `STATE-1.1`, alongside the shared scope contract tag.
This method checks recovery-contained failure state rather than universal
rollback, including both marker-owned application and marker-free journal-tail
folding. Comparisons with earlier failure/exclusivity or marker-only criteria must describe
the change and mark affected deltas `N/A (method change)`.

## Scope

This audit verifies that affected execution flows preserve invariants across
state transitions:

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

Re-check the selected boundaries in this map and record moved ownership in the
new report or conversational findings under
[Authorization And Read-Only Work](../../README.md#authorization-and-read-only-work).
Do not rewrite an earlier report to reflect the current source tree.

| State Boundary | Current Owner Paths | Adjacent Audit |
| -------------- | ------------------- | -------------- |
| schema transition admission | `crates/icydb-core/src/db/schema/transition.rs`, `crates/icydb-core/src/db/schema/transition/admission.rs` | flow convergence and duplication (semantic ownership) |
| schema DDL admission and publication | `crates/icydb-core/src/db/schema/mutation/ddl_admission.rs`, `crates/icydb-core/src/db/schema/mutation/user_index_domain.rs`, `crates/icydb-core/src/db/schema/sql_ddl/user_index_domain.rs` | invariant preservation |
| route-plan validation handoff | `crates/icydb-core/src/db/executor/planning/route/*` | flow convergence and duplication (boundary ownership) |
| commit-window open/apply/finish | `crates/icydb-core/src/db/executor/mutation/commit_window.rs`, `crates/icydb-core/src/db/commit/guard.rs` | recovery consistency |
| SQL/structural write transition barrier | `crates/icydb-core/src/db/session/sql/*`, `crates/icydb-core/src/db/session/write.rs` | completeness |
| recovery write gate handoff | `crates/icydb-core/src/db/commit/recovery.rs`, `crates/icydb-core/src/db/mod.rs` | recovery consistency |

---

## Required Modern Transition Samples

Include at least one concrete evidence row for each affected family below,
including adjacent gates needed to prove its safety. A broad baseline covers
all families. A row may be source-audit evidence, a focused test, or both;
distinguish inspection from executed behavioral proof.

| Family | Required Question | Minimum Evidence |
| ------ | ----------------- | ---------------- |
| schema DDL publication | Can staged physical work or an accepted-after schema publish before DDL admission, publication preflight, marker-backed user-index-domain replacement, final validation, and accepted snapshot handoff? | focused DDL admission/user-index-domain/reconciliation test or source guard |
| schema transition barrier | Can unsupported accepted-schema drift reach read/write staging? | focused session/executor transition-barrier test |
| route-plan handoff | Can executor route construction bypass validated planner output? | focused route structural guard |
| commit-window lifecycle | Can apply/finish occur without a persisted marker-backed commit window? | commit guard or commit-window test |
| recovery handoff | Are writes blocked or rebuilt before recovery completion? | focused recovery gate test or source guard |

If a selected family has no current applicable evidence, mark it `PARTIAL`
and name the missing probe. Justify excluded families separately; they do not pass.

---

# Ground Truth Specification

The database must behave as a deterministic state machine.

At every transition boundary:

1. Each transition's required preconditions must hold before proceeding.
2. No partial invariant violation may be externally visible.
3. Preflight rejection must leave protected durable state unchanged. After
   marker persistence, a normally returned apply error may retain partial work,
   but must retain durable recovery authority and its required recovery wake-up.
   Admission and publication guards must prevent incomplete state from becoming
   accepted runtime-visible state, and recovery must finish forward before
   normal access resumes. A best-effort local rollback is not durable authority.
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

Model the applicable lifecycle facts below using current owner predicates
(rename allowed if equivalent and explicit):

* unplanned / accepted-intent
* planned
* executing
* commit-window-open
* commit-marker-persisted
* applied
* recovered

State-model invariants to verify:

* exclusive phases are distinguished from overlapping lifecycle facts;
  a persisted marker can coexist with partially or fully applied work before
  marker retirement
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

Derive incompatible pairs from current guards rather than treating all phase
labels as disjoint. In particular, check that pending recovery cannot coexist
with normal write admission or publication of incomplete state. Do not flag
applied work plus a retained marker as a violation by itself; verify recovery
ownership, visibility containment, and eventual safe retirement instead.

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
* executing -> marker-backed commit-window-open / cursor-continuation
* commit-window-open (marker persisted) -> applied / replayed-apply
* apply failure -> retained marker / recovery-pending
* recovery-pending -> replayed-apply
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

1. Validation and preparation
2. Durable marker persistence and commit-window admission
3. Marker-owned row/index application in the current owner's order
4. Successful completion and marker retirement, or retained authority for recovery

Verify:

* Invariants validated before mutation.
* Unique constraints validated before commit.
* No mutation occurs before validation completes.
* Failure before marker authority leaves protected durable state unchanged;
  later failures retain recovery authority and contain incomplete state.
* Mutation path is unreachable without commit window.

---

## D. Delete Lifecycle

Validate:

1. Existence check
2. Referential integrity validation
3. Durable marker persistence and commit-window admission
4. Marker-owned index/store removal
5. Successful completion and marker retirement, or retained authority for recovery

Verify:

* Strong RI checked before mutation.
* Index and store removal are consistent.
* No orphaned index entries or data rows are exposed as accepted completed state.
* Interrupted removal remains marker-owned until recovery restores consistency.
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

# Durable Recovery Authority Check

Distinguish incomplete marker-owned application from committed journal-tail
folding. Both are maintained startup recovery work; marker absence alone does
not prove recovery is complete. Use `recover_domain` and `perform_recovery_page`
in `crates/icydb-core/src/db/commit/recovery.rs` as the current owner boundary.

Produce:

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

Required checks:

* marker persistence occurs before mutation visibility
* incomplete marker-owned work retains its marker until replay, fold, and effect
  validation permit retirement
* an absent marker with nonempty journal tails still enters bounded recovery
* journal batches validate before canonical effects and watermarks retire
* normal access and Ready publication remain gated until startup recovery completes
* marker and journal authority stay within the commit/recovery and journal owners;
  ordinary read/write admission observes readiness without driving recovery

---

# Partial-Execution Failure-Point Safety

Validate deterministic state ownership at failure cut points.

Use the durable failure contract in
`crates/icydb-core/src/db/commit/guard.rs` (`finish_commit`) and the scoped
apply path as authority. Verify preflight rejection is zero-write; an apply
error retains the marker and recovery wake-up; partial work stays behind
admission/publication gates; and successful recovery restores consistency
before normal access resumes. Distinguish normally returned errors from traps
and their message rollback semantics. Do not infer a defect solely from retained
durable state or use test-only rollback helpers as the production contract.

Required failure cut points:

* before marker persistence
* after marker persistence before full apply
* mid-apply
* during delete mutation
* after apply, before successful marker retirement

Produce:

| Failure Point | Expected Durable State | Recovery Owner | Visibility/Admission Gate | Result | Risk |
| ------------- | ---------------------- | -------------- | ------------------------- | ------ | ---- |

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

## 8. Durable Recovery Authority Table

| Requirement | Evidence | Result | Risk |
| ----------- | -------- | ------ | ---- |

---

## 9. Failure-Point Safety Table

| Failure Point | Expected Durable State | Recovery Owner | Visibility/Admission Gate | Result | Risk |
| ------------- | ---------------------- | -------------- | ------------------------- | ------ | ---- |

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

* query -> plan -> execute -> persist marker/open commit window -> apply -> clear marker
* apply failure -> retain marker and wake-up -> recovery gate -> replay apply -> clear marker
* startup -> ensure recovered -> replay marker -> apply -> clear marker
* startup with no marker and nonempty journal -> validate/fold batches -> verify effects -> Ready

---

## Verdict And Findings

Apply [Findings And Verdicts](../../README.md#findings-and-verdicts).
Summarize the supported verdict, each finding's consequence and severity, and
any unresolved verification. Keep owner, disposition, and action trigger with
the finding.

## Verification Readout

Include command outcomes using `PASS` / `FAIL` / `BLOCKED`.
