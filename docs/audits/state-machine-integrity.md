# audits/state-machine-integrity.md

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

# Required State Transitions To Audit

## A. Planner → Executable Plan

Verify:

* Plan shape cannot mutate after validation.
* AccessPath cannot widen.
* Ordering cannot change.
* Envelope cannot change.

---

## B. Executable Plan → Executor

Verify:

* Executor uses exactly the planned access path.
* No fallback broad scan occurs silently.
* No widening of bounds.
* No change of index id.
* No predicate reinterpretation.

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

---

## F. Recovery Lifecycle

Validate:

* Replay does not alter ordering.
* Replay does not widen envelope.
* Index/store consistency restored deterministically.
* No duplicate entries created.
* No index drift.

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

For each, state:

* Can invariant be violated?
* Can partial mutation occur?
* Can index/store divergence occur?
* Is error classification correct?
* Is state deterministic afterward?

---

# Required Output Format

## 1. Transition Integrity Table

| Transition | Invariants Checked Before? | Mutation Before Validation? | Risk |
| ---------- | -------------------------- | --------------------------- | ---- |

---

## 2. Partial Mutation Risk Table

| Operation | Partial Mutation Possible? | Protection Mechanism | Risk |
| --------- | -------------------------- | -------------------- | ---- |

---

## 3. Plan/Execution Drift Table

| Area | Plan Shape Can Drift? | Executor Can Widen? | Risk |
| ---- | --------------------- | ------------------- | ---- |

---

## 4. Recovery Determinism Table

| Scenario | Deterministic? | Structural Integrity Preserved? | Risk |
| -------- | -------------- | ------------------------------- | ---- |

---

## 5. Drift Sensitivity

Identify:

* Implicit invariants.
* Areas without structural enforcement.
* Areas relying on ordering assumptions.
* Areas without failure tests.

---

## Overall Risk Rating

* Critical
* Medium
* Low
