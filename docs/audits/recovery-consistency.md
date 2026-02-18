# WEEKLY AUDIT — Recovery Consistency & Replay Equivalence

`icydb-core`

## Purpose

Verify that recovery replay produces **exactly the same structural state, invariants, and side-effects** as normal execution.

Recovery must be:

* Semantically equivalent
* Invariant-equivalent
* Mutation-order equivalent
* Idempotent
* Deterministic

This audit does NOT evaluate:

* Performance
* Style
* Refactors
* New features

Only correctness equivalence.

---

# Core Principle

For every mutation type:

> Recovery replay must be indistinguishable from normal execution in final state and invariant guarantees.

If replay and execution differ in:

* Order of operations
* Validation ordering
* Error classification
* Reverse index mutation
* Index entry construction
* Envelope enforcement

that is a correctness risk.

---

# Scope

Analyze:

* `CommitMarker`
* `CommitRowOp`
* `begin_commit`
* `finish_commit`
* `ensure_recovered_for_write`
* Save / Replace / Delete executor flows
* Reverse-relation index mutation
* Index entry mutation
* Commit log persistence
* Recovery replay logic

---

# STEP 1 — Mutation Inventory

Enumerate all mutation types:

* Insert
* Replace
* Delete
* Reverse relation update
* Index entry creation
* Index entry removal
* Commit marker transitions

Produce:

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |

---

# STEP 2 — Side-by-Side Flow Comparison

For each mutation type:

Construct a side-by-side flow:

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |

Phases must include:

1. Pre-mutation invariant checks
2. Referential integrity validation
3. Unique constraint validation
4. Reverse relation mutation
5. Index entry mutation
6. Store mutation
7. Commit marker write
8. Finalization

You must explicitly compare:

* Operation ordering
* Validation ordering
* Error propagation behavior
* Error classification type

---

# STEP 3 — Invariant Enforcement Parity

For each invariant relevant to mutation:

* Identity match
* Key namespace
* Index id consistency
* Component arity
* Reverse relation symmetry
* Unique constraint enforcement
* Expected-key vs decoded-key match

Produce:

| Invariant | Enforced in Normal | Enforced in Recovery | Enforced at Same Phase? | Risk |

Flag:

* Enforced only in normal path
* Enforced only in recovery
* Enforced in different phase ordering
* Enforced after mutation in recovery but before mutation in normal

---

# STEP 4 — Mutation Ordering Verification

Verify:

* Reverse index mutation occurs before or after store mutation consistently.
* Index mutation order is identical.
* Commit marker transitions occur in identical relative position.
* No recovery path performs mutation earlier than validation.

Produce:

| Mutation | Normal Order | Recovery Order | Equivalent? | Risk |

---

# STEP 5 — Error Classification Equivalence

For each failure scenario:

* Unique violation
* Referential integrity violation
* Corrupt commit marker
* Corrupt index entry
* Invalid commit phase
* Double-apply replay

Verify:

| Failure Scenario | Normal Error Type | Recovery Error Type | Equivalent? | Risk |

---

# STEP 6 — Divergence Detection

Explicitly attempt to find:

* Mutation performed twice on replay.
* Reverse index applied twice.
* Store mutation skipped during replay.
* Validation skipped in recovery.
* Recovery reorders operations.
* Recovery fails to enforce invariants enforced in executor.
* prepare-phase mutation that is not rolled back.
* Commit marker partially applied state that recovery handles differently.

Produce section:

## Divergence Risks

Each item must include:

* Location
* Difference
* Consequence
* Risk Level

---

# STEP 7 — Idempotence Verification

Verify:

1. Replaying same commit twice yields identical state.
2. Replay does not:

   * Duplicate index entries
   * Duplicate reverse index entries
   * Duplicate store rows
3. Replay respects:

   * Commit phase transitions
   * Already-applied marker detection

Produce:

| Scenario | Idempotent? | Why / Why Not | Risk |

---

# STEP 8 — Partial Failure Symmetry

Simulate:

1. Failure after reverse index mutation but before store write.
2. Failure after store write but before finish_commit.
3. Failure between begin_commit and index mutation.
4. Failure during replace.
5. Failure during delete.

Verify:

* Recovery resumes safely.
* No invariant violation.
* No orphaned index entries.
* No orphaned reverse entries.
* No double-application.

Produce:

| Failure Point | Recovery Outcome | Safe? | Risk |

---

# Required Output Sections

1. Mutation Inventory
2. Side-by-Side Flow Tables
3. Invariant Enforcement Parity Table
4. Ordering Equivalence Table
5. Error Classification Equivalence Table
6. Divergence Risks
7. Idempotence Verification
8. Overall Recovery Integrity Score (1–10)

Scale:

9–10 → Strong replay equivalence
7–8 → Minor asymmetry risk
5–6 → Moderate divergence risk
3–4 → High structural fragility
1–2 → Recovery unsound

---

# Overlap With Other Audits

This overlaps partially with:

* Invariant-Preservation (enforcement mapping)
* State-Machine Integrity (transition correctness)
* Index-Integrity (mutation correctness)

However, this audit is distinct because it requires:

* Direct side-by-side execution flow comparison
* Explicit equivalence proof
* Idempotence verification
* Phase-order equivalence analysis

Keep it separate.
