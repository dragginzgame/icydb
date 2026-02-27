# WEEKLY AUDIT — Invariant Preservation (icydb-core)

## Purpose

Verify that **all structural, ordering, identity, and mutation invariants** in `icydb-core`:

* Exist explicitly
* Are enforced exactly once
* Are enforced at the correct boundary
* Are enforced in both normal execution and recovery
* Cannot drift silently

This is a correctness audit only.

Do NOT discuss:

* Performance
* Style
* DRY
* Refactoring
* Architecture redesign (unless invariant violation is found)

---

# Phase 0 — Establish the Invariant Registry

Before analysis, enumerate all invariants in the system.

You must not assume them.
You must list them explicitly.

Classify invariants into categories:

### A. Identity Invariants

* Entity primary key matches storage key
* Index id consistency
* Key namespace consistency
* Component arity stability
* Expected-key vs decoded-entity match

### B. Ordering Invariants

* Raw index key lexicographic ordering is canonical
* Logical ordering matches raw ordering
* Cursor resume is strictly monotonic
* Bound inclusivity semantics preserved
* Envelope containment preserved

### C. Structural Invariants

* AccessPath shape stability
* Plan shape immutability after validation
* No widening of predicate envelope
* Unique constraint guarantees
* Reverse relation symmetry

### D. Mutation Invariants

* Save mutates index + store consistently
* Delete removes index + store consistently
* Reverse index mutation symmetry
* Referential integrity enforcement

### E. Recovery Invariants

* Replay is idempotent
* Replay does not widen envelope
* Replay does not duplicate index entries
* Replay restores exact structural shape
* Replay error classification matches runtime

Produce:

| Invariant | Category | Subsystem(s) Impacted |

This becomes the baseline for all checks.

---

# Phase 1 — Boundary Mapping

Identify all boundary crossings:

* serialize → deserialize
* RawIndexKey encode → decode
* identity types → storage key
* planner → executable plan
* executable plan → executor
* save executor → commit
* delete executor → commit
* commit → recovery replay
* cursor decode → cursor planning
* reverse-relation mutation
* index store read → index key interpretation

For each boundary:

| Boundary | Input Assumptions | Output Guarantees |

---

# Phase 2 — Invariant Enforcement Mapping

For each invariant:

You must identify:

A. Where it is assumed
B. Where it is enforced
C. Whether enforcement is:

* Exactly once
* At the narrowest boundary
* Too early
* Too late
* Duplicated
* Missing

Produce:

| Invariant | Assumed At | Enforced At | Exactly Once? | Narrowest Boundary? | Correct Error Class? | Risk |

---

# Phase 3 — Symmetry & Recovery Audit

For each invariant:

Verify:

1. Enforced in normal execution
2. Enforced in recovery replay
3. Enforced in cursor continuation
4. Enforced in reverse relation mutation
5. Enforced in index encode/decode

Produce:

| Invariant | Normal Exec | Recovery | Cursor | Reverse Index | Risk |

Flag any invariant enforced only in forward execution.

---

# Phase 4 — High-Risk Focus Areas

Explicitly deep-audit:

## A. Cursor Envelope Safety

* Anchor cannot escape original envelope
* Bound conversion uses Excluded
* Upper bound never modified
* Index id cannot change
* Namespace cannot change
* Arity cannot change

## B. Index Key Ordering Guarantees

* Encode preserves lexicographic order
* Decode does not reinterpret ordering
* No ordering assumptions outside raw key compare
* Composite prefix ordering preserved

## C. Reverse Relation Index Correctness

* Reverse index updated symmetrically on save
* Reverse index updated symmetrically on delete
* Reverse index consistent during recovery
* No orphaned reverse entries

## D. Recovery Idempotence

* Replay twice produces identical state
* Index and store match after replay
* No duplicate index keys
* No widening of access path

## E. Expected-Key vs Decoded-Entity Match

* Decoded entity key must equal storage key
* Enforced before returning entity
* Enforced during recovery
* Error classification correct

---

# Phase 5 — Enforcement Quality Evaluation

Flag invariants that are:

* Enforced in multiple layers
* Enforced after mutation
* Enforced only implicitly
* Enforced via assumption rather than explicit check
* Not enforced on corrupted input
* Not enforced in recovery
* Not covered by tests

Produce sections:

---

## High Risk Invariants

Invariants where:

* Missing enforcement
* Late enforcement
* Recovery asymmetry
* Multiple enforcement sites with drift risk

---

## Redundant Enforcement

Invariants enforced in:

* Planner + executor
* Executor + store
* Store + recovery

Highlight potential drift pressure.

---

## Missing Enforcement

Any invariant that:

* Is assumed but never explicitly validated
* Is only validated in one path
* Is not validated during replay
* Is not validated during cursor continuation

---

# Phase 6 — Drift Sensitivity Analysis

For each invariant, assess:

| Invariant | Sensitive To | Drift Risk |

Examples:

* Adding DESC
* Adding composite access paths
* Adding new index types
* Adding new commit markers
* Adding new error classes

This anticipates silent invariant erosion.

---

# Final Output Structure

1. Invariant Registry (complete list)
2. Boundary Map
3. Enforcement Mapping Table
4. Recovery Symmetry Table
5. High Risk Invariants
6. Redundant Enforcement
7. Missing Enforcement
8. Drift Sensitivity Summary
9. Overall Invariant Risk Index (1–10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Anti-Shallow Requirement

Do NOT:

* Say “looks correct”
* Say “appears enforced”
* Provide generic statements
* Skip enforcement location
* Skip recovery symmetry check

Every invariant must:

* Be named
* Be mapped
* Be located
* Be proven
