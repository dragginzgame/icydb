# WEEKLY AUDIT — Index Integrity

`icydb-core`

## Purpose

Verify that the index subsystem preserves:

* Ordering guarantees
* Namespace isolation
* Index id containment
* Mutation symmetry
* Unique enforcement equivalence
* Recovery idempotence
* Row/index atomic coupling

This is a correctness audit only.

Do NOT discuss:

* Performance
* Refactoring
* Style
* Code aesthetics

---

# Core Principle

The index layer must be:

* Deterministic
* Lexicographically ordered
* Namespace-isolated
* Idempotent under replay
* Coupled 1:1 with row mutations

If index state diverges from row state, integrity is compromised.

---

# STEP 0 — Index Invariant Registry

Enumerate all index-level invariants before analysis.

Categories:

### A. Ordering Invariants

* Raw key ordering is lexicographic
* Component ordering is stable
* Prefix encoding preserves sort order
* No implicit logical ordering layer

### B. Namespace Invariants

* Index id is encoded in key
* Key namespace prevents cross-index decode
* No key can decode as different index id
* Index id mismatch is detected

### C. Structural Invariants

* Key encode/decode symmetry
* Entry layout stable
* No partial decode acceptance
* Component arity fixed per index

### D. Mutation Invariants

* Row mutation → index mutation (always)
* Index mutation → row mutation (never standalone)
* Reverse index symmetry
* Unique enforcement consistency

### E. Recovery Invariants

* Replay is idempotent
* Replay enforces same unique checks
* Replay enforces same reverse mutation logic
* No duplicate entry creation
* No partial mutation replay

Produce:

| Invariant | Category | Enforced Where |

---

# STEP 1 — Key Encoding & Ordering Audit

## 1A. Encode/Decode Symmetry

Verify:

* `encode(decode(key)) == key`
* `decode(encode(logical_key)) == logical_key`
* No silent truncation
* No partial decode acceptance
* No component count mismatch acceptance

Produce:

| Key Type | Symmetric? | Failure Mode | Risk |

---

## 1B. Lexicographic Ordering Proof

Verify:

* Byte ordering corresponds to logical component ordering
* Prefix encoding does not break sort order
* Float handling is canonical
* Signed types preserve lexicographic ordering
* Composite keys compare component-wise lexicographically

Explicitly test reasoning for:

* Negative vs positive numbers
* Zero values
* Composite key prefix differences
* Variable-length encoding boundaries

Produce:

| Case | Lexicographically Stable? | Why | Risk |

---

# STEP 2 — Namespace & Index ID Isolation

Verify:

* Index id embedded in key namespace
* No two index ids share overlapping key prefixes
* Decode path validates index id before interpreting payload
* Index id mismatch produces invariant error
* No key from index A can decode as valid key of index B

Attempt to find:

* Cross-index decode acceptance
* Key collision across namespaces
* Prefix confusion
* Partial prefix collisions

Produce:

| Scenario | Can Cross-Decode? | Prevented Where | Risk |

---

# STEP 3 — IndexStore Entry Layout

Map:

* Raw entry layout
* Key bytes
* Payload bytes
* Any fingerprint or marker data
* Entry boundaries

Verify:

* Entry layout is deterministic
* Entry layout is stable across replay
* No variable-length ambiguity
* Decode cannot misalign entry boundary

Produce:

| Entry Component | Layout Stable? | Decode Safe? | Risk |

---

# STEP 4 — Reverse Relation Index Integrity

Verify:

* Reverse entries created during save
* Reverse entries removed during delete
* Replace flow handles both old + new reverse entries
* Recovery applies identical reverse mutation logic
* No reverse entry can exist without corresponding forward relation

Attempt to find:

* Orphan reverse entries
* Reverse duplication on replay
* Reverse mutation ordering mismatch

Produce:

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |

---

# STEP 5 — Unique Index Enforcement

Verify:

* Unique violation detected before mutation
* Unique violation classification consistent
* Recovery re-enforces unique constraint
* Replay does not skip unique validation
* Replace handles same-value update correctly
* Delete + reinsert same value allowed

Attempt to find:

* Replay bypass of unique check
* Double insert allowed
* Replace violating unique invariant
* Partial unique mutation during prepare

Produce:

| Scenario | Unique Enforced? | Recovery Enforced? | Risk |

---

# STEP 6 — Row ↔ Index Coupling

Verify:

* No index mutation without corresponding row mutation
* No row mutation without index mutation
* Reverse mutation tightly coupled
* Mutation ordering consistent between save and recovery
* Partial failure cannot leave index without row or row without index

Simulate:

1. Failure after index insert before row write
2. Failure after row write before index insert
3. Failure during reverse index update
4. Replay after partial commit

Produce:

| Failure Point | Divergence Possible? | Prevented? | Risk |

---

# STEP 7 — Recovery Replay Equivalence

Compare:

Normal Save Path vs Replay Path
Normal Delete Path vs Replay Path
Normal Replace Path vs Replay Path

For each:

| Phase | Normal | Replay | Equivalent? | Risk |

Verify:

* Same invariant checks
* Same mutation order
* Same error classification
* Same reverse mutation logic
* Idempotence

---

# STEP 8 — Explicit Attack Scenarios

Attempt to find:

* Key collisions across index ids
* Component arity confusion
* Namespace prefix overlap
* Partial decode acceptance
* Index id mismatch vulnerability
* Reverse orphan after replay
* Double unique insert on replay
* Delete skipping reverse cleanup
* Replace partial mutation

For each:

| Attack | Possible? | Why / Why Not | Risk |

---

# STEP 9 — High Risk Mutation Paths

Identify:

* Complex flows with multiple mutation phases
* Replace flow with dual mutation
* Recovery mutation entry points
* Reverse mutation code paths

Produce:

| Path | Complexity | Divergence Risk | Risk Level |

---

# STEP 10 — Storage-Layer Assumptions

Explicitly list assumptions such as:

* Stable memory write atomicity per entry
* Deterministic iteration order
* Key comparison strictly byte-wise
* No external mutation of raw storage
* No concurrent writes

Produce:

| Assumption | Required For | Violation Impact |

---

# Required Output Sections

1. Index Invariant Registry
2. Encode/Decode Symmetry Table
3. Ordering Stability Analysis
4. Namespace Isolation Table
5. Entry Layout Analysis
6. Reverse Relation Integrity
7. Unique Enforcement Equivalence
8. Row/Index Coupling Analysis
9. Replay Equivalence Table
10. High Risk Mutation Paths
11. Storage-Layer Assumptions
12. Overall Index Risk Index (1–10, lower is better)

---

# Scoring Model

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Why This Version Is Stronger

It forces:

* Byte-level reasoning
* Namespace isolation proof
* Encode/decode symmetry proof
* Replay equivalence proof
* Reverse symmetry mapping
* Mutation ordering comparison
* Explicit attack simulation
* Storage assumption declaration

Index integrity failures are catastrophic.

This audit must be the most rigorous one you run.
