# WEEKLY AUDIT — Strict Error Taxonomy

`icydb-core` (+ facade)

---

## Purpose

Verify that all error types:

* Are correctly classified
* Preserve semantic meaning across layers
* Are never downgraded or escalated incorrectly
* Preserve origin fidelity
* Do not mix unrelated semantic domains
* Do not leak incorrect layer attribution

This is a classification audit only.

Do NOT:

* Suggest renaming
* Propose refactors
* Discuss style
* Discuss performance
* Propose architectural changes

Only verify classification correctness.

---

# STEP 0 — Semantic Domain Definitions

Use only these domains:

---

## 1. **Corruption**

Applies when:

* Persistent state is invalid
* Structural invariant of stored bytes is broken
* Decode failure occurs on trusted storage
* Commit marker inconsistency is detected
* Index or row bytes cannot be decoded correctly

Rule:

> If the violation originates from persisted bytes being structurally invalid, classify as **Corruption**.

Corruption must never originate from user input parsing.

---

## 2. **Unsupported**

Applies when:

* Feature is intentionally unsupported
* Value is intentionally not indexable
* Explicitly blocked operation
* Storage encoding policy rejects a representable value

Unsupported is a policy fence, not a failure of integrity.

---

## 3. **Invalid Input**

Applies when:

* Malformed cursor
* Invalid query shape
* Invalid user-provided value
* Identity decode from untrusted input
* Operation-level expectation failures (e.g. NotFound, NotUnique, Conflict)

Clarification:

> Operation-level semantic expectation failures (NotFound, Conflict, NotUnique) are classified under **Invalid Input** for taxonomy purposes, because they result from caller intent conflicting with current state — not from corruption or invariant breakage.

Invalid Input must never be escalated to Corruption.

---

## 4. **Invariant Violation**

Applies when:

* Logical internal assumption is broken
* Unexpected execution state occurs
* Index/row mismatch detected during runtime checks
* Reverse relation inconsistency discovered
* Planner/executor disagreement detected

Rule:

> If state is well-formed at the byte level but logically inconsistent during execution, classify as **Invariant Violation**.

Invariant violations must not be downgraded to Invalid Input.

---

## 5. **System Failure**

Applies when:

* Out-of-memory
* Stable memory failure
* Trap-level runtime error
* Unexpected runtime failure not attributable to input or corruption

System Failure must never be disguised as Invalid Input.

---

No other semantic domains are allowed.

---

# STEP 1 — Full Error Enumeration

Enumerate:

* `InternalError`
* `PlanError`
* `QueryError`
* `ErrorClass`
* `ErrorOrigin`
* `CursorDecodeError`
* `IdentityDecodeError`
* Serialize-related errors
* Store-layer errors
* Commit marker errors
* Recovery errors

For each enum:

Produce:

| Enum | Variant | Declared Meaning | Layer |

No variant may be skipped.

---

# STEP 2 — Per-Variant Semantic Classification

For each variant:

Assign exactly one semantic domain.

Produce:

| Variant | Semantic Domain | Justification |

Flag:

* Variants that straddle multiple domains
* Variants whose name suggests one domain but behavior suggests another
* Variants whose domain is unclear

---

# STEP 3 — Upward Mapping Verification

Trace how each error variant propagates upward.

Example:

```
StoreError → InternalError → QueryError → public Error
```

At each mapping layer verify:

* No reclassification unless domain-compatible
* Corruption is never downgraded
* Invalid Input never escalated to Corruption
* Unsupported never classified as Invariant Violation
* System Failure never disguised as Invalid Input

Produce:

| Source Variant | Mapped To | Domain Preserved? | Escalation? | Downgrade? | Risk |

---

# STEP 4 — Corruption Containment Audit

Explicitly verify:

* All corruption-class errors remain Corruption at public boundary
* No corruption is exposed as Invalid Input
* Corruption sets correct `ErrorOrigin`
* Corruption never originates from user input parsing

Produce:

| Corruption Variant | Public Classification | Origin | Correct? | Risk |

---

# STEP 5 — Invalid Input Containment Audit

Verify:

* `CursorDecodeError` is always Invalid Input
* Identity decode from untrusted input is Invalid Input
* PlanError for malformed query is Invalid Input
* No Invalid Input becomes InternalError(Corruption)
* No Invalid Input sets Corruption origin

Produce:

| Invalid Input Variant | Final Classification | Correct? | Risk |

---

# STEP 6 — Invariant Violation Audit

Verify:

* InvariantViolation variants are never downgraded
* Internal invariants do not leak as Invalid Input
* Executor invariants are not reclassified as Unsupported
* Recovery invariant violations remain Invariant Violations

Produce:

| Invariant Variant | Propagation Path | Classification Preserved? | Risk |

---

# STEP 7 — Origin Fidelity Audit

For each error, verify correct `ErrorOrigin`.

Expected origins may include:

* Planner
* Executor
* Store
* Recovery
* Cursor
* Identity
* Serialization
* Interface

Check:

* Store-origin errors not reported as Planner
* Planner errors not reported as Executor
* Recovery errors clearly marked
* Cursor errors not misattributed to Executor
* No origin dropped during mapping

Produce:

| Variant | True Origin | Reported Origin | Match? | Risk |

---

# STEP 8 — Layer Violation Detection

Detect:

* Lower-layer errors inspecting higher-layer types
* Planner wrapping executor errors improperly
* Executor reclassifying planner errors
* Recovery reinterpreting planner errors
* Serialize errors misclassified as Corruption without justification

Produce:

| Violation | Location | Classification Impact | Risk |

---

# STEP 9 — Cross-Path Consistency

Verify classification consistency between:

* Normal execution
* Recovery replay
* Cursor continuation
* Save vs Replace
* Delete vs Replay Delete

If the same failure yields different classification in different paths, flag.

Produce:

| Scenario | Normal Classification | Replay Classification | Consistent? | Risk |

---

# STEP 10 — Mixed-Domain Enum Detection

Identify enums that mix:

* Corruption + Invalid Input
* Invariant Violation + Unsupported
* System Failure + Planner errors

Flag as taxonomy pressure.

Produce:

| Enum | Mixed Domains? | Risk |

---

# STEP 11 — Incorrect Classification List

List:

* Domain misclassifications
* Downgrade risks
* Escalation risks
* Origin mismatches
* Mixed-domain structural problems

---

# STEP 12 — Error Classification Matrix

Produce master matrix:

| Variant | Layer | Domain | Origin | Final Public Classification | Correct? |

---

# STEP 13 — Overall Taxonomy Risk Index

Taxonomy Risk Index (1–10, lower is better):

1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Hard Constraints

Do NOT:

* Suggest renaming
* Suggest splitting enums
* Suggest merging enums
* Suggest refactors
* Suggest architectural changes

Only classify and identify violations.

