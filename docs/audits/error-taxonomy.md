# WEEKLY AUDIT — Strict Error Taxonomy

`icydb-core` (+ facade)

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

1. **Corruption**

   * Persistent state invalid
   * Structural invariant broken
   * Decode failure of trusted storage
   * Commit marker inconsistency

2. **Unsupported**

   * Feature not supported
   * Value intentionally not indexable
   * Explicitly blocked operation

3. **Invalid Input**

   * Malformed cursor
   * Invalid query shape
   * Invalid user-provided value
   * Identity decode from untrusted input

4. **Invariant Violation**

   * Logical internal assumption broken
   * Unexpected execution state
   * Index/row mismatch
   * Reverse relation inconsistency

5. **System Failure**

   * Out-of-memory
   * Stable memory failure
   * Trap-level error
   * Unexpected runtime failure

No other semantic domains allowed.

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

Trace how each error variant propagates upward:

Example flow:

```
StoreError → InternalError → QueryError → public Error
```

For each mapping layer:

Verify:

* No reclassification unless domain-compatible
* Corruption is never downgraded
* Invalid input never escalated to corruption
* Unsupported never classified as invariant violation
* System failure never disguised as invalid input

Produce:

| Source Variant | Mapped To | Domain Preserved? | Escalation? | Downgrade? | Risk |

---

# STEP 4 — Corruption Containment Audit

Explicitly verify:

* All corruption-class errors are marked as corruption in public surface
* No corruption is exposed as invalid input
* Corruption always sets correct `ErrorOrigin`
* Corruption never originates from user input parsing

Produce:

| Corruption Variant | Public Classification | Origin | Correct? | Risk |

---

# STEP 5 — Invalid Input Containment Audit

Verify:

* CursorDecodeError is always classified as Invalid Input
* IdentityDecodeError from untrusted source is Invalid Input
* PlanError for malformed query is Invalid Input
* No Invalid Input becomes InternalError
* No Invalid Input sets Corruption origin

Produce:

| Invalid Input Variant | Final Classification | Correct? | Risk |

---

# STEP 6 — Invariant Violation Audit

Verify:

* InvariantViolation variants are never downgraded
* Internal invariants do not leak as Invalid Input
* Executor invariants do not get reclassified as Unsupported
* Recovery invariant violations remain invariant violations

Produce:

| Invariant Variant | Propagation Path | Classification Preserved? | Risk |

---

# STEP 7 — Origin Fidelity Audit

For each error:

Verify correct `ErrorOrigin`:

Expected origins might include:

* Planner
* Executor
* Store
* Recovery
* Cursor
* Identity
* Serialization

Check:

* Store-origin errors not reported as Planner origin
* Planner errors not reported as Executor origin
* Recovery errors clearly marked
* Cursor errors not misattributed to executor
* No origin dropped during mapping

Produce:

| Variant | True Origin | Reported Origin | Match? | Risk |

---

# STEP 8 — Layer Violation Detection

Detect:

* Lower-layer errors inspecting higher-layer types
* Planner wrapping executor errors
* Executor reclassifying planner errors
* Recovery reinterpreting planner errors
* Serialize errors misclassified as corruption without justification

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

If same failure yields different classification in different paths, flag.

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

* Any domain misclassifications
* Any downgrade risks
* Any escalation risks
* Any origin mismatches
* Any mixed-domain structural problems

---

# STEP 12 — Error Classification Matrix

Produce master matrix:

| Variant | Layer | Domain | Origin | Final Public Classification | Correct? |

---

# STEP 13 — Overall Taxonomy Integrity Score

Score:

9–10 → Strict domain containment
7–8 → Minor mapping asymmetries
5–6 → Mixed-domain or escalation risks
3–4 → Frequent reclassification drift
1–2 → Corruption or invalid-input misclassification risk

---

# Hard Constraints

Do NOT:

* Suggest renaming
* Suggest splitting enums
* Suggest merging enums
* Suggest refactors
* Suggest architectural changes

Only classify and identify violations.

---

# Why This Version Is Stronger

It forces:

* Full variant enumeration
* Domain assignment per variant
* Upward propagation tracing
* Origin integrity validation
* Replay vs normal classification comparison
* Corruption containment proof
* Mixed-domain detection
* Escalation/downgrade proof

Error taxonomy is the semantic firewall of the engine.

If classification drifts, the system becomes misleading under failure.

