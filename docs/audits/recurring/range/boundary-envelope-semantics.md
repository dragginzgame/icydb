# WEEKLY AUDIT — Planner Boundary & Envelope Semantics

`icydb-core`

## Purpose

Verify strict preservation of:

* Range envelope containment
* Inclusive/exclusive semantics
* Strict continuation rules
* Raw vs logical ordering alignment
* AccessPath immutability under cursor continuation

This audit evaluates correctness only.

Do NOT discuss:

* Performance
* Refactoring
* Feature additions
* Style

---

# STEP 0 — Invariant Registry (Mandatory First Step)

Before analyzing code, enumerate and freeze the invariants.

At minimum:

### A. Resume Invariant

* Continuation must always rewrite lower bound as:

  ```
  Bound::Excluded(anchor)
  ```
* Resume must be strictly monotonic.
* Resume must never include the anchor.

### B. Envelope Containment Invariant

* Anchor must lie within original `[lower, upper]` envelope.
* Continuation must not widen the envelope.
* Upper bound must remain immutable.

### C. Inclusivity/Exclusivity Invariant

* Logical `> >= < <=` semantics must map 1:1 to raw bounds.
* No inversion of inclusive/exclusive flags.
* Equal-bound tightening must only make range stricter, never looser.

### D. Ordering Alignment Invariant

* Raw index key lexicographic ordering defines canonical order.
* Logical comparator must not diverge from raw ordering.
* No secondary ordering path may exist.

### E. AccessPath Immutability Invariant

* Cursor continuation must not:

  * Change index id
  * Change access path variant
  * Widen predicate
  * Modify upper bound
  * Introduce composite path

Produce:

| Invariant | Enforced Where | Structural or Implicit? |

---

# STEP 1 — Bound Transformation Proof Table

For each transformation:

* Identify the invariant it must preserve.
* Explain *why* it preserves that invariant.
* Identify whether protection is:

  * Structural (enforced by type or logic)
  * Guarded (runtime check)
  * Implicit (assumed by construction)

Produce:

| Location | Transformation | Invariant Preserved | Enforcement Type | Risk |

Do NOT use “Correct? Yes”.

---

# STEP 2 — Envelope Containment Attack Matrix

Simulate explicitly:

1. Anchor == lower (Included)
2. Anchor == lower (Excluded)
3. Anchor == upper (Included)
4. Anchor == upper (Excluded)
5. Anchor just below lower
6. Anchor just above upper
7. Empty range
8. Single-element range
9. Unbounded range (`lower=None`, bounded upper)
10. Unbounded range (bounded lower, `upper=None`)
11. Continuation produces empty envelope
12. Composite or mutated AccessPath

Required empty-envelope proof (`#11`):

* continuation rewrite yields strict empty envelope
* traversal stops immediately
* no store scan occurs after empty-envelope detection

For each:

* Is escape structurally impossible?
* Is escape prevented by runtime check?
* Is it only prevented by tests?
* Is it drift-sensitive?
* Is unbounded-side handling explicit (`lower=None` / `upper=None`)?

Produce:

| Scenario | Lower=None? | Upper=None? | Structural Prevention? | Runtime Guard? | Test Only? | Risk |

---

# STEP 3 — Upper Bound Immutability Verification

Explicitly verify:

* Cursor continuation does not modify upper bound.
* No code path rewrites upper bound.
* No tightening or widening of upper occurs during continuation.
* Upper bound is passed through unchanged to store traversal.

Produce:

| Code Path | Upper Modified? | Proven Immutable? | Risk |

---

# STEP 4 — Raw vs Logical Ordering Alignment

Explicitly verify:

* Canonical encode preserves lexicographic ordering.
* Logical comparator is identical to raw ordering comparator.
* No alternate comparator path exists.
* No fallback scan reorders entities.

Produce:

| Layer | Ordering Source | Divergence Possible? | Risk |

---

# STEP 4A — Logical → Raw Bound Mapping Table (Mandatory)

Explicitly restate and verify the logical-operator to raw-bound mapping.

Produce:

| Logical Operator | Raw Lower Bound | Raw Upper Bound | Enforced Where | Drift Risk |
| ---------------- | --------------- | --------------- | -------------- | ---------- |

Required mapping rows:

* `>`  -> `Excluded(v)` lower
* `>=` -> `Included(v)` lower
* `<`  -> `Excluded(v)` upper
* `<=` -> `Included(v)` upper

---

# STEP 5 — Anchor/Boundary Consistency Check

Explicitly evaluate:

* Anchor validity check
* Boundary validity check
* Mutual consistency between anchor and boundary

Determine:

* Is inconsistency structurally impossible?
* Is it guarded?
* Is it drift-sensitive?
* Is it a correctness hole or only test gap?

Produce:

| Issue | Structural? | Guarded? | Drift-Sensitive? | Risk Level |

---

# STEP 6 — Composite AccessPath + Cursor/Plan Binding Containment

Verify explicitly:

* Cursor cannot convert IndexRange to composite path.
* Cursor cannot introduce new path type.
* Cursor cannot change index id.
* Planner revalidation prevents mutation of plan shape.
* Cursor contains access-path fingerprint (or equivalent shape identity).
* Resume plan must match bound cursor plan identity.
* Mismatched plan identity is rejected before execution.

Produce:

| Property | Mutable? | Prevention Mechanism | Risk |

---

# STEP 7 — Duplication / Omission Guarantee

## STEP 7A — Resume Monotonicity Proof (Mandatory)

Produce:

| Property | Mechanism | Structural? | Risk |
| -------- | --------- | ----------- | ---- |

Required properties:

* resume lower bound strictly increases (`Bound::Excluded(anchor)`)
* anchor cannot reappear in resumed scan
* equal-bound collapse is handled deterministically

Explicitly verify:

* Strict monotonicity proof.
* No equal-bound duplication.
* No off-by-one omission.
* Store traversal respects bounds strictly.
* Logical post-filtering does not reintroduce duplicates.

Produce:

| Mechanism | Duplication Possible? | Omission Possible? | Risk |

---

# STEP 8 — Canonical Envelope Definition (Mandatory)

Auditor must restate canonical continuation envelope semantics before drift
analysis.

Required definition:

* `effective_envelope = (lower', upper)`
* `lower' = Bound::Excluded(anchor)` for resumed scans

Also state:

* upper bound is immutable across continuation rewrite
* empty-envelope detection condition and expected no-scan outcome

Produce:

| Definition Element | Stated? | Verified In Code? | Risk |
| ------------------ | ------- | ----------------- | ---- |

---

# STEP 9 — Drift Sensitivity Analysis

Identify:

* Assumptions not enforced structurally.
* Areas relying on canonical ordering alignment.
* Areas lacking adversarial tests.
* Areas where adding DESC would multiply risk.
* Areas where composite support would introduce envelope ambiguity.

Produce:

| Drift Vector | Impacted Invariant | Risk |

---

# Required Output Sections

0. Run Metadata + Comparability Note
1. Invariant Registry
2. Bound Transformation Proof Table
3. Envelope Attack Matrix (including unbounded and empty-envelope scenario)
4. Upper Bound Immutability
5. Ordering Alignment
6. Logical → Raw Bound Mapping Table
7. Anchor/Boundary Consistency
8. Composite + Cursor/Plan Binding Containment
9. Resume Monotonicity Proof
10. Duplication/Omission Proof
11. Canonical Envelope Definition
12. Drift Sensitivity
13. Overall Envelope Risk Index (1–10, lower is better)
14. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Run metadata must include:

- compared baseline report path (or `N/A`)
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

---

# Scoring Model

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Why This Is Stronger

This version:

* Eliminates shallow “Correct? Yes” answers
* Forces invariant restatement
* Forces immutability verification
* Separates structural vs test-based safety
* Forces composite-path containment validation
* Forces drift analysis
* Forces monotonicity proof

It converts the audit from evaluation into formal reasoning.
