# `docs/audits/AUDIT_GOVERNANCE.md`

# Audit Governance (META-AUDIT)

This document audits the quality, rigor, scope discipline, and structural coherence of the audit suite itself.

This is NOT a code audit.

This evaluates:

* Audit clarity
* Audit focus discipline
* Audit redundancy
* Audit drift
* Coverage completeness
* Governance integrity

Run this quarterly or at major milestones.

---

# STEP 1 — Audit Inventory

Enumerate all audit definition files in:

```
docs/audits/
```

Produce:

| Audit | Primary Focus | Last Modified | Overlaps With |

If an audit does not have a clearly defined primary focus in its first 20 lines, flag it.

---

# STEP 2 — Scope Discipline Scoring

For each audit:

Evaluate:

1. Scope is explicitly constrained
2. Non-goals are clearly declared
3. Guardrails are defined
4. It does not drift into:

   * Style commentary
   * Refactor suggestions
   * Feature proposals
   * Performance discussion

Score:

| Audit | Scope Clarity (1–10) | Drift Risk | Notes |

Scoring rubric:

9–10 → Strict boundaries, explicit guardrails
7–8 → Mostly constrained, minor narrative drift
5–6 → Mixed scope, occasional bleed
3–4 → Significant scope creep
1–2 → Unbounded or vague

---

# STEP 3 — Invariant Precision Quality

For audits that claim to evaluate invariants:

Check:

* Are invariants explicitly enumerated?
* Are boundary cases listed?
* Are failure scenarios enumerated?
* Are enforcement locations required?
* Are symmetry checks required (normal vs recovery)?
* Is envelope containment explicitly tested?

Score:

| Audit | Invariant Precision (1–10) | Boundary Enumeration | Risk |

---

# STEP 4 — Structural Depth Evaluation

For each audit:

Check whether it:

* Reasons about layer boundaries
* Identifies cross-layer leakage
* Detects idempotence risk
* Detects envelope escape risk
* Detects mutation ordering differences
* Detects amplification risk
* Detects drift sensitivity

Score:

| Audit | Structural Depth (1–10) | Missing Dimensions | Risk |

---

# STEP 5 — Signal-to-Noise Ratio

Identify audits that:

* Contain vague phrases (“looks clean”, “appears correct”)
* Allow narrative answers without tables
* Do not enforce quantitative measures
* Allow unstructured conclusions

Score:

| Audit | Signal Density (1–10) | Narrative Drift Risk |

---

# STEP 6 — Risk Identification Discipline

For each audit:

Check whether it:

* Requires risk classification
* Requires risk ranking
* Separates high-risk from low-risk findings
* Labels hypothetical vs observed risks
* Defines scoring scale

Score:

| Audit | Risk Discipline (1–10) | Missing Scoring | Risk |

---

# STEP 7 — Redundancy & Overlap Matrix

Construct a matrix of invariant categories vs audits.

Invariant categories may include:

* Ordering
* Envelope safety
* Identity enforcement
* Index consistency
* Reverse relation symmetry
* Recovery idempotence
* Plan immutability
* Layer boundary discipline
* Visibility discipline
* Complexity growth
* Velocity amplification
* DRY divergence

Produce:

| Invariant Category | Audits Covering It | Necessary Overlap? | Redundant? |

Flag:

* Categories covered by 3+ audits unnecessarily
* Categories not covered by any audit

---

# STEP 8 — Audit Bloat Detection

Flag audits that:

* Exceed reasonable length
* Attempt to evaluate too many domains
* Combine structural + velocity + invariant checks
* Contain overlapping tables with other audits

Produce:

| Audit | Scope Expansion Since Last Review | Bloat Risk |

---

# STEP 9 — Drift Since Last Governance Audit

If prior governance audit exists:

Compare:

* Audit count growth
* Scope expansion
* Overlap increase
* Scoring consistency
* Structural focus drift

Produce:

| Area | Previous | Current | Drift | Risk |

---

# STEP 10 — Missing Audit Dimensions

Identify invariant categories or structural dimensions not covered by any audit, such as:

* Plan cache correctness
* Index encode/decode symmetry
* Error taxonomy containment
* Public API creep
* Commit phase expansion risk
* Composite path amplification risk
* AccessPath growth sensitivity

Produce:

| Missing Dimension | Impact | Recommend New Audit? |

---

# STEP 11 — Consolidation Opportunities

Identify:

* Audits that could be merged without losing focus
* Audits that overlap heavily
* Audits that could be narrowed

Do NOT propose redesign.
Only identify governance-level consolidation.

---

# STEP 12 — Governance Health Score

Score the audit framework overall:

| Dimension           | Score (1–10) |
| ------------------- | ------------ |
| Scope Discipline    |              |
| Invariant Precision |              |
| Structural Depth    |              |
| Redundancy Control  |              |
| Drift Detection     |              |
| Risk Clarity        |              |

Overall Audit Governance Score (1–10)

Scale:

9–10 → Strong audit discipline, low meta-drift
7–8 → Minor overlap, manageable
5–6 → Growing audit sprawl
3–4 → Significant duplication or scope bleed
1–2 → Audit framework unstable

---

# Required Output Sections

1. Audit Quality Score per Document
2. Structural Weaknesses per Document
3. Overlap Matrix
4. Drift Warnings
5. Missing Dimensions
6. Consolidation Opportunities
7. Governance Health Score

---

# Hard Constraints

Do NOT:

* Evaluate code
* Evaluate performance
* Propose feature changes
* Suggest refactors outside audit definitions
* Collapse audits without clear redundancy proof

---

# Purpose

Audit governance ensures:

* Audit suite does not sprawl
* Invariant coverage is complete but not redundant
* Structural focus remains sharp
* Risk scoring remains consistent
* Architectural telemetry remains meaningful

Without governance, audits themselves drift.

---

# Why This Version Is Better

It:

* Forces quantification
* Forces overlap mapping
* Detects meta-bloat
* Detects scope creep
* Detects coverage gaps
* Enforces scoring discipline
* Tracks audit growth over time

This keeps the audit framework from becoming what it was meant to prevent.

