# WEEKLY AUDIT — Complexity Accretion (icydb-core)

## Purpose

Measure **conceptual growth, branching pressure, and cognitive load expansion** in `icydb-core`.

This audit tracks structural entropy over time.

It is NOT a correctness audit.
It is NOT a style audit.
It is NOT a redesign proposal exercise.

Only evaluate conceptual complexity growth.

If structural risk is extreme, you may mark it — but do not propose redesign unless necessary.

---

# Hard Constraints

Do NOT discuss:

* Performance
* Code style
* Naming
* Macro aesthetics
* Minor duplication
* Refactors unless risk is high

Focus strictly on:

* Variant growth
* Branch growth
* Flow multiplication
* Concept scattering
* Cognitive stack depth

Assume this audit runs weekly and results are diffed.

---

# STEP 1 — Variant Surface Growth

Quantify the following:

* `PlanError` variant count
* `QueryError` variant count
* `ErrorClass` variant count
* Cursor-related error variants (all types)
* Commit marker types
* `AccessPath` variants
* Policy error types
* Predicate AST node variants
* Commit-phase enums
* Store-layer error variants

For each:

| Enum | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |

Flag:

1. Enums that:

   * Mix planning + execution + storage concerns.
   * Mix policy + invariant + internal failures.
2. Variants that:

   * Duplicate semantic meaning under different names.
   * Exist only to wrap other variants.
3. Enums that are >8–10 variants and still growing.

Identify the fastest-growing enum families.

---

# STEP 2 — Execution Branching Pressure

Identify high-branch-density functions:

Flag functions with:

* > 3 nested `match`
* > 3 nested conditional layers
* > 5 invariant guard returns
* Match-on-enum followed by inner match on another enum
* Plan-type branching inside executor

For each hotspot:

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |

Also detect:

* Branching that depends on:

  * AccessPath variant
  * Predicate type
  * Cursor presence
  * Plan shape
  * Unique vs non-unique index
  * Recovery mode

Flag if:

* A single function handles >3 conceptual domains.
* Branching grows proportionally with enum expansion.

---

# STEP 3 — Execution Path Multiplicity

Count independent execution flows for:

* Save
* Replace
* Delete
* Load
* Recovery replay
* Cursor continuation
* Index mutation
* Referential integrity enforcement

Define “independent flow” as:

A distinct logical sequence that cannot share the same invariant stack.

Produce:

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |

Flag:

* Flows that re-implement similar guard logic.
* Flows that vary only slightly in ordering of steps.
* Flows that require mental simulation of >3 branching axes.

If total flow count per operation exceeds 4, mark as pressure.

---

# STEP 4 — Cross-Cutting Concern Spread

For each concept, count modules implementing it:

* Index id validation
* Key namespace validation
* Component arity enforcement
* Envelope boundary checks
* Reverse relation mutation
* Unique constraint enforcement
* Error origin mapping
* Plan shape enforcement
* Anchor validation
* Bound conversions

Produce:

| Concept | Modules Involved | Centralized? | Risk |

Flag if:

* Appears in >4 modules.
* Implemented with slight variation.
* Requires mental linking across layers.

This measures scattering, not duplication.

---

# STEP 5 — Cognitive Load Indicators

Detect signals of rising mental stack depth:

1. Functions >80–100 logical lines.
2. Test files >3k lines.
3. Repeated invariant check patterns across files.
4. Repeated formatted error strings across modules.
5. Multi-stage validation logic duplicated in different layers.
6. Deep call stacks that mix 3+ conceptual domains.

Produce:

| Area | Indicator Type | Severity | Risk |

Flag if:

* A developer must hold >4 invariants simultaneously in a single function.
* A single change would require edits in >4 modules.

---

# STEP 6 — Drift Sensitivity Index

Identify areas where:

* Enum growth directly increases branching.
* Adding a new AccessPath variant would multiply logic.
* Adding DESC would multiply branches.
* Adding composite paths would double flow count.

For each:

| Area | Growth Vector | Drift Sensitivity | Risk |

---

# STEP 7 — Complexity Risk Score

Provide:

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | --------------- | -------------- | ---------- |

Then compute:

### Overall Complexity Score (1–10)

Scale:

1–3  → Minimal structural pressure
4–6  → Manageable but growing
7–8  → High branching pressure emerging
9–10 → Structural fragility risk

---

# Required Summary

1. Overall Complexity Score
2. Fastest Growing Concept Families
3. Variant Explosion Risks
4. Branching Hotspots
5. Flow Multiplication Risks
6. Cross-Cutting Spread Risks
7. Early Structural Pressure Signals (only if high risk)

---

# Explicit Anti-Shallow Requirement

Do NOT:

* Say “code looks clean”
* Give generic statements
* Provide unquantified claims
* Comment on naming
* Comment on macro usage
* Comment on formatting

Every claim must reference:

* Count
* Structural pattern
* Growth vector
* Branch multiplication factor

---

# Long-Term Goal of This Audit

Detect:

* Variant explosion before it becomes branching explosion.
* Flow duplication before it becomes semantic divergence.
* Concept scattering before it becomes drift.
* Cognitive load before it becomes fragility.

This audit measures structural entropy, not quality.
