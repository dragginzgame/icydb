# `docs/audits/AUDIT_HOWTO.md`

# IcyDB Audit How-To

This document defines the operational procedure for running, recording, and interpreting architectural audits for `icydb-core`.

Audits are not essays.
They are structural telemetry.

They exist to detect drift before it becomes fragility.

---

# 1. Purpose of Audits

Audits measure:

* Invariant preservation
* Recovery symmetry
* Ordering safety
* Structural discipline
* Complexity growth
* Velocity sustainability
* Redundancy divergence risk

Audits are:

* Deterministic
* Structured
* Date-stamped
* Comparable across time

Audits are NOT:

* Refactoring sessions
* Style reviews
* Feature proposals
* Subjective commentary

---

# 2. Audit Types

Current audit definitions live in:

```
docs/audits/
```

Typical audits include:

* invariant-preservation
* recovery-consistency
* cursor-ordering
* boundary-semantics
* structure-visibility
* complexity-accretion
* velocity-preservation
* dry-consolidation

Audit definitions must not be modified during a run.

---

# 3. When to Run Audits

## Mandatory Weekly

Run weekly:

* Invariant Preservation
* Recovery Consistency
* Structure / Visibility
* Complexity Accretion
* Cursor / Ordering Safety

## Per Feature

Run immediately after:

* Adding new AccessPath variant
* Modifying commit marker logic
* Changing cursor encoding
* Expanding error taxonomy
* Introducing new index types
* Expanding recovery logic

## Before Minor Release

Run full audit suite before:

* x.y.0 releases
* Any feature milestone completion

---

# 4. How to Run an Audit

For each audit:

1. Copy the audit definition prompt.
2. Execute it against the current repository state.
3. Do not modify the prompt mid-run.
4. Do not mix multiple audits into one output.
5. Do not summarize findings prematurely.

Each audit must produce:

* Structured tables
* Risk levels
* A numeric score (if applicable)
* Drift-sensitive findings

---

# 5. Storing Audit Results

Audit results must be stored in:

```
docs/audit-results/YYYY-MM-DD/
```

Use ISO format only:

```
2026-02-18
```

Each run must create a new directory.

Never overwrite previous results.

Example:

```
docs/audit-results/2026-02-18/
    invariant-preservation.md
    recovery-consistency.md
    complexity-accretion.md
    velocity-preservation.md
    summary.md
```

---

# 6. Required `summary.md`

Each dated directory must include:

```
summary.md
```

Format:

```
# Audit Summary — YYYY-MM-DD

Invariant Integrity Score: X/10
Recovery Integrity Score: X/10
Cursor/Ordering Safety Score: X/10
Structure Integrity Score: X/10
Complexity Score: X/10
Velocity Score: X/10
DRY Score: X/10

Notable Changes Since Previous Audit:
- +1 AccessPath variant
- +2 PlanError variants
- New branching in load executor
- Reverse index logic expanded
- No public surface change

High Risk Areas:
- ...

Medium Risk Areas:
- ...

Drift Signals:
- ...
```

---

# 7. Rating Guidelines

Scores must follow these scales unless the audit defines its own:

### 9–10

Strong structural health. No material drift.

### 7–8

Minor friction or mild drift signals.

### 5–6

Growing complexity or coupling pressure.

### 3–4

High structural fragility risk emerging.

### 1–2

Critical architectural instability.

Do not inflate scores.

---

# 8. Drift Comparison Procedure

When running a new audit:

1. Compare against the previous date folder.
2. Identify:

   * Enum growth
   * Branch growth
   * Public API growth
   * Cross-layer coupling increase
   * Invariant enforcement movement
3. Record deltas in `summary.md`.

Example:

```
Drift Since 2026-02-11:
- PlanError variants: 8 → 10
- AccessPath variants: unchanged
- Largest module: +120 LOC
- Recovery logic: unchanged
```

Audits are meaningful only when compared.

---

# 9. What to Do With Findings

Findings should be categorized:

* High Risk
* Medium Risk
* Low Risk
* Defensive Duplication (keep)
* Drift-Sensitive

Only after categorization should refactors be considered.

Never refactor during an audit run.

Audit first. Act later.

---

# 10. Audit Discipline Rules

Never:

* Modify audit definitions during a run.
* Collapse multiple audits into one report.
* Skip tables.
* Skip numeric scores.
* Replace structured findings with narrative text.
* Overwrite previous results.
* Downgrade a finding without explanation.

Always:

* Date-stamp.
* Store results.
* Compare to previous run.
* Maintain consistent scoring scale.

---

# 11. Long-Term Goal

After several months, you should be able to observe:

* Variant growth trends
* Branch depth trends
* Cross-layer coupling trends
* Public surface creep
* Recovery symmetry stability
* Velocity degradation (if any)

Audits turn architectural health into measurable telemetry.

Without telemetry, drift is invisible.

---

# 12. Philosophy of Auditing

Audits are:

* Preventative
* Structural
* Non-reactive
* Discipline-enforcing

They are not reactive debugging tools.

They exist to prevent the class of bugs that appear 6 months later.

---

# 13. Optional Enhancement (Future)

Consider adding:

```
docs/audit-metrics.csv
```

With columns such as:

```
date,
plan_error_count,
access_path_count,
public_types_count,
largest_module_loc,
avg_match_depth,
total_audit_score
```

This converts architectural health into measurable data.

---

# 14. Summary

IcyDB audits form a feedback loop:

1. Build feature.
2. Run audit.
3. Record results.
4. Compare drift.
5. Address structural pressure.
6. Repeat.

This keeps the engine:

* Deterministic
* Bounded
* Architecturally stable
* Safe to extend

Audits are part of the engineering system, not optional reviews.
