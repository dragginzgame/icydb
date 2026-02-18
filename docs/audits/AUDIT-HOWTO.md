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

For each audit date folder (once per day):

6. Run a codebase size snapshot from the workspace `crates/` directory:

```
cd crates && cloc .
```

7. Save the resulting counts in that date folder's `summary.md`.

Each audit must produce:

* Structured tables
* Risk levels
* A numeric risk index (if applicable)
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

Invariant Integrity Risk Index: X/10
Recovery Integrity Risk Index: X/10
Cursor/Ordering Risk Index: X/10
Structure Integrity Risk Index: X/10
Complexity Risk Index: X/10
Velocity Risk Index: X/10
DRY Risk Index: X/10

Codebase Size Snapshot (`cd crates && cloc .`):
- Rust: files=..., blank=..., comment=..., code=...
- SUM: files=..., blank=..., comment=..., code=...

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files): ...
- PlanError variants: ...

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

# 7. Risk Index Guidelines

## Scoring Model

All audits use a Risk Index (1-10 scale).

Lower scores indicate stronger structural health.
Higher scores indicate greater architectural pressure.

This project does not use "health scores."
All ratings are risk-oriented for governance clarity.

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

Threshold guidance:
- 8+ requires architectural attention.
- 9–10 indicates structural instability.

Risk indices must follow this model unless the audit defines a stricter variant:

### 1–3

Low risk / structurally healthy.

### 4–6

Moderate risk / manageable pressure.

### 7–8

High risk / requires monitoring.

### 9–10

Critical risk / structural instability.

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

Governance trigger for planning-surface entropy:

* Track `PlanError` variants weekly.
* For the 0.14 cycle, split `PlanError` into sub-domain enums (for example
  `OrderPlanError`, `AccessPlanError`, `CursorPlanError`) while preserving
  typed variants.
* Do not replace typed variants with string-only catch-alls such as
  `InvalidPlan(String)`.

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
* Maintain consistent risk-index polarity (lower is better).

---

# 11. Long-Term Goal

After several months, you should be able to observe:

* Variant growth trends
* Branch depth trends
* Cross-layer coupling trends
* Public surface creep
* Recovery symmetry stability
* Velocity degradation (if any)

Audits turn architectural risk into measurable telemetry.

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
