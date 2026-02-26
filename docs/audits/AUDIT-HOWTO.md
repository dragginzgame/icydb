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

1. Freeze the architecture model first by restating the `0.30` layer stack from `docs/design/0.30-execution-kernel.md` (`Frozen Layer Stack` section), and require findings to evaluate strictly against that model.
2. Copy the audit definition prompt.
3. Execute it against the current repository state.
4. Do not modify the prompt mid-run.
5. Do not mix multiple audits into one output.
6. Do not summarize findings prematurely.
7. Immediately create or update that date folder's `summary.md` after the audit completes.
8. Update `summary.md` after **every** audit run on that date (not only at end-of-day).

For each audit date folder (once per day):

9. Run codebase size snapshots from the workspace `crates/` directory, separating test files (`tests.rs` and anything under `tests/`) from non-test files whenever possible:

```
cd crates
cloc . --not-match-f='(^|/)(tests\.rs$|tests/)'
cloc . --match-f='(^|/)(tests\.rs$|tests/)'
```

10. Save both snapshots in that date folder's `summary.md`:
   - non-test snapshot (`--not-match-f='(^|/)(tests\.rs$|tests/)'`)
   - test snapshot (`--match-f='(^|/)(tests\.rs$|tests/)'`)
   - optional combined total if needed for trend continuity
11. Capture the current Rust test count with:

```
rg -o '#\\[(tokio::)?test\\]' crates --glob '*.rs' | wc -l
```

12. Save the resulting test count in that date folder's `summary.md`.

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

`summary.md` is a rolling artifact for that date folder:
- It MUST be created on the first audit run of the day.
- It MUST be updated after each subsequent audit run the same day.
- It MUST always reflect the latest completed audit state for that date.

Format template:

```md
# Audit Summary — YYYY-MM-DD

## Risk Index Summary

| Risk Index          | Score | Run Context                                         |
| ------------------- | ----- | --------------------------------------------------- |
| Invariant Integrity | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Recovery Integrity  | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Cursor/Ordering     | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Index Integrity     | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| State-Machine       | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Structure Integrity | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Complexity          | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Velocity            | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| DRY                 | X/10  | (from YYYY-MM-DD) or (run on current working tree) |
| Taxonomy            | X/10  | (from YYYY-MM-DD) or (run on current working tree) |

## Risk Index Summary (Vertical Format)

Invariant Integrity
- Score: X/10
- Run Context: ...

Recovery Integrity
- Score: X/10
- Run Context: ...

Cursor/Ordering
- Score: X/10
- Run Context: ...

Index Integrity
- Score: X/10
- Run Context: ...

State-Machine
- Score: X/10
- Run Context: ...

Structure Integrity
- Score: X/10
- Run Context: ...

Complexity
- Score: X/10
- Run Context: ...

Velocity
- Score: X/10
- Run Context: ...

DRY
- Score: X/10
- Run Context: ...

Taxonomy
- Score: X/10
- Run Context: ...

Codebase Size Snapshot (split `cloc` runs):
- Non-test Rust (`cd crates && cloc . --not-match-f='(^|/)(tests\.rs$|tests/)'`): files=..., blank=..., comment=..., code=...
- Test files only (`cd crates && cloc . --match-f='(^|/)(tests\.rs$|tests/)'`): files=..., blank=..., comment=..., code=...
- Optional combined total (if reported): files=..., blank=..., comment=..., code=...

Structural Stress Metrics:
- AccessPath fan-out count (non-test db files, `rg -l "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): ...
- AccessPath token references (non-test db files, `rg -n "AccessPath::" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs' | wc -l`): ...
- PlanError variants: ...
- Test count (`rg -o '#\\[(tokio::)?test\\]' crates --glob '*.rs' | wc -l`): ...

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

`Risk Index Summary` must include both, in this order:
- `## Risk Index Summary` (table)
- `## Risk Index Summary (Vertical Format)` (one block per index)

`Risk Index Summary` table formatting is strict:
- Use padded spaces so raw markdown columns are visually aligned.
- Use the exact header names (`Risk Index`, `Score`, `Run Context`).
- Keep score cells width-aligned (`X/10` style).

Vertical blocks must use this exact shape:
- `<Risk Index Name>`
- `- Score: X/10`
- `- Run Context: ...`
- one blank line between index blocks

Free-form single-line score lists are not allowed.

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
