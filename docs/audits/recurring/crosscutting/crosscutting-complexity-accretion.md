# WEEKLY AUDIT — Complexity Accretion (icydb-core)

## Purpose

Measure **conceptual growth, branching pressure, and cognitive load expansion** in `icydb-core`.

This audit tracks structural entropy over time.

It is NOT a correctness audit.
It is NOT a style audit.
It is NOT a redesign proposal exercise.

Only evaluate conceptual complexity growth.

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

# STEP 0 — Baseline Capture (Mandatory)

Capture baseline values before computing current metrics.

Baseline source rule:

* first run of day (`complexity-accretion.md`): compare to the latest prior
  comparable complexity report (or `N/A` if none)
* same-day rerun (`complexity-accretion-*.md`): compare to that day's
  `complexity-accretion.md` baseline

Produce:

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Total runtime files in scope |  |  |  |
| Runtime LOC |  |  |  |
| Files >= 600 LOC |  |  |  |
| Continuation mentions |  |  |  |
| Continuation decision owners |  |  |  |
| AccessPath decision owners |  |  |  |
| AccessPath executor dispatch sites |  |  |  |
| RouteShape decision owners |  |  |  |
| Predicate coercion decision owners |  |  |  |
| Continuation execution consumers |  |  |  |
| Continuation plumbing modules |  |  |  |

If no prior comparable report exists for the first run of day, mark previous
values as `N/A` and treat that first run as the daily baseline.

---

# STEP 1 — Variant Surface Growth + Branch Multiplier

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

| Enum | Variants | Switch Sites | Branch Multiplier | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ----: | ----: | ----: | ---- | ---- | ---- |

Definitions:

* `switch_sites` = number of distinct match/switch callsites over that enum in runtime scope.
* `branch_multiplier` = `variants × switch_sites`.
* `AccessPath executor dispatch sites` = distinct runtime executor callsites that branch on executable AccessPath shape (for example via centralized dispatch adapters).

Flag:

* `branch_multiplier` trend up week-over-week.
* Enums >8 variants and still growing.
* Enums mixing planning + execution + storage semantics.
* Any increase in `AccessPath executor dispatch sites` without an explicit dispatch-consolidation note.

---

# STEP 2 — Execution Branching Pressure (Trend-Based)

Identify high-branch-density functions and compare against previous run.

For each hotspot:

| Function | Module | Branch Layers | Match Depth | Previous Branch Layers | Delta | Domains Mixed | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Also detect axis coupling in each function:

* Access path type
* Predicate type
* Cursor presence
* Plan shape
* Index uniqueness
* Recovery mode

Flag:

* Any function with `domains_mixed > 3`.
* Positive weekly branch-layer growth.
* Functions where enum growth directly increased branch layers.

---

# STEP 3 — Execution Path Multiplicity (Effective Flows)

For each core operation (`save`, `replace`, `delete`, `load`, `recovery replay`, `cursor continuation`, `index mutation`), compute flow count via decision axes.

Use this model:

1. `theoretical_space = Π(axis cardinalities)`
2. Apply contract constraints and remove illegal combinations.
3. `effective_flows = sum(valid combinations)`

Required axis set (add/remove only with explicit note):

* operation type
* access path type
* cursor presence
* recovery mode
* index uniqueness
* ordering mode

Produce:

| Operation | Axes Used | Axis Cardinalities | Theoretical Space | Effective Flows | Previous Effective Flows | Delta | Shared Core? | Risk |
| ---- | ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |

Flag:

* `effective_flows > 4` (pressure)
* `axis_count >= 4` (multiplication onset)
* growth in effective flows without equivalent owner consolidation

---

# STEP 4 — Cross-Cutting Concern Spread (Authority vs Plumbing)

For each concept, classify usage by ownership and layer.

Target concepts:

* Continuation / cursor anchor semantics
* AccessPath decision semantics
* RouteShape decision semantics
* Predicate coercion decision semantics
* Envelope boundary checks
* Bound conversions
* Plan shape enforcement
* Error origin mapping
* Index id / namespace validation

Produce:

| Concept | Decision Owners | Execution Consumers | Plumbing Modules | Total Modules | Semantic Layers | Transport Layers | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |

Definitions:

* `decision owners` = modules that define protocol rules/policies.
* `execution consumers` = modules that branch on concept state to execute behavior.
* `plumbing modules` = DTO/transport/projection modules that only carry values.

Risk should be driven by `decision owners` and `semantic layers`, not raw mention totals.

Flag:

* `semantic_layer_count >= 3` (architectural leakage).
* semantic owner growth without explicit boundary consolidation.
* Any increase in `AccessPath`, `RouteShape`, or predicate coercion decision-owner count without an explicit ownership-consolidation note.

---

# STEP 5 — Cognitive Load Indicators (Hub + Call Depth)

Compute structural mental-load signals:

1. Functions > 80–100 logical lines.
2. Deep core-operation call depth.
3. Hub pressure modules.

Hub pressure definition:

* `LOC > 600` AND `domain_count >= 3`

Domain count categories:

* cursor/continuation
* access/index
* predicate/filter
* query/plan
* storage/commit

Produce:

| Module/Operation | LOC or Call Depth | Domain Count | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- |

Flag:

* `call_depth > 6` for core operations.
* rising hub pressure across consecutive runs.

---

# STEP 6 — Drift Sensitivity (Axis Count)

Quantify areas where growth vectors multiply structural cost.

Produce:

| Area | Decision Axes | Axis Count | Branch Multiplier | Drift Sensitivity | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |

Flag:

* `axis_count >= 4`
* branch multiplier growth tied to new variants

---

# STEP 7 — Complexity Risk Index (Semi-Mechanical)

Score each bucket 1–10, then compute weighted aggregate:

* variant explosion risk ×2
* branching pressure trend ×2
* flow multiplicity ×2
* cross-layer spread ×3
* hub pressure + call depth ×2

Produce:

| Area | Score (1-10) | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |

`overall_index = weighted_sum / weight_sum`

Interpretation:

* 1–3 = Low risk / structurally healthy
* 4–6 = Moderate risk / manageable pressure
* 7–8 = High risk / requires monitoring
* 9–10 = Critical risk / structural instability

---

# STEP 8 — Refactor Noise Filter

Before finalizing risk, apply this filter:

* If concept mentions increase **and** decision owners decrease/hold,
  mark as `refactor transient`.
* If decision-owner count increases for `AccessPath`, `RouteShape`, or predicate coercion,
  do NOT mark as transient without a documented ownership consolidation.
* If file count increases due module split **and** hub pressure decreases,
  mark as `structural improvement`.

Produce:

| Signal | Raw Trend | Noise Filter Result | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |

---

# Required Summary

0. Run Metadata + Comparability Note
1. Overall Complexity Risk Index
2. Fastest Growing Concept Families
3. Highest Branch Multipliers
4. Flow Multiplication Risks (axis-based)
5. Cross-Layer Spread Risks (owner vs plumbing aware)
6. Hub Pressure + Call-Depth Warnings
7. Refactor-Transient vs True-Entropy Findings
8. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Run metadata must include:

- compared baseline report path (daily baseline rule: first run of day compares
  to latest prior comparable report or `N/A`; same-day reruns compare to that
  day's `complexity-accretion.md` baseline)
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

---

# Explicit Anti-Shallow Requirement

Do NOT:

* Say "code looks clean"
* Give generic statements
* Provide unquantified claims
* Comment on naming
* Comment on macro usage
* Comment on formatting

Every claim must reference:

* Count
* Structural pattern
* Growth vector
* Branch multiplier or axis product

---

# Long-Term Goal of This Audit

Detect:

* Variant explosion before branching explosion
* Flow multiplication before semantic divergence
* Concept leakage before cross-layer drift
* Cognitive load growth before fragility

This audit measures structural entropy, not quality.
