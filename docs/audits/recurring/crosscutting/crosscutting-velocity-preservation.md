# WEEKLY AUDIT — Velocity Preservation

`icydb-core`

## Purpose

Evaluate whether the current architecture still supports:

* Rapid feature iteration
* Contained feature changes
* Low cross-layer amplification
* Predictable extension

This is NOT:

* A correctness audit
* A DRY audit
* A style audit
* A redesign proposal exercise

This audit measures structural feature agility.

---

# Core Principle

Low-risk velocity architecture has:

* Contained change surfaces
* Stable layer boundaries
* Low cross-cutting amplification
* Clear ownership per subsystem
* Predictable growth vectors

Velocity degrades when:

* Features require multi-layer edits
* Planner / executor / recovery are tightly coupled
* Modules become gravity wells
* A single enum addition multiplies branch count across layers

---

# STEP 0 — Baseline Capture (Mandatory)

Capture previous-run values first.

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index |  |  |  |
| Cross-layer leakage crossings |  |  |  |
| Avg files touched per feature slice |  |  |  |
| p95 files touched |  |  |  |
| Top gravity-well fan-in |  |  |  |

If previous data is unavailable, mark baseline as `N/A`.

---

# STEP 1 — Change Surface Mapping (Empirical, Revised CAF)

Analyze the last 3–5 major feature slices.

For each feature, produce:

| Feature | Files Modified | Subsystems | Layers | Flow Axes | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `revised_caf = max(subsystems, layers) × flow_axes`
* `ELS (Extension Locality Score) = primary_subsystem_files / total_files_modified`
* `containment_score = subsystems_modified / total_subsystems`

Flow-axis examples:

* cursor presence
* access path type
* ordering
* recovery mode
* index uniqueness

ELS interpretation:

* `>0.70` good locality
* `0.40–0.70` moderate
* `<0.40` poor locality

Containment interpretation:

* `<=0.30` strongly contained
* `0.30–0.60` moderate
* `>0.60` cross-system change

Flag:

* revised CAF trend up week-over-week
* low ELS on core slices
* high containment scores on routine features

---

# STEP 2 — Boundary Leakage (Mechanical)

Track import and type-reference crossings with explicit rules.

Required checks:

* planner -> executor types
* executor -> planner validation helpers
* index -> query-layer AST/types
* cursor -> executable plan internals
* recovery -> query semantics

Produce:

| Boundary | Import Crossings | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |

This step must be regex/mechanical first, then manually triaged.

---

# STEP 3 — Gravity Well Growth Rate

Identify gravity-well modules using growth-rate signals.

Produce:

| Module | LOC | LOC Delta | Fan-In | Fan-In Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Gravity-well condition:

* `LOC delta > 2x weekly average` AND `fan-in delta > 1`

Escalation condition:

* high fan-in + high edit frequency

Domain count categories:

* planner/query
* executor/runtime
* cursor/continuation
* access/index
* storage/recovery

---

# STEP 4 — Change Multiplier Matrix (Deterministic)

Map feature axes to subsystems, then compute deterministic multiplier.

Produce:

| Feature Axis | Planner | Executor | Cursor | Index | Recovery | Subsystem Count |
| ---- | ---- | ---- | ---- | ---- | ---- | ----: |

`subsystem_count = number of checked cells`

Then summarize:

| Candidate Feature | Axes Involved | Subsystem Count | Friction |
| ---- | ---- | ----: | ---- |

---

# STEP 5 — Enum Shock Radius (Density-Adjusted)

Track enum expansion velocity impact.

Produce:

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `switch_density = switch_sites / modules_using_enum`
* `shock_radius = variants × switch_density × subsystems`

Flag:

* high shock-radius enums with upward trend.

---

# STEP 6 — Edit Blast Radius (Empirical)

Use feature slices in the current audit window (or PR history when available).

Produce:

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice |  |  |  |
| median files touched |  |  |  |
| p95 files touched |  |  |  |

If PR-level history is unavailable locally, compute from audited feature slices and mark as `slice-sampled`.

---

# STEP 7 — Subsystem Independence Score (Size-Adjusted)

Measure subsystem self-sufficiency with small-module noise suppression.

Produce:

| Subsystem | Internal Imports | External Imports | LOC | Independence | Adjusted Independence | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `independence = internal / (internal + external)`
* `adjusted_independence = independence × log(module_loc)`

Low adjusted independence means feature work is coupling-driven in materially sized subsystems.

---

# STEP 8 — Decision-Axis Growth (Independence-Aware)

Track axis growth for core operations.

Produce:

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |

Risk should be driven by `independent_axes`, not raw axis count.

---

# STEP 9 — Decision Surface Size

Track where behavior is actually decided for key enums.

Produce:

| Enum | Decision Sites | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |

`decision_sites = match/if decision points over that enum`

This is an early warning for branch growth before variant growth.

---

# STEP 10 — Refactor Noise Filter

Before finalizing risk, classify transient spikes.

Rules:

* If module split increases file count but reduces fan-in, mark `structural improvement`.
* If change surface grows while revised CAF and shock radius are flat/down, mark `refactor transient`.

Produce:

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |

---

# STEP 11 — Velocity Risk Index (Semi-Mechanical)

Score each bucket (1–10), then apply weighted aggregate:

* enum shock radius ×3
* CAF trend ×2
* cross-layer leakage ×2
* gravity-well growth ×2
* edit blast radius ×1

Produce:

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |

`overall_index = weighted_sum / weight_sum`

Interpretation:

* 1–3 = Low risk / structurally healthy
* 4–6 = Moderate risk / manageable pressure
* 7–8 = High risk / requires monitoring
* 9–10 = Critical risk / structural instability

---

# Final Output

1. Velocity Risk Index (1–10, lower is better)
2. Revised CAF + ELS + Containment summary
3. Boundary Leakage Trend Table
4. Gravity-Well Growth + Edit Frequency Table
5. Density-Adjusted Enum Shock Radius Hotspots
6. Edit Blast Radius Summary
7. Size-Adjusted Subsystem Independence Scores
8. Independent-Axis Growth Warnings
9. Decision Surface Size Trends
10. Refactor-Transient vs True-Drag Findings

---

# Anti-Shallow Rule

Do NOT say:

* "Seems modular"
* "Looks maintainable"
* "Separation is clear"

Every claim must include:

* Subsystems involved
* Layer count or dependency count
* Change multiplier estimate
* Growth vector

---

# Why This Audit Matters

Velocity audits measure whether the system still bends without breaking when features are added.

That is architectural longevity.
