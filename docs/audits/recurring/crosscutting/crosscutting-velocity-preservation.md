# WEEKLY AUDIT — Velocity Preservation

`icydb-core`

## Purpose

Evaluate whether the architecture still supports:

* Rapid feature iteration
* Contained feature changes
* Low cross-layer amplification
* Predictable extension cost

This is NOT:

* A correctness audit
* A DRY audit
* A style audit
* A redesign proposal exercise

This audit measures structural feature agility.

## Audit Identity

Keep this as a distinct recurring audit.

Do not merge it with:

* Complexity accretion
* DRY consolidation
* Layer violation
* Module structure

Velocity is the change-cost lens: how expensive feature evolution is in practice.

## Internal Modes (Single Artifact)

Use two internal sections inside one weekly artifact.

Mode A — Empirical Change Surface:

* Feature slice selection
* Change surface mapping (`revised_caf`, `ELS`, containment)
* Edit blast radius and concentration

Mode B — Structural Extension Friction:

* Boundary leakage
* Gravity wells and hub containment
* Enum shock radius
* Subsystem independence
* Independent-axis growth
* Decision surface size

Do not split this into two separate recurring audits.

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
* Planner, executor, and recovery are tightly coupled
* Modules become gravity wells
* A single enum change multiplies update sites across layers

---

# STEP 0 — Run Metadata + Method / Comparability (Mandatory)

Capture method metadata before scoring.

Required run metadata:

* compared baseline report path
* method tag/version
* feature-slice selection source
* subsystem taxonomy version
* boundary-crossing regex/rule set version
* fan-in definition
* hub-family taxonomy version
* independent-axis rule version
* facade/adapters inclusion mode

Produce:

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules |  |  |  |
| subsystem taxonomy |  |  |  |
| boundary crossing rule set |  |  |  |
| fan-in definition |  |  |  |
| hub-family taxonomy |  |  |  |
| independent-axis rule |  |  |  |
| facade/adapters inclusion |  |  |  |

Comparability gate:

* Mark run `non-comparable` if any method component above changed.
* If `non-comparable`, still compute metrics but do not treat deltas as trend evidence.

---

# STEP 1 — Baseline Capture (Mandatory)

Capture baseline values first.

Baseline source rule:

* first run of day (`velocity-preservation.md`): compare to latest prior comparable velocity report (or `N/A`)
* same-day rerun (`velocity-preservation-*.md`): compare to that day's `velocity-preservation.md` baseline

Produce:

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Velocity Risk Index |  |  |  |
| Cross-layer suspect crossings |  |  |  |
| Avg files touched per feature slice |  |  |  |
| Median files touched |  |  |  |
| p95 files touched |  |  |  |
| Top gravity-well fan-in |  |  |  |
| Route-planner high-impact cross-layer families |  |  |  |
| Edit concentration in top 5 modules (%) |  |  |  |
| Fan-in concentration in top 5 modules (%) |  |  |  |
| Decision-site concentration in top 3 enums (%) |  |  |  |

If no prior comparable report exists for the first run of day, mark baseline as `N/A` and treat that run as the daily baseline.

---

# STEP 2 — Feature Slice Selection (Mandatory)

Analyze exactly 3-5 major feature slices per run.

Selection priority:

1. merged PR slices in window
2. milestone/tracker slices when PR metadata is unavailable
3. contiguous commit groups tied to one feature objective when tracker metadata is unavailable

Selection rules:

* Exclude refactor-only slices unless they directly supported a shipped feature objective.
* Use the same selected slice set for STEP 3 and STEP 4.
* Record source type for each slice to keep run-to-run comparability stable.

Produce:

| Feature Slice | Source Type (`PR`/`tracker`/`commits`/`manual`) | Source Reference | Included Reason | Exclusions |
| ---- | ---- | ---- | ---- | ---- |

---

# STEP 3 — Change Surface Mapping (Mode A, Empirical)

Map each selected slice and compute deterministic locality/amplification metrics.

Produce:

| Feature Slice | Files Modified | Subsystems | Layers | Flow Axes Total | Flow Axes Material | Revised CAF | ELS | Containment Score | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Fixed subsystem taxonomy:

* planner/query
* executor/runtime
* cursor/continuation
* access/index
* storage/recovery
* facade/adapters (include only when run metadata says included)

Definitions:

* `revised_caf = max(subsystems, layers) × flow_axes_material`
* `flow_axes_total`: all axes present in the feature shape
* `flow_axes_material`: axes that actually drove edits across distinct logic families
* `ELS (Extension Locality Score) = primary_subsystem_files / total_files_modified`
* `containment_score = subsystems_modified / total_subsystems`

Primary subsystem rule:

* Primary subsystem is the subsystem containing the plurality of modified files.
* Tie-break using documented semantic feature owner when available.

Interpretation:

* `ELS > 0.70`: good locality
* `ELS 0.40-0.70`: moderate
* `ELS < 0.40`: poor locality
* `containment_score <= 0.30`: strongly contained
* `containment_score 0.30-0.60`: moderate
* `containment_score > 0.60`: cross-system change

Flag:

* revised CAF trend up week-over-week
* low ELS on core slices
* high containment scores on routine features

---

# STEP 4 — Edit Blast Radius Summary (Mode A, Empirical)

Use the same selected slices from STEP 2.

Produce sample metadata:

| Sampling Mode | Sample Source | Sample Size | Slice IDs | Comparable |
| ---- | ---- | ----: | ---- | ---- |

Produce blast-radius metrics:

| Metric | Current | Previous | Delta |
| ---- | ----: | ----: | ----: |
| average files touched per feature slice |  |  |  |
| median files touched |  |  |  |
| p95 files touched |  |  |  |

SLO gates:

* median files touched `<= 8`
* p95 files touched `<= 15`

Edit concentration metrics:

| Concentration Metric | Current | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |
| % of slice edits in top 5 modules |  |  |  |  |
| % of fan-in in top 5 modules |  |  |  |  |
| % of decision sites in top 3 enums |  |  |  |  |

If an SLO gate is missed in a comparable run, record an explicit follow-up with owner boundary and target date.

---

# STEP 5 — Boundary Leakage (Mode B, Mechanical + Triaged)

Track crossings with two-stage classification.

Required checks:

* planner -> executor types
* executor -> planner validation helpers
* index -> query-layer AST/types
* cursor -> executable plan internals
* recovery -> query semantics

Produce:

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Previous Suspect | Delta | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Method:

* First pass must be mechanical (regex/rule-set driven).
* Second pass triages into allowed vs suspect crossings.

---

# STEP 6 — Gravity Wells + Hub Containment (Mode B)

Fan-in definition for this audit:

* `fan_in = number of runtime modules referencing the module`
* count import and type-reference sites at module granularity
* exclude tests by default
* exclude generated code by default
* count re-export-driven runtime references when resolved

Produce gravity-well table:

| Module | Class | LOC | LOC Delta | Fan-In | Fan-In Delta | Domains | Edit Frequency (30d) | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Gravity-well classes:

* `growth gravity well`: `LOC delta > 2x weekly average` and `fan-in delta > 1`
* `stable gravity well`: already high fan-in with high edit frequency and multi-domain pressure

Fixed domain categories:

* planner/query
* executor/runtime
* cursor/continuation
* access/index
* storage/recovery

Track hub contract containment with fixed family taxonomy.

Produce:

| Hub Module | Contract Boundary | Cross-Layer Families | Previous | Delta | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |

Cross-layer family taxonomy:

* planner semantics
* access-route contracts
* executor dispatch
* terminal/load shaping
* cursor/continuation
* storage/recovery

Required hubs:

* `executor/route/planner/mod.rs`
* `executor/load/mod.rs` (or split successor modules)

Required route-planner contract:

* planner -> `RouteShape` -> executor dispatch
* route planner consumes access-route contracts, not access internals

Required load-hub containment direction:

* decompose toward `dispatch`, `strategy`, `terminal` seams
* avoid mixed policy + dispatch + terminal logic in one hub file

Gate guidance:

* route-planner high-impact cross-layer families target `<=1`
* persistent `>1` for two comparable runs requires explicit decomposition plan

---

# STEP 7 — Enum Shock Radius (Mode B, Density-Adjusted)

Track enum expansion velocity impact.

Produce:

| Enum | Variants | Switch Sites | Modules Using Enum | Switch Density | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `switch_density = switch_sites / modules_using_enum`
* `shock_radius = variants × switch_density × subsystems`

Flag:

* high shock-radius enums with upward trend
* concentration where top 3 enums dominate decision-site share

---

# STEP 8 — Subsystem Independence Score (Mode B, Size-Adjusted)

Measure subsystem self-sufficiency with small-module noise suppression.

Produce:

| Subsystem | Internal Imports | External Imports | LOC | Independence | Adjusted Independence | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `independence = internal / (internal + external)`
* `adjusted_independence = independence × log(module_loc)`

Low adjusted independence means feature work is coupling-driven in materially sized subsystems.

---

# STEP 9 — Decision-Axis Growth (Mode B, Independence-Aware)

Track axis growth for core operations.

Produce:

| Operation | Axes | Axis Count | Independent Axes | Previous Independent Axes | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- |

Axis-independence rule:

* Count an axis as independent only if it can vary without forcing another axis by contract.
* It must change behavior at a distinct subsystem or decision site.

Risk should be driven by `independent_axes`, not raw axis count.

---

# STEP 10 — Decision Surface Size (Mode B, Change-Relevant)

Track where enum behavior changes require feature updates.

Produce:

| Enum | Decision Sites Requiring Feature Updates | Previous | Delta | Risk |
| ---- | ----: | ----: | ----: | ---- |

Method:

* Mechanical scan is allowed for candidate sites.
* Final count must include only change-relevant decision sites, not every syntactic match.

---

# STEP 11 — Refactor Noise Filter

Before finalizing risk, classify transient spikes.

Rules:

* If module split increases file count but reduces fan-in, mark `structural improvement`.
* If change surface grows while revised CAF and shock radius are flat/down, mark `refactor transient`.

Produce:

| Signal | Raw Trend | Noise Classification | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |

---

# STEP 12 — Velocity Risk Index (Semi-Mechanical, Rubric-Anchored)

Score each bucket (1-10), then apply weighted aggregate.

Weighted buckets:

* enum shock radius ×2
* CAF trend ×2
* cross-layer leakage (suspect) ×2
* gravity-well growth/stability ×2
* hub contract containment ×2
* edit blast radius (SLO-based) ×2

Produce:

| Area | Score | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |

`overall_index = weighted_sum / weight_sum`

Rubric anchors:

* CAF trend:
* `2` median revised CAF `<=4` with flat/down comparable trend
* `5` median `5-6` or one transient spike
* `8` median `>6` with upward comparable trend
* `10` sustained `>6` for two comparable runs without containment improvement
* cross-layer leakage (suspect):
* `2` suspect crossings `<=2` and non-increasing
* `5` suspect crossings `3-5` or flat at moderate level
* `8` suspect crossings `>=6` or delta `>=+2`
* `10` sustained high suspect crossings for two comparable runs
* gravity wells:
* `2` no growth class and no stable high-risk wells
* `5` one stable or one growth well with active containment plan
* `8` multiple stable/growth wells or repeated same-hub pressure
* `10` repeated worsening with no active containment plan
* hub containment:
* `2` all hubs at/below allowed max
* `5` one hub exceeds by `+1` in one run
* `8` persistent overage for two comparable runs
* `10` widening overage with no decomposition commitment
* enum shock radius:
* `2` hotspots flat/down and concentration controlled
* `5` one moderate hotspot rising
* `8` multiple rising hotspots or concentration spike
* `10` sustained hotspot growth with concentrated decision pressure
* edit blast radius:
* `2` median `<=6` and p95 `<=12`
* `5` median `7-8` or p95 `13-15`
* `8` median `>8` and p95 `>15`
* `10` repeated SLO misses across comparable runs

Concentration adjustment:

* If any concentration metric exceeds `70%` in a comparable run, apply `+1` to the most relevant bucket (`enum shock radius`, `gravity wells`, or `edit blast radius`), capped at `10`.

Interpretation:

* `1-3` low risk and structurally healthy
* `4-6` moderate risk and manageable pressure
* `7-8` high risk and needs active containment
* `9-10` critical risk and structural instability

---

# STEP 13 — Final Output + Verification Readout

Final output order:

1. Run Metadata + Method / Comparability
2. Baseline Capture
3. Feature Slice Selection
4. Empirical Change Surface Mapping
5. Edit Blast Radius Summary
6. Boundary Leakage
7. Gravity Wells + Hub Containment
8. Enum Shock Radius
9. Subsystem Independence
10. Independent-Axis Growth
11. Decision Surface Size
12. Refactor Noise Filter
13. Velocity Risk Index
14. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Verification readout must include:

* method comparability status (`comparable` or `non-comparable` with reason)
* whether all mandatory steps/tables are present
* whether SLO gates were evaluated against comparable samples

---

# Anti-Shallow Rule

Do NOT say:

* "Seems modular"
* "Looks maintainable"
* "Separation is clear"

Every claim must include:

* subsystems involved
* layer count or dependency count
* change multiplier estimate
* growth vector

---

# Why This Audit Matters

Velocity audits measure whether the system still bends without breaking when features are added.

That is architectural longevity.
