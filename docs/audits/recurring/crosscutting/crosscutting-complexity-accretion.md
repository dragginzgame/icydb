This audit must not reuse structural counts from other audits in the same run.
All metrics must originate from STEP -1 enumeration or the metrics dataset.

# WEEKLY AUDIT — Complexity Accretion (icydb-core)

## Purpose

Measure conceptual growth, branching pressure, and cognitive load expansion in
`icydb-core`.

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

# Measurement Classes + Evidence Modes (Mandatory)

Column classes:

* `[M]` Mechanical: directly derivable from code or dataset.
* `[C]` Classified: analyst judgment required.
* `[D]` Derived: formula over mechanical columns.

Evidence modes:

* `mechanical` (high confidence)
* `semi-mechanical` (medium confidence)
* `classified` (medium/low confidence)

Requirements:

* Every produced metric column MUST be labeled by `[M]`, `[C]`, or `[D]`
  (either in the header or via an explicit `Class` column).
* Every step MUST declare one evidence mode.
* Mention counts are weak context signals and MUST NOT drive risk alone.

---

# Method Contract (Mandatory)

Method manifest (include exactly in run metadata):

* `method_version = CA-1.4`
* `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
* `domain_taxonomy = D-2`
* `flow_axis_model = F-1`
* `switch_site_rule = S-1`
* `risk_rubric = R-1`
* `trend_filter_rule = T-1`

CA-1.4 changes from CA-1.3:

* introduces explicit completion status (`complete`, `partial`, `blocked`)
* requires a status row for every step, including skipped classified sections
* separates generator-backed mechanical risk from full overall complexity risk
* adds a mandatory issue ledger for follow-up actions
* adds explicit artifact naming for enum, concept, flow, invalidating-signal,
  and risk-bucket outputs

Comparability gate:

* Baseline is comparable only when all method manifest tags are unchanged.
* Mark run `non-comparable` if any of the following changed since baseline:
  * runtime scope definition
  * exclusion rules
  * metric generator
  * branch-counting rules
  * concept taxonomy
  * flow axis set/model
  * domain taxonomy mapping
  * switch-site rule

Completion gate:

* `complete` = STEP -1 through STEP 9 all have produced tables or explicit
  `N/A` rows allowed by this method, and STEP 7 includes all risk buckets.
* `partial` = STEP -1 completed, but one or more non-mechanical/classified
  steps are blocked or intentionally deferred.
* `blocked` = STEP -1 failed or the runtime metrics dataset is missing.
* A `partial` run MUST NOT publish an unqualified "overall complexity risk
  index"; it must publish `mechanical-only risk index` or `partial risk index`
  and list missing sections in the report preamble.
* A `blocked` run MUST NOT compare against a baseline except to say the run is
  non-comparable.
* Silent omission is forbidden: every step from STEP -1 through STEP 9 must
  appear in the report with status `PASS`, `N/A`, or `BLOCKED`.

Generator-governance note:

* The preferred generator is the canonical method artifact. Codex may run its
  embedded Python locally for audit extraction when that Python does not become
  committed project scripts, CI, tests, build helpers, or repo tooling. If the
  generator cannot run for environmental reasons, mark STEP -1 `BLOCKED` and
  do not substitute ad-hoc extraction unless the report is explicitly marked
  `non-comparable`.

---

# Execution Integrity Requirements (Mandatory)

You must read the entire runtime module tree before computing metrics.
Do not reuse partial results from other audits.

Run crosscutting audits sequentially in this order (do not run in parallel):

1. `complexity-accretion`
2. `canonical-semantic-authority`
3. `dry-consolidation`
4. `layer-violation`
5. `module-structure`
6. `velocity-preservation`
7. `wasm-footprint`

Generate runtime metrics once per run and reuse that dataset in later
crosscutting audits.

Preferred generator: `scripts/audit/runtime_metrics.sh`

Report preamble MUST include:

| Field [M] | Value |
| ---- | ---- |
| `method_version` | `CA-1.4` |
| `completion_status` | `complete` / `partial` / `blocked` |
| `risk_index_kind` | `overall` / `partial` / `mechanical-only` / `N/A` |
| `baseline_report` | path or `N/A` |
| `comparability_status` | `comparable` / `comparable with caveat` / `non-comparable` |
| `missing_sections` | comma-separated step IDs or `none` |

Every report MUST include this step-status table before conclusions:

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP -1 |  |  |  |
| STEP 0 |  |  |  |
| STEP 1 |  |  |  |
| STEP 2 |  |  |  |
| STEP 2A |  |  |  |
| STEP 3 |  |  |  |
| STEP 4 |  |  |  |
| STEP 4A |  |  |  |
| STEP 4B |  |  |  |
| STEP 5 |  |  |  |
| STEP 5A |  |  |  |
| STEP 6 |  |  |  |
| STEP 7 |  |  |  |
| STEP 8 |  |  |  |
| STEP 8A |  |  |  |
| STEP 8B |  |  |  |
| STEP 9 |  |  |  |

Allowed statuses:

* `PASS`: table and supporting artifact/evidence are present.
* `N/A`: method explicitly allows no prior data or no matching concept.
* `BLOCKED`: evidence could not be produced; reason and comparability impact
  are mandatory.

Required dataset columns:

* `module [M]`
* `loc [M]`
* `match_count [M]`
* `match_arms_total [M]`
* `avg_match_arms [D]`
* `if_count [M]`
* `if_chain_count [M]`
* `max_branch_depth [M]`
* `fanout [M]`
* `branch_sites_total [D]`

Required report artifacts:

| Artifact [M] | Producer [M/C] | Required When [M] | Purpose [M] |
| ---- | ---- | ---- | ---- |
| `runtime-metrics.tsv` | `scripts/audit/runtime_metrics.sh` | every run | STEP -1 source dataset |
| `module-branch-hotspots.tsv` | derived from `runtime-metrics.tsv` | every run | top branch/fanout review |
| `enum-surface.tsv` | semi-mechanical extraction | complete run | STEP 1 variant and switch-site evidence |
| `enum-switch-sites.tsv` | semi-mechanical extraction | complete run | STEP 1 site identities |
| `function-branch-hotspots.tsv` | semi-mechanical extraction | complete run | STEP 2 function-level hotspots |
| `concept-branch-summary.tsv` | semi-mechanical extraction | complete run | STEP 2A concept branch modules |
| `concept-branch-map.tsv` | semi-mechanical extraction | complete run | STEP 2A site/module evidence |
| `flow-constraint-ledger.tsv` | classified with evidence anchors | complete run | STEP 3 constraints |
| `flow-counts.tsv` | classified/derived | complete run | STEP 3 effective flow totals |
| `semantic-spread.tsv` | classified | complete run | STEP 4 role-aware concept spread |
| `ownership-drift.tsv` | classified | complete run | STEP 4A owner trend |
| `concentration-ratios.tsv` | derived from artifacts | complete run | STEP 5A concentration trend |
| `invalidating-signals.tsv` | classified | complete run | STEP 8B noise handling |
| `risk-buckets.tsv` | rubric-derived | every non-blocked run | STEP 7 scoring evidence |
| `issue-ledger.tsv` | classified | every non-blocked run | STEP 9 follow-up accountability |

If an artifact is missing, the report MUST include an "Artifact Coverage" table:

| Artifact [M] | Status [C] | Reason [C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |

---

# STEP -1 — Runtime Module Enumeration (Mandatory)

Evidence mode: `mechanical`

Enumerate all runtime modules under:

`crates/icydb-core/src`

Exclude:

* `tests`
* `benches`
* `examples`
* generated files unless explicitly included and marked

Produce:

| module [M] | file [M] | LOC [M] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_count [M] | if_chain_count [M] | max_branch_depth [M] | fanout [M] | branch_sites_total [D] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: |

Definitions:

* `module` = Rust module path in `icydb-core` runtime scope.
* `LOC` = logical non-empty lines in the module file.
* `match_count` = count of source-level `match` expressions.
* `match_arms_total` = total arm count across all source-level `match` expressions.
* `avg_match_arms` = `match_arms_total / match_count` (0 when `match_count = 0`).
* `if_count` = count of `if` and `else if` branches.
* `if_chain_count` = number of distinct `if`/`else if` chains (chain roots).
* `max_branch_depth` = maximum nested depth of `match` + `if` branching.
* `fanout` = number of internal runtime module imports in that module file.
* `branch_sites_total` = `match_count + if_chain_count`.

Branch-counting rules:

* Count `if let` as `if` branch forms.
* Count source code only; exclude macro-expanded branches.
* Boolean operators inside a single condition (`&&`, `||`) do not increment branch counts.
* Keep counting rules stable week-over-week under `method_version`.

Store this dataset for later steps.

Do not continue until enumeration completes.

---

# STEP 0 — Baseline Capture (Mandatory)

Evidence mode: `semi-mechanical`

Capture baseline values before computing current metrics.

Baseline source rule:

* baseline = most recent comparable `complexity-accretion` run.
* if no comparable run exists, baseline = `N/A`.

Produce:

| Metric | Class | Signal Strength | Previous | Current | Delta |
| ---- | ---- | ---- | ----: | ----: | ----: |
| Total runtime files in scope | `[M]` | primary |  |  |  |
| Runtime LOC | `[M]` | primary |  |  |  |
| Runtime fanout (sum) | `[M]` | primary |  |  |  |
| Modules with fanout > 12 | `[D]` | primary |  |  |  |
| Super-nodes (`fanout > 20 OR domain_count >= 3`) | `[D]` | primary |  |  |  |
| Continuation decision owners | `[C]` | primary |  |  |  |
| Continuation execution consumers | `[C]` | primary |  |  |  |
| Continuation plumbing modules | `[C]` | primary |  |  |  |
| AccessPath decision owners | `[C]` | primary |  |  |  |
| AccessPath executor dispatch sites | `[M]` | primary |  |  |  |
| AccessPath branch modules | `[M]` | primary |  |  |  |
| RouteShape decision owners | `[C]` | primary |  |  |  |
| RouteShape branch modules | `[M]` | primary |  |  |  |
| Predicate coercion decision owners | `[C]` | primary |  |  |  |
| Continuation mentions (context only) | `[M]` | weak |  |  |  |

If no prior comparable report exists, mark previous values as `N/A` and treat
this run as the new baseline.

Do not replace this table with a shorter narrative summary. If a classified
baseline metric cannot be recovered from the previous report, keep the row,
set previous to `N/A`, and add a one-line reason in the step-status table.

---

# STEP 1 — Variant Surface Growth + Branch Multiplier

Evidence mode: `semi-mechanical`

Quantify:

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

Switch-site rule (`S-1`):

* Count only direct runtime `match` or `if let` sites on the target enum type.
* Count only sites in runtime scope (`crates/icydb-core/src` exclusions apply).
* Exclude pass-through delegations that only forward the enum without adding branch semantics.
* Exclude `matches!` macro predicates, guard-only checks, tests, and generated code.
* Distinct site identity: `module::function::line`.

Produce:

| Enum [M] | Variants [M] | Switch Sites [M] | Branch Multiplier [D] | Decision Owners [C] | Domain Scope [C] | Mixed Domains? [C] | Growth Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |

Switch-site evidence requirement:

* The report may show only the summary table, but
  `enum-switch-sites.tsv` MUST list each counted site as
  `enum`, `module`, `function`, `line`, `site_kind`, `included_reason`.
* Excluded candidates that materially affect interpretation MUST be listed in
  an `excluded_reason` note or a separate section.
* If direct enum matching cannot be established mechanically, mark the row
  `semi-mechanical` and cite the searched symbols.

Definitions:

* `branch_multiplier = variants × switch_sites`.
* `AccessPath executor dispatch sites [M]` = distinct runtime executor callsites
  that branch on executable `AccessPath` shape.

Flag:

* `branch_multiplier` trend up week-over-week.
* enums `> 8` variants and still growing.
* enums mixing planning + execution + storage semantics.
* any increase in `AccessPath executor dispatch sites` without explicit dispatch-consolidation note.

---

# STEP 2 — Local Branching Pressure (Function-Level)

Evidence mode: `semi-mechanical`

Identify high-branch-density runtime functions and compare against previous run.

Produce:

| Function [M] | Module [M] | Branch Layers [D] | match_count [M] | match_arms_total [M] | avg_match_arms [D] | if_chain_count [M] | max_branch_depth [M] | Axis Count [C] | Previous Branch Layers [M] | Delta [D] | Domains Mixed [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ----: | ---- |

Minimum row rule:

* Include at least the top 15 functions by `branch_layers` or all functions
  above the hotspot threshold, whichever is larger.
* If function-level extraction is unavailable, STEP 2 is `BLOCKED`; do not
  substitute module-level branch sites as function-level evidence.

Axis coupling checklist (classified):

* access path type
* predicate type
* cursor presence
* plan shape
* index uniqueness
* recovery mode

Flag:

* any function with `domains_mixed > 3`.
* positive weekly branch-layer growth.
* enum growth directly increasing branch layers.

---

# STEP 2A — Concept Branch Distribution Across Modules

Evidence mode: `semi-mechanical`

Count distinct runtime modules branching on each concept:

* `AccessPath`
* `RouteShape`

Produce:

| Concept [M] | Branch Modules [M] | Decision Owners [C] | Branch/Owner Ratio [D] | Previous Branch Modules [M] | Delta [D] |
| ---- | ----: | ----: | ----: | ----: | ----: |

Flag:

* any positive week-over-week branch-module delta.
* branch-module count increasing while decision-owner count is unchanged.

---

# STEP 3 — Execution Path Multiplicity (Effective Flows)

Evidence mode: `semi-mechanical`

For each core operation (`save`, `replace`, `delete`, `load`, `recovery replay`,
`cursor continuation`, `index mutation`), compute flow count via decision axes.

Flow model (`F-1`):

1. `theoretical_space = Π(axis cardinalities)`
2. apply only explicit, evidenced constraints
3. `effective_flows = theoretical_space - constrained_combinations`

Required axis set (add/remove only with explicit note and comparability impact):

* operation type
* access path type
* cursor presence
* recovery mode
* index uniqueness
* ordering mode

Required constraint ledger (mandatory before effective-flow totals):

| Operation [M] | Constraint [C] | Axes Restricted [M] | Combinations Removed [D] | Evidence [M/C] |
| ---- | ---- | ---- | ----: | ---- |

Flow table:

| Operation [M] | Axes Used [M] | Axis Cardinalities [M] | Theoretical Space [D] | Effective Flows [D] | Previous Effective Flows [M] | Delta [D] | Shared Core? [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |

Rules:

* Every removed combination MUST map to exactly one ledger row.
* If ledger evidence is incomplete, mark section `low confidence` and run `non-comparable`.
* If no constraint ledger is produced, STEP 3 is `BLOCKED`; do not publish
  effective-flow totals inferred only from prose.

Flag:

* `effective_flows > 4` (pressure)
* `axis_count >= 4` (multiplication onset)
* growth in effective flows without equivalent owner consolidation

---

# STEP 4 — Semantic Authority vs Execution Spread

Evidence mode: `classified`

For each concept, classify usage by ownership and execution role.

Target concepts:

* continuation / cursor anchor semantics
* `AccessPath` decision semantics
* `RouteShape` decision semantics
* predicate coercion decision semantics
* envelope boundary checks
* bound conversions
* plan shape enforcement
* error origin mapping
* index id / namespace validation

Produce:

| Concept [M] | Decision Owners [C] | Execution Consumers [C] | Plumbing Modules [C] | Owner Count [D] | Consumer Count [D] | Plumbing Count [D] | Semantic Layers [C] | Transport Layers [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ----: | ----: | ----: | ---- | ---- | ---- |

Definitions:

* `Decision Owner` = module defining semantic rules or protocol contracts.
* `Execution Consumer` = module branching on concept state to drive behavior.
* `Plumbing Module` = module transporting concept values without branching.

Role-aware counts are primary; mention totals are context only.

Flag:

* `semantic_layer_count >= 3` (architectural leakage).
* semantic-owner growth without explicit boundary consolidation.
* any increase in `AccessPath`, `RouteShape`, or predicate coercion decision-owner count without explicit ownership-consolidation note.

---

# STEP 4A — Concept Ownership Drift (Only)

Evidence mode: `classified`

For each concept:

* continuation
* `AccessPath`
* `RouteShape`
* predicate coercion
* index range
* canonicalization

Produce:

| Concept [M] | Decision Owners [C] | Previous Owners [C] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |

Flag:

* `owner_count > 2`
* any owner-count increase week-over-week

---

# STEP 4B — Fanout Pressure

Evidence mode: `mechanical`

Compute module fanout trend using the STEP -1 dataset.

Produce:

| Module [M] | Fanout [M] | Previous Fanout [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |

Flag:

* `fanout > 12`
* fanout growth week-over-week

---

# STEP 5 — Cognitive Load Indicators (Super-Node + Call Depth)

Evidence mode: `mechanical`

Compute structural mental-load signals:

1. functions > 80-100 logical lines.
2. deep core-operation call depth.
3. super-node modules.

Super-node definition:

* `fanout > 20` OR `domain_count >= 3`

Domain taxonomy mapping (`D-2`, fixed):

| Domain Family [M] | Module Prefix Rules [M] |
| ---- | ---- |
| cursor/continuation | `db::cursor::*`, `db::continuation::*` |
| access/index | `db::index::*`, `db::access::*` |
| predicate/filter | `db::predicate::*`, `db::filter::*` |
| query/plan | `db::query::*`, `db::plan::*` |
| storage/commit | `db::storage::*`, `db::commit::*` |

`domain_count(module) [D]` rule:

* distinct domain families referenced by internal runtime imports and fully-qualified internal paths in that module.
* ignore `std::*`, external crates, and test-only imports.
* unresolved internal path prefixes must be labeled `other` and called out explicitly.

Produce:

| Module/Operation [M] | LOC or Call Depth [M] | Fanout [M] | Domain Count [D] | Previous [M] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Flag:

* `call_depth > 6` for core operations.
* rising super-node pressure across consecutive comparable runs.

---

# STEP 5A — Complexity Concentration Ratios

Evidence mode: `mechanical`

Measure how concentrated structural complexity is.

Produce:

| Metric [M] | Current [D] | Previous [D] | Delta [D] | Risk [C] |
| ---- | ----: | ----: | ----: | ---- |
| Fanout concentration (top 10 modules) |  |  |  |  |
| Branch-site concentration (top 10 modules) |  |  |  |  |
| AccessPath branch concentration (top 3 modules) |  |  |  |  |
| RouteShape branch concentration (top 3 modules) |  |  |  |  |

Definitions:

* `fanout concentration = sum(fanout_top10) / sum(fanout_all)`
* `branch-site concentration = sum(branch_sites_top10) / sum(branch_sites_all)`
* `AccessPath branch concentration = sum(accesspath_sites_top3) / sum(accesspath_sites_all)`
* `RouteShape branch concentration = sum(routeshape_sites_top3) / sum(routeshape_sites_all)`

---

# STEP 6 — Drift Sensitivity (Axis Count)

Evidence mode: `semi-mechanical`

Quantify areas where growth vectors multiply structural cost.

Produce:

| Area [M] | Decision Axes [M] | Axis Count [D] | Branch Multiplier [D] | Drift Sensitivity [C] | Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ---- |

Flag:

* `axis_count >= 4`
* branch multiplier growth tied to new variants

---

# STEP 7 — Complexity Risk Index (Rubric-Guided)

Evidence mode: `semi-mechanical`

Score each bucket 1-10, then compute weighted aggregate:

* variant explosion risk x2
* branching pressure + centralization trend x2
* flow multiplicity x2
* cross-layer spread x3
* authority fragmentation x2
* fanout pressure + super-node load x2
* call-depth pressure x1

Bucket scoring anchors (`R-1`):

| Bucket [M] | 2 Anchor [C] | 5 Anchor [C] | 8 Anchor [C] | 10 Anchor [C] |
| ---- | ---- | ---- | ---- | ---- |
| Variant explosion risk | no enum growth and flat multipliers | one enum > 8 variants or multiplier growth in one concept | multiple growing enums with multiplier growth | growth across planning + execution + storage mixed enums |
| Branching pressure + centralization trend | flat hotspot count and stable branch-module spread | hotspot growth in one core area | multiple hotspot deltas and branch-module spread growth | sustained growth with no owner consolidation |
| Flow multiplicity | no effective-flow increase | one operation `effective_flows > 4` | multiple operations with rising effective flows | widespread axis multiplication with owner drift |
| Cross-layer spread | semantic layers stable (`<=2`) | one concept at 3 layers | several concepts at 3+ layers | broad semantic leakage across subsystems |
| Authority fragmentation | owner counts stable and `<=2` | one concept owner delta +1 | multiple concept owner increases | repeated owner drift in core concepts without consolidation |
| Fanout pressure + super-node load | flat fanout profile and no new super-nodes | one module `fanout > 12` growth | multiple modules with rising fanout and super-node growth | widespread super-node growth across comparable runs |
| Call-depth pressure | no core op above depth 6 | one core op above depth 6 | multiple core ops above depth 6 with growth | deep call chains across most core operations |

Produce:

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |

`overall_index = weighted_sum / weight_sum`

Risk-index publication rule:

* If every bucket is scored from complete evidence, label the result
  `overall complexity risk index`.
* If one or more classified buckets are blocked, label the result
  `partial complexity risk index`, omit blocked buckets from the denominator,
  and list omitted weights.
* If only STEP -1/STEP 0 mechanical metrics are available, label the result
  `mechanical-only complexity signal`; do not express it as a 1-10 overall
  index.
* Every bucket score must cite at least one source row from an artifact or
  section table.

Interpretation:

* 1-3 = low risk / structurally healthy
* 4-6 = moderate risk / manageable pressure
* 7-8 = high risk / requires monitoring
* 9-10 = critical risk / structural instability

---

# STEP 8 — Trend Interpretation Filter (Structural Noise Filter)

Evidence mode: `semi-mechanical`

Before finalizing risk, apply this interpretation filter:

* if concept mentions increase and decision owners decrease/hold, mark as `refactor transient`.
* if mentions increase and execution consumers increase while decision owners unchanged, mark as `benign surface growth`.
* if decision-owner count increases for `AccessPath`, `RouteShape`, or predicate coercion, do NOT mark as transient without documented ownership consolidation.
* if file count increases due to module split and super-node pressure decreases, mark as `structural improvement`.

Produce:

| Signal [M/C] | Raw Trend [M/D] | Filter Result [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |

---

# STEP 8A — Complexity Trend Table (Required)

Evidence mode: `mechanical` (primary) + `classified` (secondary)

Show trend direction, not only pairwise deltas.

Produce a time-series table across at least 4 comparable run dates
(or all available comparable dates if fewer):

| Metric [M/C] | <date-1> | <date-2> | <date-3> | <date-4> |
| ---- | ----: | ----: | ----: | ----: |
| continuation decision-owner count `[C]` |  |  |  |  |
| continuation execution-consumer count `[C]` |  |  |  |  |
| AccessPath branch-module count `[M]` |  |  |  |  |
| RouteShape branch-module count `[M]` |  |  |  |  |
| branch hotspots (count) `[M]` |  |  |  |  |
| super-node count `[D]` |  |  |  |  |
| AccessPath variants `[M]` |  |  |  |  |
| continuation mentions (weak context) `[M]` |  |  |  |  |

---

# STEP 8B — Invalidating Signals (Required)

Evidence mode: `classified`

Capture interpretation-distorting changes before final conclusions.

Produce:

| Signal [M/C] | Present? [C] | Expected Distortion [C] | Handling Rule [C] |
| ---- | ---- | ---- | ---- |
| large module moves |  |  |  |
| file splits without semantic change |  |  |  |
| generated code expansion |  |  |  |
| parser/table-driven conversion replacing branch expressions |  |  |  |
| branch consolidation into helper functions |  |  |  |

If any signal is present, include explicit impact in comparability note.

---

# STEP 9 — Issue Ledger (Mandatory)

Evidence mode: `classified`

Convert the highest-risk findings into owner-scoped actions. This prevents the
audit from ending as a score-only narrative.

Produce:

| Finding [C] | Anchor Metric [M/D] | Owner Boundary [C] | Trigger Threshold [M/D] | Action [C] | Next Check [M/C] |
| ---- | ---- | ---- | ---- | ---- | ---- |

Rules:

* Include at least one row for every `Risk = High` finding.
* Include at least the top three medium-or-higher findings when no high-risk
  finding exists.
* Each action must name a boundary to protect or simplify; avoid broad
  instructions like "reduce complexity".
* Each row must cite a concrete metric, threshold, or delta.

---

# Required Summary

0. Run metadata + comparability note
1. Overall complexity risk index
2. Fastest growing concept families
3. Highest branch multipliers
4. Branch distribution drift (`AccessPath` / `RouteShape`)
5. Flow multiplication risks (axis-based)
6. Semantic authority vs execution spread risks
7. Ownership drift + fanout pressure
8. Super-node + call-depth warnings
9. Trend-interpretation filter outcomes
10. Complexity trend table
11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)
12. Issue ledger summary

Summary bullet rule:

* Every summary bullet MUST cite at least one anchor metric:
  * delta
  * multiplier
  * owner count
  * axis count
  * fanout threshold
  * hotspot count

Run metadata must include:

* compared baseline report path (`baseline = most recent comparable run`)
* full method manifest tags (`CA-1.4`, `D-2`, `F-1`, `S-1`, `R-1`, `T-1`)
* comparability status (`comparable` or `non-comparable` with reason)
* completion status and risk-index kind

---

# Explicit Anti-Shallow Requirement

Do NOT:

* say "code looks clean"
* give generic statements
* provide unquantified claims
* comment on naming
* comment on macro usage
* comment on formatting

Every claim must reference:

* count
* structural pattern
* growth vector
* branch multiplier or axis product

---

# Long-Term Goal of This Audit

Detect:

* variant explosion before branching explosion
* flow multiplication before semantic divergence
* concept leakage before cross-layer drift
* cognitive load growth before fragility

This audit measures structural entropy, not quality.
