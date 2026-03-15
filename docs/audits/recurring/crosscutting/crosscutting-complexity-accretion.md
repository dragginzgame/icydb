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

* `method_version = CA-1.3`
* `runtime_metrics_generator = scripts/audit/runtime_metrics.sh`
* `domain_taxonomy = D-2`
* `flow_axis_model = F-1`
* `switch_site_rule = S-1`
* `risk_rubric = R-1`
* `trend_filter_rule = T-1`

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
* full method manifest tags (`CA-1.3`, `D-2`, `F-1`, `S-1`, `R-1`, `T-1`)
* comparability status (`comparable` or `non-comparable` with reason)

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
