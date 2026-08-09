# WEEKLY AUDIT — DRY / Redundancy / Consolidation

`icydb-core` (+ facade where relevant)

## Purpose

Identify duplicated logic and structural repetition that:

* increases maintenance burden
* increases divergence risk
* multiplies invariant enforcement points
* amplifies change surface
* raises future refactor cost

This is NOT:

* a style audit
* a correctness audit
* a redesign proposal
* a layer-merging exercise

---

# Scope Boundary (Mandatory)

This audit has one artifact with two internal modes:

* **Mode A — Consolidation Opportunity Audit**
* **Mode B — Protective Redundancy Audit**

Do not merge this audit into:

* `complexity-accretion`
* `layer-violation`
* `module-structure`
* `velocity-preservation`

Reason: this audit answers a distinct question (duplication, drift surface, and
safe consolidation boundary), even when metrics overlap.

---

# Consolidation Guardrails (Strict)

You MUST NOT recommend:

* consolidation across architectural layers
* merging planner + executor logic
* collapsing validation into mutation layers
* removing defensive duplication without invariant-safety verification
* consolidation that widens visibility
* consolidation that weakens boundary enforcement

DRY must never reduce safety or boundary clarity.

If duplication increases safety, mark it as intentional redundancy.

Route authority soft-budget policy (pre-0.25 hardening):

* new aggregate/routing features should add at most +1 route capability flag.
* new aggregate/routing features should add at most +1 execution-mode route branch/case.
* eligibility helper definitions (`is_*eligible`) should remain route-owned.

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

Requirement:

* every produced metric column MUST be labeled by `[M]`, `[C]`, or `[D]`
  (either in the header or via an explicit `Class` column).

Behavioral-equivalence confidence scale:

* `high`: same contract + same branch conditions + same outcomes.
* `medium`: same intent but minor branch or mapping differences.
* `low`: similar shape only; semantic equivalence uncertain.

---

# Method Contract + Comparability (Mandatory)

Method manifest (include exactly in run metadata):

* `method_version = DRY-1.2`
* `duplication_taxonomy = DT-1`
* `owner_layer_taxonomy = OL-1`
* `invariant_role_model = IR-1`
* `facade_inclusion_rule = FI-1`
* `consolidation_safety_model = CS-1`

Comparability gate:

* baseline = most recent comparable `dry-consolidation` run.
* run is `non-comparable` if any of the following changed since baseline:
  * duplication taxonomy
  * invariant classification rules
  * runtime/facade scope definition
  * facade inclusion rule
  * owner-layer taxonomy
  * consolidation safety model

---

# Duplication Taxonomy (Operational Definitions, Mandatory)

Classify every duplication instance as exactly one:

* `Accidental duplication`: repeated logic in the same owner layer with no boundary reason.
* `Intentional boundary duplication`: repeated enforcement across layers that preserves authority separation.
* `Defensive duplication`: repeated check intentionally duplicated to fail closed.
* `Evolution drift duplication`: once-shared logic that forked into near-parallel implementations.
* `Boilerplate duplication`: repeated low-risk scaffolding with low semantic drift cost.

---

# STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

Capture:

* baseline report path
* comparability status (`comparable` or `non-comparable` with reason)
* method manifest tags
* in-scope roots and exclusions

Runtime scope defaults:

* include: `crates/icydb-core/src`
* include facade only when it performs semantic validation/mapping, not mere re-export
* exclude: tests, benches, examples, generated code unless explicitly noted

Produce:

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |

---

# MODE A — Consolidation Opportunity Audit

# STEP 1A — Structural Duplication Scan

Evidence mode: `mechanical`

Detect structural/textual repetition such as:

* invariant checks
* key namespace validation
* index id mismatch checks
* component arity validation
* expected-key vs decoded-key validation
* raw index key envelope checks
* cursor anchor envelope validation
* bound conversion patterns
* reverse-relation index mutation blocks
* deserialize + map error wrappers
* commit marker phase mapping
* error construction blocks with similar payloads
* similar match trees across modules

Produce:

| Pattern [M] | Files [M] | Lines [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Rules:

* repeated format strings are allowed as context but are low-priority signals.
* prioritize repeated branch/mapping/enforcement logic over text similarity.

---

# STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

Detect conceptual repetition across modules:

* encode/decode wrappers with equivalent mapping logic
* `PlanError` ↔ `QueryError` conversion families
* raw-key envelope check entry points
* cursor token parsing + validation families
* index key validation paths
* reverse index mutation implementations
* commit marker phase transition implementations

Produce:

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Consolidation difficulty:

* `Low`: safe helper extraction in same owner layer.
* `Medium`: local unification with boundary-safe wiring changes.
* `High`: boundary-sensitive; likely to require authority reshaping.

---

# STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

This step is limited to structural strain that directly amplifies duplication.

Over-splitting signals:

* logic spread across 3+ files with near-identical behavior
* thin wrappers forwarding repeated mapping/enforcement logic

Under-splitting signals:

* large files combining multiple duplicated responsibilities
* single modules holding parallel duplication families that should be localized

Produce:

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |

Do NOT run a generic module-structure analysis here.

---

# STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

For each repeated invariant family, identify:

* invariant name
* canonical owner
* enforcement sites
* role per site:
  * `defining`
  * `validating`
  * `defensive re-checking`
  * `recovery re-verification`

Produce:

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |

Classification:

* `Safety-enhancing` (good redundancy)
* `Safety-neutral`
* `Divergence-prone`

Flag:

* invariants with `sites_count > 3`
* cases with variant/classification drift across sites
* boundary-semantics drift across sites

---

# STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

Prioritize structurally meaningful redundancy:

* variant mapping drift
* error-origin drift
* lower-level translation divergence

Treat as secondary/noise unless semantic impact exists:

* repeated message text / format string duplication

Produce:

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

---

# MODE B — Protective Redundancy Audit

# STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

Classify protective repetition across:

* planner/executor boundaries
* executor/recovery boundaries
* recovery/index boundaries
* cursor planning/execution boundaries
* facade/core boundaries

Produce:

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Mandatory decision order:

1. first decide whether duplication is protective.
2. only then consider consolidation.

---

# STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

Only include candidates that passed the protective-duplication gate.

Produce:

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Consolidation safety class (`CS-1`):

* `safe helper extraction`
* `safe local unification`
* `boundary-sensitive`
* `do not merge`

Do NOT provide implementation details or code sketches.

---

# STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

Explicitly list duplication that should remain separate because it:

* reinforces boundary safety
* prevents cross-layer dependency
* protects recovery symmetry
* protects cursor isolation

Produce:

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |

---

# STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

Required trend backbone metrics:

* total duplication patterns found
* total high-risk divergence patterns
* same-layer accidental duplication count
* cross-layer intentional duplication count
* defensive duplication count
* boundary-protected duplication count
* invariants with `>3` enforcement sites
* error-construction families with `>3` custom mappings

Primary outcome metric:

* drift surface reduction estimate (`low` / `medium` / `high`)

Secondary outcome metric:

* estimated LoC reduction range (conservative)

Produce:

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |

High-risk condition:

* if `total high-risk divergence patterns > 0`, include mandatory high-risk ledger and at least one explicit follow-up action with owner boundary and target run.

Required high-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

---

# STEP 9A — Analyst Verification Readout (Required)

Evidence mode: `semi-mechanical`

Provide an explicit analyst readout that separates measured from inferred signal.

Produce:

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count |  | finding rows primarily supported by `[M]` evidence (for example STEP 1A structural findings) |
| classified findings count |  | finding rows requiring `[C]` analyst judgment in the decisive column set |
| high-confidence candidate count |  | STEP 7B rows where `Behavioral Equivalence Confidence = high` and `Consolidation Safety Class` is `safe helper extraction` or `safe local unification` |
| boundary-protected findings count |  | finding rows where `Boundary-Protected? = yes` across applicable steps |

This readout is mandatory in every comparable and non-comparable run.

---

# Required Output Structure

1. Run metadata + comparability note
2. Mode A summary: high-impact consolidation opportunities
3. Mode A summary: medium opportunities
4. Mode A summary: low/cosmetic opportunities
5. Mode B summary: protective redundancies (keep separate)
6. Dangerous consolidations (do not merge)
7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)
8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)
9. Architectural risk summary
10. DRY risk index (1-10, lower is better)
11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)

Interpretation:

* 1-3 = low risk / structurally healthy
* 4-6 = moderate risk / manageable pressure
* 7-8 = high risk / requires monitoring
* 9-10 = critical risk / structural instability

---

# Anti-Shallow Requirement

Do NOT:

* recommend merging layers
* recommend collapsing planner + executor
* recommend "just make a util module"
* comment on naming
* comment on formatting
* comment on macros
* suggest public API changes

Every duplication claim must include:

* location
* pattern family
* risk classification
* drift sensitivity
* owner-layer context
