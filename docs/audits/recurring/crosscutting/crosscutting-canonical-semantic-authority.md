# WEEKLY AUDIT — Canonical Semantic Authority

`icydb-core` with cross-surface checks across schema, facade, planner, runtime,
EXPLAIN, and replay/recovery.

## Purpose

Detect semantic authority drift where one concept is represented by multiple
competing authorities instead of one canonical lowered/tokenized model.

This audit is about semantic ownership continuity across surfaces.

This is NOT:

* a style audit
* a naming audit
* a generic layering audit
* a pure DRY audit
* a refactor proposal exercise

Primary scope:

* `crates/icydb-core/src`

Secondary scope (when relevant to canonical lowering boundaries):

* `crates/icydb/src`
* `crates/icydb-build/src`
* schema/derive surfaces that emit canonical metadata or typed semantic tokens

Core question:

For each growing concept family, is there exactly one canonical internal
representation and one lowering boundary, or are there parallel semantic
authorities?

---

# Core Principle

One semantic concept must have:

1. one canonical lowered semantic form (typed token/model/AST/key item),
2. one explicit lowering boundary from frontend text/input forms,
3. one semantic owner for rule interpretation.

Frontend text input is acceptable at ingress boundaries.
Frontend text must not remain semantic authority after lowering.

Danger pattern to detect:

* raw strings, labels, or formatted diagnostics being reparsed/reinterpreted as
  semantic authority in later layers.

---

# Hard Constraints / Anti-Shallow Rules

Do NOT:

* score based on naming/style comments
* suggest "use an enum" without owner + boundary analysis
* recommend layer merges
* collapse protective redundancy that preserves fail-closed behavior
* treat textual duplication alone as representation drift

Mandatory distinction for every concept under review:

* `frontend text input`: permissive user-facing syntax/text contracts
* `canonical lowered form`: typed internal semantic authority
* `semantic side channel`: any alternate path (string metadata, formatted text,
  ad-hoc matcher) that can decide behavior

Complement boundaries (do not overlap excessively):

* `complexity-accretion`: asks how fast structural complexity grows
* `dry-consolidation`: asks where duplication can/should be consolidated
* `layer-violation`: asks where semantics leak across owner layers
* `module-structure`: asks whether boundaries/visibility remain contained
* this audit: asks whether one concept has exactly one semantic authority and
  one lowering boundary across all surfaces

---

# Method / Comparability Rules (Mandatory)

## Measurement Classes + Evidence Modes

Column classes:

* `[M]` Mechanical: directly derivable from code paths/tables.
* `[C]` Classified: analyst judgment required.
* `[D]` Derived: formula over mechanical columns.

Evidence modes:

* `mechanical` (high confidence)
* `semi-mechanical` (medium confidence)
* `classified` (medium/low confidence)

Requirement:

* every produced metric column MUST be labeled `[M]`, `[C]`, or `[D]`
  (in header or a dedicated class column).

## Method Manifest

Include exactly in run metadata:

* `method_version = CSA-1.0`
* `concept_inventory_model = CI-1`
* `representation_matrix_model = RM-1`
* `authority_count_rule = AC-1`
* `reparse_scan_rule = RS-1`
* `convergence_rule = CV-1`
* `risk_rubric = RR-1`
* `noise_filter_rule = NF-1`

Comparability gate:

* baseline = most recent comparable `canonical-semantic-authority` run.
* mark run `non-comparable` if any method manifest tag changed or if any of
  these changed since baseline:
  * concept-family inventory
  * scope inclusion/exclusion
  * definition of semantic owner
  * lowering-boundary counting rules
  * reparse detection rules
  * cross-surface parity rules

Daily baseline rule:

* first run of day (`canonical-semantic-authority.md`) compares to latest prior
  comparable run (or `N/A`).
* same-day reruns (`canonical-semantic-authority-2.md`, etc.) compare to that
  day's `canonical-semantic-authority.md` baseline.

---

# STEP 0 — Canonical Concept Inventory

Evidence mode: `semi-mechanical`

Enumerate target concept families (minimum set):

* identifiers
* predicates
* index key items
* route/statement classification
* projection labels
* expression forms
* order keys
* entity/index identity

For each concept, record current owner and growth pressure.

Produce:

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |

Growth signal examples:

* increasing variants
* increasing decision sites
* new frontend routes
* new replay/recovery branches

---

# STEP 1 — Representation Matrix

Evidence mode: `semi-mechanical`

Build one row per concept family.

Produce:

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Definitions:

* `frontend text inputs`: accepted user-facing text/syntax before lowering.
* `canonical token/model/AST`: typed representation that must own semantics.
* `side channels`: alternate semantic authorities (raw strings, reparsed labels,
  formatter output interpreted as logic input, duplicated matcher tables).

Required flags:

* any concept where planner/runtime/replay/EXPLAIN use different semantic forms
* any concept where canonical form exists in one subsystem but is bypassed
  elsewhere

---

# STEP 2 — Authority Count

Evidence mode: `semi-mechanical`

Count distinct semantic owners and lowering boundaries per concept.

Produce:

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |

Rules:

* ideal target: `owner_count = 1` and `boundary_count = 1`.
* `owner_count > 1` is drift pressure unless one owner is explicitly
  diagnostic-only with no decision authority.
* `boundary_count > 1` is drift pressure unless second boundary is strictly
  defensive validation that delegates to canonical model.

---

# STEP 3 — Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

Scan for repeated parsing/normalization/semantic reinterpretation sites.

Required scan families:

* identifier normalization
* predicate reparsing
* index key-item interpretation
* route/statement classification derivation
* projection/expression tokenization
* order-key normalization

Produce:

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |

Flag:

* `non_canonical_sites > 0`
* repeated normalization logic in multiple subsystems
* semantic decisions derived from diagnostics or formatted text

---

# STEP 4 — Cross-Surface Convergence

Evidence mode: `classified` anchored by mechanical surface mapping

Validate parity across:

* builder/fluent API
* SQL/frontends
* schema metadata lowering
* planner decisions
* runtime execution
* replay/recovery interpretation
* EXPLAIN/diagnostic reporting

Produce:

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |

Convergence rule (`CV-1`):

* parity requires the same canonical model to drive planner, runtime, replay,
  and EXPLAIN semantics.
* frontend syntax differences are acceptable only before canonical lowering.

---

# STEP 5 — Drift Risk Table

Evidence mode: `semi-mechanical`

Create a ledger of concept-specific drift threats.

Produce:

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Mandatory trigger categories:

* raw string metadata acting as authority
* duplicated lowering boundaries
* planner/access/EXPLAIN semantic forks
* schema/build/runtime representational mismatch
* replay/recovery semantics derived differently than live execution

---

# STEP 6 — Missing Canonical Models

Evidence mode: `classified` with mechanical support

Identify growing concept lines that lack canonical typed models before feature
expansion.

Produce:

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |

Hard gate:

* if a concept is growing and `canonical typed model present = no`, classify at
  least `High` risk unless constrained to a non-authoritative diagnostic-only
  surface.

---

# STEP 7 — Canonical Authority Risk Index

Evidence mode: `semi-mechanical` with rubric anchors

Score each bucket `1-10`, then apply weights.

Weighted buckets:

* semantic owner multiplicity ×3
* lowering-boundary multiplicity ×3
* raw-string/side-channel authority ×3
* reparse/normalizer duplication ×2
* cross-surface parity gaps ×2
* missing canonical models in growing concepts ×3
* replay/live semantic mismatch ×3

Produce:

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |

`overall_index = weighted_sum / weight_sum`

Interpretation:

* `1-3` low risk / canonical authority is stable
* `4-6` moderate risk / drift pressure present
* `7-8` high risk / active semantic divergence risk
* `9-10` critical risk / competing authorities likely to cause behavior drift

---

# STEP 8 — Noise Filter

Evidence mode: `classified`

Before final conclusions, classify transient vs structural signals.

Produce:

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |

Allowed transient examples:

* temporary adapter layer during one-way migration to canonical model
* module/file split where owner count and boundary count stay flat/down
* EXPLAIN formatting-only changes with no decision-path changes

Do NOT mark as transient when:

* owner count increased
* lowering boundary count increased
* replay/runtime semantics diverged
* non-canonical parse sites increased for a growing concept

---

# Required Summary

0. Run metadata + comparability note
1. Canonical concept inventory snapshot
2. Representation matrix highlights
3. Owner/boundary count deltas
4. Reparse/reinterpretation findings
5. Cross-surface convergence gaps
6. Missing canonical model blockers
7. Drift risk table (high/medium/low)
8. Canonical Authority Risk Index
9. Noise-filter interpretation
10. Follow-up actions with owner boundary + target run (required when index `>= 6` or any high-risk drift trigger exists)
11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)

Summary bullet rule:

* every summary bullet must cite at least one anchor metric:
  * owner count
  * boundary count
  * non-canonical parse-site count
  * parity gaps count
  * missing canonical model count
  * risk index

Summary integration requirement:

* when this audit is run, day-level `summary.md` must include an `Audit Run Order and Results` line for:
  * `crosscutting/crosscutting-canonical-semantic-authority` -> `canonical-semantic-authority*.md` (`Risk: x/10`)

---

# Verification Readout

Verification readout MUST include:

* method comparability status (`comparable` or `non-comparable` + reason)
* confirmation that all mandatory steps/tables are present
* confirmation that owner and boundary counts were computed from inspected
  sources, not mention counts only
* explicit status: `PASS`, `FAIL`, or `BLOCKED`

Status rules:

* `PASS`: no high/critical drift findings and no unresolved canonical-model
  blockers for growing concepts.
* `FAIL`: any confirmed high/critical drift finding, semantic side-channel
  authority, or replay/live semantic mismatch without canonical convergence.
* `BLOCKED`: insufficient repository visibility/evidence for reliable
  owner/boundary/parity determination.
