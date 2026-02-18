# WEEKLY AUDIT — DRY / Redundancy / Consolidation

`icydb-core` (+ facade where relevant)

## Purpose

Identify duplicated logic and structural repetition that:

* Increases maintenance burden
* Increases divergence risk
* Multiplies invariant enforcement points
* Amplifies change surface
* Raises future refactor cost

This is NOT:

* A style audit
* A correctness audit
* A redesign proposal
* A layer-merging exercise

---

# Consolidation Guardrails (Strict)

You MUST NOT recommend:

* Consolidation across architectural layers
* Merging planner + executor logic
* Collapsing validation into mutation layers
* Removing defensive duplication without verifying invariant safety
* Consolidation that widens visibility
* Consolidation that weakens boundary enforcement

DRY must never reduce safety or boundary clarity.

If duplication increases safety, mark it as **intentional redundancy**.

---

# STEP 1 — Structural Duplication Scan

Identify repeated blocks of logic such as:

* Invariant checks
* Key namespace validation
* Index id mismatch checks
* Component arity validation
* Expected-key vs decoded-key validation
* Raw index key envelope checks
* Cursor anchor envelope validation
* Bound conversion patterns
* Reverse-relation index mutation blocks
* Deserialize + map error wrappers
* Commit marker phase mapping
* Error construction blocks with similar payloads
* Repeated format strings
* Similar match trees across modules

For each duplication instance:

Produce:

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |

Duplication Type must be classified as:

* Accidental duplication
* Intentional boundary duplication
* Defensive duplication
* Evolution drift duplication
* Boilerplate duplication

---

# STEP 2 — Pattern-Level Redundancy

Look for repeated conceptual patterns across modules:

Examples:

* Multiple encode/decode wrappers with identical error mapping
* Multiple conversions between `PlanError` ↔ `QueryError`
* Multiple raw-key envelope check entry points
* Multiple cursor token parsing + validation blocks
* Multiple index key validation paths
* Multiple reverse index mutation implementations
* Multiple commit marker phase transitions implemented separately

For each pattern:

Produce:

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |

Consolidation Difficulty:

* Low (pure helper extraction)
* Medium (shared module refactor)
* High (involves boundary redefinition)

---

# STEP 3 — Over-Splitting / Under-Splitting Pressure

Detect:

### Over-Splitting

* Logic spread across 3+ files that conceptually belongs together.
* Small modules that forward nearly identical logic.
* Excess thin wrappers with repeated error mapping.

### Under-Splitting

* Files >600–800 lines mixing:

  * validation
  * mutation
  * error mapping
  * ordering logic
  * commit logic
* Single module implementing both plan interpretation and execution behavior.
* Single module handling both cursor decode and plan shaping.

Produce:

| Module | Size | Responsibilities Count | Split Pressure | Risk |

Do NOT recommend speculative splits.
Only flag clear structural strain.

---

# STEP 4 — Invariant Repetition Risk

Specifically detect invariant duplication across:

* Planner and executor
* Executor and recovery
* Recovery and index layer
* Cursor planning and execution
* Facade and core

For each duplicated invariant check:

Produce:

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |

Classify duplication as:

* Safety-enhancing (good redundancy)
* Safety-neutral
* Divergence-prone

Flag invariants that:

* Exist in >3 enforcement sites
* Have slightly different error classifications
* Have slightly different message text
* Use subtly different boundary semantics

---

# STEP 5 — Error Construction Redundancy

Identify:

* Similar `Error::new(...)` patterns across files
* Similar formatting strings across modules
* Multiple manual mapping blocks converting lower-level errors
* Similar match arms constructing identical variants

Produce:

| Error Pattern | Files | Consolidation Risk | Drift Risk |

Flag cases where error mapping logic differs subtly.

---

# STEP 6 — Cursor & Index Duplication Focus

Specifically inspect:

* Anchor envelope checks
* Bound conversions
* Raw key ordering comparisons
* Index entry construction logic
* Reverse index mutation symmetry logic
* Commit marker phase transitions

Produce:

| Area | Duplication Sites | Intentional? | Risk |

---

# STEP 7 — Consolidation Candidates Table

Produce:

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |

Owner Layer must respect guardrails.

Do NOT provide implementation details.
Do NOT provide code sketches.
Do NOT suggest collapsing layers.

---

# STEP 8 — Dangerous Consolidations (Do NOT Merge)

Explicitly identify duplication that should NOT be consolidated because:

* It reinforces boundary safety
* It prevents cross-layer dependency
* It protects recovery symmetry
* It protects cursor isolation

Produce:

| Area | Why Duplication Is Protective | Risk If Merged |

---

# STEP 9 — Quantitative Summary

Provide:

* Total duplication patterns found
* High-risk divergence duplications
* Defensive duplications
* Estimated LoC reduction range (conservative)

---

# OUTPUT STRUCTURE

1. High-Impact Consolidation Opportunities
2. Medium Opportunities
3. Low / Cosmetic
4. Dangerous Consolidations (Keep Separate)
5. Estimated LoC Reduction Range
6. Architectural Risk Summary
7. DRY Risk Index (1–10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Anti-Shallow Requirement

Do NOT:

* Recommend merging layers
* Recommend collapsing planner + executor
* Recommend “just make a util module”
* Comment on naming
* Comment on formatting
* Comment on macros
* Suggest public API changes

Every duplication must include:

* Location
* Pattern
* Risk classification
* Drift sensitivity
