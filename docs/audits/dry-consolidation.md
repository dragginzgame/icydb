Perform a DRY / redundancy / consolidation audit of icydb-core (and facade where relevant).

This is NOT a style audit.
This is NOT an invariant audit.
Do not refactor anything yet.

Your job is to identify duplicated logic and structural repetition that increases maintenance risk.

## Consolidation Guardrails

- Do not consolidate across architectural layers.
- Do not merge planner + executor logic.
- Do not remove defensive duplication without verifying invariant preservation.
- DRY must not reduce safety or clarit

---

STEP 1 — Structural Duplication Scan

Identify:

- Repeated invariant checks
- Repeated error construction blocks
- Repeated format strings
- Repeated index anchor validation logic
- Repeated continuation-token envelope checks
- Repeated reverse-relation index mutation patterns
- Repeated deserialize + map error wrappers
- Repeated commit marker mapping patterns
- Repeated entity key match validation

For each duplication:

A. List file + line references.
B. Explain whether the duplication is:
   - Accidental duplication
   - Intentional boundary duplication
   - Defensive duplication
   - Drift duplication

---

STEP 2 — Pattern-Level Redundancy

Look for:

- Multiple modules implementing similar encode/decode wrappers
- Multiple places converting between PlanError and QueryError
- Multiple index key validation entry points
- Multiple cursor token to wire conversions
- Multiple raw-key range envelope checks

For each pattern:

- Describe the shared pattern.
- Count occurrences.
- Estimate consolidation difficulty (Low / Medium / High).
- Identify which layer should own it.

---

STEP 3 — Over-Splitting or Under-Splitting

Detect:

- Files over ~600 lines doing more than one conceptual job.
- Modules that could be split by execution phase (plan vs execute vs validate).
- Modules that are artificially separated but tightly coupled.
- Tests with repeated setup blocks that could share helpers.

Do NOT recommend speculative splitting.
Only flag clear structural pressure points.

---

STEP 4 — Invariant Repetition Risk

Specifically detect:

- Invariant checks duplicated across planner and executor.
- Index id mismatch checks in multiple layers.
- Cursor payload validation repeated across modules.
- Reverse-relation mutation checks repeated in save + recovery.

Flag whether duplication:
- Improves safety
- Or creates divergence risk

---

STEP 5 — Consolidation Candidates

Produce a table:

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |

Do NOT provide actual refactors.
Only provide consolidation strategy direction.

---

OUTPUT FORMAT

1. High-Impact Consolidation Opportunities
2. Medium Opportunities
3. Low / Cosmetic
4. Dangerous Consolidations (should NOT be merged)
5. Estimated LoC Reduction Range
6. Architectural Risk Summary

Avoid:
- Naming opinions
- Formatting suggestions
- Macro evangelism
- Public API reshaping

Focus strictly on structural duplication and consolidation risk.
