Perform a Complexity Accretion Audit of icydb-core.

This audit measures conceptual growth and branching pressure.

Do NOT discuss correctness.
Do NOT discuss style.
Do NOT propose redesign unless complexity risk is extreme.

---

STEP 1 — Variant Growth

Count:

- PlanError variants
- QueryError variants
- ErrorClass variants
- Cursor error types
- Commit marker types
- AccessPath variants

Flag:
- Rapidly growing enums
- Enums that mix unrelated domains
- Variants that duplicate meaning

---

STEP 2 — Execution Branching

Identify functions with:

- >3 nested match blocks
- >3 nested conditional layers
- >5 early-return invariant checks
- Large semantic branching based on plan type

List hotspots.

---

STEP 3 — Path Multiplicity

Count independent execution flows for:

- Save
- Replace
- Delete
- Load
- Recovery replay
- Cursor continuation

Flag flows that:

- Duplicate logic
- Diverge subtly
- Require mental simulation of multiple branches

---

STEP 4 — Cross-Cutting Concern Spread

Check how many places implement logic for:

- Index id validation
- Key namespace checks
- Envelope boundary checks
- Reverse relation mutation
- Error origin classification

If a concept appears in >4 modules, flag it.

---

STEP 5 — Cognitive Load Indicators

Look for:

- Repeated format string patterns
- Long error messages repeated across files
- Functions exceeding ~80–100 logical lines
- Test files >3k lines with heavy setup duplication

Flag areas where mental stack depth is increasing.

---

STEP 6 — Complexity Risk Score

Provide:

| Area | Complexity Type | Accretion Rate | Risk Level |

---

OUTPUT

1. Overall Complexity Score (1–10)
2. Fastest Growing Concepts
3. Variant Explosion Risks
4. Branching Hotspots
5. Early Refactor Candidates (only if structural pressure is high)

Avoid:
- Naming complaints
- Macro commentary
- Minor cosmetic issues

Focus strictly on conceptual load growth.
