Perform a Velocity Preservation Audit of icydb-core.

This is NOT a correctness audit.
This is NOT a DRY audit.
This is NOT a style audit.

Evaluate whether the architecture still supports rapid, safe feature iteration.

---

STEP 1 — Change Surface Mapping

For the last major feature areas (e.g. range pushdown, cursor pagination, reverse relation index):

For each feature:
- Count how many files were modified.
- Count how many layers were touched.
- Identify whether changes were localized or cross-cutting.

Flag:
- Any feature requiring planner + executor + recovery + index + cursor changes simultaneously.
- Any feature that required touching more than 5 subsystems.

---

STEP 2 — Layer Boundary Integrity

Check whether:

- Planner logic leaks into executor.
- Executor logic leaks into planner.
- Recovery reimplements planner logic.
- Index logic depends on query semantics.
- Cursor codec knows plan structure.

List boundary violations (if any).

---

STEP 3 — Growth Vectors

Identify modules that:

- Are growing faster than others.
- Contain multiple conceptual responsibilities.
- Are absorbing logic from other layers.
- Are acting as “gravity wells”.

Examples to examine:
- plan/executable.rs
- load executor
- recovery.rs
- index store
- cursor continuation

Flag modules that may become architectural bottlenecks.

---

STEP 4 — Predictive Friction

Answer:

If you were to implement:
- Composite pagination
- DESC range pushdown
- Secondary index ordering
- Query caching
- Multi-index intersection

Would each require:
- Localized change?
- Or cross-system surgery?

List likely friction points.

---

STEP 5 — Velocity Risk Table

Produce:

| Risk Area | Why It Slows Future Work | Severity | Mitigation Strategy |

---

OUTPUT

1. Current Velocity Health Score (1–10)
2. Architectural Drag Sources
3. Layer Leakage Findings
4. Future Feature Friction Map
5. Immediate Structural Hardening Suggestions (if any)

No stylistic advice.
No speculative redesign.
Only structural velocity analysis.
