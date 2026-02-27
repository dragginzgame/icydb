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

# STEP 1 — Change Surface Mapping (Empirical)

Analyze the last 3–5 major feature areas:

Examples:

* Range pushdown
* Cursor pagination
* Reverse relation index
* Unique enforcement changes
* Commit marker evolution

For each feature:

Produce:

| Feature | Files Modified | Subsystems Touched | Cross-Layer? | Localized? | Change Amplification Factor |

Change Amplification Factor (CAF):

= (# subsystems touched) × (# execution flows affected)

Flag:

* CAF > 6
* Features touching >5 subsystems
* Features requiring simultaneous edits in:

  * planner
  * executor
  * recovery
  * index
  * cursor

Identify patterns:

* Does every query feature require touching recovery?
* Does every index feature require planner changes?
* Does every ordering change require touching cursor?

---

# STEP 2 — Layer Boundary Integrity (Velocity-Oriented)

This is not correctness.
This is extension friction.

Check whether:

* Planner depends on executor implementation details.
* Executor matches on plan internals excessively.
* Recovery reimplements planner validation.
* Index layer depends on query-layer abstractions.
* Cursor codec depends on executable plan internals.
* Commit logic knows query semantics.

Produce:

| Boundary | Leakage Type | Velocity Impact | Severity |

Leakage increases future feature cost.

---

# STEP 3 — Growth Vector & Gravity Well Detection

Identify modules that:

* Grow faster than average.
* Absorb logic from other subsystems.
* Contain >3 conceptual domains.
* Branch on multiple unrelated enums.
* Are imported by most other modules.

Evaluate:

* `plan/executable.rs`
* `executor/load.rs`
* `executor/save.rs`
* `recovery.rs`
* `index/store`
* `cursor/*`

Produce:

| Module | Responsibilities | Import Fan-In | Import Fan-Out | Growth Rate | Bottleneck Risk |

Flag gravity wells:

* High fan-in + high fan-out
* Multi-domain responsibility
* Increasing branch density
* Frequent modification history

---

# STEP 4 — Change Multiplier Analysis

Evaluate how many places must change if you add:

1. Composite pagination
2. DESC support
3. Secondary index ordering
4. Query caching
5. Multi-index intersection
6. New commit phase
7. New AccessPath variant

For each:

| Feature | Subsystems Likely Impacted | Change Surface Size | Friction Level |

Friction Levels:

* Localized (≤2 subsystems)
* Moderate (3–4 subsystems)
* High (5+ subsystems)
* Surgical cross-system change required

---

# STEP 5 — Amplification Hotspots

Identify patterns where:

* Adding enum variant requires executor + planner + cursor + recovery changes.
* Adding invariant requires edits in multiple layers.
* Adding index behavior requires both planner and store changes.
* Cursor logic is tightly bound to plan structure.

Produce:

| Amplification Source | Why It Multiplies Change | Risk |

---

# STEP 6 — Predictive Structural Stress Points

Answer:

Which subsystems are most likely to:

* Slow future iteration?
* Accumulate branching pressure?
* Become coordination hubs?

Produce:

| Subsystem | Stress Vector | Risk Level |

---

# STEP 7 — Velocity Risk Table

Produce:

| Risk Area | Why It Slows Work | Amplification Factor | Severity | Containment Strategy (High-Level Only) |

Containment Strategy must be high-level.
No redesign proposals.
No refactors unless structural drag is severe.

---

# STEP 8 — Drift Sensitivity Index

Assess how sensitive velocity is to:

* AccessPath growth
* Error variant growth
* Recovery evolution
* Cursor complexity
* Index type expansion

Produce:

| Growth Vector | Drift Sensitivity | Risk |

---

# Final Output

1. Velocity Risk Index (1–10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

2. Architectural Drag Sources
3. Layer Leakage Findings
4. Gravity Wells
5. Feature Friction Map
6. Change Amplification Summary

---

# Anti-Shallow Rule

Do NOT say:

* “Seems modular”
* “Looks maintainable”
* “Separation is clear”

Every claim must include:

* Subsystems involved
* Branch count or dependency count
* Change multiplier estimate
* Growth vector

---

# Why This Audit Matters

Complexity audits measure entropy.
Invariant audits measure safety.
Recovery audits measure correctness symmetry.

Velocity audits measure:

> Whether the system still bends without breaking when features are added.

That’s architectural longevity.
