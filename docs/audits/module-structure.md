# WEEKLY AUDIT — Structure / Module / Visibility Discipline

`icydb-core` (+ facade where relevant)

## Purpose

Verify that architectural boundaries remain:

* Layered
* Directional
* Encapsulated
* Narrowly exposed
* Intentionally public

This audit measures structural containment and visibility discipline.

It does NOT evaluate:

* Correctness
* Performance
* Features
* Style
* Refactoring ideas (unless boundary violation is severe)

---

# STEP 1 — Public Surface Mapping

## 1A. Crate Root Enumeration

Enumerate:

* All `pub mod` at crate root.
* All `pub use` re-exports.
* All `pub struct`, `pub enum`, `pub trait` reachable from crate root.
* All `pub fn` reachable from crate root.

Produce:

| Item | Path | Publicly Reachable From Root? | Intended Public API? | Risk |

---

## 1B. Exposure Classification

For each public item, classify:

* API Surface (intended for external callers)
* Facade-support type
* Macro support type
* Internal plumbing (should not be public)
* Accidentally exposed

Flag:

* Executor internals publicly reachable.
* Planner internals publicly reachable.
* Recovery or commit machinery publicly reachable.
* Raw storage types publicly reachable.
* `__internal` modules leaking.

---

## 1C. Public Field Exposure

Scan for:

* `pub struct` with `pub` fields.
* Enums exposing internal representation types.
* Public types exposing Raw* storage types.

Produce:

| Type | Public Fields? | Leaks Internal Representation? | Risk |

---

# STEP 2 — Subsystem Boundary Mapping

Evaluate the following subsystems:

* identity
* types
* serialize
* data
* index
* query/intent
* query/plan
* executable plan
* executor
* commit
* recovery
* cursor
* error
* facade (icydb)

For each subsystem:

## 2A. Dependency Direction

Identify:

* What it imports from.
* What imports it.

Produce:

| Subsystem | Depends On | Depended On By | Direction Clean? | Risk |

Expected high-level layering (reference model):

1. identity / types
2. serialize
3. data
4. index
5. query intent
6. planner
7. executable plan
8. executor
9. commit / recovery
10. facade

Confirm no subsystem depends upward in this hierarchy.

---

## 2B. Circular Dependency Check

Identify:

* Module-level circular references.
* Cross-subsystem back references.
* Mutual type imports across layers.

Flag any cycle.

Produce:

| Subsystem A | Subsystem B | Cycle? | Risk |

---

## 2C. Implementation Leakage

Flag cases where:

* Planner references executor internals.
* Executor references intent internal AST structures.
* Recovery references planner logic.
* Index layer references query-layer constructs.
* Error layer imports execution details.

Produce:

| Violation | Location | Description | Risk |

---

# STEP 3 — Visibility Hygiene Audit

Evaluate usage of:

* `pub`
* `pub(crate)`
* `pub(super)`
* private (default)

For each subsystem:

## 3A. Overexposure

Identify:

* `pub(crate)` that could be private.
* `pub` that could be `pub(crate)`.
* Helper functions unnecessarily exposed.

Produce:

| Item | Current Visibility | Could Be Narrower? | Risk |

---

## 3B. Under-Containment Signals

Flag patterns such as:

* Deep internal helpers used across multiple subsystems.
* Shared helpers bypassing intended boundary.
* Large modules using `pub(crate)` widely.

---

## 3C. Test Leakage

Check:

* Test-only modules exposed outside `#[cfg(test)]`.
* Internal test helpers marked pub unnecessarily.
* Test utilities imported by runtime modules.

---

# STEP 4 — Layering Integrity Validation

Using the expected layering model, validate:

### 4A. No Upward References

Confirm:

* Lower layers never depend on higher layers.
* Data layer does not depend on planner.
* Index layer does not depend on executor.
* Recovery does not depend on query planner logic.

Produce:

| Layer | Upward Dependency Found? | Description | Risk |

---

### 4B. Plan / Execution Separation

Confirm:

* Intent does not depend on execution.
* Planner does not depend on commit.
* Executor does not modify plan types.
* Plan types are immutable across layers.

---

### 4C. Facade Containment

Verify:

* Facade does not expose core internals.
* Facade does not re-export Raw* types.
* Facade maintains namespace discipline.

Produce:

| Facade Item | Leaks Core Internal? | Risk |

---

# STEP 5 — Structural Pressure Indicators

Identify signs of boundary erosion:

* Subsystems importing 5+ other subsystems.
* Large “hub” modules.
* Modules that mix identity + index + execution.
* Enums spanning multiple conceptual layers.
* Error types defined in low layers but used everywhere.

Produce:

| Area | Pressure Type | Drift Sensitivity | Risk |

---

# STEP 6 — Encapsulation Score

Evaluate:

| Category                  | Rating (1–10) |
| ------------------------- | ------------- |
| Public Surface Discipline |               |
| Layer Directionality      |               |
| Circularity Safety        |               |
| Visibility Hygiene        |               |
| Facade Containment        |               |

Then provide:

### Overall Structural Integrity Score (1–10)

Scale:

9–10 → Strong containment, clear directional layering
7–8 → Minor surface creep
5–6 → Moderate cross-layer coupling
3–4 → Architectural erosion emerging
1–2 → Structural fragility

---

# STEP 7 — Drift Sensitivity Analysis

Identify areas where:

* Adding a new AccessPath would force cross-layer edits.
* Adding DESC would require executor + planner + cursor + index edits.
* Adding new commit marker would require multiple subsystem edits.
* Adding new error type would widen visibility unnecessarily.

Produce:

| Growth Vector | Affected Subsystems | Drift Risk |

---

# Required Output Sections

1. Public Surface Map
2. Subsystem Dependency Graph
3. Circularity Findings
4. Visibility Hygiene Findings
5. Layering Violations
6. Structural Pressure Areas
7. Drift Sensitivity Summary
8. Structural Integrity Score

---

# Anti-Shallow Requirement

Do NOT:

* Say “structure looks clean.”
* Give high-level praise.
* Comment on naming.
* Comment on formatting.
* Propose redesign unless boundary violation is severe.

Every claim must identify:

* Module
* Dependency
* Visibility scope
* Directional impact
