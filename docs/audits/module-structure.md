Perform a Structure / Module / Visibility Audit of icydb-core (and facade where relevant).

Do NOT evaluate correctness.
Do NOT evaluate performance.
Do NOT propose feature redesign.

Focus only on architecture boundaries and visibility discipline.

---

STEP 1 — Public Surface Mapping

1. Enumerate:
   - All pub mod at crate root.
   - All pub use re-exports.
   - All pub types reachable from crate root.

2. Confirm:
   - Public API is intentionally namespaced.
   - No internal executor / planner / recovery types are publicly exposed unintentionally.
   - No __internal modules leak through public surface.

3. Flag:
   - Any type that should likely be pub(crate) but is pub.
   - Any accidental re-export.
   - Any public struct exposing internal fields.

---

STEP 2 — Internal Boundary Discipline

Evaluate each subsystem:

- db/query/plan
- db/query/intent
- db/executor
- db/index
- db/commit
- db/data
- cursor
- identity
- serialize
- error

For each subsystem:

1. Identify:
   - What it is allowed to depend on.
   - What depends on it.

2. Confirm:
   - No circular dependencies.
   - No upward-layer dependencies (executor referencing intent internals, etc).
   - No cross-subsystem leakage of implementation details.

Flag violations.

---

STEP 3 — Visibility Hygiene

Scan for:

- pub(crate) used where private would suffice.
- pub(super) used correctly for narrow boundary enforcement.
- Excess pub in test-only modules.
- Internal helpers exposed beyond necessary module.

For each:
- Explain whether visibility is too wide, correct, or overly restrictive.

---

STEP 4 — Layering Integrity

Validate layering assumptions:

Expected high-level layering:

- identity / types
- serialize
- data
- index
- query intent
- query planner
- executable plan
- executor
- commit / re
