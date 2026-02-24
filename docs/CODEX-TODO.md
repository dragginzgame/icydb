### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly




# Codex Task: Deep Layering Audit — `db/query/plan`

## Objective

Perform a strict architectural audit of:

```
crates/icydb-core/src/db/query/plan
```

This audit must determine:

1. Whether planning is cleanly separated from execution.
2. Whether cursor protocol is isolated from logical planning.
3. Whether index selection logic is decoupled from storage.
4. Whether plan lowering is contaminating runtime semantics.
5. Whether the module boundaries reflect a proper query compiler pipeline.

This is not a logic review. It is a **layering integrity and module separation audit**.

---

# 1️⃣ Target Architecture Model (Reference)

The plan subsystem must obey this conceptual layering:

```text
LogicalPlan (pure query intent)
    ↓
AccessPlan (index selection + scan shape)
    ↓
ExecutionRoutePlan (mode selection, streaming/materialized)
    ↓
Lowering contracts (cursor boundary, pushdown eligibility)
    ↓
Executor
```

Hard requirements:

* Planning must not depend on executor implementation.
* Planning must not depend on storage or commit logic.
* Planning may depend on:

  * predicate AST
  * schema metadata
  * index metadata (models only, not storage access)
* Cursor protocol must not leak executor mechanics.

---

# 2️⃣ Dependency Scan

For every file under:

```
db/query/plan/
```

Codex must:

### A. Extract all `use crate::...` imports.

Classify dependencies as:

* predicate
* schema/model
* index (model-level only)
* executor
* storage
* cursor
* diagnostics
* other

Produce a table:

| File | External Dependencies | Layer Type | Risk |
| ---- | --------------------- | ---------- | ---- |

### B. Explicitly flag imports of:

* `executor::*`
* `data::*`
* `storage::*`
* `commit::*`
* `Db`
* `Context`

These are high-risk in planning.

---

# 3️⃣ Responsibility Classification

For each file, classify it as primarily:

* Logical plan construction
* Validation
* Access path modeling
* Index range extraction
* Cursor boundary modeling
* Pushdown applicability
* Execution routing
* Misc utilities

Then detect:

### ❗ Mixed-responsibility files

Example:

```
plan/logical/mod.rs
Responsibilities:
  - Logical AST
  - Pushdown gating
  - Cursor boundary logic

Status: MIXED — should split
```

---

# 4️⃣ Logical vs Physical Planning Separation

Confirm that:

### LogicalPlan:

* Represents query intent only.
* Contains no:

  * index slot numbers
  * encoded values
  * storage byte details
  * executor streaming flags

### AccessPlan:

* Represents index selection and scan strategy.
* Does not:

  * read storage
  * execute anything
  * allocate streams

If LogicalPlan contains physical concerns → violation.

If AccessPlan contains executor-level stream logic → violation.

---

# 5️⃣ Cursor Protocol Audit

Identify where cursor-related types are defined:

Examples:

* `CursorBoundary`
* `CursorPlanError`
* `compute_page_window`
* Direction

Check:

### A. Is cursor protocol embedded inside logical planning?

If yes → recommend:

```
plan/
  logical/
  access/
  cursor/
```

Cursor protocol should be isolated from logical planning.

---

# 6️⃣ Pushdown & Predicate Coupling Audit

Inspect usage of:

* `IndexPredicateProgram`
* `PushdownApplicability`
* predicate slot logic

Determine:

* Is pushdown eligibility decided in plan?
* Does plan depend on runtime predicate internals?
* Is there leakage of slot-based logic into planning?

Planning should reason at structural level, not runtime-slot level.

If plan imports predicate runtime types → violation.

---

# 7️⃣ Execution Routing Boundary Audit

If `ExecutionRoutePlan` or routing logic exists in plan:

Check:

* Does it depend on executor?
* Does it encode streaming/materialized decisions?
* Does it import load or aggregate executor modules?

Routing must be a **contract**, not an execution implementation.

If plan imports executor load code → structural breach.

---

# 8️⃣ Index Model Coupling Audit

Planning may depend on:

* IndexModel
* IndexPrefixSpec
* IndexRangeSpec

But must NOT depend on:

* RawIndexKey
* EncodedValue
* index storage APIs

If any of those appear in plan → violation.

---

# 9️⃣ Access Path Integrity Review

For `AccessPath`:

Check:

* Is it purely descriptive?
* Or does it contain logic to resolve physical streams?
* Does it allocate or fetch?

If it contains execution logic (e.g., building key streams) → this belongs in executor, not plan.

Flag any functions resembling:

```
resolve_physical_key_stream
ordered_key_stream_from_access
```

If located under `query/plan`, recommend moving to executor.

---

# 🔟 Public API Surface Audit

From `query/plan/mod.rs`:

List all `pub` exports.

For each, classify:

* External DSL surface
* Internal planning type
* Executor contract type

Flag:

* Internal-only types that are publicly exported.
* Physical planning details leaking into public API.

---

# 11️⃣ Strict Layering Matrix

For each file, evaluate:

| Rule                                                  | Pass/Fail | Notes |
| ----------------------------------------------------- | --------- | ----- |
| LogicalPlan does not depend on executor               |           |       |
| Planning does not import storage                      |           |       |
| Planning does not encode physical bytes               |           |       |
| Cursor protocol isolated                              |           |       |
| Pushdown eligibility does not depend on runtime slots |           |       |

---

# 12️⃣ Required Output From Codex

Codex must produce:

---

## A. Dependency Graph Summary

Example:

```
plan::logical → predicate::ast
plan::access → index::model
plan::cursor → plan::logical
plan::route → executor::Context   ❌ violation
```

Explicitly mark violations.

---

## B. Mixed Responsibility Report

Example:

```
File: plan/logical/mod.rs
Mixed responsibilities:
  - LogicalPlan
  - CursorBoundary
  - Pushdown gating

Recommendation:
  Extract cursor logic to plan/cursor/
```

---

## C. Structural Violations

Explicit listing:

```
Violation:
plan/access.rs imports executor::OrderedKeyStream

Why problematic:
Planning must not construct runtime streams.

Fix:
Move stream resolution to executor.
```

---

## D. Refactor Plan (Ordered, Incremental)

Provide a low-risk sequence:

1. Extract cursor module.
2. Move physical stream resolution out of plan.
3. Restrict visibility of internal types.
4. Isolate pushdown applicability logic.
5. Tighten public API.

Each step must preserve compilation and tests.

---

## E. Architecture Score

Conclude with:

```
Plan Layering Integrity Score: X / 10

Primary Risks:
...

Most Urgent Refactors:
...

Structural Strengths:
...
```

---

# Success Criteria

The plan subsystem is considered architecturally sound if:

* Logical planning is purely declarative.
* Physical stream construction lives in executor.
* Cursor protocol is isolated.
* Pushdown eligibility is structural, not runtime-based.
* No storage/executor coupling exists.
* Public API surface is minimal and intentional.

