### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly


I’ve finished the code scan and pipeline trace. Next I’m compiling the final audit artifacts you asked for: per-
  file dependency/risk table, mixed-responsibility report, explicit structural violations, strict layering matrix,
  and an incremental refactor sequence.

• Audit Result
  Plan layering is partially clean but not strict. The planner itself is mostly declarative and decoupled from
  executor/storage, but plan/lowering and parts of plan/types/plan/validate mix planning with runtime/physical
  concerns.

  ### A. Dependency Graph Summary

  - plan::planner -> predicate AST/normalize/schema + model::{entity,index} + Value ✅
  - plan::validate -> predicate schema + policy + model ✅
  - plan::cursor -> query::contracts::cursor + query::cursor::spine ⚠ (cursor runtime spine dependency)
  - plan::lowering::{index_bounds,key_specs} -> db::index::{EncodedValue,RawIndexKey,raw_*} ❌ (physical encoding/raw
    key coupling inside plan)
  - plan::lowering::lowering_plan -> lowered raw specs + cursor planning + explain/fingerprint ⚠ (mixed lowering/
    runtime contract surface)
  - plan::types -> logical/access types plus cursor/streaming hints ⚠ (execution concerns in plan data model)

  ### B. Dependency Scan (per file)

  | File | External dependencies (classified) | Layer type | Risk |
  |---|---|---|---|
  | access_projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/access_projection.rs) |
  diagnostics (query::explain), other (Value) | Misc utilities | Low |
  | cursor/cursor_validation.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/cursor/
  cursor_validation.rs) | cursor (contracts::cursor, query::cursor::spine), index (Direction), other (InternalError)
  | Cursor boundary modeling | Medium |
  | cursor/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/cursor/mod.rs) | none | Cursor
  boundary wiring | Low |
  | cursor/page_window.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/cursor/page_window.rs) |
  none | Cursor boundary modeling | Low |
  | cursor/planned_cursor.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/cursor/planned_cursor.rs)
  | cursor (CursorBoundary), index raw (RawIndexKey) | Cursor boundary modeling | High |
  | logical/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/logical/mod.rs) | predicate
  (Predicate), other (QueryMode, ReadConsistency) | Logical plan construction | Low |
  | lowering/index_bounds.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/index_bounds.rs)
  | index raw/encoding APIs (EncodedValue, RawIndexKey, raw_bounds_*), schema/model (IndexModel) | Lowering contracts
  | High |
  | lowering/key_specs.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/key_specs.rs) |
  index raw/encoding APIs (EncodedValue, RawIndexKey, raw_keys_*), schema/model (IndexModel) | Lowering contracts |
  High |
  | lowering/lowering_plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/
  lowering_plan.rs) | cursor (ContinuationSignature, PlannedCursor), diagnostics (ExplainPlan, PlanFingerprint),
  index (Direction) | Execution routing/lowering boundary | High |
  | lowering/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/mod.rs) | none | Lowering
  wiring | Low |
  | mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/mod.rs) | index (Direction, KeyEnvelope),
  diagnostics (fingerprint::canonical) | Module boundary wiring | Medium |
  | planner/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/mod.rs) | predicate AST/
  schema/normalize, schema/model (EntityModel, IndexModel) | Access path modeling | Low |
  | planner/normalize.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/normalize.rs) |
  diagnostics helper (canonical) | Misc utilities | Low |
  | planner/range.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/range.rs) | predicate
  compare/coercion/schema, schema/model (EntityModel, IndexModel) | Index range extraction | Low |
  | tests.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/tests.rs) | predicate/schema/model (test)
  | Tests | Test-only |
  | types.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/types.rs) | index model (IndexModel),
  other (Value) | Access path modeling | Medium |
  | validate/access.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/access.rs) |
  predicate/schema, schema/model (EntityModel, IndexModel) | Validation | Low |
  | validate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/mod.rs) | cursor decode
  (codec::cursor), predicate/schema, policy, schema/model | Validation orchestration | Medium |
  | validate/order.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/order.rs) | predicate
  schema + model | Validation | Low |
  | validate/pushdown.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/pushdown.rs) | model
  + plan order/access shapes | Pushdown applicability | Low |
  | validate/semantics.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/semantics.rs) |
  policy | Validation | Low |
  | validate/tests.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/tests.rs) | predicate/
  schema/model (test) | Tests | Test-only |

  High-risk import flags requested

  - executor::*: none in plan production code.
  - data::*: only test-only import at lowering/lowering_plan.rs:201 (/home/adam/projects/icydb/crates/icydb-core/src/
    db/query/plan/lowering/lowering_plan.rs#L201).
  - storage::*: none.
  - commit::*: none.
  - Db / Context: none.
  - Note: executor reference appears only in doc comment at logical/mod.rs:4 (/home/adam/projects/icydb/crates/icydb-
    core/src/db/query/plan/logical/mod.rs#L4).

  ### C. Mixed Responsibility Report

  1. File: types.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/types.rs)
     Mixed:

  - Access path/data model (AccessPlan, AccessPath, SemanticIndexRangeSpec)
  - Cursor/streaming hints (CursorSupport, is_full_scan_or_key_range) at types.rs:223 (/home/adam/projects/icydb/
    crates/icydb-core/src/db/query/plan/types.rs#L223), types.rs:263 (/home/adam/projects/icydb/crates/icydb-core/
    src/db/query/plan/types.rs#L263)
    Status: MIXED

  2. File: validate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/mod.rs)
     Mixed:

  - Logical validation owner
  - Defensive executor-boundary validation
  - Cursor error taxonomy (CursorPlanError) at validate/mod.rs:166 (/home/adam/projects/icydb/crates/icydb-core/src/
    db/query/plan/validate/mod.rs#L166)
    Status: MIXED

  3. File: lowering/lowering_plan.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/
     lowering_plan.rs)
     Mixed:

  - Lowered key-spec packaging
  - Executor-ready wrapper (ExecutablePlan) at lowering/lowering_plan.rs:42 (/home/adam/projects/icydb/crates/icydb-
    core/src/db/query/plan/lowering/lowering_plan.rs#L42)
  - Cursor planning/revalidation at lowering/lowering_plan.rs:117 (/home/adam/projects/icydb/crates/icydb-core/src/
    db/query/plan/lowering/lowering_plan.rs#L117), lowering/lowering_plan.rs:175 (/home/adam/projects/icydb/crates/
    icydb-core/src/db/query/plan/lowering/lowering_plan.rs#L175)
    Status: MIXED

  ### D. Structural Violations

  1. Violation: plan layer uses raw/encoded key types
     Where:

  - lowering/index_bounds.rs:2 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/
    index_bounds.rs#L2)
  - lowering/key_specs.rs:3 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/key_specs.rs#L3)
  - cursor/planned_cursor.rs:1 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/cursor/
    planned_cursor.rs#L1)
    Why problematic: violates strict “planning must not encode physical bytes/raw keys.”
    Fix: move raw-key encoding and RawIndexKey anchor ownership into executor-side lowering/route modules.

  2. Violation: cursor protocol not fully isolated to cursor/
     Where:

  - validate/mod.rs:166 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/validate/mod.rs#L166)
    (CursorPlanError)
  - lowering/lowering_plan.rs:117 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/
    lowering_plan.rs#L117) (cursor planning methods)
  - types.rs:223 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/types.rs#L223) (CursorSupport)
    Why problematic: cursor boundary logic is spread across validation/types/lowering.
    Fix: centralize cursor domain into plan/cursor (errors, DTOs, validators), expose only minimal contract outward.

  3. Violation: plan namespace contains executor-ready wrapper
     Where:

  - lowering/lowering_plan.rs:42 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/lowering/
    lowering_plan.rs#L42)
  - created directly from intent at intent/mod.rs:525 (/home/adam/projects/icydb/crates/icydb-core/src/db/query/
    intent/mod.rs#L525)
    Why problematic: ExecutablePlan in query::plan blurs “plan contract” and “executor assembly.”
    Fix: relocate executable assembly to executor boundary; keep plan output as declarative/lowered contracts only.

  ### E. Required Boundary Checks

  Logical vs Physical

  - LogicalPlan is mostly clean intent-only (no raw bytes, no executor flags): logical/mod.rs:40 (/home/adam/
    projects/icydb/crates/icydb-core/src/db/query/plan/logical/mod.rs#L40) ✅
  - AccessPlan/AccessPath are descriptive and do not execute/fetch ✅
  - Physical concerns begin in plan/lowering and cursor/planned_cursor ❌ (strict rule violation)

  Cursor Protocol

  - Cursor types exist in cursor/ (good), but also leak via validate/mod.rs and types.rs (not fully isolated) ⚠

  Pushdown & Predicate Coupling

  - IndexPredicateProgram usage: none.
  - validate/pushdown.rs decides eligibility structurally (order + access shape), not runtime predicate slots ✅
  - field_slots in SemanticIndexRangeSpec are index-position metadata, not runtime predicate program slots (still a
    coupling point to monitor) ⚠

  Execution Routing Boundary

  - ExecutionRoutePlan exists in executor layer (executor/route/mod.rs:155 (/home/adam/projects/icydb/crates/icydb-
    core/src/db/executor/route/mod.rs#L155)) ✅
  - But route-adjacent concerns (ExecutablePlan, cursor direction derivation) remain in plan/lowering ⚠

  Index Model Coupling

  - planner/* uses model metadata only (IndexModel) ✅
  - plan/lowering/* uses EncodedValue/RawIndexKey and raw-key APIs ❌

  AccessPath Integrity

  - No stream construction/fetch functions in plan/access types ✅
  - No resolve_physical_key_stream/ordered_key_stream_from_access found ✅
  - Minor contamination via execution hints in types.rs (CursorSupport, PK streaming helper) ⚠

  Public API Surface (plan/mod.rs)

  - Public exports: OrderDirection, PlanError only ✅
  - Internal planning/physical details are not publicly exported at crate boundary (good minimal external surface) ✅

  ### Strict Layering Matrix (non-test files)

  | File | R1 No executor dep | R2 No storage/index-storage API dep | R3 No physical bytes | R4 Cursor isolated | R5
  Pushdown not runtime-slot-based |
  |---|---|---|---|---|---|
  | access_projection.rs | Pass | Pass | Pass | Pass | Pass |
  | cursor/cursor_validation.rs | Pass | Pass | Pass | Pass (external spine coupling) | Pass |
  | cursor/mod.rs | Pass | Pass | Pass | Pass | Pass |
  | cursor/page_window.rs | Pass | Pass | Pass | Pass | Pass |
  | cursor/planned_cursor.rs | Pass | Pass | Fail | Pass | Pass |
  | logical/mod.rs | Pass | Pass | Pass | Pass | Pass |
  | lowering/index_bounds.rs | Pass | Fail | Fail | Pass | Pass |
  | lowering/key_specs.rs | Pass | Fail | Fail | Pass | Pass |
  | lowering/lowering_plan.rs | Pass | Pass | Fail | Fail | Pass |
  | lowering/mod.rs | Pass | Pass | Pass | Pass | Pass |
  | mod.rs | Pass | Pass | Pass | Pass | Pass |
  | planner/mod.rs | Pass | Pass | Pass | Pass | Pass |
  | planner/normalize.rs | Pass | Pass | Pass | Pass | Pass |
  | planner/range.rs | Pass | Pass | Pass | Pass | Pass |
  | types.rs | Pass | Pass | Pass | Fail | Pass |
  | validate/access.rs | Pass | Pass | Pass | Pass | Pass |
  | validate/mod.rs | Pass | Pass | Pass | Fail | Pass |
  | validate/order.rs | Pass | Pass | Pass | Pass | Pass |
  | validate/pushdown.rs | Pass | Pass | Pass | Pass | Pass |
  | validate/semantics.rs | Pass | Pass | Pass | Pass | Pass |

  ### Refactor Plan (ordered, incremental, low-risk)

  1. Move cursor error taxonomy to plan/cursor/errors.rs; keep re-export shim in validate/mod.rs.
  2. Move CursorSupport and is_full_scan_or_key_range out of types.rs into executor route helper traits.
  3. Introduce a semantic-only IndexScanContract in plan; move raw-key encoding (EncodedValue, RawIndexKey,
     raw_keys_*) to executor-side lowering module.
  4. Split ExecutablePlan into:
      - plan output contract (LoweredPlanContract, no cursor/runtime helpers),
      - executor wrapper (ExecutablePlan) under executor namespace.
  5. Keep plan/cursor as single cursor boundary package (decode/validate/revalidate interfaces).
  6. Maintain compatibility by re-exporting old names during transition; update call sites in query::intent and
     session.
  7. After migration, tighten plan/mod.rs exports to declarative plan contracts only; run full tests.

  ### Architecture Score

  - Plan Layering Integrity Score: 6.8 / 10

  Primary Risks:

  - Physical key/encoding logic in query::plan namespace.
  - Cursor concerns spread across types, validate, and lowering.
  - ExecutablePlan defined in plan layer rather than executor boundary.

  Most Urgent Refactors:

  1. Move raw-key/encoding lowering out of plan.
  2. Consolidate cursor domain under plan/cursor.
  3. Relocate executable assembly from plan/lowering to executor.

  Structural Strengths:

  - Core planner (planner/*) is predicate/schema/index-model driven and mostly clean.
  - No production imports of executor::*, data::*, storage::*, commit::*, Db, Context.
  - External public plan API surface is minimal (OrderDirection, PlanError).
