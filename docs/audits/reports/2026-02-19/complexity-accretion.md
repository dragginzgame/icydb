# Complexity Accretion Audit - 2026-02-19

Scope: conceptual growth, branching pressure, flow multiplication, and cognitive load in `icydb-core`.

This run compares current `0.17/0.18` execution state against the 2026-02-18 baseline.

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ---- | ---- | ---- | ---- |
| `PlanError` family (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`) | 28 total (5 + 3 + 7 + 5 + 8) | plan validation + policy + cursor continuation | Yes | High |
| `QueryError` | 5 | query boundary wrappers | Yes (intent/plan/execute/response) | Medium |
| `ErrorClass` | 6 | global runtime taxonomy | No | Medium |
| Cursor error family (`CursorDecodeError`, `PrimaryKeyCursorSlotDecodeError`, `ContinuationTokenError`, `CursorPagingPolicyError`, `CursorPlanError`) | 19 total variants across 5 enums | cursor decode + token wire + policy + plan validation | Yes | High |
| Commit marker types (`CommitRowOp`, `CommitIndexOp`, `CommitMarker`) | 3 structs | commit protocol metadata | No | Low |
| `AccessPath` | 6 | planner/executor access strategy | No | High (fan-out impact) |
| Policy error enums (`PlanPolicyError`, `CursorPagingPolicyError`) | 8 variants across 2 enums | query/plan policy surface | No | Medium |
| `Predicate` AST | 12 | predicate syntax surface | No | Medium |
| `CompareOp` | 11 | operator surface | No | Medium |
| Commit-phase enums (`QueryMode`, `SaveMode`) | 5 variants across 2 enums | load/delete vs mutation mode | Yes | Medium |
| Store/data/index error family (`StoreError`, `StoreRegistryError`, data/index encode/decode families, range bound encode) | 26 variants across 11 enums | store/index/data trust boundaries | Yes | Medium-High |

Fastest-growing concept families in this snapshot: `PlanError` family (28), store/data/index error family (26), cursor error family (19), `Predicate` (12), `CompareOp` (11).

Baseline delta notes:
- `PlanError` family moved from 24 -> 28.
- `AccessPath` variants stayed at 6, but fan-out remains elevated.

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_paged_with_cursor_traced` | `crates/icydb-core/src/db/executor/load/mod.rs` | 5 (mode guard + fast-path dispatch + fallback + trace + cursor state) | 2 | plan validation + access execution + pagination + tracing | High |
| `try_execute_fast_paths` | `crates/icydb-core/src/db/executor/load/mod.rs` | 4 | 1 | fast-path gating + pagination/cursor interaction | High |
| `AccessPlan::produce_key_stream` | `crates/icydb-core/src/db/executor/context.rs` | 4 | 2 | plan-shape dispatch + composite stream composition | High |
| `AccessPath::produce_key_stream` | `crates/icydb-core/src/db/executor/context.rs` | 4 | 2 | access-path dispatch + store/index traversal + ordering normalization | High |
| `index_range_candidate_for_index` | `crates/icydb-core/src/db/query/plan/planner.rs` | 5 | 3 | predicate ops + range merge + index-shape extraction | High |
| `validate_unique_constraint` | `crates/icydb-core/src/db/index/plan/unique.rs` | 6 | 2 | unique policy + store lookup + corruption classification | High |
| `prepare_row_commit_for_entity` | `crates/icydb-core/src/db/commit/prepare.rs` | 6 | 2 | decode + index planning + reverse-relation mutation planning | High |

Pressure pattern: load/planner/commit boundaries continue to carry the highest branch density.

## Step 3 - Execution Path Multiplicity

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Save | 3 lanes (`single`, `batch_atomic`, `batch_non_atomic`) x 3 modes (`Insert/Update/Replace`) = 9 combinations | Partial | Yes | High |
| Replace | 3 (`replace`, `replace_many_atomic`, `replace_many_non_atomic`) | Yes | Yes | Medium |
| Delete | 2 (`empty short-circuit`, `commit-window apply`) | Yes | Yes | Medium |
| Load | 5 (`pk_stream`, `secondary_index_stream`, `index_range_limit_pushdown`, `fallback+budget`, `fallback+no-budget`) | Yes (`materialize_key_stream_into_page`, `finalize_rows_into_page`) | Yes | High |
| Recovery replay | 4 (`already recovered`, `marker replay`, `rebuild-only`, `rebuild rollback-on-failure`) | Partial | Yes | Medium |
| Cursor continuation | 4 (`none`, `boundary-only`, `boundary+index-range-anchor`, mismatch/reject`) | Yes | Yes | Medium |
| Index mutation | 5 (`insert`, `delete`, `update same key`, `update key move`, `no-op`) | Yes | Yes | High |
| Referential integrity enforcement | 3 (`save-time target checks`, `reverse-index mutation`, `delete-time strong-relation block`) | Partial | Yes | High |

Flow-pressure signal: save/load/index/relation paths remain above the “>4 independent flows” pressure threshold.

## Step 4 - Cross-Cutting Concern Spread

Module counts below use non-test files and grep-based structural signals.

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Index id validation | 27 | Partial | Medium-High |
| Key namespace validation | 14 | Partial | Medium |
| Component arity enforcement | 32 | No | High |
| Envelope boundary checks | 34 | Partial (range helpers central, callsites broad) | High |
| Reverse relation mutation | 17 | No | High |
| Unique constraint enforcement | 11 | Partial | Medium |
| Error origin mapping | 21 | Partial (constructor helpers improved, still broad) | Medium-High |
| Plan shape enforcement | 6 | Yes (`db::query::policy`) | Low-Medium |
| Anchor validation | 14 | Partial | Medium |
| Bound conversions | 20 | Partial (`resume_bounds`/`cursor_resume_bounds` exist) | Medium-High |

Scattering pressure remains highest in envelope/bound handling and component-shape enforcement.

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/load/mod.rs` (655 lines) | Long orchestration surface with multi-path branching | High | High |
| `crates/icydb-core/src/db/executor/tests/pagination.rs` (4699 lines) | Test-surface cognitive load >3k lines | High | Medium-High |
| `crates/icydb-core/src/db/index/plan/unique.rs` (167 lines) | Long integrity-critical function | High | High |
| `crates/icydb-core/src/db/commit/prepare.rs` (155 lines) | Long commit-preparation function mixing decode/index/relation phases | High | High |
| `"executor invariant violated"` occurrences (31 across non-test db files) | Repeated cross-layer invariant string pattern | Medium | Medium |
| Non-test db files over 150 lines (65) and over 300 lines (38) | Broad large-file footprint | Medium-High | Medium-High |

Baseline delta notes:
- `load/mod.rs` grew from 363 -> 655 lines.
- `executor/tests/pagination.rs` grew from 3348 -> 4699 lines.

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Access-path surface | `AccessPath` still referenced across 23 non-test db files | New path behavior multiplies planner/executor touch points | High |
| Plan-error surface | `PlanError` family growth (24 -> 28 total variants) | New plan/cursor states increase boundary mapping paths | High |
| Composite execution operators | Union/intersection + budget wrapper interactions | Ordering/pagination invariants span multiple stream operators | Medium-High |
| Cursor continuation channel | Boundary + direction + optional index-range anchor | Additional cursor payload/state checks multiply guard paths | Medium-High |
| Save lane x mode cross-product | New mode/rule changes propagate across 9 combinations | Cross-product amplification | High |
| Bound-conversion semantics | 20 non-test modules touched by bound constructs | Small bound-rule changes have broad blast radius | Medium-High |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | enum family growth around plan/cursor/store boundaries | Moderate-High | High |
| Execution Branching | high-branch orchestration in load/planner/commit paths | High | High |
| Path Multiplicity | load/save/index flow multiplication | High | High |
| Cross-Cutting Spread | boundary and bound semantics spread | Moderate-High | High |
| Cognitive Load | large orchestration/test surfaces and deep invariant stacks | High | High |

Overall Complexity Risk Index (1–10, lower is better): **7/10**

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

## Required Summary

1. Overall Complexity Risk Index
- 7/10

2. Fastest Growing Concept Families
- `PlanError` family (28 total variants), store/data/index error family (26), cursor error family (19), `Predicate` (12), `CompareOp` (11).

3. Variant Explosion Risks
- Plan/cursor validation growth is concentrated in `PlanError` family expansion and continuation-related error surfaces.
- `AccessPath` variant count is stable, but high fan-out keeps each behavioral extension expensive.

4. Branching Hotspots
- `execute_paged_with_cursor_traced`, `try_execute_fast_paths`, `AccessPlan::produce_key_stream`, `AccessPath::produce_key_stream`, `index_range_candidate_for_index`, `validate_unique_constraint`, `prepare_row_commit_for_entity`.

5. Flow Multiplication Risks
- Save remains a 9-combination lane/mode matrix.
- Load has 5 active execution routes when budgeted and non-budgeted fallback are counted separately.
- Index/relation mutation still introduces additional per-row branch surfaces.

6. Cross-Cutting Spread Risks
- Envelope/bound semantics and component-shape checks remain distributed across many modules.
- Error-origin mapping improved but still spans 21 non-test db files.

7. Early Structural Pressure Signals
- AccessPath fan-out remains at 23 non-test db files.
- `load/mod.rs` and `pagination.rs` size growth materially increased cognitive stack depth since baseline.
