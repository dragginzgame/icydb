# Complexity Accretion Audit - 2026-02-20

Scope: conceptual growth, branching pressure, flow multiplication, and cognitive load in `icydb-core`.

This run compares current `main` workspace state against the 2026-02-19 complexity baseline.

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
| Store/data/index error family (`StoreError`, `StoreRegistryError`, `IndexRangeBoundEncodeError`, `StorageKeyEncodeError`, `RawRowError`, `RowDecodeError`, `DataKeyEncodeError`, `KeyDecodeError`, `DataKeyDecodeError`, `FingerprintVerificationError`, `OrderedValueEncodeError`, `IndexEntryEncodeError`) | 27 variants across 12 enums | store/index/data trust boundaries | Yes | Medium-High |

Fastest-growing concept families in this snapshot: `PlanError` family (28), store/data/index error family (27), cursor error family (19), `Predicate` (12), `CompareOp` (11).

Baseline delta notes:
- `PlanError` family stayed at 28.
- `AccessPath` stayed at 6 variants; fan-out remains high.

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_paged_with_cursor_traced` | `crates/icydb-core/src/db/executor/load/mod.rs` | 4 | 0 | cursor validation + route planning + fallback orchestration | Medium-High |
| `try_execute_fast_path_plan` | `crates/icydb-core/src/db/executor/load/execute.rs` | 4 | 0 | fast-path precedence + materialization handoff | Medium-High |
| `AccessPath::produce_key_stream` | `crates/icydb-core/src/db/executor/context.rs` | 3 | 1 | access-path dispatch + store/index traversal + ordering normalization | High |
| `index_range_candidate_for_index` | `crates/icydb-core/src/db/query/plan/planner.rs` | 5 | 2 | predicate ops + range merge + index-shape extraction | High |
| `is_index_range_limit_pushdown_shape_eligible` | `crates/icydb-core/src/db/executor/load/index_range_limit.rs` | 5 | 0 | order-shape validation + index-range constraints | Medium-High |
| `validate_unique_constraint` | `crates/icydb-core/src/db/index/plan/unique.rs` | 6 | 0 | unique policy + store lookup + corruption classification | High |
| `prepare_row_commit_for_entity` | `crates/icydb-core/src/db/commit/prepare.rs` | 6 | 0 | decode + index planning + reverse-relation mutation planning | High |

Pressure pattern: planner + commit/index integrity functions remain the highest branch-density surface; load orchestration branch pressure is now split across `load/{mod,route,execute,page,...}.rs`.

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

Flow-pressure signal: save/load/index/relation paths remain above the `>4 independent flows` pressure threshold.

## Step 4 - Cross-Cutting Concern Spread

Module counts below use non-test `db/` files and grep-based structural signals.

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Index id validation | 24 | Partial | Medium-High |
| Key namespace validation | 25 | Partial | Medium |
| Component arity enforcement | 24 | Partial | Medium-High |
| Envelope boundary checks | 46 | No | High |
| Reverse relation mutation | 10 | Partial | Medium |
| Unique constraint enforcement | 4 | Yes | Low-Medium |
| Error origin mapping | 44 | Partial | High |
| Plan shape enforcement | 35 | Partial | Medium-High |
| Anchor validation | 13 | Partial | Medium |
| Bound conversions | 20 | Partial | Medium-High |

Scattering pressure remains highest in boundary handling and error-origin mapping.

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/load/mod.rs` (361 lines) | Orchestration root shrank but still coordinates many phases | Medium | Medium |
| `crates/icydb-core/src/db/executor/tests/pagination.rs` (4911 lines) | Test-surface cognitive load >3k lines | High | High |
| `validate_unique_constraint` in `crates/icydb-core/src/db/index/plan/unique.rs` (147 logical lines) | Long integrity-critical function | High | High |
| `prepare_row_commit_for_entity` in `crates/icydb-core/src/db/commit/prepare.rs` (132 logical lines) | Long commit-preparation function mixing decode/index/relation phases | High | High |
| `"executor invariant violated"` occurrences (28 across non-test db files) | Repeated invariant-string pattern across layers | Medium | Medium |
| Non-test db files over 150 lines (66) and over 300 lines (39) | Broad large-file footprint | Medium-High | Medium-High |

Baseline delta notes:
- `load/mod.rs` shrank from 655 -> 361 lines after load-module decomposition.
- `executor/tests/pagination.rs` grew from 4699 -> 4911 lines.
- `"executor invariant violated"` occurrences decreased from 31 -> 28.

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Access-path surface | `AccessPath` appears in 23 non-test db files and 252 references | New path behavior still fans into planner/executor/index surfaces | High |
| Plan-error surface | `PlanError` family stable at 28 variants | New plan/cursor states still increase boundary mapping branches | High |
| Load route matrix | 3 fast paths + fallback, with fallback budget gating | Route additions multiply validation + cursor continuation interactions | Medium-High |
| Cursor continuation channel | Boundary + direction + optional index-range anchor | Payload/state changes still multiply guard paths | Medium-High |
| Save lane x mode cross-product | 9 active combinations | New save rules propagate across all lane/mode combinations | High |
| Bound-conversion semantics | 20 modules use bound-conversion constructs | Small bound-rule changes still have broad blast radius | Medium-High |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | enum family growth around plan/cursor/store boundaries | Moderate-High | High |
| Execution Branching | planner/commit/index hotspots remain branch-dense | Moderate-High | High |
| Path Multiplicity | save/load/index flow multiplication remains elevated | High | High |
| Cross-Cutting Spread | boundary + error-origin concepts remain broad | High | High |
| Cognitive Load | pagination test size and long integrity helpers | High | High |

Overall Complexity Risk Index (1-10, lower is better): **6/10**

Interpretation:
1-3  = Low risk / structurally healthy
4-6  = Moderate risk / manageable pressure
7-8  = High risk / requires monitoring
9-10 = Critical risk / structural instability

## Required Summary

1. Overall Complexity Risk Index
- 6/10

2. Fastest Growing Concept Families
- `PlanError` family (28 total variants), store/data/index error family (27), cursor error family (19), `Predicate` (12), `CompareOp` (11).

3. Variant Explosion Risks
- Plan/cursor validation remains concentrated in `PlanError` and continuation-related error surfaces.
- `AccessPath` variant count is stable, but high fan-out keeps each behavioral extension expensive.

4. Branching Hotspots
- `index_range_candidate_for_index`, `validate_unique_constraint`, `prepare_row_commit_for_entity`, `AccessPath::produce_key_stream`, `try_execute_fast_path_plan`.

5. Flow Multiplication Risks
- Save remains a 9-combination lane/mode matrix.
- Load still carries 5 routes when budgeted and non-budgeted fallback are separated.
- Index/relation mutation paths continue adding per-row branch surfaces.

6. Cross-Cutting Spread Risks
- Boundary semantics and error-origin mapping remain distributed across many modules.
- Plan-shape enforcement is explicit but still spread across validation and executor routing.

7. Early Structural Pressure Signals
- AccessPath fan-out remains at 23 non-test db files, with references increasing 251 -> 252.
- The load orchestration root shrank, but complexity moved into additional load submodules rather than disappearing.
- Pagination tests continue growing and remain the largest cognitive surface.
