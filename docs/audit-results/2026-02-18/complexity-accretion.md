# Complexity Accretion Audit - 2026-02-18

Scope: conceptual growth, branching pressure, flow multiplication, and cognitive load in `icydb-core`.

No previous dated complexity result exists in this repository snapshot, so this run establishes the baseline for future weekly diffs.

This baseline includes the pre-DESC structural containment refactor (`Direction`, centralized resume bounds, centralized envelope checks) without enabling DESC traversal.

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ---- | ---- | ---- | ---- |
| `PlanError` | 24 | plan validation + cursor semantics | Yes (policy + cursor wire + plan shape) | High |
| `QueryError` | 5 | top-level query boundary | Yes (validate/plan/intent/response/execute wrappers) | Medium |
| `ErrorClass` | 6 | global runtime taxonomy | No | Medium |
| Cursor error family (`CursorDecodeError`, `ContinuationTokenError`, `PrimaryKeyCursorSlotDecodeError`, cursor policy enums, cursor-focused `PlanError` variants) | 20 total variants across 6 enums | cursor decode + policy + plan validation | Yes | High |
| Commit marker types (`CommitRowOp`, `CommitIndexOp`, `CommitMarker`) | 3 structs | commit protocol metadata | No | Low |
| `AccessPath` | 6 | planner/executor access strategy | No | High (fan-out impact) |
| Policy error enums (`PlanPolicyError`, `CursorPagingPolicyError`, `CursorOrderPolicyError`) | 9 variants across 3 enums | intent/plan policy | No | Medium |
| `Predicate` AST | 12 | query predicate syntax | No | Medium |
| `CompareOp` | 11 | predicate operator surface | No | Medium |
| Commit-phase enums (`QueryMode`, `SaveMode`) | 5 variants across 2 enums | load/delete mode + mutation mode | Yes (query + mutation lane) | Medium |
| Store/data/index error family (`StoreError`, `StoreRegistryError`, key/row/storage/index encode/decode errors, range bound encode) | 20 variants across 10 enums | data/index/store boundaries | Yes | Medium |

Fastest-growing concept families by size (current snapshot): `PlanError` (24), cursor error family (20 total), store/data/index error family (20 total), `ValidateError` (16), `Predicate` (12).

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_paged_with_cursor` | `crates/icydb-core/src/db/executor/load/mod.rs` | 4 (mode gate + 3 fast-path gates + fallback) | 1 | trace + policy + access + pagination/cursor | High |
| `candidates_from_access_with_index_range_anchor` | `crates/icydb-core/src/db/executor/context.rs` | 4 | 2 | access-path dispatch + range-anchor + store lookup | High |
| `execute` | `crates/icydb-core/src/db/executor/delete/mod.rs` | 4 | 2 | plan validation + relation checks + commit protocol | High |
| `prepare_row_commit_for_entity` | `crates/icydb-core/src/db/commit/prepare.rs` | 5 | 3 | decode + index planning + reverse-relation planning | High |
| `index_range_candidate_for_index` | `crates/icydb-core/src/db/query/plan/planner.rs` | 4 | 3 | predicate normalization + range merge + index-shape extraction | High |
| `validate_unique_constraint` | `crates/icydb-core/src/db/index/plan/unique.rs` | 5 | 2 | unique policy + store lookup + corruption classification | High |
| `validate_index_range_anchor` | `crates/icydb-core/src/db/query/plan/executable.rs` | 4 | 2 | cursor payload + index key decode + envelope checks | Medium |

Observed pressure pattern: branching remains concentrated in planner/executor/commit glue points where access-path decisions and commit safety checks intersect.

## Step 3 - Execution Path Multiplicity

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Save | 3 lanes (`single`, `batch_atomic`, `batch_non_atomic`) x 3 modes (`Insert/Update/Replace`) = 9 behavioral combinations | Partial (`save_entity`, commit helpers) | Yes (atomic vs non-atomic commit semantics) | High |
| Replace | 3 (`replace`, `replace_many_atomic`, `replace_many_non_atomic`) | Yes | Yes (lane semantics differ despite same mode) | Medium |
| Delete | 2 (`empty short-circuit`, `commit-window apply`) | Yes | Yes (relation + marker path only on non-empty) | Medium |
| Load | 4 (`pk_stream`, `secondary_index_stream`, `index_range_limit_pushdown`, `fallback`) | Yes (`finalize_rows_into_page`) | Yes (different pre/post-access ordering surfaces) | High |
| Recovery replay | 4 (`already recovered`, `marker replay`, `rebuild-only`, `rebuild rollback-on-failure`) | Partial | Yes | Medium |
| Cursor continuation | 4 (`no cursor`, `boundary-only`, `boundary+anchor`, `boundary+anchor+direction`) | Yes | Yes | Medium |
| Index mutation | 5 (`insert`, `delete`, `update same key`, `update key move`, `no-op`) | Yes (`build_commit_ops_for_index`) | Yes | High |
| Referential integrity enforcement | 3 (`save-time target existence`, `reverse-index mutation`, `delete-time strong-relation block`) | Partial | Yes | High |

Flow-pressure signal: load/save/index/relation operations all exceed 4 independent logical flows when mode/path dimensions are included.

## Step 4 - Cross-Cutting Concern Spread

Module counts below use non-test files and grep-based structural signals.

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Index id validation | 5 | Partial | Medium |
| Key namespace validation | 3 | Partial | Medium |
| Component arity enforcement | 5 | No | Medium |
| Envelope boundary checks | 5 | Partial | Medium |
| Reverse relation mutation | 8 | No | High |
| Unique constraint enforcement | 5 | Yes | Low |
| Error origin mapping | 41 | No | High |
| Plan shape enforcement | 6 | Partial (`policy` is central but enforcement is multi-layer) | Medium |
| Anchor validation | 9 | Partial | Medium |
| Bound conversions | 16 | Partial (improved via `resume_bounds` + `anchor_within_envelope`) | Medium-High |

Scattering pressure remains highest for `ErrorOrigin` mapping and bound-conversion semantics, though bound-rewrite and envelope checks are now partially centralized.

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/commit/prepare.rs` (174 lines) | Long critical commit-path function | High | High |
| `crates/icydb-core/src/db/index/plan/unique.rs` (181 lines) | Long critical unique-check function | High | High |
| `crates/icydb-core/src/db/executor/delete/mod.rs` (227 lines) | Long critical delete execution function | High | High |
| `crates/icydb-core/src/db/executor/load/mod.rs` (363 lines) | Multi-branch load orchestration file | High | High |
| `crates/icydb-core/src/db/executor/tests/pagination.rs` (3348 lines) | Test-surface cognitive load >3k lines | High | Medium |
| `"executor invariant violated"` string appears 12 times across 4 non-test db files | Repeated invariant pattern | Medium | Medium |
| Plan-shape checks in `policy`, `intent`, `plan::validate`, `plan::logical`, `executable`, `executor` | Multi-stage validation across layers | Medium | Medium |
| Save call chain (`save_batch_atomic` -> `open_commit_window` -> `preflight_prepare_row_ops` -> `prepare_row_commit_for_entity` -> `plan_index_mutation_for_entity`) | Deep cross-domain stack (query + commit + index + relation) | High | High |

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Access-path surface | New `AccessPath` variant (currently referenced in 21 non-test db files) | High fan-out multiplier | High |
| Secondary ORDER pushdown matrix | `SecondaryOrderPushdownRejection` growth + eligibility branching | Matrix expands with order/path combinations | Medium |
| Cursor continuation channel | Boundary + anchor + direction across planner/executor/context/store | Additional cursor fields multiply consistency checks | Medium |
| Save lanes x mode | New mutation mode multiplies across single + atomic + non-atomic flows | Cross-product expansion | High |
| Bound conversion semantics | Bound logic appears across 16 non-test db files | Small semantic changes still have broad blast radius | Medium-High |
| Error taxonomy plumbing | `ErrorOrigin` usage in 41 non-test db files | Adding/remapping origins touches many files | High |
| DESC introduction pressure | Reverse traversal still pending | Branch growth is still expected, but AccessPath/type fan-out risk is reduced by direction containment | Medium |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | enum growth and cross-domain wrappers | Moderate | Medium-High |
| Execution Branching | branch density in planner/executor/commit glue | Moderate-High | High |
| Path Multiplicity | access/mode/lane flow multiplication | High | High |
| Cross-Cutting Spread | semantic checks scattered across modules | Moderate-High | High |
| Cognitive Load | long critical functions + deep call stacks | Moderate-High | High |

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
- `PlanError` (24 variants), cursor-error family (20 total), store/data/index error family (20 total), `ValidateError` (16), `Predicate` (12).

3. Variant Explosion Risks
- Cursor handling spans multiple enums and also injects variants into `PlanError`, increasing mapping/normalization overhead at plan boundaries.
- Top-level wrapper enums (`QueryError`, `PlannerError`) remain small, but downstream growth concentrates in `PlanError` and validation families.

4. Branching Hotspots
- `execute_paged_with_cursor`, `execute` (delete), `prepare_row_commit_for_entity`, `validate_unique_constraint`, and `index_range_candidate_for_index`.

5. Flow Multiplication Risks
- Load has 4 active execution routes.
- Save behavior is a lane/mode cross-product (9 combinations).
- Index/relation maintenance introduces additional per-row mutation branches.

6. Cross-Cutting Spread Risks
- `ErrorOrigin` mapping (41 non-test db modules), bound conversion semantics (16), anchor validation (9), reverse-relation mutation (8).

7. Early Structural Pressure Signals
- Access-path variant fan-out is already broad (21 non-test db files).
- Critical correctness paths rely on several 170+ line functions.
- Direction containment reduced projected DESC type fan-out, but reverse traversal will still add branch pressure when enabled.
