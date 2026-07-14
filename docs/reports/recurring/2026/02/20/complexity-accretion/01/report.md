# Complexity Accretion Audit - 2026-02-20

Scope: conceptual growth, branch pressure, flow multiplication, scattering, and cognitive-load pressure in `icydb-core`.

Run context: current working tree (includes cursor-offset continuation updates).

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ---- | ---- | ---- | ---- |
| `PlanError` family (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`) | 29 total (5 + 3 + 7 + 5 + 9) | plan validation + policy + cursor continuation | Yes | High |
| `QueryError` | 5 | query boundary wrappers | Yes | Medium |
| `ErrorClass` | 6 | runtime taxonomy | No | Medium |
| Cursor error family (`CursorDecodeError`, `PrimaryKeyCursorSlotDecodeError`, `ContinuationTokenError`, `CursorPagingPolicyError`, `CursorPlanError`) | 19 total (3 + 2 + 3 + 2 + 9) | decode + token wire + policy + plan validation | Yes | High |
| Commit marker types (`CommitRowOp`, `CommitIndexOp`, `CommitMarker`) | 3 structs | commit protocol metadata | No | Low |
| `AccessPath` | 6 | planner/executor access strategy | No | High (fan-out impact) |
| Policy error enums (`PlanPolicyError`, `CursorPagingPolicyError`) | 7 total (5 + 2) | query/plan policy | No | Medium |
| `Predicate` AST | 12 | predicate syntax surface | No | Medium |
| `CompareOp` | 11 | operator surface | No | Medium |
| Commit-phase enums (`QueryMode`, `SaveMode`) | 5 total (2 + 3) | load/delete + mutation mode | Yes | Medium |
| Store/data/index error family (12 enums across store/index/data codecs) | 27 variants (grep-based prior family baseline unchanged) | storage trust boundaries | Yes | Medium-High |

Fastest-growing family this run: `PlanError` family (28 -> 29) via `ContinuationCursorWindowMismatch` at `crates/icydb-core/src/db/query/plan/validate/mod.rs:196`.

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_paged_with_cursor_traced` | `crates/icydb-core/src/db/executor/load/mod.rs` | 4 | 0 | cursor revalidation + route planning + fallback orchestration | Medium-High |
| `evaluate_fast_path` | `crates/icydb-core/src/db/executor/load/execute.rs` | 4 | 0 | precedence routing + plan-shape gates | Medium |
| `resolve_physical_key_stream` | `crates/icydb-core/src/db/executor/physical_path.rs` | 3 | 1 | access dispatch + store/index reads + direction normalization | High |
| `validate_structured_cursor` | `crates/icydb-core/src/db/query/plan/cursor_spine.rs` | 5 | 1 | boundary typing + anchor validation + consistency checks | High |
| `index_range_candidate_for_index` | `crates/icydb-core/src/db/query/plan/planner.rs` | 5 | 2 | predicate ops + range merge + index-shape extraction | High |
| `prepare_row_commit_for_entity` | `crates/icydb-core/src/db/commit/prepare.rs` | 6 | 0 | decode + index planning + reverse relation planning | High |
| `validate_unique_constraint` | `crates/icydb-core/src/db/index/plan/unique.rs` | 6 | 0 | uniqueness policy + store lookup + classification | High |

## Step 3 - Execution Path Multiplicity

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Save | 3 lanes x 3 modes = 9 combinations | Partial (`open_commit_window` + apply helpers) | Yes | High |
| Replace | 3 (`replace`, `replace_many_atomic`, `replace_many_non_atomic`) | Yes | Yes | Medium |
| Delete | 2 (`empty short-circuit`, `commit-window apply`) | Yes | Yes | Medium |
| Load | 5 (`pk`, `secondary`, `index-range-limit`, `fallback-budget`, `fallback-no-budget`) | Yes | Yes | High |
| Recovery replay | 4 (`no marker`, `marker replay`, `rebuild`, `restore snapshot on rebuild failure`) | Partial | Yes | Medium |
| Cursor continuation | 5 (`none`, `boundary`, `boundary+anchor`, `offset mismatch reject`, `signature/version reject`) | Yes | Yes | Medium-High |
| Index mutation | 5 (`insert`, `delete`, `update-same-key`, `update-key-move`, `no-op`) | Yes | Yes | High |
| Referential integrity enforcement | 3 (save strong checks, reverse index mutation, delete strong guard) | Partial | Yes | High |

## Step 4 - Cross-Cutting Concern Spread

Counts are grep-based on non-test `crates/icydb-core/src/db/**/*.rs`.

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Index-id validation signals (`IndexId`/mismatch checks) | 10 | Partial | Medium-High |
| Key-namespace validation signals | 4 | Partial | Medium |
| Component-arity enforcement signals | 5 | Partial | Medium |
| Envelope/boundary checks (`KeyEnvelope`/`Bound::*`) | 18 | Partial | High |
| Reverse relation mutation signals | 12 | Partial | Medium |
| Unique enforcement signals | 7 | Partial | Medium |
| Error-origin mapping (`ErrorOrigin::*`) | 12 | Partial | High |
| Plan-shape enforcement signals | 6 | Partial | Medium |
| Anchor validation signals | 14 | Partial | Medium-High |
| Bound conversion signals (`Bound::*`) | 15 | Partial | High |

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/tests/pagination.rs:1` | test surface at 6218 lines | High | High |
| `crates/icydb-core/src/db/query/plan/planner.rs:1` | planner hotspot at 789 lines | High | High |
| `crates/icydb-core/src/db/query/plan/logical.rs:1` | multi-phase post-access semantics at 778 lines | High | High |
| `crates/icydb-core/src/db/query/plan/cursor_spine.rs:1` | continuation invariants and envelope logic at 508 lines | Medium-High | Medium-High |
| `executor invariant violated` string occurrences | 25 occurrences across non-test db files | Medium | Medium |
| non-test db files over 150 lines / 300 lines | 60 / 33 | Medium-High | Medium-High |

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Access-path surface | 6 variants with 22-file non-test fan-out and 210 direct references | High change amplification per variant | High |
| Plan/cursor error surface | `CursorPlanError` grew to 9 variants | each new token rule adds plan/executor mapping branches | High |
| Cursor continuation channel | token now carries direction + boundary + optional anchor + initial_offset | protocol-shape drift is sensitive | High |
| Save lane x mode matrix | 9 combinations persist | new save invariants propagate across all lanes | High |
| Bound semantics | 15 non-test modules use `Bound::*` constructs | off-by-one or inclusion drift has broad blast radius | Medium-High |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | plan/cursor taxonomy growth | Moderate | High |
| Branching | planner/commit/cursor hotspots | Moderate-High | High |
| Path Multiplicity | save/load/index flow matrix | High | High |
| Cross-Cutting Spread | bounds, anchors, error origins | High | High |
| Cognitive Load | large planner/logical/test surfaces | High | High |

Overall Complexity Risk Index (1-10, lower is better): **6/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## Required Summary

1. Overall Complexity Risk Index
- 6/10

2. Fastest Growing Concept Families
- `PlanError` family is the only family that grew in this run (28 -> 29).
- Cursor protocol state also expanded structurally via token `initial_offset`.

3. Variant Explosion Risks
- Cursor validation and plan error mapping continue to carry the largest growth pressure.
- `AccessPath` count is stable, but fan-out remains high.

4. Branching Hotspots
- `index_range_candidate_for_index`, `prepare_row_commit_for_entity`, `validate_unique_constraint`, `validate_structured_cursor`, `resolve_physical_key_stream`.

5. Flow Multiplication Risks
- Save remains a 9-combination lane/mode matrix.
- Load still has 5 active route shapes with fallback variants.

6. Cross-Cutting Spread Risks
- Boundary and anchor semantics remain distributed across planner + index + executor edges.
- Error-origin mapping remains broad and drift-sensitive.

7. Early Structural Pressure Signals
- Pagination test surface grew to 6218 lines and is now the largest cognitive hotspot.
- `CursorPlanError` growth confirms cursor protocol expansion pressure.
