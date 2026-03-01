# Complexity Accretion Audit - 2026-03-01

Scope: conceptual growth, branching pressure, path multiplicity, and cognitive load in `icydb-core` (`db/`).

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ---- | ---- | ---- | ---- |
| `PlanError` family (`PlanError`, `OrderPlanError`, `AccessPlanError`, `PolicyPlanError`, `CursorPlanError`, `GroupPlanError`) | 49 (6 + 4 + 7 + 5 + 8 + 19) | plan + access + cursor + grouped policy | Yes | High |
| `QueryError` | 5 | query surface wrapper | No | Low-Medium |
| `ErrorClass` | 6 | runtime classification | No | Medium |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`, `CursorPagingPolicyError`) | 17 (8 + 4 + 3 + 2) | cursor + boundary decode | Yes | Medium-High |
| `AccessPath` | 6 | access/runtime path selection | No | High fan-out |
| Policy family (`PolicyPlanError`, `CursorPagingPolicyError`, `IntentKeyAccessPolicyViolation`, `FluentLoadPolicyViolation`) | 14 | intent + plan policy | Yes | Medium-High |
| Predicate AST (`Predicate`) | 12 | query predicate model | No | Medium |
| `CompareOp` | 11 | predicate operator surface | No | Medium |
| Commit marker/core mutation types (`CommitMarker`, `CommitRowOp`, `CommitIndexOp`, `PreparedIndexDeltaKind`) | 3 structs + 1 enum (5 variants) | commit/replay protocol | No | Medium |
| Store-layer error variants (data/index storage boundary) | 11 enums / 28 variants | encode/decode + bounds + key/entry validation | Yes | Medium-High |

Fastest-growing family vs 2026-02-24 baseline: `PlanError` family growth is now concentrated in grouped-policy variants (`GroupPlanError` at 19 variants).

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_grouped_path` (~468 lines) | `executor/load/mod.rs` | cursor + route + streaming + grouped fold + paging + having | high | planner handoff + runtime + cursor + aggregate | High |
| `execute_paged_with_cursor_traced` (~99 lines) | `executor/load/mod.rs` | route mode + continuation + mode-specific paging | medium | route + tracing + pagination | Medium-High |
| `plan_compare` (~74 lines) + `index_prefix_from_and` (~78) | `query/plan/planner.rs` | predicate type + operator + index shape + range bounds | medium-high | predicate + access planning + index fit | Medium-High |
| `validate_group_spec` (~74) + `validate_global_distinct_aggregate_without_group_keys` (~53) | `query/plan/validate.rs` | grouped semantics + policy gates + symbol resolution | medium-high | grouped policy + order + having | Medium-High |
| `build_plan_model` (~66) + `validate_intent` (~47) | `query/intent/mod.rs` | intent mode + policy + lowering + plan checks | medium | intent + planning + policy | Medium |

File-level branch pressure signals:
- `executor/load/mod.rs`: `match=16`, `if=45`, `return Err(...)=20`
- `query/plan/planner.rs`: `match=18`, `if=40`
- `query/plan/validate.rs`: `if=44`, `return Err(...)=38`

## Step 3 - Execution Path Multiplicity

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Load | 8 (6 access paths + union/intersection handling) | Partial | Yes (grouped and continuation branches) | High |
| Save / Replace | 4 (insert/update/delete semantics + commit-window apply) | Yes | Yes | Medium-High |
| Delete | 3 (limited/unlimited + relation checks + replay safety) | Yes | Medium | Medium |
| Recovery replay | 3 (startup replay, read-guard recovery, marker cleanup invariants) | Yes | Low-Medium | Medium |
| Cursor continuation | 4 (initial, scalar boundary, grouped boundary, index-range anchor) | Partial | Yes | High |
| Index mutation | 4 (`IndexInsert`, `IndexRemove`, `ReverseIndexInsert`, `ReverseIndexRemove`) | Yes | Low | Medium |
| Referential integrity enforcement | 3 (save validation, delete strong checks, reverse index updates) | Partial | Yes | Medium-High |

## Step 4 - Cross-Cutting Concern Spread

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Access-path branching (`AccessPath::`) | 23 non-test modules / 212 references | No | High |
| Anchor + continuation handling | 31 modules | No | High |
| Plan-shape enforcement (`validate_*plan*`, `LogicalPlan`) | 29 modules | Partial | Medium-High |
| Bound conversions (`Bound::{Included,Excluded,Unbounded}`) | 24 modules | Partial | Medium |
| Reverse relation mutation | 6 modules | Partial | Medium |
| Unique enforcement logic | 11 modules | Partial | Medium |
| Error-origin / mapping surfaces | 60 modules | No | Medium-High |

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | very long function (`execute_grouped_path` ~468 lines) | High | High |
| Large runtime files | 11 non-test files >= 600 LOC | High | High |
| Invariant-check repetition | 127 `query_executor_invariant`/`executor_invariant` calls across 37 modules | Medium-High | Medium-High |
| Error-mapping spread | 59 modules with `map_err`/error mapping forms | Medium | Medium |
| Test-scale warning | no single test file >3000 LOC (improved from earlier pressure) | Low | Low |

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Grouped plan policy (`GroupPlanError`) | new grouped constraints increase variant+branch pressure | High | High |
| Access-path/runtime route matrix | new path or route case multiplies load dispatch logic | High | High |
| Cursor protocol surface | continuation variants and grouped cursor contracts | Medium-High | Medium-High |
| Store/decode error families | additive boundary variants increase mapping sites | Medium | Medium |
| Commit delta taxonomy | currently stable (`PreparedIndexDeltaKind` 5) | Low-Medium | Low-Medium |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | grouped-policy expansion | Medium-High | High |
| Branching | concentrated in load/planner/validate hubs | High | High |
| Path Multiplicity | load + cursor + mutation flow matrix | Medium-High | High |
| Cross-Cutting Spread | access/cursor/error concerns across many modules | High | High |
| Cognitive Load | long functions + large module concentration | High | High |

### Overall Complexity Risk Index (1-10, lower is better)

**7/10**

## Required Summary

1. Overall Complexity Risk Index
- **7/10**

2. Fastest Growing Concept Families
- Grouped plan-policy surface (`GroupPlanError`) and grouped load execution branches.

3. Variant Explosion Risks
- Highest near grouped-policy and cursor/policy boundary families, not in core runtime taxonomy (`ErrorClass` stable at 6).

4. Branching Hotspots
- `executor/load/mod.rs::execute_grouped_path`
- `query/plan/planner.rs` predicate-to-access planning functions
- `query/plan/validate.rs` grouped policy validation functions

5. Flow Multiplication Risks
- Access-path fan-out plus continuation mode and grouped-mode branching.

6. Cross-Cutting Spread Risks
- Access-path, continuation/anchor semantics, and error mapping span many modules.

7. Early Structural Pressure Signals
- Long grouped execution orchestration path and concentration of multi-domain logic in a few large runtime hubs.
