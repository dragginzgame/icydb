# Complexity Accretion Audit - 2026-03-02

Scope: conceptual growth, branching pressure, path multiplicity, and cognitive load in `icydb-core` (`db/`).

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Domain Scope | Mixed Domains? | Growth Risk |
| ---- | ---- | ---- | ---- | ---- |
| `PlanError` family (`PlanError`, `OrderPlanError`, `AccessPlanError`, `PolicyPlanError`, `CursorPlanError`, `GroupPlanError`) | 51 (7 + 4 + 7 + 5 + 9 + 19) | plan + access + cursor + grouped policy | Yes | High |
| `QueryError` | 5 | query boundary wrapper | No | Low-Medium |
| `ErrorClass` | 6 | runtime classification | No | Medium |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`, `CursorPagingPolicyError`) | 18 (9 + 4 + 3 + 2) | cursor boundary + token decode | Yes | Medium-High |
| `AccessPath` | 6 | access/runtime path selection | No | High fan-out |
| Policy family (`PolicyPlanError`, `CursorPagingPolicyError`, `IntentKeyAccessPolicyViolation`, `FluentLoadPolicyViolation`) | 14 | intent + plan policy | Yes | Medium-High |
| Predicate AST (`Predicate`) | 12 | query predicate model | No | Medium |
| `CompareOp` | 11 | predicate operator surface | No | Medium |
| Commit marker/core mutation types | 3 structs + 1 enum (`PreparedIndexDeltaKind` = 5 variants) | commit/replay protocol | Partial | Medium |
| Store-layer error families | broad multi-enum boundary set (data/index/codec/error mappings) | encode/decode + bounds + key/entry validation | Yes | Medium-High |

Fastest-growing conceptual family remains grouped policy + projection semantics inside planner/runtime boundaries.

## Step 2 - Execution Branching Pressure

| Function | Module | Branch Layers | Match Depth | Semantic Domains Mixed | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `execute_grouped_path` | `executor/load/mod.rs` | grouped route + continuation + fold + paging + having | high | planner handoff + runtime + cursor + aggregate | High |
| `execute_paged_with_cursor_traced` | `executor/load/mod.rs` | route mode + continuation + paging | medium | route + tracing + pagination | Medium-High |
| `plan_compare` / `index_prefix_from_and` / `compare_range_bound_values` | `query/plan/planner.rs` | predicate type + operator + index fit + range bounds | medium-high | predicate + access planning + index semantics | Medium-High |
| `validate_group_spec_policy` / `validate_global_distinct_aggregate_without_group_keys` | `query/plan/validate.rs` | grouped policy + distinct admissibility + target validation | medium-high | grouped policy + aggregate semantics | Medium-High |
| `build_plan_model` / `validate_intent` | `query/intent/mod.rs` | intent mode + policy + lowering + checks | medium | intent + planning + policy | Medium |

File-level branch signals:
- `executor/load/mod.rs`: `match=15`, `if=48`, `return Err(...)=28`
- `query/plan/planner.rs`: `match=17`, `if=39`
- `query/plan/validate.rs`: `match=11`, `if=42`, `return Err(...)=35`
- `query/intent/mod.rs`: `match=19`, `if=12`

## Step 3 - Execution Path Multiplicity

| Operation | Independent Flows | Shared Core? | Subtle Divergence? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Load | 8 | Partial | Yes (grouped/scalar + continuation branches) | High |
| Save / Replace | 4 | Yes | Yes | Medium-High |
| Delete | 3 | Yes | Medium | Medium |
| Recovery replay | 3 | Yes | Low-Medium | Medium |
| Cursor continuation | 4 | Partial | Yes | High |
| Index mutation | 4 | Yes | Low | Medium |
| Referential integrity enforcement | 3 | Partial | Yes | Medium-High |

## Step 4 - Cross-Cutting Concern Spread

| Concept | Modules Involved | Centralized? | Risk |
| ---- | ---- | ---- | ---- |
| Access-path branching (`AccessPath::`) | 17 runtime files / 187 references | No | High |
| Continuation + anchor handling | 66 runtime files / 611 mentions | No | High |
| Plan-shape enforcement (`validate_*plan*`, `LogicalPlan`) | 19 runtime files | Partial | Medium-High |
| Bound conversions (`Bound::{Included,Excluded,Unbounded}`) | 15 runtime files | Partial | Medium |
| Reverse relation/index mutation | 6 runtime files | Partial | Medium |
| Unique-enforcement logic | 11 runtime files | Partial | Medium |
| Error origin/classification mapping | 40 runtime files | No | Medium-High |

## Step 5 - Cognitive Load Indicators

| Area | Indicator Type | Severity | Risk |
| ---- | ---- | ---- | ---- |
| runtime hubs | 13 runtime files >=600 LOC | High | High |
| largest orchestration files | `executor/load/mod.rs` (1428), `query/plan/semantics.rs` (1190), `query/plan/validate.rs` (1108), `executor/load/projection.rs` (1076), `query/intent/mod.rs` (1074) | High | High |
| invariant constructor spread | `query_executor_invariant` / `executor_invariant` in 43 callsites across 38 runtime files | Medium-High | Medium-High |
| error mapping spread | `map_err(` in 168 callsites across 66 runtime files | Medium-High | Medium-High |
| policy surface growth | `GroupPlanError` references in 24 non-test locations | Medium-High | Medium-High |

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| grouped policy + projection semantics | new grouped constraints and expression cases increase branch pressure | High | High |
| access-path/runtime route matrix | new path semantics fan out to planner/runtime/cursor/explain | High | High |
| continuation protocol surface | signature/boundary compatibility across grouped + scalar flows | Medium-High | Medium-High |
| numeric semantic boundary | planner/predicate/runtime numeric authority must stay converged | Medium | Medium |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | grouped + cursor policy growth | Medium-High | High |
| Branching | concentrated in load/planner/validate hubs | High | High |
| Path Multiplicity | load + cursor + mutation flow matrix | Medium-High | High |
| Cross-Cutting Spread | access/continuation/error concerns across many modules | High | High |
| Cognitive Load | large hub concentration + wide invariant spread | High | High |

### Overall Complexity Risk Index (1-10, lower is better)

**7/10**

## Required Summary

1. Overall Complexity Risk Index: **7/10**
2. Fastest Growing Concept Families: grouped policy + projection/continuation boundary semantics.
3. Variant Explosion Risks: highest in plan/cursor/grouped policy families.
4. Branching Hotspots: `executor/load/mod.rs`, `query/plan/planner.rs`, `query/plan/validate.rs`.
5. Flow Multiplication Risks: access-path + continuation + grouped/scalar bifurcation.
6. Cross-Cutting Spread Risks: access-path, continuation, and error mapping spread.
7. Early Structural Pressure Signals: high coordination pressure concentrated in a small number of large modules.
