# Complexity Accretion Audit - 2026-02-24

Scope: branch pressure, path multiplicity, and growth concentration in `icydb-core`.

## Step 1 - Variant Surface Growth

| Enum / Family | Variant Count | Growth Signal | Risk |
| ---- | ---- | ---- | ---- |
| `PlanError` family (`PlanError` + `OrderPlanError` + `AccessPlanError` + `PolicyPlanError` + `CursorPlanError`) | 29 (5 + 3 + 7 + 5 + 9) | unchanged vs prior full run | Medium-High |
| `AccessPath` | 6 | unchanged | High fan-out risk |
| `ErrorClass` | 6 | unchanged | Medium |
| Load projection terminal family | 5 (`values_by`, `distinct_values_by`, `values_by_with_ids`, `first_value_by`, `last_value_by`) | grew in 0.28.x | Medium |

## Step 2 - Execution Branching Pressure

| Function/Module | Branch Pressure Signal | Risk |
| ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/load/aggregate.rs` (1698 LOC) | terminal routing + projection + aggregate fold paths co-located | High |
| `crates/icydb-core/src/db/executor/route/mod.rs` (1163 LOC) | route ownership for multiple execution lanes | High |
| `crates/icydb-core/src/db/query/plan/logical/mod.rs` (707 LOC) | post-access pipeline phases + cursor/paging semantics | Medium-High |
| `crates/icydb-core/src/db/query/plan/planner/mod.rs` + `planner/range.rs` (796 LOC total) | path planning + range lowering | Medium-High |
| `crates/icydb-core/src/db/query/cursor/spine.rs` (425 LOC) | cursor compatibility + envelope invariants | Medium |

## Step 3 - Execution Path Multiplicity

| Operation | Active Flows | Shared Core? | Risk |
| ---- | ---- | ---- | ---- |
| Load | multiple route-owned shapes (pk/index/index-range/composite/fallback) | Partial | High |
| Projection terminals | 5 load terminal surfaces over same canonical execution | Yes | Medium |
| Save/Delete | commit-window + marker + replay matrix | Yes | Medium-High |
| Cursor continuation | none/boundary/index-range-anchor + compatibility gates | Yes | Medium-High |

## Step 4 - Cross-Cutting Concern Spread

| Concept | Signal | Risk |
| ---- | ---- | ---- |
| `AccessPath::` fan-out (non-test db files) | 17 files | High |
| `AccessPath::` token references | 163 references | High |
| cursor compatibility rules | spread across continuation + spine + executable + tests | Medium-High |
| projection parity constraints | executor + session + facade + tests | Medium |

## Step 5 - Cognitive Load Indicators

| Area | Indicator | Risk |
| ---- | ---- | ---- |
| large non-test db files | 69 files >150 LOC; 49 files >300 LOC | Medium-High |
| projection + aggregate concentration | single large `aggregate.rs` hub | High |
| pagination test topology | 7944 LOC across modular pagination suite | Medium |
| total Rust tests | 1029 | Medium (maintenance load) |

## Step 6 - Drift Sensitivity Index

| Area | Growth Vector | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| projection terminal family | additive terminal APIs in one implementation hub | Medium | Medium |
| route ownership surface | remains broad and central | High | High |
| cursor protocol | stable this cycle, still sensitive to payload growth | Medium | Medium |
| plan error surface | stable this cycle | Low | Low-Medium |

## Step 7 - Complexity Risk Index

| Area | Complexity Type | Accretion Rate | Risk Level |
| ---- | ---- | ---- | ---- |
| Variant Surface | mostly stable enums | Low | Medium |
| Branching | concentrated hubs | Medium | High |
| Path Multiplicity | load + mutation lanes | Medium-High | High |
| Cross-Cutting Spread | access-path and cursor concerns | Medium-High | High |
| Cognitive Load | large file concentration | High | High |

### Overall Complexity Risk Index (1-10, lower is better)

**6/10**

## Required Summary

1. Overall Complexity Risk Index
- 6/10

2. Fastest Growing Concept Families
- Load projection terminal family (0.28.x additions).

3. Variant Explosion Risks
- No enum explosion this run; pressure remains in branching and path fan-out.

4. Branching Hotspots
- `aggregate.rs`, `route/mod.rs`, `logical/mod.rs`, planner modules.

5. Flow Multiplication Risks
- Multiple load/mutation/cursor execution lanes remain active.

6. Cross-Cutting Spread Risks
- Access-path and cursor semantics remain multi-module and drift-sensitive.

7. Early Structural Pressure Signals
- Large executor hubs and high test-count growth remain the primary warning signals.
