# Complexity Accretion Audit - 2026-03-04

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`crates/icydb-core/src/db`, non-test runtime files).

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | still broad; grouped and cursor policy remain largest contributors | High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; moderate variant pressure | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 74 across 10 runtime files | Medium |
| Predicate/operator surface | `Predicate` + `CompareOp` remain stable but broad | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan/planner` subtree | `match=17`, `if=39` | High |
| `executor/route` subtree | `match=15`, `if=82` | High |
| `executor/load` subtree | `match=37`, `if=63` | Medium-High |
| `query/plan/validate` subtree | `match=10`, `if=43` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs 2026-03-03 | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 788 across 75 runtime files | +208 mentions, +8 files | High |
| `AccessPath::` runtime mentions | 74 across 10 runtime files | -43 mentions, -3 files | Medium |
| `map_err(` runtime mentions | 170 across 64 runtime files | +2 mentions, +2 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files >=600 LOC | 11 | Medium-High |
| Largest runtime files | `predicate/schema.rs` (843), `executor/aggregate/contracts/grouped.rs` (687), `query/plan/expr/type_inference.rs` (670), `session.rs` (650), `query/explain/mod.rs` (640) | Medium-High |
| Former large fluent hub | `query/fluent/load.rs` split into `query/fluent/load/*` | Positive structural containment |

## Complexity Risk Index

**6/10**

Notes:
- The `query/fluent/load` split reduced one high-pressure hub, improving local comprehension in the fluent layer.
- Planner/route branching and continuation-related surface area remain the dominant complexity drivers.
