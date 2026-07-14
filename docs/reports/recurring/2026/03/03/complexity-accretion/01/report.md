# Complexity Accretion Audit - 2026-03-03

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`db/`).

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | still broad; grouped and cursor policy remain largest contributors | High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; moderate variant pressure | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 117 across 13 runtime files | Medium-High |
| Predicate/operator surface | `Predicate` + `CompareOp` remain stable but broad | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan/planner.rs` | `match=17`, `if=39` | High |
| `executor/route` subtree | `match=19`, `if=79` across route planner files | High |
| `executor/load` subtree | `match=38`, `if=57` across split load modules | Medium-High |
| `query/plan/validate` subtree | `match=11`, `if=42` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs 2026-03-02 | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 580 across 67 runtime files | -101 mentions, -3 files | High |
| `AccessPath::` runtime mentions | 117 across 13 runtime files | -71 mentions, -4 files | Medium-High |
| `map_err(` runtime mentions | 168 across 62 runtime files | 0 mentions, -4 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files >=600 LOC | 10 | Medium-High |
| Largest runtime files | `query/plan/expr.rs` (883), `query/fluent/load.rs` (878), `query/plan/planner.rs` (858), `predicate/schema.rs` (843) | Medium-High |
| `.as_inner()` runtime usage | 11 callsites | Medium |

## Complexity Risk Index

**6/10**

Notes:
- Risk improved from the prior snapshot due route/load/query file splits.
- Continuation and planner/route branching remain primary complexity drivers.
