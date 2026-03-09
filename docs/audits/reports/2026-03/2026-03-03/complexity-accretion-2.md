# Complexity Accretion Audit - 2026-03-03 (Rerun 2)

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`db/`) runtime non-test modules.

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | still broad; grouped and cursor policy remain largest contributors | High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; moderate variant pressure | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 112 across 12 runtime files | Medium-High |
| Predicate/operator surface | `Predicate` + `CompareOp` remain stable but broad | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan/planner.rs` | `match=17`, `if=39` | High |
| `executor/route` subtree | `match=18`, `if=81` across route planner files | High |
| `executor/load` subtree | `match=37`, `if=60` across split load modules | Medium-High |
| `query/plan/validate` subtree | `match=11`, `if=42` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs 2026-03-03 (earlier run) | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 699 across 70 runtime files | +119 mentions, +3 files | High |
| `AccessPath::` runtime mentions | 112 across 12 runtime files | -5 mentions, -1 file | Medium-High |
| `map_err(` runtime mentions | 168 across 62 runtime files | 0 mentions, 0 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files >=600 LOC | 12 | Medium-High |
| Largest runtime files | `query/plan/expr.rs` (883), `query/fluent/load.rs` (878), `query/plan/planner.rs` (858), `predicate/schema.rs` (843) | Medium-High |
| `.as_inner()` runtime usage | 0 callsites | Low |

## Complexity Risk Index

**6/10**

Notes:
- Branching pressure remains concentrated in planner/route/load.
- Continuation concern spread is still the primary complexity multiplier.
