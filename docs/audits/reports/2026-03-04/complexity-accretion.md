# Complexity Accretion Audit - 2026-03-04

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`db/`) runtime non-test modules.

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | still broad; grouped and cursor policy remain largest contributors | High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; moderate variant pressure | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 68 across 10 runtime files | Medium |
| Predicate/operator surface | `Predicate` + `CompareOp` remain stable but broad | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan/planner.rs` | `match=17`, `if=39` | High |
| `executor/route` subtree | `match=16`, `if=82` across route planner files | High |
| `executor/load` subtree | `match=37`, `if=60` across split load modules | Medium-High |
| `query/plan/validate` subtree | `match=10`, `if=41` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs 2026-03-03 (Rerun 2) | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 976 across 72 runtime files | +277 mentions, +2 files | High |
| `AccessPath::` runtime mentions | 68 across 10 runtime files | -44 mentions, -2 files | Medium |
| `map_err(` runtime mentions | 168 across 62 runtime files | 0 mentions, 0 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files >=600 LOC | 13 | Medium-High |
| Largest runtime files | `query/plan/expr.rs` (883), `query/fluent/load.rs` (878), `query/plan/planner.rs` (845), `predicate/schema.rs` (843) | Medium-High |
| `.as_inner()` runtime usage | 0 callsites | Low |

## Complexity Risk Index

**6/10**

Notes:
- Branching pressure remains concentrated in planner/route/load.
- Continuation concern spread remains the primary complexity multiplier.
