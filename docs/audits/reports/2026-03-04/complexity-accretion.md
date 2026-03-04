# Complexity Accretion Audit - 2026-03-04

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`db/`) runtime non-test modules.

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Layer-authority enum fan-out | `AccessPath::=4`, `AggregateKind::=3`, `ContinuationMode::=2` (from layer-authority check) | Medium |
| Plan validation family (`PlanError` + split policy/group/cursor enums) | broad but still segmented by domain | Medium-High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; no merger pressure increase | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 74 across 10 runtime files | Medium |
| Predicate/operator surface | predicate normalization and comparison authority remains centralized | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan/planner.rs` | `match=17`, `if=39` | High |
| `executor/route` subtree | `match=15`, `if=82` | High |
| `executor/load` subtree | `match=37`, `if=62` | Medium-High |
| `query/plan/validate` subtree | `match=10`, `if=43` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs existing 2026-03-04 report | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 757 across 73 runtime files | -219 mentions, +1 file | High |
| `AccessPath::` runtime mentions | 74 across 10 runtime files | +6 mentions, 0 files | Medium |
| `map_err(` runtime mentions | 169 across 64 runtime files | +1 mention, +2 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files in audited scope | 237 | Medium |
| Runtime files >=600 LOC | 14 | Medium-High |
| Largest runtime files | `query/plan/expr.rs` (883), `query/fluent/load.rs` (879), `query/plan/planner.rs` (845), `predicate/schema.rs` (843), `query/intent/intent_ast.rs` (749) | Medium-High |
| `.as_inner()` runtime usage | 0 callsites | Low |

## Complexity Risk Index

**6/10**

Notes:
- Branching pressure remains concentrated in planner/route/load.
- Continuation concern spread remains the primary complexity multiplier.
- `query/intent` approval and DTO-based stage boundaries reduce coupling risk, but do not yet materially reduce branch density.
