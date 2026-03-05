# Complexity Accretion Audit - 2026-03-05

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`db/`).

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | broad but better segmented after planner/module splits | Medium-High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | explicit and split; moderate variant pressure | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 74 across 10 runtime files | Medium |
| Predicate/operator surface | grouped + compare semantics remain broad | Medium |

## Branching and Hotspots

| Area | Signal | Risk |
| ---- | ---- | ---- |
| `query/plan` subtree | top hotspot `expr/type_inference.rs` (31 `if|match` hits) | Medium-High |
| `executor/route` subtree | `match=15`, `if=82` | High |
| `executor/load` subtree | `match=37`, `if=63` | Medium-High |
| `query/plan/validate` subtree | `match=10`, `if=43` | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs 2026-03-03 | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 790 across 77 runtime files | +210 mentions, +10 files | High |
| `AccessPath::` runtime mentions | 74 across 10 runtime files | -43 mentions, -3 files | Medium |
| `map_err(` runtime mentions | 171 across 64 runtime files | +3 mentions, +2 files | Medium |

## Cognitive Load Indicators

| Indicator | Current | Risk |
| ---- | ---- | ---- |
| Runtime files >=600 LOC | 10 | Medium-High |
| Largest runtime files | grouped aggregate contracts (687), type inference (670), load entrypoints (668), session (659) | Medium-High |
| Runtime lines (`db/`, non-test) | 50,722 | Medium |

## Complexity Risk Index

**6/10**

Notes:
- Access-path fan-out improved from the previous run.
- Continuation/anchor concern spread increased and is the largest current complexity driver.
