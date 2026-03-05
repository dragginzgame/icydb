# Complexity Accretion Audit - 2026-03-05 (Rerun)

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`crates/icydb-core/src/db`, non-test runtime files).

## Variant Surface Snapshot

| Family | Current Signal | Risk |
| ---- | ---- | ---- |
| Plan validation family (`PlanError` + split sub-enums) | broad but segmented across planner/validate modules | Medium-High |
| Cursor error family (`CursorPlanError`, `CursorDecodeError`, `TokenWireError`) | split by boundary and stable in shape | Medium |
| Access-path runtime fan-out | `AccessPath::` references: 76 across 11 runtime files | Medium |
| Predicate/operator surface | broad (`query/plan/expr` + grouped semantics) but localized | Medium |

## Branching and Hotspots

| Area | Signal | Delta vs previous 2026-03-05 run | Risk |
| ---- | ---- | ---- | ---- |
| `query/plan` subtree | top hotspot remains `expr/type_inference.rs` (31 `if|match` hits) | stable | Medium-High |
| `executor/route` subtree | `match=15`, `if=56` | `if` down from 82 | Medium |
| `executor/load` subtree | `match=37`, `if=63` | stable | Medium-High |
| `query/plan/validate` subtree | `match=10`, `if=43` | stable | Medium-High |

## Cross-Cutting Spread

| Metric | Current | Delta vs previous 2026-03-05 run | Risk |
| ---- | ---- | ---- | ---- |
| `continuation|anchor` runtime mentions | 849 across 76 runtime files | +59 mentions, -1 file | High |
| `AccessPath::` runtime mentions | 76 across 11 runtime files | +2 mentions, +1 file | Medium |
| `map_err(` runtime mentions | 171 across 64 runtime files | no change | Medium |
| `.as_inner()` runtime mentions | 0 | no change | Low |

## Cognitive Load Indicators

| Indicator | Current | Delta vs previous 2026-03-05 run | Risk |
| ---- | ---- | ---- | ---- |
| Runtime files >=600 LOC | 12 | +2 | Medium-High |
| Largest runtime files | `executor/load/mod.rs` (864), `access/execution_contract.rs` (732), grouped aggregate contracts (687), type inference (670), load entrypoints (667) | shifted toward load/access hubs | Medium-High |
| Runtime lines (`db/`, non-test) | 52,157 | +1,435 | Medium |

## Complexity Risk Index

**6/10**

Notes:
- Route-local branch pressure decreased materially (`if` 82 -> 56), mainly due continuation gate localizations under route contracts.
- Continuation/anchor spread remains the dominant accretion vector and keeps overall risk in the moderate-high band.
