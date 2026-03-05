# Velocity Preservation Audit - 2026-03-04

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 74 runtime references / 10 files | Medium |
| Continuation surface | 788 runtime references / 75 files | High |
| Error mapping spread | 170 `map_err(` references / 64 runtime files | Medium |
| Large hub modules | 11 runtime files >= 600 LOC | Medium-High |

## Layer-Leakage Velocity Checks

| Check | Result |
| ---- | ---- |
| `query -> executor` upward dependency leak | none detected in non-test runtime |
| `index/data/commit -> query` upward leak | none detected in non-test runtime |
| continuation token constructors outside cursor/continuation owners | none detected |

## Change Amplification Areas

| Area | Why It Multiplies Changes | Severity |
| ---- | ---- | ---- |
| Continuation protocol evolution | touches cursor spine + route contracts + load entrypoints + index scan guards | High |
| Grouped policy/runtime alignment | planner grouped policy + grouped runtime fold + route strategy surfaces | High |
| Access-path shape expansion | planner + access + route + executor explain surfaces | Medium-High |

## Gravity Wells (Hub Import Pressure)

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `executor/load/entrypoints.rs` | `executor`, `cursor`, `index`, `response` | 4 | 3 | 0 | Medium-High |
| `executor/route/contracts.rs` | `executor`, `query`, `access`, `direction` | 4 | 3 | N/A (newly tracked module) | Medium |
| `executor/executable_plan.rs` | `query`, `executor`, `cursor`, `access`, `predicate` | 5 | 4 | N/A (newly tracked module) | High |

## Velocity Risk Index

**6/10**

Notes:
- The fluent load split reduced one localized façade bottleneck and lowered local change friction in query fluent APIs.
- Continuation and grouped execution coordination remain the main cross-layer drag sources.
