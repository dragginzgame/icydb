# Velocity Preservation Audit - 2026-03-04

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 74 runtime references / 10 files | Medium |
| Continuation surface | 757 runtime references / 73 files | High |
| Error mapping spread | 169 `map_err(` references / 64 runtime files | Medium |
| `.as_inner()` cross-boundary adapters | 0 runtime callsites | Low |

## Layer-Leakage Velocity Checks

| Check | Result |
| ---- | ---- |
| query -> executor upward dependency leak (tracked edges) | none detected |
| index/data/commit -> query upward leak (tracked edges) | none detected |
| runtime cursor token construction outside continuation owner | none detected |
| include_str-based architecture scans | none detected |

## Change Amplification Areas

| Area | Why It Multiplies Changes | Severity |
| ---- | ---- | ---- |
| Continuation protocol evolution | touches cursor spine + route + load entrypoints + scan guards | High |
| Grouped policy/runtime alignment | planner grouped policy + grouped runtime fold + route strategy observability | Medium-High |
| Access-path shape expansion | planner + route + executor + explain | Medium |
| Query intent surface expansion | now stage-DTO bounded (`AccessPlanningInputs`, `LogicalPlanningInputs`) | Low-Medium |

## Hub Import Pressure Signals

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query`, `access`, `predicate` | 3 | 2 | reduced | Medium |
| `executor/route/planner/feasibility.rs` | `executor`, `query`, `direction` | 3 | 2 | increased (explicit `direction`) | Medium |
| `executor/load/entrypoints.rs` | `executor`, `cursor`, `index`, `response` | 4 | 3 | reduced (`query` import removed) | Medium |

## Velocity Risk Index

**5/10**

Notes:
- Strict layer-authority checks remain clean with no comparator or upward-import leaks.
- Cross-subsystem continuation coordination remains the largest drag source.
- `query/intent` is now approved with explicit stage DTO boundaries, which reduced intent/planner change amplification risk.
