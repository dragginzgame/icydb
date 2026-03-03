# Velocity Preservation Audit - 2026-03-03 (Rerun 2)

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 112 runtime references / 12 files | Medium-High |
| Continuation surface | 699 runtime references / 70 files | High |
| Error mapping spread | 168 `map_err(` references / 62 runtime files | Medium |
| `.as_inner()` cross-boundary adapters | 0 runtime callsites | Low |

## Layer-Leakage Velocity Checks

| Check | Result |
| ---- | ---- |
| query -> executor upward dependency leak (tracked edges) | none detected |
| index/data/commit -> query upward leak (tracked edges) | none detected |
| runtime cursor token construction outside continuation owner | none detected |

## Change Amplification Areas

| Area | Why It Multiplies Changes | Severity |
| ---- | ---- | ---- |
| Continuation protocol evolution | touches cursor spine + route + load entrypoints + scan guards | High |
| Grouped policy/runtime alignment | planner grouped policy + grouped runtime fold + route strategy observability | High |
| Access-path shape expansion | planner + route + executor + explain | Medium-High |

## Hub Import Pressure Signals

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query(7)`, `access(2)`, `index(2)`, `predicate(2)` | 4 | 3 | `access` token count reduced | Medium-High |
| `executor/route/planner/feasibility.rs` | `executor(5)`, `query(2)`, `cursor(1)` | 3 | 2 | stable | Medium |
| `executor/load/entrypoints.rs` | `executor(3)`, `response(1)`, `query(1)`, `index(1)`, `cursor(1)` | 5 | 4 | stable | Medium-High |

## Velocity Risk Index

**6/10**

Notes:
- Comparator-authority cleanup reduced one high-risk cross-layer drift point.
- Cross-subsystem continuation and grouped-policy coordination remains the largest drag source.
