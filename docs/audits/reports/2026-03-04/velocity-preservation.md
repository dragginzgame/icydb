# Velocity Preservation Audit - 2026-03-04

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 68 runtime references / 10 files | Medium |
| Continuation surface | 976 runtime references / 72 files | High |
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
| Access-path shape expansion | planner + route + executor + explain | Medium |

## Hub Import Pressure Signals

| Module | Top Imports | Unique Sibling Imports | Cross-Layer Dependency Count | Delta vs Previous | Velocity Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query(7)`, `access(2)`, `index(2)`, `predicate(2)` | 4 | 3 | stable | Medium-High |
| `executor/route/planner/feasibility.rs` | `executor(5)`, `query(2)` | 2 | 1 | reduced (`cursor` removed) | Medium |
| `executor/load/entrypoints.rs` | `executor(3)`, `response(1)`, `query(1)`, `index(1)`, `cursor(1)` | 5 | 4 | stable | Medium-High |

## Velocity Risk Index

**6/10**

Notes:
- Strict layer-authority checks remain clean with no comparator or upward-import leaks.
- Cross-subsystem continuation and grouped-policy coordination remains the largest drag source.
