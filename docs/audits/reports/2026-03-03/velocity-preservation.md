# Velocity Preservation Audit - 2026-03-03

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 117 runtime references / 13 files | Medium-High |
| Continuation surface | 580 runtime references / 67 files | High |
| Error mapping spread | 168 `map_err(` references / 62 runtime files | Medium |
| `.as_inner()` cross-boundary adapters | 11 runtime callsites | Medium |

## Layer-Leakage Velocity Checks

| Check | Result |
| ---- | ---- |
| query -> executor upward dependency leak | none detected |
| index/data/commit -> query upward leak | none detected |
| runtime cursor token construction outside continuation owner | none detected |

## Change Amplification Areas

| Area | Why It Multiplies Changes | Severity |
| ---- | ---- | ---- |
| Continuation protocol evolution | touches cursor spine + route + load entrypoints + scan guards | High |
| Grouped policy/runtime alignment | planner grouped policy + grouped runtime fold + route strategy observability | High |
| Access-path shape expansion | planner + route + executor + explain | Medium-High |

## Velocity Risk Index

**6/10**

Notes:
- Recent refactors reduced hub-size pressure and localized several continuation responsibilities.
- Cross-subsystem continuation and grouped policy coordination remains the largest drag source.
