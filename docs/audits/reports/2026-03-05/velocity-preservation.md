# Velocity Preservation Audit - 2026-03-05

Scope: feature agility and change amplification across planner/route/executor surfaces.

## Current Friction Signals

| Signal | Current | Risk |
| ---- | ---- | ---- |
| Access-path fan-out | 74 runtime references / 10 files | Medium |
| Continuation surface | 790 runtime references / 77 files | High |
| Error mapping spread | 171 `map_err(` references / 64 runtime files | Medium |
| `.as_inner()` cross-boundary adapters | 0 runtime callsites | Low |

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
| Access-path shape expansion | planner + route + executor + explain | Medium |

## Velocity Risk Index

**6/10**

Notes:
- Access-path pressure improved compared with the previous run.
- Continuation/anchor spread remains the largest drag source.
