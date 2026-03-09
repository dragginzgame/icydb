# DRY / Redundancy / Consolidation Audit - 2026-03-03 (Rerun 2)

Scope: duplication and divergence pressure while preserving boundary ownership.

## Structural Duplication Scan

| Pattern | Classification | Risk |
| ---- | ---- | ---- |
| Planner/executor defensive validation overlap (cursor/access) | Intentional boundary duplication | Medium |
| Grouped DISTINCT policy + runtime guards | Defensive duplication | Medium-High |
| Commit-window preflight/apply + replay symmetry checks | Defensive duplication | Medium |
| Error constructor + mapping spread (`map_err`) | Boilerplate duplication | Medium |

## Pattern-Level Findings

| Area | Evidence | Drift Risk |
| ---- | ---- | ---- |
| Access canonicalization ownership is centralized but invoked from multiple planners | `normalize_access_plan_value` use in intent/planner | Low-Medium |
| Continuation token construction centralized | no non-test `ContinuationToken::new*` outside `executor/continuation/mod.rs` | Low |
| Cursor-boundary derivation centralized | no non-test `cursor_boundary_from_entity` outside `executor/continuation/mod.rs` | Low |
| Key bound containment in commit-window now delegates to index-owned helper | `key_within_bounds -> key_within_envelope(...)` in `executor/mutation/commit_window.rs` | Low |
| Grouped policy/runtime split remains broad | `query/plan/validate/grouped.rs` + grouped fold runtime paths | Medium-High |

## Dangerous Consolidations (Do Not Merge)

| Area | Why Keep Separate |
| ---- | ---- |
| Planner validation vs executor revalidation | preserves semantic-owner and fail-closed runtime boundaries |
| Cursor spine checks vs index scan continuation checks | preserves independent envelope and monotonicity guards |
| Commit marker guard vs recovery replay | preserves durable atomicity authority |

## Quantitative Summary

- Duplication patterns noted: 9
- High-risk divergence-prone patterns: 1
- Overall DRY Risk Index: **4/10**
