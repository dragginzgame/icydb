# DRY / Redundancy / Consolidation Audit - 2026-03-04

Scope: duplication and divergence pressure while preserving boundary ownership.

## Structural Duplication Scan

| Pattern | Classification | Risk |
| ---- | ---- | ---- |
| Planner/executor defensive validation overlap (cursor/access) | Intentional boundary duplication | Medium |
| Grouped DISTINCT policy + runtime guards | Defensive duplication | Medium-High |
| Commit-window preflight/apply + replay symmetry checks | Defensive duplication | Medium |
| Error constructor + mapping spread (`map_err`) | Boilerplate duplication | Medium |
| Continuation safety checks across cursor/index/route/load | Safety-enhancing redundancy | Medium |

## Pattern-Level Findings

| Area | Evidence | Drift Risk |
| ---- | ---- | ---- |
| Access canonicalization ownership remains centralized | canonicalization entrypoints: 1 (layer-authority check) | Low |
| Continuation token construction remains centralized | no non-test `ContinuationToken::new*` outside `executor/continuation/mod.rs` | Low |
| Cursor-boundary derivation remains centralized | no non-test `cursor_boundary_from_entity` outside `executor/continuation/mod.rs` | Low |
| Intent/planner handoff duplication reduced | stage-specific DTOs now formalized (`AccessPlanningInputs`, `LogicalPlanningInputs`) | Low |
| Grouped policy/runtime split remains broad | `query/plan/validate/grouped.rs` + grouped fold/runtime route handling | Medium-High |

## Dangerous Consolidations (Do Not Merge)

| Area | Why Keep Separate |
| ---- | ---- |
| Planner validation vs executor revalidation | preserves semantic-owner and fail-closed runtime boundaries |
| Cursor spine checks vs index scan continuation checks | preserves independent envelope and monotonicity guards |
| Commit guard vs recovery replay marker protocol | preserves durable atomicity authority |
| Route feasibility checks vs index comparator authority | preserves policy-vs-ordering boundary |

## Quantitative Summary

- Duplication patterns noted: 8
- High-risk divergence-prone patterns: 1
- Defensive/protective duplication patterns: 4
- Overall DRY Risk Index: **4/10**
