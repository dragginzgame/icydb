# DRY / Redundancy / Consolidation Audit - 2026-03-05

Scope: duplication and divergence pressure while preserving boundary ownership.

## Structural Duplication Scan

| Pattern | Classification | Risk |
| ---- | ---- | ---- |
| Planner/executor defensive validation overlap (cursor/access) | Intentional boundary duplication | Medium |
| Grouped DISTINCT policy + runtime guards | Defensive duplication | Medium |
| Commit-window preflight/apply + replay symmetry checks | Defensive duplication | Medium |
| Error constructor + mapping spread (`map_err`) | Boilerplate duplication | Medium |

## Pattern-Level Findings

| Area | Evidence | Drift Risk |
| ---- | ---- | ---- |
| Access canonicalization remains centralized | `normalize_query_predicate` and access canonicalization stay under query/access owners | Low-Medium |
| Continuation token construction remains centralized | no non-test `ContinuationToken::new*` outside continuation owner module | Low |
| Comparator authority duplication reduced | layer-authority checks report `Comparator definitions outside index: 0` | Low |
| Grouped policy/runtime split remains broad | planner grouped policy + grouped runtime fold remain separate by design | Medium |

## Dangerous Consolidations (Do Not Merge)

| Area | Why Keep Separate |
| ---- | ---- |
| Planner validation vs executor revalidation | preserves semantic-owner and fail-closed runtime boundaries |
| Cursor spine checks vs index scan continuation checks | preserves independent envelope and monotonicity guards |
| Commit marker guard vs recovery replay | preserves durable atomicity authority |

## Quantitative Summary

- Duplication patterns noted: 9
- High-risk divergence-prone patterns: 1
- Overall DRY Risk Index: **5/10**
