# DRY / Redundancy / Consolidation Audit - 2026-03-04

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
| Access canonicalization ownership remains centralized | `normalize_access_plan_value` in `access` boundary with planner consumption | Low-Medium |
| Continuation token construction remains boundary-owned | no token constructor callsites outside cursor/continuation owners | Low |
| Cursor-boundary derivation stays centralized | `cursor_boundary_from_entity` owned in `db/cursor` | Low |
| Comparator logic leak previously seen in commit window is now delegated | `executor/mutation/commit_window.rs` delegates to `key_within_envelope` | Low |
| Grouped policy/runtime split remains broad | `query/plan/validate/grouped/*` + grouped runtime fold paths | Medium |

## Dangerous Consolidations (Do Not Merge)

| Area | Why Keep Separate |
| ---- | ---- |
| Planner validation vs executor revalidation | preserves semantic-owner and fail-closed runtime boundaries |
| Cursor spine checks vs index scan continuation checks | preserves independent envelope and monotonicity guards |
| Commit marker guard vs recovery replay | preserves durable atomicity authority |

## Quantitative Summary

- Duplication patterns noted: 9
- High-risk divergence-prone patterns: 1
- Defensive duplications: 4
- Estimated conservative LoC reduction potential (safe, intra-owner only): 120-220
- Overall DRY Risk Index: **4/10**
