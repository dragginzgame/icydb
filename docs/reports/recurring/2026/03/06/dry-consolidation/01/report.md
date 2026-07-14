# DRY / Redundancy / Consolidation Audit - 2026-03-06

Scope: duplication and divergence pressure while preserving boundary ownership.

## Structural Duplication Scan

| Pattern | Classification | Risk |
| ---- | ---- | ---- |
| Scalar non-paged gate checks repeated across fluent scalar terminals (`ensure_non_paged_mode_ready`) | Intentional boundary duplication | Low-Medium |
| Persisted payload byte summation logic appears in runtime terminal and test parity helper | Defensive duplication | Medium |
| Row-length saturating conversion appears in runtime helper and test-side expected-value helper | Boilerplate duplication | Low-Medium |
| Planner validation + executor boundary guard overlap for paging/shape constraints | Intentional boundary duplication | Medium |

## Pattern-Level Findings

| Area | Evidence | Drift Risk |
| ---- | ---- | ---- |
| `bytes()` semantic ownership is centralized in executor terminal | `LoadExecutor::bytes(plan)` holds runtime behavior under load terminal boundary | Low |
| `bytes()` parity checks use same effective-window baseline in matrix/session tests | aggregate/session matrix tests compare `bytes()` with execute-window payload sums | Low |
| Access-path capability checks remain route/access owned | no new `bytes()`-specific access capability forks detected | Low-Medium |
| Continuation policy logic remains separate from `bytes()` terminal | no continuation policy re-derivation introduced by `bytes()` | Low |

## Dangerous Consolidations (Do Not Merge)

| Area | Why Keep Separate |
| ---- | ---- |
| Planner policy validation vs executor runtime rechecks | maintains fail-closed behavior when planner contracts are bypassed |
| Runtime `bytes()` implementation vs test-side payload calculators | tests need independent expected-value derivation to catch semantic regressions |

## Quantitative Summary

- Duplication patterns noted: 8
- High-risk divergence-prone patterns: 1
- Overall DRY Risk Index: **5/10**

Key conclusion:
- `bytes()` landed without introducing a new high-risk duplication surface; existing medium DRY pressure remains concentrated in intentional planner/executor guard overlap.
