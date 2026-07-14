# State Machine & Transition Integrity Audit - 2026-03-03

Scope: planner -> execution -> commit window -> recovery transition integrity.

## Transition Integrity

| Transition | Validation Before Mutation | Result | Risk |
| ---- | ---- | ---- | ---- |
| Plan validation -> executable cursor state | Yes (`query/plan/validate` + cursor spine) | PASS | Low |
| Commit-window open -> apply | Yes (preflight prepare before `begin_commit`) | PASS | Medium |
| Apply success/failure marker semantics | marker cleared only on success, retained on failure | PASS | Low |
| Recovery gate before operations | `ensure_recovered` precedes operation-specific logic | PASS | Low |

## Partial Mutation Risk

| Operation | Partial Mutation Possible? | Protection | Risk |
| ---- | ---- | ---- | ---- |
| Save/Delete commit-window apply | transient in-process only | marker authority + replay | Low-Medium |
| Apply failure after marker persistence | yes (transient) | marker retained, replay authoritative | Medium |
| Recovery replay | deterministic re-application with rebuild | low divergence | Medium |

## Drift Sensitivity

- `CommitApplyGuard` rollback closures are explicitly transitional; durable correctness remains marker/replay-owned.
- Continuation and grouped route semantics are still the highest coordination-risk transition seams.

## Overall State-Machine Risk Index

**4/10**
