# Invariant Preservation Audit - 2026-02-20

Scope: enforcement existence, placement, symmetry, and drift-sensitivity of core invariants.

## 1. Invariant Registry

| Invariant | Category | Subsystem(s) Impacted |
| ---- | ---- | ---- |
| data-key matches decoded entity key | Identity | data, executor, commit/recovery |
| index id and key namespace consistency | Identity | index, cursor spine |
| component arity stability for cursor/index keys | Identity | query plan, index |
| raw-key ordering is canonical | Ordering | index store, cursor, logical plan |
| continuation is strictly monotonic | Ordering | index range, lookup, logical filter |
| bound inclusivity/exclusivity preservation | Ordering | index range encode/decode |
| access-path shape immutability | Structural | planner/executable/cursor spine |
| unique constraint consistency | Structural/Mutation | index plan, commit prepare |
| reverse relation symmetry | Mutation | relation reverse index, commit prepare |
| replay idempotence | Recovery | commit recovery/apply |

## 2. Boundary Map

| Boundary | Input Assumptions | Output Guarantees |
| ---- | ---- | ---- |
| cursor bytes -> token decode | untrusted bytes | typed token or typed plan error |
| token -> planned cursor | token matches plan signature/direction/offset | immutable planned cursor state |
| logical plan -> executable plan | validated plan shape | fixed direction/access/cursor signature surface |
| executor -> commit window | prevalidated row ops | mechanical apply under commit marker |
| commit marker -> recovery replay | persisted marker may be partial | deterministic replay or fail-closed |
| raw row -> entity decode | bytes may be malformed | corruption/invariant classification |

## 3. Enforcement Mapping Table

| Invariant | Assumed At | Enforced At | Exactly Once? | Narrowest Boundary? | Correct Error Class? | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| cursor signature compatibility | plan cursor load path | `decode_validated_cursor` | Yes | Yes | Yes | Low |
| cursor offset compatibility | plan/revalidate cursor paths | cursor spine mismatch checks | Yes | Yes | Yes | Low |
| anchor envelope containment | anchor decode/continuation | `KeyEnvelope::contains` checks | Yes | Yes | Yes | Low |
| relation strong delete safety | delete executor | relation validator before commit | Yes | Yes | Yes | Low |
| row/index mutation coupling | save/delete/recovery | commit prepare/apply flows | Partial (normal+replay) | Yes | Yes | Medium |
| planner/executor shape parity | executor entrypoints | `validate_executor_plan` | Defensive duplicate | Yes | Yes | Medium |

## 4. Recovery Symmetry Table

| Invariant | Normal Exec | Recovery | Cursor | Reverse Index | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| row key identity match | Yes | Yes | n/a | n/a | Low |
| unique enforcement | Yes | Yes (same prepare path) | n/a | n/a | Low |
| reverse relation symmetry | Yes | Yes | n/a | Yes | Low |
| continuation monotonicity | Yes | n/a | Yes | n/a | Low |
| plan shape immutability | Yes | n/a | Yes | n/a | Medium |

## 5. High Risk Invariants

- None currently at high-risk threshold.

## 6. Redundant Enforcement

- Planner and executor both validate plan shape (`validate_logical_plan_model` and `validate_executor_plan`).
- Cursor compatibility is validated both for incoming token decode and executor cursor-state revalidation.
- Index continuation monotonicity is guarded both in envelope math and store traversal.

## 7. Missing Enforcement

- No critical missing enforcement found in audited paths.
- Drift-sensitive gap: signature compatibility is a single conceptual guard for cross-query cursor reuse; if signature profile fields drift, risk increases.

## 8. Drift Sensitivity Summary

| Invariant | Sensitive To | Drift Risk |
| ---- | ---- | ---- |
| cursor compatibility | token schema/continuation signature expansion | Medium |
| envelope containment | new AccessPath variants with continuation support | Medium |
| plan/executor parity | growth of plan policy variants | Medium |
| replay equivalence | commit marker shape expansion | Medium |

## 9. Overall Invariant Risk Index

Overall Invariant Preservation Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
