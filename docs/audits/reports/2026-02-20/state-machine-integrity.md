# State Machine & Transition Integrity Audit - 2026-02-20

Scope: transition correctness across plan -> execute -> commit -> recovery.

## 1. Transition Integrity Table

| Transition | Invariants Checked Before? | Mutation Before Validation? | Risk |
| ---------- | -------------------------- | --------------------------- | ---- |
| Planner -> ExecutablePlan | Yes (`validate_logical_plan_model`) | No | Low |
| ExecutablePlan -> LoadExecutor | Yes (`validate_executor_plan`, cursor revalidation) | No | Low |
| Save intent -> commit window | Yes (sanitize/validate/relations + preflight prepare) | No | Low |
| Delete intent -> commit window | Yes (plan validate + relation guard + preflight prepare) | No | Low |
| Commit marker -> apply | Yes (marker persisted first, apply mechanical) | No semantic mutation before marker | Low |
| Recovery marker -> replay | Yes (marker decode/prepare per row op) | No (prepare failure rolls back) | Low-Medium |

## 2. Partial Mutation Risk Table

| Operation | Partial Mutation Possible? | Protection Mechanism | Risk |
| --------- | -------------------------- | -------------------- | ---- |
| Save single-row | transient in-process only | commit marker authority + rollback + recovery replay | Low |
| Save atomic batch | transient in-process only | preflight + one marker + mechanical apply | Low |
| Save non-atomic batch | yes by design | explicit non-atomic contract | Medium |
| Delete batch | transient in-process only | preflight + marker + replay | Low |
| Cursor decode/validation | no mutation path | pure validation boundary | Low |

## 3. Plan/Execution Drift Table

| Area | Plan Shape Can Drift? | Executor Can Widen? | Risk |
| ---- | --------------------- | ------------------- | ---- |
| Access path | No (validated, typed plan) | No | Low |
| Cursor continuation | No (signature/direction/offset checks) | No | Low |
| Pagination window | No (effective offset logic explicit) | No | Low |
| Fast-path routing | bounded by validated plan shape | No | Medium |

## 4. Recovery Determinism Table

| Scenario | Deterministic? | Structural Integrity Preserved? | Risk |
| -------- | -------------- | ------------------------------- | ---- |
| marker absent | Yes | Yes | Low |
| marker present replay once | Yes | Yes | Low |
| replay after crash mid-apply | Yes | Yes (replay of stored row ops) | Low |
| replay repeated twice | Yes | Yes | Low |
| rebuild-secondary-index fallback | Yes (snapshot restore on rebuild failure) | Yes | Medium |

## 5. Drift Sensitivity

- Implicit invariant sensitivity remains around commit protocol evolution: any new row-op field must be mirrored in prepare/apply/replay.
- Cursor protocol now includes `initial_offset`; future token-field additions must be validated in both decode and revalidation paths.
- Non-atomic save mode remains intentionally divergent and should not be conflated with atomic state-machine guarantees.

Overall State-Machine Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
