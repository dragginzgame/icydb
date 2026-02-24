# State Machine & Transition Integrity Audit - 2026-02-24

Scope: transition correctness across planning, execution, mutation, and recovery.

## 1. Transition Integrity Table

| Transition | Invariants Checked Before? | Mutation Before Validation? | Risk |
| ---- | ---- | ---- | ---- |
| Query intent -> validated plan | Yes (`validate_logical_plan_model`) | No | Low |
| Validated plan -> executable plan | Yes (`PlanError` mapping + cursor validation) | No | Low |
| Executable plan -> load execution | Yes (route-owned execution + logical validation) | No | Low |
| Save/Delete intent -> commit window | Yes (preflight prepare + relation/unique checks) | No | Low |
| Commit marker -> apply closure | Yes (marker persisted first) | No semantic mutation before marker | Low |
| Recovery gate -> replay | Yes (`ensure_recovered`) | No | Low |

## 2. Partial Mutation Risk Table

| Operation | Partial Mutation Possible? | Protection Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| save atomic batch | transient only | marker authority + rollback + replay | Low |
| save non-atomic batch | yes (by contract) | explicit mode semantics | Medium |
| delete batch | transient only | commit window + replay | Low |
| read projection terminals (`values_by*`, `first/last`) | no mutation path | read-only terminal transform | Low |

## 3. Plan/Execution Drift Table

| Area | Plan Shape Can Drift? | Executor Can Widen? | Risk |
| ---- | ---- | ---- | ---- |
| Access plan shape | No (typed and validated) | No | Low |
| Cursor continuation | No (signature/direction/offset checks) | No | Low |
| Projection terminals | No route change | No planner widening | Low |
| Mutation route stages | constrained via mutation router | No | Medium |

## 4. Recovery Determinism Table

| Scenario | Deterministic? | Structural Integrity Preserved? | Risk |
| ---- | ---- | ---- | ---- |
| marker absent | Yes | Yes | Low |
| marker present replay | Yes | Yes | Low |
| replay after crash mid-apply | Yes | Yes (marker row ops authoritative) | Low |
| replay after previous replay | Yes (idempotent) | Yes | Low |
| startup rebuild fallback | deterministic fail-closed behavior | Yes | Medium |

## 5. Drift Sensitivity

- Projection additions in `0.28.1` are read-only terminals and do not add new state transitions.
- Highest sensitivity remains commit row-op schema evolution and cursor protocol evolution.
- Large orchestrator modules (`aggregate.rs`, `route/mod.rs`) increase review cost but do not currently indicate transition contract breakage.

## 6. Canonical Execution Contract Cross-Check

| Contract Check | Current State | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| Single logical lowering path for query classes | Present | `Query::plan()` -> `ExecutablePlan` -> mode dispatch | Low |
| Single commit boundary authority | Present | `begin_commit`/`finish_commit` marker protocol | Low |
| Fast-path precedence is centralized | Present | route-owned `*_FAST_PATH_ORDER` arrays | Low |
| Index mutation derivation singularity | Present | `prepare_row_commit_for_entity` + `plan_index_mutation_for_entity` | Medium |
| Continuation validation singularity | Dual-layer by design | cursor-spine + store-layer advancement guard | Medium |
| Aggregate shadow-flow drift pressure | Controlled but present | aggregate `try_execute_*` path set under route order | Medium |

## Overall State-Machine Risk Index (1-10, lower is better)

**4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
