# Cross-Cutting Layer Violation Audit - 2026-03-04

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer direction reference: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

This rerun audits semantic authority ownership (not import-direction only).

## Layer Health Snapshot (scripted)

`bash scripts/ci/check-layer-authority-invariants.sh`

| Check | Result |
| --- | --- |
| Upward imports (tracked edges) | 0 |
| Cross-layer policy re-derivations | 0 |
| Cross-layer predicate duplication count | 1 |
| Enum fan-out >2 layers | 2 |
| Comparator definitions outside index | 0 |
| Canonicalization entrypoints | 1 |

## STEP 1 - Policy Re-Derivation Scan

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |
| Grouped DISTINCT admissibility | `query/plan/semantics/group_distinct.rs`, `query/plan/validate/grouped.rs`, `query/plan/group.rs`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Projected contract + defensive boundary checks, no semantic fork found | Low |
| Grouped projection layout validity | `query/plan/grouped_layout.rs`, `query/plan/group.rs`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Executor uses planner-projected validity contract | Low |
| Cursor paging requires explicit order+limit | `query/plan/validate/policy.rs`, `query/policy.rs`, `query/intent/errors.rs` | `query/plan` | query surface only | Delegation to one policy owner | Low |
| Secondary pushdown applicability | `query/plan/semantics/logical.rs`, `executor/route/pushdown.rs` | `executor/route` + route profile contract | `query/plan` (projection only) | Coupled but no duplicate algorithm | Medium |

## STEP 2 - Ordering Authority Leakage

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |
| Commit-window range containment delegates to index helper `key_within_envelope` | `executor/mutation/commit_window.rs` | `index` | Legitimate delegation | Low |
| Anchor envelope containment through `KeyEnvelope::contains` | `cursor/anchor.rs` | `index` | Legitimate delegation | Low |

Result: no raw key comparator reimplementation detected outside `index/*`.

## STEP 3 - Continuation Authority Leakage

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Strict advance delegated to index primitive `continuation_advanced(...)` | `cursor/continuation.rs` | `index` | No (delegation) | Low |
| Anchor containment delegated through `KeyEnvelope` | `cursor/anchor.rs` | `index` | No (delegation) | Low |
| Continuation rewrite helpers outside `index/*` and `cursor/*` | none found | `index` / `cursor` | No | Low |

## STEP 4 - Access Capability Fan-Out

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | --- | --- | --- |
| `AccessPath` runtime references (`AccessPath::...`) | 74 non-test references | `query`, `access`, `executor` | Medium (3 layers) |
| Pushdown/capability predicates (`eligible`, `applicability`, `pushdown_shape_eligible`, etc.) | distributed across route/access/planner surfaces | `access`, `query/plan`, `executor/route` | Medium (3 layers) |
| `ExecutionMode` branching/references | localized to planning + execution orchestration | mostly `executor`, some `query` surfaces | Low (2 layers) |

## STEP 5 - Invariant Enforcement Spread

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |
| Envelope containment | `index/envelope.rs`, `index/scan.rs`, `cursor/anchor.rs` | `index` | Yes | Low |
| Strict advancement | `index/envelope.rs`, `index/scan.rs`, `cursor/continuation.rs`, `executor/route/*`, `executor/load/entrypoints.rs` | split authority (`index` comparator, `route/load` policy) | Yes | Medium |
| Unique enforcement | `index/plan/unique.rs`, `index/plan/mod.rs`, `index/scan.rs` | `index` | Yes | Low |
| Commit marker lifecycle | `commit/guard.rs`, `commit/recovery.rs`, `commit/replay.rs` | `commit/storage` | Yes | Low |
| Cursor signature compatibility | `cursor/spine.rs`, `executor/executable_plan.rs`, `session.rs` | `cursor` | Yes | Low |

## STEP 6 - Error Classification Cross-Layer Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| `InternalError` -> `QueryExecuteError` | `query/intent/errors.rs` | No (class preserved) | Low |
| `ExecutorError` -> `InternalError` | `executor/mod.rs` | No class drift in sampled paths | Low |
| `StoreRegistryError` -> `InternalError` | `registry.rs` | No same-concept mismatch found | Low |

## STEP 7 - Semantic Fan-Out Metric

| Surface | Count | Risk Level |
| --- | --- | --- |
| Enums matched in >=3 modules | 2 (`AccessPath`, `AggregateKind`) | Medium |
| Policy predicates implemented in >=3 modules | 0 | Low |
| Invariants enforced in >3 sites | 1 (`strict advancement`) | Medium |
| Continuation/anchor logic references outside cursor/index | 0 | Low |

## STEP 8 - Legitimate Cross-Cutting (Do NOT Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner grouped DISTINCT policy + executor runtime recheck | Planner owns semantics; executor defends runtime boundary misuse | Invalid grouped DISTINCT shapes could execute |
| Planner grouped projection layout + executor boundary assertion | Planner computes canonical layout; executor checks handoff integrity | Silent projection-layout drift |
| Cursor anchor validation + index scan envelope checks | Cursor validates token envelope; index scan enforces store-level safety | Out-of-envelope anchors on malformed inputs |
| Commit guard + recovery replay marker protocol | In-process lifecycle guard and durable replay authority are distinct | Weakened durability/recovery guarantees |

## Output Summary

### High-Risk Cross-Cutting Violations

- None detected in this rerun.

### Medium-Risk Drift Surfaces

- Access capability/pushdown decisions remain spread across `access`, `query/plan`, and `executor/route`.
- Strict advancement remains intentionally enforced in >3 sites; keep delegation paths aligned.

### Low-Risk / Intentional Redundancy

- Comparator/canonicalization ownership remains centralized (`index` + one canonicalization entrypoint).
- Planner/executor and cursor/index defensive boundaries remain explicit.
- `query/intent` now projects stage DTOs instead of leaking planner internals; this reduced one prior semantic drift vector.

### Quantitative Snapshot

- Policy duplications found: **4**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **2**
- Invariants enforced in >3 sites: **1**
- Protective redundancies: **4**
- Cross-Cutting Risk Index (1-10): **4**

Interpretation: low-to-moderate semantic spread with no hard authority leaks in this rerun.
