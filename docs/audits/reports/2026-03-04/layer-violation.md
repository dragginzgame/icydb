# Cross-Cutting Layer Violation Audit - 2026-03-04

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer direction reference: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

This rerun audits semantic authority ownership (not import-direction only).

## STEP 1 - Policy Re-Derivation Scan

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |
| Grouped DISTINCT admissibility | `query/plan/semantics/group_distinct.rs`, `query/plan/validate/grouped.rs`, `query/plan/group.rs`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Shared helper + projected contract rather than re-derivation | Low |
| Grouped projection layout validity | `query/plan/grouped_layout.rs`, `query/plan/group.rs`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Executor boundary uses planner helper assertions; no separate algorithm | Low |
| Cursor paging requires order+limit | `query/plan/validate/policy.rs`, `query/policy.rs`, `query/intent/errors.rs` | `query/plan` | `query` surfaces only | Delegation to one validator; no semantic fork detected | Low |
| Delete-limit requires ordering | `query/plan/validate/policy.rs`, `executor/kernel/post_access/mod.rs` | `query/plan` (policy), `executor` (phase ordering guard) | `executor/kernel` | Defensive runtime phase assertion complements plan-time validation | Low |
| Secondary ORDER pushdown applicability | `executor/route/pushdown.rs`, `query/plan/semantics/logical.rs` | `executor/route` | `query/plan` | Delegation to route-owned assessor from planner profile; coupling but no duplicate algorithm | Medium |

## STEP 2 - Ordering Authority Leakage

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |
| Commit-window range containment delegates to index-owned helper `key_within_envelope` | `executor/mutation/commit_window.rs` | `index` | Legitimate delegation | Low |
| Envelope containment via `KeyEnvelope::new(...).contains(...)` | `cursor/anchor.rs` | `index` | Legitimate delegation | Low |

Result: no raw key comparator reimplementation detected outside `index/*` in this rerun.

## STEP 3 - Continuation Authority Leakage

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Strict advance check delegated to index primitive `continuation_advanced(...)` during token build | `cursor/continuation.rs` | `index` | No (delegation) | Low |
| Anchor containment check delegated through `KeyEnvelope` | `cursor/anchor.rs` | `index` | No (delegation) | Low |
| Continuation rewrite helpers (`resume_bounds_from_refs`, `anchor_within_envelope`) outside `index/*` and `cursor/*` | none found | `index` / `cursor` | No | Low |

Result: no continuation rewrite/advancement logic detected outside `index/*` and `cursor/*`.

## STEP 4 - Access Capability Fan-Out

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | --- | --- | --- |
| `AccessPath` runtime references (`AccessPath::...`) | 68 non-test references | `query`, `access`, `executor` | Medium (structural routing descriptor fan-out across 3 layers) |
| Pushdown/capability predicates (`eligible`, `applicability`, `pushdown_shape_eligible`, etc.) | distributed across route/access/planner surfaces | `access`, `query/plan`, `executor/route` | Medium (3 layers) |
| `ExecutionMode` branching/references | localized to execution planning/execution paths | mostly `executor`, some `query` surfaces | Low (2 layers) |

## STEP 5 - Invariant Enforcement Spread

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |
| Envelope containment | `index/envelope.rs`, `index/scan.rs`, `cursor/anchor.rs` | `index` | Yes (planner/cursor + scan guard) | Low |
| Strict advancement | `index/envelope.rs`, `index/scan.rs`, `cursor/continuation.rs`, `executor/route/{contracts,planner/feasibility}`, `executor/load/entrypoints.rs` | Layered ownership: `index` comparator authority + `executor/route` policy authority | Yes (intentional fail-closed redundancy) | Medium only if delegation diverges from index primitive |
| Unique enforcement | `index/plan/unique.rs`, `index/plan/mod.rs`, `index/scan.rs` | `index` | Yes | Low |
| Reverse symmetry / reverse consistency | `relation/reverse_index.rs`, `relation/validate.rs`, `commit/prepare.rs` | `relation` / `commit` | Yes | Low |
| Commit marker lifecycle | `commit/guard.rs`, `commit/recovery.rs`, `commit/replay.rs` | `commit/storage` | Yes | Low |
| Cursor signature compatibility | `cursor/spine.rs`, `executor/executable_plan.rs`, `session.rs` | `cursor` | Yes | Low |

## STEP 6 - Error Classification Cross-Layer Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| `InternalError` -> `QueryExecuteError` | `query/intent/errors.rs` | No; one-to-one by `ErrorClass` | Low |
| `ExecutorError` -> `InternalError` | `executor/mod.rs` | No; `Conflict`/`Corruption` mapping is explicit and stable | Low |
| `StoreRegistryError` -> `InternalError` | `registry.rs` | No intra-concept drift (domain-specific mapping is explicit) | Low |

No clear same-concept cross-layer class mismatch detected in sampled runtime mappings.

## STEP 7 - Semantic Fan-Out Metric

| Surface | Count | Risk Level |
| --- | --- | --- |
| Enums matched in >=3 modules | 2 (`AccessPath`, `AggregateKind`) | Medium |
| Policy predicates implemented in >=3 modules | 0 | Low |
| Invariants enforced in >=3 modules | 1 (`strict advancement`) | Medium |
| Continuation/anchor logic references outside cursor/index | 0 | Low |

## STEP 8 - Legitimate Cross-Cutting (Do NOT Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner grouped DISTINCT policy + executor boundary recheck | Planner owns semantics; executor defends runtime boundary misuse | Invalid grouped DISTINCT shapes could execute if planner contract is bypassed |
| Planner grouped projection layout + executor debug assertion | Planner computes canonical layout; executor asserts boundary integrity | Silent projection-layout drift at runtime boundary |
| Cursor anchor envelope validation + index scan envelope validation | Cursor validates token envelope; scan enforces store-level safety | Out-of-envelope anchors could slip through on malformed/misrouted inputs |
| Strict advancement across index/cursor/route/load | Comparator truth remains index-owned while route/load enforce continuation-policy feasibility | Conflating comparator and policy layers would weaken fail-closed guarantees |
| Commit guard + recovery replay marker protocol | Guard handles in-process lifecycle; replay owns durable correctness | Loss of durable recovery authority on partial failure |

## STEP 9 - Output Summary

### High-Risk Cross-Cutting Violations

- None detected in this rerun.

### Medium-Risk Drift Surfaces

- Pushdown applicability/capability signals are spread across `access`, `query/plan`, and `executor/route`.
- Strict advancement is enforced in more than three runtime locations by design (comparator vs policy layering); risk is conditional on helper delegation divergence.
- Continuation/anchor concern spread is high in raw mention count even with centralized comparator authority.

### Low-Risk / Intentional Redundancy

- Grouped DISTINCT and grouped projection layout checks use planner-owned helpers/contracts with boundary revalidation.
- Envelope containment and cursor signature checks are intentionally duplicated across planner/cursor/store boundaries for fail-closed behavior.
- Error classification mappings reviewed are explicit and consistent.

### Quantitative Snapshot

- Policy duplications found: **5**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **2**
- Invariants enforced in >3 sites: **1**
- Protective redundancies: **5**
- Cross-Cutting Risk Index (1-10): **5**

Interpretation: moderate semantic spread; comparator/canonicalization ownership remains centralized with no hard authority leaks in this rerun.
