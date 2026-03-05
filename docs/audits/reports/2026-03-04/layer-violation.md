# Cross-Cutting Layer Violation Audit - 2026-03-04

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer direction reference: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

This run audits semantic authority ownership (not import-direction only).

## STEP 1 - Policy Re-Derivation Scan

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |
| Grouped DISTINCT admissibility | `query/plan/semantics/group_distinct.rs`, `query/plan/validate/grouped/*`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Executor boundary consumes planner-owned policy contracts; no semantic fork observed | Low |
| Grouped projection layout validity | `query/plan/grouped_layout.rs`, `query/plan/group.rs`, `executor/load/grouped_route.rs` | `query/plan` | `executor/load` | Runtime checks remain contract assertions over planner output | Low |
| Cursor paging requires order+limit | `query/plan/validate/*`, `query/policy.rs`, `query/intent/errors.rs` | `query/plan` | `query` surface | Delegation to a single validator path; no duplicate implementation observed | Low |
| Delete-limit requires ordering | `query/plan/validate/policy.rs`, `executor/kernel/post_access/mod.rs` | split by phase (`query/plan` policy, `executor` boundary gate) | `executor` | Defensive phase enforcement rather than semantic re-derivation | Low |
| Secondary ORDER pushdown applicability | `query/plan/semantics/logical.rs`, `executor/route/pushdown.rs` | `executor/route` | `query/plan` | Shared contract shape across planner and route remains coupling-sensitive | Medium |

## STEP 2 - Ordering Authority Leakage

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |
| Commit-window index bound check now delegates to index helper (`key_within_envelope`) | `executor/mutation/commit_window.rs` | `index` | Legitimate delegation | Low |
| Envelope containment via index contract | `cursor/anchor.rs` | `index` | Legitimate delegation | Low |

Result: no direct comparator reimplementation was found outside `db/index/*` in this run.

## STEP 3 - Continuation Authority Leakage

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Strict advance check delegated to `index::continuation_advanced` | `cursor/continuation.rs` | `index` | No (delegation) | Low |
| Anchor containment delegated through index envelope contract | `cursor/anchor.rs` | `index` | No (delegation) | Low |
| Continuation rewrite helpers outside `index/*` and `cursor/*` | none found | `index` / `cursor` | No | Low |

## STEP 4 - Access Capability Fan-Out

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | --- | --- | --- |
| `AccessPath` runtime references | 74 non-test references across 10 files | `query`, `access`, `executor`, `cursor` | Medium (4-layer descriptor fan-out) |
| Capability/pushdown predicates | 67 references across 12 files | `access`, `query/plan`, `executor/route` | Medium (3 layers) |
| `ExecutionMode` references | 46 references across 9 files | mostly `executor`, some `query` | Low-Medium |

## STEP 5 - Invariant Enforcement Spread

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |
| Envelope containment | `index/envelope.rs`, `index/scan.rs`, `cursor/anchor.rs` | `index` | Yes | Low |
| Strict advancement | `index/envelope.rs`, `index/scan.rs`, `cursor/continuation.rs`, `executor/route/*`, `executor/load/entrypoints.rs`, `query/plan/model.rs` | split authority (`index` comparator + route policy) | Yes | Medium |
| Unique enforcement | `index/plan/unique.rs`, `index/plan/mod.rs`, `index/scan.rs` | `index` | Yes | Low |
| Reverse symmetry / reverse consistency | `relation/reverse_index.rs`, `relation/validate.rs`, `commit/prepare.rs` | `relation` / `commit` | Yes | Low |
| Commit marker lifecycle | `commit/guard.rs`, `commit/recovery.rs`, `commit/replay.rs` | `commit/storage` | Yes | Low |
| Cursor signature compatibility | `cursor/spine.rs`, `executor/executable_plan.rs`, `session.rs` | `cursor` | Yes | Low |

## STEP 6 - Error Classification Cross-Layer Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| `InternalError` -> `QueryExecuteError` | `query/intent/errors.rs` | No | Low |
| `ExecutorError` -> `InternalError` | `executor/mod.rs` | No | Low |
| `StoreRegistryError` -> `InternalError` | `registry.rs` | No | Low |

## STEP 7 - Semantic Fan-Out Metric

| Surface | Count | Risk Level |
| --- | --- | --- |
| Enums matched in >=3 modules | 1 (`AccessPath`) | Medium |
| Policy predicates implemented in >=3 modules | 0 | Low |
| Invariants enforced in >=3 sites | 1 (`strict advancement`) | Medium |
| Continuation/anchor rewrite logic references outside cursor/index | 0 | Low |

## STEP 8 - Legitimate Cross-Cutting (Do NOT Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner grouped DISTINCT policy + executor boundary recheck | Planner owns semantics; executor protects runtime boundary | Invalid grouped DISTINCT shapes could execute when planner contracts are bypassed |
| Cursor envelope validation + index scan guard | Token validation and store-level safety are independent boundaries | Out-of-envelope anchors could slip through malformed inputs |
| Strict advancement checks across index/cursor/route/load | Comparator truth remains index-owned while route/load enforce policy gates | Conflating comparator and policy layers weakens fail-closed behavior |
| Commit guard + recovery replay marker protocol | Guard is in-process lifecycle; replay is durable correctness authority | Partial-failure recovery authority would degrade |

## STEP 9 - Output Summary

### High-Risk Cross-Cutting Violations

- None found in this run.

### Medium-Risk Drift Surfaces

- `AccessPath` remains propagated across four layers (`query`, `access`, `executor`, `cursor`).
- Pushdown/capability decisions remain distributed across three layers by design.
- Strict advancement is intentionally enforced in multiple layers and remains drift-sensitive if delegation contracts diverge.

### Low-Risk / Intentional Redundancy

- Grouped DISTINCT and grouped projection checks are planner-owned with boundary revalidation.
- Envelope containment and cursor signature checks are intentionally duplicated across planner/cursor/store boundaries.
- Comparator authority remains index-owned after commit-window delegation cleanup.

### Quantitative Snapshot

- Policy duplications found: **5**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **2**
- Invariants enforced in >3 sites: **1**
- Protective redundancies: **4**
- Cross-Cutting Risk Index (1-10): **5**
