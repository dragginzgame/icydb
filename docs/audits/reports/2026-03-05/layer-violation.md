# Cross-Cutting Layer Violation Audit - 2026-03-05

Scope: `crates/icydb-core/src/db/` non-test runtime modules.

Layer direction reference: `intent -> query/plan -> access -> executor -> index/storage -> codec`.

This run audits semantic authority ownership (not import-direction only).

## STEP 1 - Policy Re-Derivation Scan

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |
| Grouped DISTINCT admissibility and legality | `query/plan/semantics/group_distinct.rs`, `query/plan/validate/grouped/*.rs`, grouped runtime fold files | `query/plan` | `executor/load` (revalidation only) | delegated contract usage, not semantic fork | Low |
| Cursor paging requires order+limit | `query/plan/validate/policy.rs`, `query/policy.rs` | `query/plan` | query surface only | no re-derivation | Low |
| Delete-limit ordering contract | `query/plan/validate/policy.rs`, `executor/kernel/post_access/mod.rs` | `query/plan` + runtime phase guards | `executor/kernel` | intentional defensive check | Low |

Layer-authority invariant check output:
- `Cross-layer policy re-derivations: 0`

## STEP 2 - Ordering Authority Leakage

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |
| Raw comparator logic outside `index/*` | none detected | `index` | none | Low |
| Envelope containment delegation | `cursor/anchor.rs` -> `KeyEnvelope::contains` | `index` | legitimate delegation | Low |

Layer-authority invariant check output:
- `Comparator definitions outside index: 0`

## STEP 3 - Continuation Authority Leakage

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Bound rewrite (`resume_bounds_from_refs`) | `index/envelope.rs`, consumed via `index/scan.rs` | `index` | No (single owner) | Low |
| Strict advance comparator (`continuation_advanced`) | `index/envelope.rs`, used by `index/scan.rs` + `cursor/continuation.rs` | `index` | No (delegated use only) | Low |
| Anchor containment helper (`anchor_within_envelope`) | `index/envelope.rs` | `index` | No (delegated use only) | Low |

## STEP 4 - Access Capability Fan-Out

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | --- | --- | --- |
| `AccessPath` runtime references | 74 non-test references | 4 layers (`query`, `access`, `executor`, `cursor`) | Medium |
| Route feasibility predicates | concentrated in route owner modules with one cross-layer duplicate signal | 2-3 layers | Medium |
| `ExecutionMode` branching/references | route + load boundary use | mostly `executor` | Low |

## STEP 5 - Invariant Enforcement Spread

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |
| Envelope containment | `index/envelope.rs`, `index/scan.rs`, `cursor/anchor.rs` | `index` | Yes | Low |
| Strict advancement | `index/envelope.rs`, `index/scan.rs`, `cursor/continuation.rs`, route/load boundary checks | `index` comparator authority + route policy | Yes | Medium |
| Unique enforcement | `index/plan/unique.rs`, `index/scan.rs`, planning glue | `index` | Yes | Low |
| Commit marker lifecycle | `commit/guard.rs`, `commit/recovery.rs`, `commit/replay.rs` | `commit/storage` | Yes | Low |

## STEP 6 - Error Classification Cross-Layer Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| `InternalError` -> `QueryExecuteError` | `query/intent/errors.rs` | No | Low |
| `ExecutorError` -> `InternalError` | `executor/mod.rs` | No | Low |
| Store/registry errors -> internal taxonomy | `registry.rs`, `error.rs` | No semantic mismatch found | Low |

## STEP 7 - Semantic Fan-Out Metric

| Surface | Count | Risk Level |
| --- | --- | --- |
| Enums matched in `>=3` layers | 2 (from layer-authority metric) | Medium |
| Policy predicates implemented in `>=3` layers | 0 confirmed (`cross-layer policy re-derivations: 0`) | Low |
| Invariants enforced in `>=3` sites | 1 (strict advancement) | Medium |
| Continuation/anchor owner leaks outside cursor/index | 0 | Low |

## STEP 8 - Legitimate Cross-Cutting (Do NOT Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner grouped-policy checks + executor boundary rechecks | planner owns semantics; executor enforces fail-closed boundary | invalid shapes could execute if planner contract is bypassed |
| Cursor anchor checks + scan envelope checks | token validation and store traversal guard different trust boundaries | envelope escapes on malformed inputs |
| Commit guard + recovery replay protocol | in-process lifecycle vs durable correctness authority split | partial-failure recovery holes |

## STEP 9 - Output Summary

### High-Risk Cross-Cutting Violations

- None observed in this run.

### Medium-Risk Drift Surfaces

- `AccessPath` fan-out across four layers remains a velocity/coordination pressure surface.
- Strict-advancement invariants remain intentionally multi-site and should stay delegation-based.

### Low-Risk / Intentional Redundancy

- Comparator and continuation authority centralization is currently clean (`0` leak findings).
- Error-class mapping remains explicit and stable across sampled boundaries.

### Quantitative Snapshot

- Policy duplications found: **3**
- Comparator leaks: **0**
- Capability fan-out >2 layers: **2**
- Invariants enforced in >3 sites: **1**
- Protective redundancies: **3**
- Cross-Cutting Risk Index (1-10): **5**
