# Layer Violation Audit - 2026-06-06

## Report Preamble

- scope: authority layering and semantic ownership boundaries across `crates/icydb-core/src/db/`
- compared baseline report path: `docs/audits/reports/2026-05/2026-05-04/layer-violation.md`
- code snapshot identifier: `c373182f3` with dirty working tree at scan time
- method tag/version: `Method V3`
- comparability status: `comparable`, snapshot-qualified because `Cargo.lock` was already modified before this audit run

## Working Tree Scope

| Path | Audit Treatment | Layer Impact |
| ---- | ---- | ---- |
| `Cargo.lock` | treated as existing user work | dependency lockfile churn only; no inspected layer-authority impact |
| `docs/audits/reports/2026-06/2026-06-06/*` | audit output from this run | documentation only |

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations, `0` cross-layer predicate duplication`) | Low |
| Route planner import boundary | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS (`1` root import family: `executor`) | Low |
| Access and route authority fan-out | layer-health snapshot (`AccessPath decision owners: 1`, `RouteShape decision owners: 2`) | PASS; `AccessPath` decision ownership improved from the 2026-05-04 report's `2` to `1` | Low |
| Predicate coercion ownership concentration | layer-health snapshot (`Predicate coercion owners: 4`, `Predicate boundary drift imports: 3`) | PASS; unchanged from the prior comparable report | Medium |
| Enum fan-out beyond two layers | layer-health snapshot (`Enum fan-out > 2 layers: 1`; `AggregateKind::=4`) | PASS; `AggregateKind` remains at the watched four-layer baseline | Medium |
| Ordering / comparator leakage outside index | layer-health snapshot (`Comparator definitions outside index: 0`) plus targeted `.cmp(`/`sort_by` scan | PASS; non-index comparator hits are local schema/data identity ordering, explain determinism, predicate semantics, result ordering, or tests rather than index-key ordering authority | Low |
| Continuation envelope ownership | invariant script plus targeted scan for `anchor_within_envelope`, `resume_bounds_from_refs`, `continuation_advanced`, strict advancement, and cursor signatures | PASS; strict advancement and resume-bound rewriting remain owned by `db/index/envelope`, with cursor/query/executor call sites delegating to cursor/index contracts | Low |
| Commit marker lifecycle ownership | targeted scan for `with_commit_store`, `CommitMarker`, and marker lifecycle terms | PASS; commit store access remains inside `db/commit`, with executor mutation code building payloads and opening commit windows through commit-owned APIs | Low |
| Runtime compile with current boundary wiring | `cargo check -p icydb-core --features sql` | PASS | Low |

## Policy Re-Derivation

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |
| Cursor paging requirements | `db/query/plan/validate/cursor_policy.rs`, `db/cursor/*`, `db/executor/planning/continuation/*` | query/plan for legality; cursor for token validation | executor consumes planned continuation contracts | Low: executor delegates to planned/cursor contracts instead of reconstructing cursor signatures | Low |
| Grouped HAVING semantics | `db/query/plan/expr/*`, `db/query/plan/validate/*`, `db/sql/lowering/*` | query/plan | SQL lowering parses and maps syntax into planner-owned aggregate semantics | Low: no separate executor-side HAVING policy found | Low |
| Aggregate route feasibility | `db/executor/planning/route/planner/*`, `db/executor/aggregate/*`, `db/query/plan/*`, `db/sql/lowering/*` | executor/route for capability; query/plan for semantic shape | SQL/parser and planner surface `AggregateKind` variants | Medium: variant fan-out is expected, but each layer still owns a distinct concern | Medium |

## Ordering Authority Leakage

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |
| Index key ordering | `db/index/key/*`, `db/index/envelope/*` | index | Owner implementation | Low |
| Access-plan canonical ordering | `db/access/canonical.rs` | access | Legitimate access canonicalization, not index-key ordering | Low |
| Predicate/value comparison | `db/predicate/*`, `db/numeric/*` | predicate/numeric | Domain-local comparison semantics | Low |
| Result-row ordering and aggregate ranking | `db/executor/order.rs`, `db/executor/terminal/ranking/*`, `db/executor/aggregate/*` | executor | Runtime result ordering, not index-key ordering | Low |

## Continuation Authority Leakage

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Excluded-anchor resume bounds | `db/index/envelope/mod.rs` | index | No | Low |
| Strict directional advancement | `db/index/envelope/mod.rs`, consumed by `db/cursor/continuation.rs` and executor continuation planning | index | Defensive delegation | Low |
| Cursor signature validation | `db/cursor/spine.rs`, `db/query/plan/continuation.rs` | cursor/query plan | Protective split between immutable plan contract and token validation | Low |

## Access Capability Fan-Out

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | ---: | --- | --- |
| `AccessPath` decisions | 1 owner by invariant script | access | Low |
| `RouteShape` decisions | 2 owners by invariant script | executor/route, explain/descriptor projection | Low |
| `AggregateKind` | 4 layer families by invariant script | SQL parser/lowering, query/plan, executor/route, executor/aggregate runtime | Medium |
| `ContinuationMode` | 1 owner by invariant script | executor continuation planning | Low |

## Invariant Enforcement Spread

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |
| Envelope containment | `db/index/envelope/mod.rs` | index | Owner-only | Low |
| Strict advancement | `db/index/envelope/mod.rs`, cursor/executor route assertions | index | Safety-enhancing redundancy | Low |
| Cursor signature compatibility | `db/cursor/spine.rs`, `db/query/plan/continuation.rs`, prepared-plan revalidation wrappers | cursor/query plan | Safety-enhancing redundancy | Low |
| Commit marker lifecycle | `db/commit/*`, executor mutation commit-window APIs | commit | Safety-enhancing boundary checks | Low |
| Reverse relation validation | `db/relation/*` | relation | Owner-local | Low |

## Error Classification Cross-Layer Drift

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |
| Cursor signature mismatch | `db/cursor/error.rs`, `db/cursor/spine.rs`, `db/query/plan/continuation.rs` | No inspected drift; query plan delegates to cursor error construction | Low |
| Commit marker corruption/lifecycle | `db/commit/*`, `db/executor/mutation/commit_window.rs` | No inspected drift; commit-owned errors remain the lifecycle authority | Low |
| Predicate/runtime comparison failures | `db/predicate/*`, `db/executor/mutation/save_validation.rs` | No layer violation found; executor uses predicate/numeric helpers for semantic comparison | Low |

## Semantic Fan-Out Metric

| Surface | Count | Risk Level |
| --- | ---: | --- |
| Enums matched in `>=3` layer families | 1 (`AggregateKind`) | Medium |
| Policy predicates implemented in `>=3` modules | 0 from invariant script | Low |
| Invariants enforced in `>3` owner sites | 0 blocking findings | Low |
| Continuation/anchor owner references outside cursor/index | Several delegating call sites, no duplicate resume-bound authority | Low |

## Legitimate Cross-Cutting (Do Not Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner continuation contracts and cursor/runtime transport | planner owns immutable query shape while cursor/runtime validate and transport opaque state | High |
| Route capability derivation and explain/metrics projections | route owns executable capability truth while explain and metrics project already-decided contracts | High |
| Commit marker store, marker payload shape, and executor commit windows | commit owns marker lifecycle and persistence; executor owns row mutation preparation and delegates commit-window opening | High |
| Predicate coercion and runtime/index consumers | predicate/query planning owns semantic coercion; runtime/index consumers apply compiled contracts | Medium |
| Aggregate semantic shape and aggregate runtime execution | SQL/query layers define semantic intent; executor route/runtime own executable strategy and fold state | Medium |

## Follow-Up Actions

- No mandatory layer-violation follow-up actions for this run.
- Monitoring-only: keep `AggregateKind` fan-out at or below the current four-layer baseline, especially after aggregate SQL, fluent builder, or executor-route changes.
- Monitoring-only: keep predicate coercion ownership at the current four-owner set.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core --features sql` -> PASS

## Verdict

No strict layer violations were detected. The tracked layer-authority scripts remain clean, route-planner imports stay under the configured ceiling, and access-path decision ownership improved relative to the prior comparable run. The remaining medium-risk watch item is still `AggregateKind` fan-out across SQL, query planning, route planning, and aggregate runtime code; current inspected sites preserve distinct owner responsibilities rather than duplicating one layer's policy.

Cross-cutting risk index: **3.0/10**
