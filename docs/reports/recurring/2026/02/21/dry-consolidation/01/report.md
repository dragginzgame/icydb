# DRY / Redundancy / Consolidation Audit - 2026-02-21

Scope: duplication risk and divergence pressure only. No layer-collapsing recommendations.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Fast-path spec-arity guards split between load and aggregate entry points | `crates/icydb-core/src/db/executor/load/execute.rs`, `crates/icydb-core/src/db/executor/load/aggregate.rs` | `execute.rs:57`, `aggregate.rs:64`, `aggregate.rs:79` | Evolution drift duplication | Yes | Medium | Medium |
| Index-spec alignment checks repeated at composite planning and physical access path edges | `crates/icydb-core/src/db/executor/composite_stream.rs`, `crates/icydb-core/src/db/executor/physical_path.rs`, `crates/icydb-core/src/db/executor/load/secondary_index.rs`, `crates/icydb-core/src/db/executor/load/index_range_limit.rs` | `composite_stream.rs:13`, `physical_path.rs:156`, `secondary_index.rs:42`, `index_range_limit.rs:36` | Defensive duplication | Yes | Low-Medium | Medium |
| Aggregate fast-path routing branch tree partially mirrors load fast-path routing | `crates/icydb-core/src/db/executor/load/execute.rs`, `crates/icydb-core/src/db/executor/load/aggregate.rs` | `execute.rs:116`, `aggregate.rs:358` | Evolution drift duplication | Yes | Medium-High | Medium-High |
| Index-range bound encode reason strings duplicated across plan/cursor boundaries | `crates/icydb-core/src/db/query/plan/executable.rs`, `crates/icydb-core/src/db/query/cursor/anchor.rs` | `executable.rs:24`, `executable.rs:457`, `anchor.rs:76` | Boilerplate duplication | Yes | Medium | Medium |
| Invalid component-range inputs collapse to empty inclusive bounds at multiple exits | `crates/icydb-core/src/db/index/key/build.rs` | `build.rs:235`, `build.rs:246`, `build.rs:255`, `build.rs:263` | Defensive duplication | Yes | Low | Low |
| Invariant construction fan-out via `query_executor_invariant(...)` | non-test db modules | 43 call sites | Boilerplate duplication | Yes | Medium | Medium |
| Access-stream input bundles overlap (`AccessStreamInputs`, `AccessPlanStreamRequest`, `ExecutionInputs`) | `crates/icydb-core/src/db/executor/context.rs`, `crates/icydb-core/src/db/executor/load/execute.rs` | `context.rs:34`, `context.rs:121`, `execute.rs:24` | Boilerplate duplication | No | Low-Medium | Low-Medium |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Fast-path arity enforcement (secondary/index-range) | 3 helpers across load/aggregate | executor/load + executor/aggregate | No (executor-local) | Low | `db::executor::load` shared helper boundary | Medium |
| Fast-path routing precedence (PK/secondary/index-range plus aggregate-only composite) | 2 routing trees | executor/load + executor/aggregate | No (executor-local) | Medium | shared route policy in `db::executor::load` | Medium-High |
| Index-spec alignment checks at stream production and physical path execution | 4 non-test modules | executor composite + physical path + fast paths | No (executor-local) | Medium | keep dual owner (`composite_stream` + `physical_path`) | Medium |
| Index-range bound encode reason mapping | 2 call paths | query/plan + query/cursor + index helper | Yes | Low | `db::index::range` (already owner for mapping) | Low-Medium |
| Input bundle shaping (`ctx/specs/direction/hint`) | 3 structs | executor/context + executor/load | No | Medium | `db::executor` shared types (if expanded further) | Low-Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/load/aggregate.rs` | 1047 lines | fast-path routing + fold engine + window semantics + pushdown gating + parity guards | Under-split pressure | High |
| `crates/icydb-core/src/db/query/plan/executable.rs` | 731 lines | executable plan shaping + cursor signature checks + index spec lowering + boundary mapping | Under-split pressure | High |
| `crates/icydb-core/src/db/query/plan/logical/mod.rs` | 693 lines | logical phases + ordering/window behavior + cursor compatibility shaping | Under-split pressure | Medium-High |
| `crates/icydb-core/src/db/executor/context.rs` | 664 lines | stream production + row reads + spec consumption orchestration + invariants/tests | Under-split pressure | Medium-High |
| `crates/icydb-core/src/db/query/plan/planner/mod.rs` | 446 lines | plan construction + predicate flow + range shaping | Balanced | Medium |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Consumed index spec must match access-path index id | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/secondary_index.rs`, `executor/load/index_range_limit.rs` | Yes | Low | Low-Medium |
| All lowered specs must be consumed exactly once | `executor/context.rs` (`AccessSpecCursor::validate_consumed`) + stream traversal consumers | Yes | Medium (order drift risk) | Medium |
| Continuation must stay monotonic relative to anchor | `index/range.rs`, `index/store/lookup.rs`, `query/cursor/anchor.rs` | Yes | Low | Low |
| Aggregate COUNT key-only mode must exclude composite paths | `executor/load/aggregate.rs` (`is_composite_access_shape`, fold mode selection) + aggregate tests | Yes | Medium | Medium |

Classification summary:
- Safety-enhancing: index-spec alignment checks, continuation monotonic checks, spec-consumption final validation.
- Safety-neutral: repeated input-bundle shaping structs.
- Divergence-prone: dual fast-path routing trees and split arity guard helpers.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| `InternalError::query_executor_invariant(...)` fan-out (43 non-test call sites) | `crates/icydb-core/src/db/**` | Low (constructor already centralized) | Medium |
| Index-range "not indexable" reason strings | `query/plan/executable.rs`, `query/cursor/anchor.rs` | Low | Medium |
| Index-range spec mismatch invariant payloads repeated | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/secondary_index.rs`, `executor/load/index_range_limit.rs` | Medium | Medium |
| Index corruption decode-context strings (`"... during {context}"`) | `index/store/lookup.rs` and adjacent decode wrappers | Medium | Low-Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor envelope checks | `query/cursor/anchor.rs`, `index/range.rs` | Yes | Low |
| Bound conversions + continuation rewrite | `index/range.rs`, `index/store/lookup.rs` | Yes | Low |
| Raw key monotonic continuation checks | `index/range.rs`, `index/store/lookup.rs` | Yes | Low |
| Index spec/index-id alignment checks before index traversal | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/index_range_limit.rs`, `executor/load/secondary_index.rs` | Yes | Medium |
| Empty-range fallback shaping in index key builders | `index/key/build.rs` | Yes (defensive) | Low |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Fast-path arity guard helpers | `executor/load/execute.rs`, `executor/load/aggregate.rs` | Evolution drift duplication | Medium | `db::executor::load` shared guard helper |
| Fast-path routing precedence declarations | `executor/load/execute.rs`, `executor/load/aggregate.rs` | Evolution drift duplication | Medium-High | `db::executor::load` routing policy utilities |
| Index spec mismatch message templates | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/*` | Boilerplate duplication | Medium | `InternalError` constructor helpers |
| Index-range reason-string constants | `query/plan/executable.rs`, `query/cursor/anchor.rs` | Boilerplate duplication | Medium | `db::index::range`-adjacent constants (without boundary merge) |
| Input bundle struct overlap | `executor/context.rs`, `executor/load/execute.rs` | Boilerplate duplication | Low-Medium | executor-local shared input model |

## Step 8 - Dangerous Consolidations (Keep Separate)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Composite stream alignment checks + physical-path alignment checks | One protects spec traversal ordering; the other protects final index-store dispatch correctness | A single missed check could route to wrong index without immediate crash |
| Planner/cursor anchor envelope validation + store continuation advancement checks | Planner rejects malformed continuation payloads early; store still enforces monotonic progression at runtime | Layer-blur and weaker runtime defense for hostile/corrupted payloads |
| Aggregate direct paths + canonical fallback stream path | Direct paths provide bounded optimizations; fallback remains the parity baseline | Over-consolidation can remove the safe fallback boundary and increase regression blast radius |

## Step 9 - Quantitative Summary

- Total duplication patterns found: **12** (audit-classified).
- High-risk divergence duplications: **1** (fast-path routing drift between load and aggregate trees).
- Defensive/intentional duplications: **8**.
- Conservative estimated LoC reduction range without boundary erosion: **80-150 LoC**.

## Output Summary

1. High-Impact Consolidation Opportunities
- Unify executor-local fast-path arity guard helpers.
- Reduce route-precedence drift between `resolve_execution_key_stream` and aggregate fast-path routing.

2. Medium Opportunities
- Normalize index-spec mismatch invariant message construction.
- Consolidate index-range reason-string constants used by plan/cursor boundaries.

3. Low / Cosmetic
- Reduce overlap among input bundle structs where fields are identical.

4. Dangerous Consolidations (Keep Separate)
- Keep composite-stream alignment checks separate from physical-path alignment checks.
- Keep planner cursor-envelope validation separate from runtime continuation advancement enforcement.
- Keep aggregate direct-path logic separate from canonical fallback execution path.

5. Estimated LoC Reduction Range
- **80-150 LoC** (conservative, boundary-safe only).

6. Architectural Risk Summary
- Current duplication remains mostly intentional and safety-oriented.
- Primary drift pressure moved to executor fast-path routing growth, especially with the new aggregate branches.
- Structural health is still stable, but `aggregate.rs` has crossed a size threshold where incremental extraction is warranted.

7. DRY Risk Index (1-10, lower is better): **5/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
