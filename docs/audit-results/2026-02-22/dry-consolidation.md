# DRY / Redundancy / Consolidation Audit - 2026-02-22

Scope: duplication risk and divergence pressure only. No layer-collapsing recommendations.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Fast-path spec-arity checks still split between load evaluator and aggregate fast-path guards | `crates/icydb-core/src/db/executor/load/execute.rs`, `crates/icydb-core/src/db/executor/load/aggregate_guard.rs`, `crates/icydb-core/src/db/executor/load/aggregate.rs` | `execute.rs:62`, `aggregate_guard.rs:56`, `aggregate.rs:463` | Evolution drift duplication | Yes | Medium | Medium |
| Index/spec identity alignment checks repeated across composite dispatch and concrete fast paths | `crates/icydb-core/src/db/executor/composite_stream.rs`, `crates/icydb-core/src/db/executor/physical_path.rs`, `crates/icydb-core/src/db/executor/load/secondary_index.rs`, `crates/icydb-core/src/db/executor/load/index_range_limit.rs` | `composite_stream.rs:14`, `physical_path.rs:157`, `secondary_index.rs:35`, `index_range_limit.rs:32` | Defensive duplication | Yes | Low-Medium | Medium |
| Route safety checks duplicated defensively at page materialization boundary | `crates/icydb-core/src/db/executor/load/route.rs`, `crates/icydb-core/src/db/executor/load/page.rs` | `route.rs:286`, `page.rs:30` | Defensive duplication | Yes | Low | Low |
| Aggregate executor re-validates executor plan before and after `ExecutablePlan` consumption | `crates/icydb-core/src/db/executor/load/aggregate.rs` | `aggregate.rs:144`, `aggregate.rs:168` | Defensive duplication | Yes | Low | Low |
| Invariant constructor fan-out (`query_executor_invariant`) remains broad | non-test db modules | 45 call sites | Boilerplate duplication | Yes | Medium | Medium |
| Access-stream request payloads overlap (`ExecutionInputs`, `AccessPlanStreamRequest`, `AccessStreamInputs`) | `crates/icydb-core/src/db/executor/load/execute.rs`, `crates/icydb-core/src/db/executor/context.rs` | `execute.rs:24`, `context.rs:34`, `context.rs:129` | Boilerplate duplication | No | Low-Medium | Low-Medium |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Fast-path arity enforcement (prefix/range specs) | 3 helper paths | executor/load + executor/aggregate | No (executor-local) | Low | `db::executor::load::aggregate_guard` | Medium |
| Fast-path routing trees (load and aggregate) with shared precedence but separate dispatch logic | 2 route trees | executor/load + executor/aggregate | No (executor-local) | Medium | `db::executor::load::route` policy boundary | Medium |
| Index-spec/index-id alignment checks | 4 non-test modules | executor composite + physical path + fast paths | No (executor-local) | Medium | keep dual owner (`composite_stream` + concrete path executors) | Medium |
| Hint derivation channels (count pushdown vs aggregate probes) now explicitly separated but still composed into one physical hint | 1 composition boundary | executor/load/route | No | Low | `db::executor::load::route` | Low |
| Input-bundle shaping for stream production | 3 structs | executor/context + executor/load | No | Medium | `db::executor` shared request model (if further expanded) | Low-Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/executor/load/route.rs` | 975 lines | capability derivation + eligibility gating + hint planning + route tests | Under-split pressure | High |
| `crates/icydb-core/src/db/query/plan/executable.rs` | 726 lines | executable shaping + cursor compatibility + lowered spec derivation | Under-split pressure | High |
| `crates/icydb-core/src/db/executor/load/aggregate.rs` | 723 lines | aggregate terminals + fast-path routing + fold orchestration + metrics | Under-split pressure | Medium-High |
| `crates/icydb-core/src/db/executor/context.rs` | 704 lines | store reads + stream production + spec consumption + row materialization | Under-split pressure | Medium-High |
| `crates/icydb-core/src/db/query/plan/logical/mod.rs` | 693 lines | logical phases + cursor/order interaction + page/window semantics | Under-split pressure | Medium-High |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Spec/index identity must match before index traversal | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/secondary_index.rs`, `executor/load/index_range_limit.rs` | Yes | Low | Low-Medium |
| Streamed specs must be consumed exactly once | `executor/context.rs` (`AccessSpecCursor::validate_consumed`) + stream producers | Yes | Medium (ordering drift) | Medium |
| Continuation progression must be strictly monotonic | `db/index/range.rs`, `db/index/store/lookup.rs`, `db/query/cursor/anchor.rs` | Yes | Low | Low |
| COUNT key-only fold eligibility restricted to safe access shapes | `executor/load/route.rs`, aggregate fold-mode consumers | Yes | Medium | Medium |

Classification summary:
- Safety-enhancing: spec/index alignment checks, continuation monotonic checks, spec-consumption final validation.
- Safety-neutral: overlapping executor input payload structs.
- Divergence-prone: split arity enforcement call graph and dual load/aggregate fast-path dispatch trees.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| `InternalError::query_executor_invariant(...)` fan-out | `crates/icydb-core/src/db/**` | Low (constructor centralized) | Medium |
| Index spec mismatch invariant messages | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/*` | Medium | Medium |
| Cursor/continuation invalid payload wording | `query/plan/executable.rs`, `query/cursor/anchor.rs`, `query/plan/validate/mod.rs` | Medium | Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor envelope checks | `query/cursor/anchor.rs`, `index/range.rs` | Yes | Low |
| Bound conversion + continuation rewriting | `index/range.rs`, `index/store/lookup.rs` | Yes | Low |
| Raw key ordering/advance checks | `index/store/lookup.rs`, `query/cursor/anchor.rs` | Yes | Low |
| Index traversal guardrails (spec alignment + arity constraints) | `executor/composite_stream.rs`, `executor/load/aggregate_guard.rs`, `executor/load/execute.rs`, `executor/physical_path.rs` | Yes | Medium |
| Index range fetch bound enforcement | `executor/load/route.rs`, `executor/load/index_range_limit.rs`, `executor/load/secondary_index.rs` | Yes | Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Fast-path arity enforcement wrappers | `executor/load/execute.rs`, `executor/load/aggregate_guard.rs` | Evolution drift duplication | Medium | `db::executor::load::aggregate_guard` |
| Invariant message template alignment for index/spec mismatch | `executor/composite_stream.rs`, `executor/physical_path.rs`, `executor/load/*` | Boilerplate duplication | Medium | `InternalError` constructor surface |
| Access-stream payload overlap | `executor/context.rs`, `executor/load/execute.rs` | Boilerplate duplication | Low-Medium | executor-local shared request payload |
| Route/page scan-budget defensive checks | `executor/load/route.rs`, `executor/load/page.rs` | Defensive duplication | Low | keep split; unify message constants only |

## Step 8 - Dangerous Consolidations (Keep Separate)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner/cursor envelope validation and store-level continuation advancement | Defense-in-depth across decode-time and runtime advancement checks | Malformed cursor/state could bypass one check class |
| Composite stream spec alignment checks and physical-path alignment checks | One guards spec traversal shape, the other guards concrete store dispatch | Incorrect index dispatch could become silent drift |
| Route-level eligibility gates and page-level bounded-scan invariants | Route decides; page reasserts safety at execution boundary | Removing runtime assertion weakens invariant defense under future drift |

## Step 9 - Quantitative Summary

- Total duplication patterns found: **11** (audit-classified).
- High-risk divergence duplications: **0**.
- Defensive/intentional duplications: **8**.
- Conservative estimated LoC reduction range without boundary erosion: **60-120 LoC**.

## Output Summary

1. High-Impact Consolidation Opportunities
- Consolidate executor-local fast-path arity wrapper usage into one guard surface (`aggregate_guard`) while keeping boundary checks intact.
- Normalize repeated index/spec mismatch invariant message construction.

2. Medium Opportunities
- Reduce overlap among stream-request payload structs where field sets are identical.

3. Low / Cosmetic
- Normalize defensive route/page scan-budget invariant messages.

4. Dangerous Consolidations (Keep Separate)
- Planner/store continuation checks, composite vs physical-path alignment checks, and route/page safety duplication should remain separated.

5. Estimated LoC Reduction Range
- **60-120 LoC** (conservative, boundary-safe only).

6. Architectural Risk Summary
- DRY pressure is moderate and improving after recent routing and aggregate cleanup.
- Remaining duplication is primarily defensive and boundary-protective, not algorithmically divergent.
- Main drift vector is executor-local helper split (arity wrappers and payload shaping), not cross-layer coupling.

7. DRY Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
