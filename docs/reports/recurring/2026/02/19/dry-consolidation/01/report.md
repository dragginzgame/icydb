# DRY / Redundancy / Consolidation Audit - 2026-02-19

Scope: `icydb-core` structural duplication and consolidation safety on current 0.16 working tree.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Relation target raw-key normalization + error mapping | `crates/icydb-core/src/db/relation/mod.rs`, `crates/icydb-core/src/db/executor/save/relations.rs` | `relation/mod.rs:137`, `executor/save/relations.rs:49` | Evolution drift duplication | Yes | Medium | Medium |
| Index-range bound encode error -> payload reason mapping | `crates/icydb-core/src/db/query/plan/cursor_spine.rs`, `crates/icydb-core/src/db/index/store/lookup.rs` | `cursor_spine.rs:316`, `lookup.rs:103` | Boilerplate duplication | Yes | Medium | Medium |
| Union merge construction + direction propagation staging | `crates/icydb-core/src/db/executor/context.rs`, `crates/icydb-core/src/db/executor/ordered_key_stream.rs` | `context.rs:282`, `context.rs:308`, `ordered_key_stream.rs:103`, `ordered_key_stream.rs:160` | Intentional boundary duplication | Yes | Low | Low |
| Direction handling (ASC/DESC) distributed across path and fast-path producers | `crates/icydb-core/src/db/executor/context.rs`, `crates/icydb-core/src/db/executor/load/pk_stream.rs`, `crates/icydb-core/src/db/executor/load/secondary_index.rs` | `context.rs:440`, `pk_stream.rs:171`, `secondary_index.rs:56` | Evolution drift duplication | Yes | Medium | Medium |
| Continuation payload rejection construction appears in multiple cursor stages | `crates/icydb-core/src/db/query/plan/cursor_spine.rs`, `crates/icydb-core/src/db/query/plan/executable.rs` | `cursor_spine.rs:92`, `executable.rs:193`, `executable.rs:199` | Accidental duplication | Yes | Medium | Medium |
| Composite candidate-set materialization retained for `Intersection` while `Union` streams | `crates/icydb-core/src/db/executor/context.rs` | `context.rs:273`, `context.rs:328`, `context.rs:350` | Intentional boundary duplication | Yes | Low | Low |
| Commit-marker decode wrappers (length + decode + classify corruption) | `crates/icydb-core/src/db/commit/decode.rs`, `crates/icydb-core/src/db/commit/prepare.rs`, `crates/icydb-core/src/db/commit/store.rs` | `decode.rs:17`, `prepare.rs:29`, `store.rs:40` | Defensive duplication | Yes | Low | Low |
| Commit row-op shape check appears in validation and preparation | `crates/icydb-core/src/db/commit/validate.rs`, `crates/icydb-core/src/db/commit/prepare.rs` | `validate.rs:18`, `prepare.rs:88` | Defensive duplication | Yes | Medium | Medium |
| Access-path branch trees repeated at planner, projection, and executor surfaces | `crates/icydb-core/src/db/query/plan/planner.rs`, `crates/icydb-core/src/db/query/plan/access_projection.rs`, `crates/icydb-core/src/db/executor/context.rs` | `planner.rs:109`, `access_projection.rs:56`, `context.rs:263` | Intentional boundary duplication | Yes | Medium | Medium |
| Generic `InternalError::new(...)` formatting fan-out | `crates/icydb-core/src/db/**` | `104` non-test call sites across `33` files | Boilerplate duplication | Yes | Medium | Medium |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Relation target key encode/decode wrappers with similar error mapping | 2 primary sites | relation + executor/save | Yes | Medium | `db::relation` | Medium |
| Index-range bound encode reason mapping | 2 sites | query/plan + index/store | Yes | Low | `db::index::range` helper-owned reason mapping | Medium |
| Union merge direction checks | 2 sites | executor/context + executor/stream | No (executor-local) | Low | `db::executor::ordered_key_stream` | Low |
| Direction derivation + reverse handling for ordered key producers | 3 sites | executor/context + load fast paths | No (executor-local) | Medium | `db::executor::direction` helper module (future) | Medium |
| Continuation payload error construction (`InvalidContinuationCursorPayload`) | 3 non-test construction sites | cursor spine + executable validation | No (query stack) | Medium | `db::query::plan::cursor_spine` | Medium |
| Commit-marker corruption/decode wrappers | 3-4 sites | commit/decode + prepare + store | Yes | Medium | `db::commit` local helpers | Medium |
| AccessPath dispatch skeletons | 3+ sites | planner + projection + executor | Yes | High | no single owner without boundary change | Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/planner.rs` | 789 | 5 (predicate extraction, access candidate generation, OR/AND plan composition, ordering assumptions, normalization) | Under-splitting | High |
| `crates/icydb-core/src/db/executor/load/mod.rs` | 606 | 5 (fast-path gating, shared materialization, cursor paging, trace/metrics routing, pushdown coordination) | Under-splitting | High |
| `crates/icydb-core/src/db/query/plan/executable.rs` | 578 | 4 (plan shaping, cursor validation, boundary/anchor consistency, continuation signature checks) | Under-splitting | Medium-High |
| `crates/icydb-core/src/db/query/plan/continuation.rs` | 564 | 4 (token decode, boundary decode, PK boundary decode, payload mapping) | Under-splitting | Medium |
| `crates/icydb-core/src/db/executor/context.rs` | 446 | 4 (access stream production, composite merge staging, row load, decode bridge) | Medium concentration | Medium |
| `crates/icydb-core/src/db/commit/{decode,prepare,store,validate}.rs` | 78 + 175 + 273 + 27 | 4 (decode, shape guard, persistence envelope, preparation) | Over-splitting pressure is low; boundaries stay clear | Low |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Anchor allowed only for index-range-capable path | `query/plan/cursor_spine.rs:329`, `executor/context.rs` (index-range-only feed path) | Yes | Low | Low |
| Anchor must stay inside original raw-key envelope | `query/plan/cursor_spine.rs:329`, `index/range.rs:101`, `index/store/lookup.rs:119` | Yes | Low | Low |
| Continuation must advance strictly past anchor | `index/range.rs:117`, `index/store/lookup.rs:135`, `index/store/lookup.rs:161` | Yes | Low | Low |
| Union child streams must match explicit direction | `executor/context.rs:308`, `executor/ordered_key_stream.rs:160` | Yes | Low | Low |
| Commit row-op shape cannot be empty (`before=None && after=None`) | `commit/validate.rs:18`, `commit/prepare.rs:88` | Yes | Medium | Medium |
| Relation target key decode/entity-name consistency | `relation/mod.rs:157`, `relation/mod.rs:176`, `executor/save/relations.rs:49` | Partially | Medium | Medium |

Classification summary:
- Safety-enhancing: anchor admissibility/envelope/advance checks, merge direction checks, commit row-op shape guards.
- Safety-neutral: direction handling split across key producers.
- Divergence-prone: relation error vocabulary and distributed `InternalError::new(...)` payload text.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| `InternalError::new(...)` repeated with local format strings (`104` non-test call sites in `33` files) | `crates/icydb-core/src/db/**` | High if globally merged (would blur boundary semantics) | Medium |
| `InvalidContinuationCursorPayload` reason strings built in multiple stages | `query/plan/cursor_spine.rs`, `query/plan/executable.rs` | Medium | Medium |
| Index-range bound encode reason mapping duplicated | `query/plan/cursor_spine.rs`, `index/store/lookup.rs` | Low | Medium |
| Relation strong-key storage compatibility / target-name error wording | `relation/mod.rs`, `executor/save/relations.rs` | Medium | Medium |
| Merge direction mismatch invariant payload wording | `executor/ordered_key_stream.rs` (single owner) | Low | Low |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor envelope checks | `query/plan/cursor_spine.rs:329`, `index/range.rs:101`, `index/store/lookup.rs:119` | Yes (planner + runtime defense) | Low |
| Bound conversions for continuation resume | `query/plan/cursor_spine.rs:100`, `index/range.rs:83`, `index/store/lookup.rs:119` | Mostly centralized wrappers | Low |
| Raw-key ordering continuation guard | `index/range.rs:117`, `index/store/lookup.rs:135`, `index/store/lookup.rs:161` | Yes | Low |
| Index-range bound encode reason mapping | `query/plan/cursor_spine.rs:316`, `index/store/lookup.rs:103` | Partially | Medium |
| Direction handling in key producers | `executor/context.rs:440`, `executor/load/pk_stream.rs:171`, `executor/load/secondary_index.rs:56` | Partially | Medium |
| Commit marker phase transitions and shape checks | `commit/store.rs`, `commit/validate.rs`, `commit/prepare.rs` | Defensive | Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Direction handling policy for key producers | `executor/context.rs`, `executor/load/pk_stream.rs`, `executor/load/secondary_index.rs` | Evolution drift duplication | Medium | `db::executor` shared helper |
| Index-range bound encode reason mapping | `query/plan/cursor_spine.rs`, `index/store/lookup.rs` | Boilerplate duplication | Medium | `db::index::range` |
| Relation target-key error vocabulary | `relation/mod.rs`, `executor/save/relations.rs` | Evolution drift duplication | Medium | `db::relation` |
| Continuation payload reason construction | `query/plan/cursor_spine.rs`, `query/plan/executable.rs` | Accidental duplication | Medium | `db::query::plan::cursor_spine` |
| Commit-marker corruption message templates | `commit/decode.rs`, `commit/prepare.rs`, `commit/store.rs` | Boilerplate duplication | Medium | `db::commit` |

## Step 8 - Dangerous Consolidations (Do NOT Merge)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner cursor validation + store traversal progression guard | Maintains early user-input rejection plus runtime corruption defense | Boundary blur; invalid cursor states could escape to deeper layers |
| Union merge construction + stream-level order enforcement | Context owns merge composition; stream owns monotonic validation | Losing either layer removes defense-in-depth on ordering invariants |
| Commit validation + commit preparation row-op checks | Validation protects marker shape; preparation protects replay behavior under typed decode | Replay-safety regressions if one phase bypasses guard |
| AccessPath dispatch split across planner/projection/executor | Preserves subsystem boundaries (selection vs projection vs execution) | Cross-layer coupling and API leakage |

## Step 9 - Quantitative Summary

- Total duplication patterns found: 10 structural patterns (Step 1), 7 pattern-level redundancies (Step 2).
- High-risk divergence duplications: 0.
- Medium-risk divergence duplications: 5.
  - Direction handling fan-out across producers.
  - Index-range bound encode reason mapping.
  - Relation error vocabulary drift.
  - Continuation payload reason construction.
  - Generic `InternalError::new(...)` fan-out.
- Defensive duplications: 4.
  - Anchor admissibility/envelope/advance checks.
  - Merge direction invariant checks.
  - Commit row-op shape guards.
- Estimated LoC reduction range (conservative): 90-180 LoC, mostly via local helper extraction inside existing owner layers.

## 1. High-Impact Consolidation Opportunities

- Consolidate direction handling policy for ordered key producers under one executor-local helper to prevent DESC drift.
- Consolidate index-range bound encode reason mapping to one helper used by planner cursor validation and store lookup.

## 2. Medium Opportunities

- Consolidate relation target-key error vocabulary in `db::relation` to keep source/target mismatch payloads consistent.
- Consolidate continuation payload reason constructors in cursor-spine owner layer.

## 3. Low / Cosmetic

- Normalize repeated invariant-message prefixes for grep-based audits.
- Reduce repeated inline format payload order where same fields are listed with different wording.

## 4. Dangerous Consolidations (Keep Separate)

- Keep planner and runtime continuation checks separate.
- Keep merge composition in context and merge monotonic enforcement in stream type.
- Keep commit validation and preparation guards separate.

## 5. Estimated LoC Reduction Range

- Conservative range: 90-180 LoC.

## 6. Architectural Risk Summary

- DRY pressure remains moderate and concentrated in message mapping and direction-handling fan-out.
- Safety-critical duplication remains mostly intentional and boundary-protective.
- 0.16 union-stream work improved consolidation in composite execution, but introduced a new medium drift surface in distributed direction handling.

## 7. DRY Risk Index (1-10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

- **4/10** (moderate duplication pressure; no high-risk DRY break, with localized medium-risk drift surfaces).
