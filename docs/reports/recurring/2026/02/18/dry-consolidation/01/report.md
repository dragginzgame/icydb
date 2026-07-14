# DRY / Redundancy / Consolidation Audit - 2026-02-18

Scope: `icydb-core` structural duplication and consolidation safety, with guardrails preserved.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Relation target raw-key normalization + error mapping | `crates/icydb-core/src/db/relation/mod.rs`, `crates/icydb-core/src/db/executor/save/relations.rs` | `relation/mod.rs:119`, `executor/save/relations.rs:50` | Evolution drift duplication | Yes | Medium | Medium |
| Index-range anchor support guard (non-index path rejection) | `crates/icydb-core/src/db/query/plan/executable.rs`, `crates/icydb-core/src/db/executor/context.rs` | `executable.rs:179`, `executable.rs:250`, `context.rs:298`, `context.rs:312` | Intentional boundary duplication | Yes | Low | Low |
| Bound-encode error -> user-facing reason mapping | `crates/icydb-core/src/db/query/plan/executable.rs`, `crates/icydb-core/src/db/index/store/lookup.rs` | `executable.rs:229`, `lookup.rs:127` | Boilerplate duplication | Yes | Medium | Medium |
| Commit-marker decode wrappers (length + decode + classify corruption) | `crates/icydb-core/src/db/commit/decode.rs`, `crates/icydb-core/src/db/commit/prepare.rs` | `decode.rs:13`, `decode.rs:35`, `decode.rs:57`, `prepare.rs:39` | Defensive duplication | Yes | Low | Low |
| Commit-marker row-op shape check appears in validation and preparation | `crates/icydb-core/src/db/commit/validate.rs`, `crates/icydb-core/src/db/commit/prepare.rs` | `validate.rs:17`, `prepare.rs:88` | Defensive duplication | Yes | Medium | Medium |
| Continuation payload rejection construction fan-out | `crates/icydb-core/src/db/query/plan/executable.rs`, `crates/icydb-core/src/db/query/plan/continuation.rs` | `executable.rs:179-292`, `continuation.rs:385`, `continuation.rs:400` | Accidental duplication | Yes | High | High |
| Boundary arity/type checks at multiple continuation stages | `crates/icydb-core/src/db/query/plan/continuation.rs`, `crates/icydb-core/src/db/query/plan/logical.rs` | `continuation.rs:133`, `continuation.rs:406`, `logical.rs:499` | Evolution drift duplication | Yes | Medium | Medium |
| Commit-marker size-limit enforcement messages | `crates/icydb-core/src/db/commit/store.rs`, `crates/icydb-core/src/db/commit/decode.rs` | `store.rs:39`, `store.rs:56`, `decode.rs:38` | Boilerplate duplication | Yes | Low | Low |
| Access-path branching trees repeated at plan and execution surfaces | `crates/icydb-core/src/db/query/plan/planner.rs`, `crates/icydb-core/src/db/executor/context.rs`, `crates/icydb-core/src/db/query/plan/access_projection.rs` | `planner.rs:448`, `context.rs:211`, `access_projection.rs:1` | Intentional boundary duplication | Yes | Medium | Medium |
| PlanError conversion wrappers from lower layers | `crates/icydb-core/src/db/mod.rs`, `crates/icydb-core/src/db/query/intent/mod.rs`, `crates/icydb-core/src/db/query/plan/planner.rs`, `crates/icydb-core/src/db/query/expr.rs` | `db/mod.rs:435`, `intent/mod.rs:583`, `planner.rs:36`, `expr.rs:64` | Boilerplate duplication | Yes | Low | Low |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Relation target key encode/decode wrappers with similar error mapping | 2 primary sites | relation + executor/save | Yes | Medium | `db::relation` | Medium |
| Cursor anchor admissibility checks (path supports anchor?) | 2 sites | plan + executor | Yes | Medium | keep split by boundary (planner/executor) | Low |
| `IndexRangeBoundEncodeError` message mapping | 2 sites | plan + index/store | Yes | Low | `db::index::range` helper-owned reason mapping | Medium |
| Continuation payload error construction (`InvalidContinuationCursorPayload`) | 18 call sites | continuation decode + executable validation | No (within query stack) | Medium | `db::query::plan::continuation` | High |
| Commit-marker corruption/decode wrappers | 4 sites | commit/decode + prepare + store + validate | Yes | Medium | `db::commit` local helpers | Medium |
| Boundary arity/type enforcement | 3 sites | continuation + logical ordering | No | Medium | `db::query::plan::continuation` for boundary shape | Medium |
| `PlanError` to wrapper-error conversion | 4 sites | query intent + planner + sort + top-level api | Yes | Low | each boundary wrapper (intent/planner/expr/api) | Low |
| AccessPath dispatch skeletons | 3+ sites | planner + projection + executor | Yes | High | no single owner without boundary change | Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/planner.rs` | 783 | 5 (predicate extraction, path candidate generation, ordering, cursor signature inputs, policy transitions) | Under-splitting | High |
| `crates/icydb-core/src/db/query/plan/continuation.rs` | 691 | 5 (token decode, boundary decode, order-field validation, signature checks, payload conversion) | Under-splitting | High |
| `crates/icydb-core/src/db/query/plan/executable.rs` | 601 | 4 (plan shaping, cursor validation, anchor envelope checks, boundary/anchor consistency) | Under-splitting | Medium-High |
| `crates/icydb-core/src/db/commit/{decode,prepare,store,validate}.rs` | 77 + 174 + 257 + 27 | 4 (marker decode, shape validation, storage envelope, row-op preparation) | Over-splitting pressure is low; boundaries are clear | Low |
| `crates/icydb-core/src/db/relation/mod.rs` + `crates/icydb-core/src/db/executor/save/relations.rs` | 237 + 190 | 3 shared concepts across two layers | Mild over-splitting on error mapping only | Medium |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Anchor allowed only for `AccessPath::IndexRange` | `query/plan/executable.rs:179`, `executor/context.rs:291` | Yes | Low | Low |
| Anchor must stay inside original raw-key envelope | `query/plan/executable.rs:242`, `index/range.rs:102`, `index/store/lookup.rs:140` | Yes | Low | Low |
| Continuation must advance strictly past anchor | `index/range.rs:120`, `index/store/lookup.rs:153` | Yes | Low | Low |
| Boundary slot arity must match ordering | `query/plan/continuation.rs:406`, `query/plan/logical.rs:499` | Partially | Medium (two error surfaces) | Medium |
| Commit row-op shape cannot be empty (`before=None && after=None`) | `commit/validate.rs:17`, `commit/prepare.rs:88` | Yes | Medium (different messages/classification path) | Medium |
| Relation target key must decode and target entity must match | `relation/mod.rs:133`, `executor/save/relations.rs:50` | Partially | Medium | Medium |

Classification summary:
- Safety-enhancing (good redundancy): anchor admissibility, anchor envelope, strict advancement, commit row-op non-empty guard.
- Safety-neutral: boundary arity duplication.
- Divergence-prone: continuation payload reason construction and relation error-message mapping.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| `InternalError::new(...)` repeated with local format strings (`115` call sites in `32` non-test db files) | `crates/icydb-core/src/db/**` | High if globally merged (would blur boundary semantics) | Medium |
| `PlanError::InvalidContinuationCursorPayload { reason }` repeated (`18` non-test query call sites) | `query/plan/executable.rs`, `query/plan/continuation.rs` | Medium | High |
| Commit-marker corruption text families (`"commit marker ... corrupted/invalid"`; `82` matches in `db/commit`) | `commit/decode.rs`, `commit/prepare.rs`, `commit/store.rs`, `commit/validate.rs` | Medium | Medium |
| Relation strong-key storage-compat and target-name errors repeated with near-identical payload fields | `relation/mod.rs`, `executor/save/relations.rs` | Medium | Medium |
| `IndexRangeBoundEncodeError` reason mapping duplicated at query and store entry points | `query/plan/executable.rs`, `index/store/lookup.rs` | Low | Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor envelope checks | `query/plan/executable.rs:242`, `index/range.rs:102`, `index/store/lookup.rs:140` | Yes (planner validation + runtime guard) | Low |
| Bound conversions for continuation resume | `index/range.rs:81`, `index/store/lookup.rs:140` | Centralized; duplication reduced | Low |
| Raw-key ordering continuation guard | `index/range.rs:120`, `index/store/lookup.rs:153` | Yes | Low |
| Index entry decode/validation wrappers | `commit/decode.rs:35`, `commit/prepare.rs:111` | Partially | Medium |
| Reverse relation mutation symmetry logic | `relation/mod.rs`, `executor/save/relations.rs`, `commit/prepare.rs` | Mostly intentional boundary duplication | Medium |
| Commit marker phase transitions and shape checks | `commit/store.rs`, `commit/validate.rs`, `commit/prepare.rs` | Defensive | Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Continuation payload reason construction | `query/plan/executable.rs`, `query/plan/continuation.rs` | Accidental duplication | High | `db::query::plan::continuation` |
| Relation target-key error mapping vocabulary | `relation/mod.rs`, `executor/save/relations.rs` | Evolution drift duplication | Medium | `db::relation` |
| Index bound encode error reason mapping | `query/plan/executable.rs`, `index/store/lookup.rs` | Boilerplate duplication | Medium | `db::index::range` |
| Commit-marker corruption message templates | `commit/decode.rs`, `commit/prepare.rs`, `commit/store.rs`, `commit/validate.rs` | Boilerplate duplication | Medium | `db::commit` |
| Boundary shape checks (arity/type) | `query/plan/continuation.rs`, `query/plan/logical.rs` | Evolution drift duplication | Medium | `db::query::plan::continuation` |

## Step 8 - Dangerous Consolidations (Do NOT Merge)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner cursor validation + executor anchor support checks | Keeps malformed client payload rejection at plan boundary and invariant enforcement at execution boundary | Boundary blur; malformed tokens may become runtime-only failures |
| Envelope validation in planner + store advancement guard | Planner blocks invalid envelopes early; store still enforces monotonic progression during traversal | Single-point reliance; higher corruption/tamper blast radius |
| Commit validation + commit preparation checks | Validation guards persisted marker shape; preparation guards replay semantics under actual typed decode | Replay safety regression if one phase bypasses the guard |
| AccessPath dispatch across planner/projection/executor | Mirrors intentional subsystem boundaries (selection vs projection vs execution) | Type coupling and cross-layer dependency growth |

## Step 9 - Quantitative Summary

- Total duplication patterns found: 10 structural patterns (Step 1), 8 pattern-level redundancies (Step 2).
- High-risk divergence duplications: 2.
  - Continuation payload reason fan-out.
  - Concentrated `InternalError::new(...)` free-form message construction in many modules.
- Defensive duplications: 4.
  - Anchor admissibility checks.
  - Anchor envelope and strict advancement checks.
  - Commit row-op shape guards.
- Estimated LoC reduction range (conservative): 120-220 LoC, mostly by local helper extraction within existing owner layers.

## 1. High-Impact Consolidation Opportunities

- Consolidate continuation payload reason constructors to one local helper path inside `db::query::plan::continuation`.
- Consolidate relation strong-key error vocabulary and message formatting in `db::relation`.
- Consolidate commit-marker corruption message templates within `db::commit` helper(s), without collapsing phase boundaries.

## 2. Medium Opportunities

- Consolidate `IndexRangeBoundEncodeError` -> reason mapping into a single helper in `db::index::range`.
- Consolidate boundary arity/type check wording between `query/plan/continuation` and `query/plan/logical`.

## 3. Low / Cosmetic

- Normalize similar string prefixes (`executor invariant violated`, `commit marker ...`) for easier grep-based audits.
- Reduce repeated inline format payload order where fields are identical but wording differs only slightly.

## 4. Dangerous Consolidations (Keep Separate)

- Keep planner and executor invariant checks separate for cursor anchor admissibility.
- Keep planner envelope checks and store traversal progression guards separate.
- Keep commit validation and commit preparation row-op shape checks separate.
- Keep AccessPath branching split by subsystem boundary.

## 5. Estimated LoC Reduction Range

- Conservative range: 120-220 LoC.

## 6. Architectural Risk Summary

- DRY pressure is moderate and mostly localized to error construction and message mapping.
- Safety-critical duplication is mostly intentional and boundary-protective.
- Highest drift risk is not algorithm duplication; it is semantic-message drift in continuation and relation/commit error paths.

## 7. DRY Risk Index (1-10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

- **4/10** (moderate duplication pressure, currently controlled, with clear localized consolidation targets).
