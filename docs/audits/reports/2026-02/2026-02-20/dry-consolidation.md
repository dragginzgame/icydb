# DRY / Redundancy / Consolidation Audit - 2026-02-20

Scope: duplication risk and divergence pressure only. No layer-collapsing recommendations.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor boundary/type/arity validation split across decode + structured validation | `crates/icydb-core/src/db/query/plan/cursor_spine.rs`, `crates/icydb-core/src/db/query/plan/executable.rs` | ~80 | Defensive duplication | Yes | Medium | Medium |
| Repeated invariant-string construction (`executor invariant violated`) | 8 non-test db files | 25 occurrences | Evolution drift duplication | Yes | Medium | Medium |
| Bound conversion and envelope guards in index + planner edges | `crates/icydb-core/src/db/index/range.rs`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs`, `crates/icydb-core/src/db/query/plan/logical.rs` | ~120 | Intentional boundary duplication | Yes | Medium | Medium |
| Commit-window open/apply mechanics in save/delete | now centralized in `crates/icydb-core/src/db/executor/mutation.rs` | n/a | Safety-enhancing consolidation already done | Yes | Low | Low |
| Error constructor prefixing for index-plan corruption | `crates/icydb-core/src/error.rs`, call sites across index modules | ~20 call sites | Defensive duplication reduced | Yes | Low | Low |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor token compatibility checks (signature/direction/offset) | 2 call paths (plan + revalidate) | planner/executor boundary | Yes | Medium | query/plan cursor spine | Medium |
| Plan validation mapping into internal invariant errors | 2 primary constructors (`from_cursor_plan_error`, `from_executor_plan_error`) | query + error | Yes | Low | `InternalError` owning type | Low |
| Bound semantics helpers | 3 major helper families | index + plan + executor | Yes | Medium | `KeyEnvelope` + `index/range` | Medium |
| Commit apply orchestration | 1 shared helper path | save + delete + commit | Yes | Low | executor/mutation | Low |
| Fast-path finalization in load | unified path already in `finalize_execution` | executor/load | No | Low | executor/load | Low |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/planner.rs` | 789 lines | predicate normalization + path selection + range shaping | Under-split pressure | High |
| `crates/icydb-core/src/db/query/plan/logical.rs` | 778 lines | post-access phases + pagination math + ordering helpers | Under-split pressure | High |
| `crates/icydb-core/src/db/query/plan/cursor_spine.rs` | 508 lines | token validation + envelope logic + boundary typing | Under-split pressure | Medium-High |
| `crates/icydb-core/src/db/executor/load/*` | multiple smaller modules | routing/materialization/trace split | Balanced | Medium-Low |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Cursor requires ordering | `query/policy.rs`, `query/plan/executable.rs` | Yes | Low | Low |
| Continuation strict-after semantics | `query/plan/cursor_spine.rs`, `index/range.rs`, `index/store/lookup.rs` | Yes | Medium (if one side drifts) | Medium |
| Commit recovery before mutation | `executor/save/mod.rs`, `executor/delete/mod.rs`, `commit/recovery.rs` | Yes | Low | Low |
| Row-key identity validation before use | `executor/context.rs`, `executor/save/mod.rs`, recovery prepare path | Yes | Medium | Medium |

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| `query_invariant(...)` construction | many db modules | Low (constructor already centralized) | Medium (message drift) |
| index corruption prefixes | index/plan + store decode paths | Low | Low |
| cursor payload invalidation messages | cursor spine + executable mappings | Medium | Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor envelope checks | `index/range.rs`, `cursor_spine.rs` | Yes (boundary defense) | Medium |
| Bound conversions | `index/range.rs`, `query/plan/logical.rs` | Yes | Medium |
| Raw ordering comparisons | `index/store/lookup.rs`, `cursor_spine.rs`, `logical.rs` | Yes | Medium |
| Index entry construction/mutation apply | commit prepare/apply/recovery | Yes | Medium |
| Commit marker phase transitions | commit guard + recovery | Yes | Low-Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Invariant message phrase consistency | query/plan + executor + commit modules | Evolution drift duplication | Medium | internal error constructors |
| Cursor payload compatibility checks documentation alignment | cursor spine + session docs + continuation token docs | Boilerplate duplication | Low | query/plan cursor docs |
| Planner/logical boundary helper calls for bounds | planner + logical | Intentional boundary duplication | Medium | keep split, shared helper-only |

## Step 8 - Dangerous Consolidations (Keep Separate)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner vs executor validation | catches boundary misuse and internal invariant violations separately | semantic-owner blur and weaker guardrails |
| Store-level continuation advancement + planner-level envelope checks | defense-in-depth against malformed token/state drift | single-check bypass if one layer regresses |
| Save/delete commit-window orchestration vs commit protocol internals | keeps mutation semantics separate from protocol primitives | layer leakage into commit internals |

## Step 9 - Quantitative Summary

- Total duplication patterns found: **14** (audit-classified)
- High-risk divergence duplications: **2**
- Defensive/intentional duplications: **9**
- Conservative estimated LoC reduction range without boundary erosion: **60-110 LoC**

## Output Summary

1. High-Impact Consolidation Opportunities
- Normalize invariant-message phrase templates through owning error constructors.
- Keep boundary checks split but reduce message-shape drift.

2. Medium Opportunities
- Tighten cursor compatibility check locality in cursor-spine docs/comments.

3. Low / Cosmetic
- Minor repeated format strings in internal errors.

4. Dangerous Consolidations (Keep Separate)
- planner/executor validation ownership, store/planner continuation checks, commit protocol boundaries.

5. Estimated LoC Reduction Range
- 60-110 LoC (conservative, boundary-safe only).

6. Architectural Risk Summary
- Current duplication is mostly defensive; largest remaining risk is message/shape drift, not algorithmic divergence.

7. DRY Risk Index (1-10, lower is better): **4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
