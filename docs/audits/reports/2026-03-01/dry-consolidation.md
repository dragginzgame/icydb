# DRY / Redundancy / Consolidation Audit - 2026-03-01

Scope: duplication and divergence pressure in `icydb-core` (+ facade where relevant), with strict boundary-preserving guardrails.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines (approx) | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor paging policy messaging (`"cursor pagination requires ..."`) duplicated across query/plan, intent, cursor boundary checks | 3 runtime files | low | defensive duplication | Yes | Medium | Medium |
| Access canonicalization entrypoints invoked from planner + intent | 7 modules (calls to `canonical_by_keys_path` / `normalize_access_plan_value`) | medium | intentional boundary duplication | Yes | Low-Medium | Medium |
| Grouped DISTINCT admission + guard checks spread across planning and executor grouped runtime | 33 modules reference grouped/distinct surfaces | high | evolution drift duplication | Yes | Medium-High | High |
| Commit-marker guard/replay semantics spread across commit + mutation boundaries | 11 runtime files mention commit marker protocol | medium | defensive duplication | Yes | Medium | Medium |
| Invariant error construction (`query_executor_invariant`/`executor_invariant`) | 37 runtime modules / 127 callsites | high | boilerplate + defensive duplication | Yes | Medium-High | Medium-High |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor order/limit paging policy gates | 3 primary runtime surfaces | query + cursor | Yes | Medium | `query::plan::validate` + cursor spine adapters | Medium |
| Access canonicalization application | planner + intent + executor validation consumption | access + query + executor | Yes | Medium | `db::access` | Medium |
| Grouped DISTINCT admissibility checks | planner policy + executor grouped runtime checks | query + executor | Yes | High | planner policy (semantic), executor defensive checks (runtime) | High |
| Commit marker lifecycle checks | commit guard + mutation commit-window + recovery | executor + commit | Yes | High | `db::commit` protocol boundary | Medium-High |
| Error mapping wrappers (`map_err`, `From<...Error>`) | 59 modules include mapping forms | multi-layer | Yes | Medium | domain-owned error types per module | Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | 1198 LOC | 5+ (routing, continuation, grouped fold, paging, trace) | Under-splitting | High |
| `query/intent/mod.rs` | 1006 LOC | 4+ (intent API, coercion, lowering, policy checks) | Under-splitting | High |
| `query/plan/validate.rs` | 914 LOC | 4+ (scalar+group validation, policy, cursor readiness) | Under-splitting | High |
| `query/plan/planner.rs` | 827 LOC | 4+ (predicate planning, index fit, range merges, normalization) | Under-splitting | High |
| `query/fluent/load.rs` | 846 LOC | 3+ | Under-splitting | Medium-High |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Cursor paging requires explicit order/limit | `query/plan/validate.rs`, `query/intent/mod.rs`, `cursor/mod.rs` | Yes | Medium (message/policy drift) | Medium |
| Access structure validity before runtime execution | `query/plan/*`, `executor/plan_validate.rs` | Yes | Medium | Medium |
| Grouped DISTINCT shape admissibility | planner grouped policy + grouped runtime path checks | Yes | Medium-High | High |
| Commit marker lifecycle invariants | `commit/guard.rs`, `executor/mutation/commit_window.rs`, `commit/recovery.rs` | Yes | Medium | Medium |

Classification:
- Safety-enhancing: cursor/commit dual checks.
- Safety-neutral: repeated wrapper mappings where semantics are identical.
- Divergence-prone: grouped DISTINCT and policy/error text spread.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| executor/query invariant constructor usage | 37 runtime modules | Medium (could obscure context if over-consolidated) | Medium-High |
| cursor policy error text variants | query/plan + intent + cursor modules | Medium | Medium |
| lower-level decode/encode error mapping to `InternalError` | data/index/codec/commit boundaries | Medium | Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor + continuation checks | cursor spine/mod + executor grouped/scalar paging paths | Yes (defensive) | Medium-High |
| Bound conversions / index range lowering | planner range + index range helpers + stream access | Partial | Medium |
| Raw key ordering / key canonicalization | index key + executor group key + contracts semantics | Yes (boundary-safe) | Medium |
| Reverse index mutation symmetry | relation reverse-index + commit prepared ops + mutation execution | Yes | Medium |
| Commit marker phase transitions | commit guard + mutation commit-window + replay | Yes | Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Cursor paging policy text/variant normalization | query/plan/validate + query/intent + cursor/mod | evolution drift duplication | Medium | query policy boundary |
| Grouped DISTINCT policy contract wording and mapping | query/plan + executor/load | evolution drift duplication | High | planner policy + executor defensive adapter |
| Invariant constructor boilerplate concentration | executor/query runtime modules | boilerplate duplication | Medium | owning error type (`InternalError`) helpers |
| Access canonicalization invocation patterns | query intent/planner surfaces | intentional boundary duplication | Medium | access boundary |

## Step 8 - Dangerous Consolidations (Do NOT Merge)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner semantic checks vs executor defensive checks | preserves semantic-vs-runtime authority split | boundary collapse and weaker invariant ownership |
| Cursor decode/spine checks vs executor continuation checks | fail-closed across decode + execution boundary | latent continuation corruption/shape drift |
| Commit guard vs replay checks | protects atomicity and idempotent recovery authority | replay asymmetry and marker-lifecycle ambiguity |
| Access canonicalization boundary vs runtime consumption | keeps path normalization owner explicit | cross-layer coupling and hidden canonicalization logic |

## Step 9 - Quantitative Summary

- Total duplication patterns found: **13**
- High-risk divergence-prone duplications: **3**
- Defensive duplications: **6**
- Conservative estimated LoC reduction range (boundary-safe only): **120-220 LOC**

## Output Structure

1. High-Impact Consolidation Opportunities
- grouped DISTINCT policy/message normalization; invariant-constructor boilerplate concentration.

2. Medium Opportunities
- cursor paging policy text harmonization; access canonicalization invocation consistency.

3. Low / Cosmetic
- small repeated wrappers with identical behavior where owner-layer boundaries are already clear.

4. Dangerous Consolidations (Keep Separate)
- planner vs executor validation, cursor decode vs executor continuation, commit guard vs replay.

5. Estimated LoC Reduction Range
- **120-220 LOC** conservative.

6. Architectural Risk Summary
- current DRY pressure is primarily divergence risk across policy/error and grouped-runtime boundary checks, not raw copy/paste volume.

7. DRY Risk Index (1-10, lower is better)
- **6/10**
