# DRY / Redundancy / Consolidation Audit - 2026-03-02

Scope: duplication and divergence pressure in `icydb-core` (+ facade where relevant), with boundary-preserving guardrails.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Lines (approx) | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor paging policy text (`cursor pagination requires ...`) now centralized in `cursor/error.rs` and reused via adapters | 1 definition + adapter callsites | low | intentional boundary consolidation | Yes | Low | Low |
| Access canonicalization invocation (`normalize_access_plan_value` / `canonical_by_keys_path`) | 4 files / 13 references | low-medium | intentional boundary duplication | Yes | Low-Medium | Medium |
| Grouped DISTINCT policy and runtime admission spread | 26 files / 177 references | high | evolution drift duplication | Yes | Medium-High | High |
| Commit marker + commit-window lifecycle checks across mutation/recovery boundaries | 21 files (commit+executor) | medium | defensive duplication | Yes | Medium | Medium |
| Invariant constructor usage (`query_executor_invariant` / `executor_invariant`) | 38 files / 43 callsites | medium-high | boilerplate + defensive duplication | Yes | Medium-High | Medium-High |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Cross-Layer? | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Cursor paging readiness checks | centralized message source + multiple boundary adapters | query + cursor | Yes | Low-Medium | `cursor` policy text authority + query adapters | Low-Medium |
| Access canonicalization at intent/planner boundaries | 13 references across 4 files | access + query | Yes | Medium | `db::access` | Medium |
| Grouped DISTINCT admissibility checks | planner policy + executor grouped runtime guards | query + executor | Yes | High | planner semantic policy + executor defensive checks | High |
| Commit marker lifecycle invariants | commit guard + commit-window + replay | commit + executor | Yes | High | `db::commit` protocol boundary | Medium-High |
| Error mapping wrappers (`map_err`) | 168 callsites across 66 files | multi-layer | Yes | Medium | domain-owned error types per module | Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Module | Size | Responsibilities Count | Split Pressure | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `executor/load/mod.rs` | 1428 LOC | 5+ (routing, continuation, grouped fold, paging, trace) | Under-splitting | High |
| `query/intent/mod.rs` | 1074 LOC | 4+ (intent API, lowering, policy, access mapping) | Under-splitting | High |
| `query/plan/validate.rs` | 1108 LOC | 4+ (shape + grouped + expr + policy checks) | Under-splitting | High |
| `query/plan/planner.rs` | 858 LOC | 4+ (predicate planning, index fit, range merges, normalization) | Under-splitting | High |
| `query/fluent/load.rs` | 878 LOC | 3+ | Under-splitting | Medium-High |

## Step 4 - Invariant Repetition Risk

| Invariant | Locations | Defensive? | Divergence Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- |
| Cursor pagination policy gating | `cursor/error.rs`, `query/plan/validate.rs`, intent/fluent adapters | Yes | Low-Medium (text now centralized) | Low-Medium |
| Access structure validation ownership | planner semantic validation + executor plan defensive checks | Yes | Medium | Medium |
| Grouped DISTINCT admissibility and shape checks | planner grouped policy + grouped runtime enforcement | Yes | Medium-High | High |
| Commit marker lifecycle invariants | `commit/*` + `executor/mutation/*` + recovery checks | Yes | Medium | Medium |

Classification:
- Safety-enhancing: planner/executor split checks, commit guard/replay checks.
- Safety-neutral: repeated wrapper mappings with consistent owner-layer taxonomy.
- Divergence-prone: grouped DISTINCT policy/runtime parallel checks.

## Step 5 - Error Construction Redundancy

| Error Pattern | Files | Consolidation Risk | Drift Risk |
| ---- | ---- | ---- | ---- |
| invariant constructor forms across runtime boundaries | 38 files | Medium | Medium-High |
| lower-level error mapping via `map_err` | 66 files | Medium | Medium |
| boundary error conversions (`From<...Error>`) | distributed across query/cursor/executor boundaries | Medium | Medium |

## Step 6 - Cursor & Index Duplication Focus

| Area | Duplication Sites | Intentional? | Risk |
| ---- | ---- | ---- | ---- |
| Anchor + continuation checks | cursor spine + executor paging | Yes | Medium-High |
| Bound conversions | planner/index/access helper boundaries | Partial | Medium |
| Raw key ordering/canonicalization | index key + access canonical + grouped key | Yes | Medium |
| Reverse index mutation symmetry | commit + relation + mutation boundaries | Yes | Medium |
| Commit marker phase transitions | commit guard + mutation commit-window + replay | Yes | Medium |

## Step 7 - Consolidation Candidates Table

| Area | Files | Duplication Type | Risk Level | Suggested Owner Layer |
| ---- | ---- | ---- | ---- | ---- |
| Grouped DISTINCT policy/message normalization | planner grouped policy + executor grouped runtime | evolution drift duplication | High | planner policy + executor defensive adapter |
| Invariant constructor boilerplate shaping | runtime hub modules | boilerplate duplication | Medium | owning error constructors on internal error types |
| Access canonicalization call pattern consistency | intent/planner callsites | intentional boundary duplication | Medium | `db::access` |
| Cursor readiness adapter simplification | query intent/fluent/cursor adapters | intentional boundary duplication | Medium | `db::cursor` + query policy adapter boundary |

## Step 8 - Dangerous Consolidations (Do NOT Merge)

| Area | Why Duplication Is Protective | Risk If Merged |
| ---- | ---- | ---- |
| Planner semantic checks vs executor defensive checks | preserves semantic/runtime authority split | boundary collapse and weaker invariant ownership |
| Cursor decode/spine checks vs executor continuation checks | keeps fail-closed behavior across decode + runtime phases | latent continuation-shape drift |
| Commit guard vs replay checks | protects atomicity and deterministic recovery | replay asymmetry and marker lifecycle ambiguity |
| Access canonicalization owner vs runtime consumers | preserves clear owner-layer authority | hidden canonicalization logic in higher layers |

## Step 9 - Quantitative Summary

- Total duplication patterns found: **12**
- High-risk divergence-prone duplications: **3**
- Defensive duplications: **6**
- Conservative LoC reduction range (boundary-safe only): **100-180 LOC**

## Output Structure

1. High-Impact Consolidation Opportunities: grouped DISTINCT policy/runtime normalization, large-hub invariant boilerplate shaping.
2. Medium Opportunities: cursor readiness adapters, access canonicalization call pattern consistency.
3. Low / Cosmetic: repeated wrappers where ownership boundaries are already explicit.
4. Dangerous Consolidations (Keep Separate): planner vs executor semantics, cursor decode vs runtime checks, commit guard vs replay.
5. Estimated LoC Reduction Range: **100-180 LOC**.
6. Architectural Risk Summary: DRY pressure is concentrated in divergence-prone grouped policy/runtime overlap, not raw copy volume.
7. DRY Risk Index (1-10, lower is better): **6/10**.

## Rerun Addendum - 2026-03-02 (post continuation + load entrypoint unification)

Targeted DRY rerun findings:

- Grouped paging window derivation now routes through
  `executor/continuation/mod.rs` (`grouped_paging_contract`), removing duplicate
  grouped cursor-window math from grouped fold helpers.
- Grouped next-cursor token construction now routes through
  `executor/continuation/mod.rs` (`grouped_next_cursor_token`), removing direct
  token construction in grouped page finalization.
- Executor runtime drift checks:
  - `ContinuationToken::new*` / `GroupedContinuationToken::new*` outside
    continuation facade (non-test): **0**.
  - Cursor-boundary derivation outside cursor protocol (non-test): **0**.

Residual pressure:

- Planner/executor grouped DISTINCT policy layering remains the dominant
  divergence-prone seam.
- Executor `.as_inner()` usage remains at **11** non-test callsites and should
  be tracked to prevent semantic backdoor growth.

Rerun conclusion:

- DRY pressure improved in continuation/load orchestration.
- Overall DRY Risk Index improves slightly to **5/10** for this rerun snapshot.
