# Flow Convergence And Duplication

## Preamble And Comparability

- scope: `flow-convergence-and-duplication`
- definition:
  `docs/audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md`
- method: `FCD-1.0`
- run: `2026-08-26/01`
- auditor: `Codex`
- code snapshot: `7fa5cb5d35def70d1883a7c8e11f99718962d941`
  (`0.244.0`), tree `2201153fcf7933e5eea2da9a9d58cfcefa4c7323`
- worktree relevance: the audit started with one authorized documentation-only
  addition to `docs/governance/simplicity-and-maintainability.md`; it does not
  change the runtime under inspection. This run adds only its report and
  structured findings.
- compared baseline:
  `docs/reports/recurring/2026/08/10/flow-convergence-and-duplication/01/report.md`
- comparability: non-comparable scope change; the prior broad baseline predates
  the 0.238-0.244 exact/metadata optimization family. Its stable single-owner
  and planner-artifact anchors remain contextual evidence, while numerical
  deltas are `N/A`.
- run mode: affected-owner cross-cutting audit of ordered `DISTINCT`, exact
  cardinality, exact range count, and exact indexed numeric aggregation; no
  production edits, external services, full repository suite, or new design.

## Verdict

`PASS WITH FINDINGS`

The 0.238-0.244 performance routes retain one accepted-schema authority, one
planner/session selection chain, one fingerprint-bound exact-or-prepared
command slot, and one authoritative `IndexPrefixCardinality` owner. Ordered
`DISTINCT` carries a complete planner-owned group-seek contract into execution;
exact aggregate execution never reselects an index or establishes freshness
from a cache hit. No parallel semantic authority or fallback cache was found.

Two active findings remain. The first-component metadata family now repeats a
bounded traversal/execution envelope that must be consolidated before another
numeric kind or aggregate family is added. The generalized exact-aggregate
module also retains count-only internal vocabulary after accepting numeric
targets. Neither finding changes current correctness or blocks retaining
0.244, but both are explicit stop conditions for the next adjacent extension.

## Behavior And Owner Map

| Behavior | Canonical owner | Inputs | Carried contract | Consumers |
| --- | --- | --- | --- | --- |
| Exact aggregate admission | SQL aggregate lowering | accepted schema, structural query, aggregate strategies and projection | `AggregateShapeFacts` plus `SqlGlobalAggregateCommand` | exact aggregate session preparation and `EXPLAIN` |
| Complete first-component index selection | query-plan pipeline under accepted catalogue visibility | pinned `EntityAuthority`, `SchemaInfo`, visible accepted indexes and target field | generation-bound `IndexId` in `SqlGlobalAggregateCachedPlan` | exact aggregate executor |
| Exact-or-prepared command ownership | compiled SQL command | accepted-catalogue fingerprint and selected exact or prepared plan | one `SqlGlobalAggregatePlanCacheEntry` | ordinary and measured global aggregate adapters |
| Exact cardinality and numeric execution | aggregate exact terminal | pinned authority, immutable exact target and request budget | typed exact result or unavailable outcome | SQL projection or canonical prepared fallback |
| Leading-value metadata freshness | `IndexStore` / `IndexPrefixCardinality` | index mutations, row generation, replay, fold and recovery | generation-matched leading value multiplicities | exact count, range, numeric and planner evidence consumers |
| Ordered scalar `DISTINCT` group seek | covering query planner | accepted index order, direct projection, authored order and bounded window | `OrderedDistinctGroupSeekContract` inside `CoveringReadExecutionPlan` | covering projection executor |
| Prepared fallback | shared query-plan cache and prepared aggregate executor | original structural query and pinned accepted authority | `SharedPreparedExecutionPlan` | every unavailable or ineligible exact path |

## Flow Trace

| Entry surface | Frontend-only work | Convergence point | Runtime path | Result projection |
| --- | --- | --- | --- | --- |
| `COUNT(*)`, non-null count and exact prefix/range count | SQL parsing and aggregate lowering | one exact candidate plus accepted planned access | exact-or-prepared command entry -> exact cardinality terminal -> synchronized cardinality metadata | shared global-aggregate SQL projection |
| `COUNT(DISTINCT direct_int32)` | direct aggregate/projection admission | deterministic `exact_first_component_metadata_index` selection | first-component range-cardinality fold through the same exact command owner | shared global-aggregate SQL projection |
| unfiltered indexed `SUM` / `AVG` over admitted `Int32` | direct aggregate/projection admission | the same deterministic first-component selector and command slot | numeric `(count, sum)` metadata fold through the exact terminal | shared SQL value-row projection |
| ordered scalar `DISTINCT ... LIMIT/OFFSET` | scalar query lowering and covering access planning | immutable `OrderedDistinctGroupSeekContract` | covering executor performs bounded group-boundary seeks | ordinary scalar projection and adjacent DISTINCT accounting |
| unavailable, stale or over-bound exact evidence | none after the exact attempt | existing shared prepared-plan owner | canonical prepared aggregate executor | predecessor result or typed error |

## Findings

| ID | Class | Risk | Owner | Evidence | Friction | Disposition | Action trigger |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FCD-002 | `DuplicateFlow` | `MEDIUM` | `db::index::cardinality` and `db::executor::aggregate::exact_terminal` | `exact_first_component_range_count` and `exact_first_component_numeric_fold` separately implement generation availability, identity-bounded map traversal, stop-after handling, nonzero multiplicity validation, examined-work accounting and completion; their terminal callers separately repeat accepted-index matching, ready-state, remaining-budget, data-generation and fallback envelopes | each additional exact aggregate or numeric kind would require another place to preserve freshness, corruption, budget and fallback semantics, making drift more likely even though the current two paths agree | `CONSOLIDATE` | before authorizing another first-component aggregate, numeric kind, or change to the shared freshness/work-budget contract, introduce one owner-local traversal/execution envelope and measure that it does not regress hot-path instructions or raw Wasm; do not introduce a generic cache or independent framework |
| FCD-003 | `StaleSurface` | `LOW` | `db::session::sql::execute::exact_aggregate` | the generalized module now executes cardinality and numeric results, while `ExactCountTarget`, `ExactCountOutcome`, `CountPlan`, `exact_count_metadata_candidate`, and public-in-module resolver names still describe only the predecessor family | searches and reviews obscure that numeric execution already uses the same owner, increasing the chance that a future contributor adds a sibling route instead of extending the singular contract | `LOCALIZE` | at the next authorized edit to this module, hard-cut the private vocabulary to exact-aggregate terminology without aliases or behavior changes; do not create a standalone compatibility cleanup |

No `HIGH` finding exists. The report creates no active debt ledger and does not
authorize either correction.

## Retained Separations

- Ordered `DISTINCT` group seeking remains separate from metadata aggregation.
  It visits physical index boundaries and preserves ordered-window semantics;
  exact aggregates fold synchronized heap metadata. Combining them would erase
  distinct physical contracts rather than converge equivalent behavior.
- `accepted_index_target_matches` repeats an identity check after planner
  selection as fail-closed executor-boundary validation. It cannot select or
  refresh a schema and is retained as protective duplication.
- The cache enum retains distinct exact payload variants for entity count,
  first-component distinct, first-component range, prefix families and numeric
  aggregation. They occupy one mutually exclusive fingerprint-bound slot and
  do not constitute parallel caches or authorities.
- Ordinary and diagnostics-enabled aggregate adapters retain separate outer
  attribution plumbing but share target construction and exact execution
  helpers. This is a watch point, not an active finding: convergence through
  callbacks or dynamic machinery requires performance and Wasm evidence.
- Cardinality and numeric finalization remain semantically distinct. FCD-002
  concerns their duplicated traversal and execution envelope, not their typed
  result arithmetic or resource charges.

## Complexity And State-Space Delta

Numerical comparison with the 2026-08-10 baseline is `N/A` because the audited
route family did not exist there. As a current discovery signal, the scoped
aggregate, cardinality, covering-plan, covering-executor and command-cache
files changed by 0.238-0.244 contain 2,020 added and 885 removed lines relative
to `v0.237.3`, a net increase of 1,135 lines. This includes four independently
measured performance outcomes and a prior direct-count consolidation; it is not
itself classified as duplication debt.

The current global-aggregate command choice has six mutually exclusive states:
five exact payload families and one prepared plan. It remains one choice in one
`OnceLock`, derived under one accepted-catalogue fingerprint. No public mode,
configuration, persisted state, schema authority, result cache, invalidation
edge or compatibility path has been added by this route family.

0.244 alone reports +241 maintained production Rust lines and +5,740 final raw
SQL Wasm bytes for approximately 97.6M saved instructions. The performance is
proportionate for the retained slice, but implementation structure is more
complex. FCD-002 therefore freezes the next adjacency point: broader SQL
performance work must first share the physical metadata envelope rather than
append another isolated loop and terminal.

The documentation-only governance change present at audit start adds no state
axis. It records the same broad-surface requirement as a future design gate.

## Focused Verification Readout

| Verification | Status | Result |
| --- | --- | --- |
| Accepted-authority and planner-artifact trace | `PASS` | one authority selects and fingerprints targets; executor checks but cannot select or refresh |
| Layer-authority invariant gate | `PASS` | repository layer ownership invariants verified |
| SQL branch-ownership invariant gate | `PASS` | SQL-facing projection and branch ownership invariants verified |
| Exact command-cache ownership | `PASS` | one fingerprint-bound exact-or-prepared slot; no nested numeric or fallback cache |
| Ordered `DISTINCT` convergence | `PASS` | planner-owned contract carries direction, slot and validated window; executor does not reconstruct eligibility |
| Exact metadata convergence | `PASS` | count, distinct, range and numeric paths converge on the same synchronized `IndexPrefixCardinality` owner and prepared fallback |
| 0.244 focused correctness/performance evidence | `PASS` | committed status records semantic, fallback, recovery, feature, instruction, Wasm and Candid gates passing; this structural audit did not rerun them |
| Production changes by this audit | `PASS` | none |
| Full repository suite | `BLOCKED` | user-owned and prohibited for this audit by `AGENTS.md` |
| Live performance and Wasm rerun | `BLOCKED` | outside FCD scope; retained 0.238-0.244 measurements are contextual evidence, not new measurements |

## Follow-Up

Retain 0.244. Do not begin an implementation from this report alone. Before
the next adjacent exact metadata optimization is designed, use FCD-002 as a
hard review gate and demonstrate a shared, owner-local physical capability
covering the proposed types and query families. FCD-003 may be resolved only as
part of the next authorized edit to its owner or a separately requested
behavior-neutral cleanup.
