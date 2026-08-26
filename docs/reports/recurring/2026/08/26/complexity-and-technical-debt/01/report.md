# Complexity And Technical Debt

## Preamble And Comparability

- scope: `complexity-and-technical-debt`
- definition:
  `docs/audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md`
- method: `CTD-1.0`
- run: `2026-08-26/01`
- auditor: `Codex`
- code snapshot: `7fa5cb5d35def70d1883a7c8e11f99718962d941`
  (`0.244.0`), tree `2201153fcf7933e5eea2da9a9d58cfcefa4c7323`
- worktree relevance: the audit includes the authorized FCD-003 private naming
  correction, current 0.244 documentation, and the documentation-only SQL
  performance breadth rule. These changes add no runtime behavior; this run
  adds only its report and structured findings.
- compared baseline:
  `docs/reports/recurring/2026/08/10/complexity-and-technical-debt/01/report.md`
- comparability: non-comparable affected-owner scope; the prior broad baseline
  predates the 0.238-0.244 SQL optimization family. Its authority and
  state-space method remains contextual, while numerical deltas are `N/A`.
- shared flow evidence:
  `docs/reports/recurring/2026/08/26/flow-convergence-and-duplication/01/report.md`
- run mode: affected-owner audit of ordered `DISTINCT`, exact aggregate
  planning/execution, synchronized leading-value metadata and their focused
  proof surfaces; no production edits, external services or new design.

## Verdict

`PASS WITH FINDINGS`

The current SQL optimization family remains controlled. It adds no public
mode, configuration, persisted field, schema authority, result cache or
independent invalidation lifecycle. Five exact aggregate payload families and
one prepared plan are mutually exclusive values in a single
accepted-catalogue-fingerprint-bound slot. Ordered `DISTINCT` carries a
separate planner contract because it performs physical group seeks rather than
metadata aggregation.

One `MEDIUM` accepted-until-trigger debt item remains. The first-component
range and numeric folds currently duplicate a traversal and execution envelope,
so another adjacent type or aggregate would cross the same six owners and
multiply freshness, budget and corruption decisions. Current 0.244 remains
proportionate and is not blocked; the next adjacent design must consolidate
that physical capability before adding another specialized path.

## State-Space Map

| Axis | Values | Canonical owner | Combining axes | Invalid combinations |
| --- | --- | --- | --- | --- |
| Global aggregate cached plan | exact entity count, leading distinct count, leading range count, prefix-family count, leading numeric fold, prepared plan | `SqlGlobalAggregateCachedPlan` inside one compiled command slot | accepted-catalogue fingerprint and metadata availability | one command cannot retain both exact and prepared entries; stale fingerprint cannot authorize execution |
| Exact evidence availability | synchronized complete, unavailable/incomplete, invariant failure | `IndexStore` / `IndexPrefixCardinality` | selected exact payload and remaining request budget | unavailable evidence cannot produce an exact result; corruption cannot silently fall back |
| Exact execution outcome | direct result, prepared-plan hit, fallback requiring prepared resolution, disabled | exact aggregate session owner | ordinary or diagnostics-enabled execution | direct and fallback outcomes are mutually exclusive; fallback cannot replace the compiled exact target |
| Ordered `DISTINCT` group seek | absent or one immutable bounded contract | covering query planner | index direction, direct projected slot and output window | unbounded, unordered, derived, nullable or unsupported scalar shapes cannot carry the contract |
| Request snapshot | one immutable accepted catalogue fingerprint/authority | accepted runtime root and compiled-command cache boundary | old or newly published request | one request cannot switch accepted roots during execution |

Rejected SQL shapes and unsupported field kinds are not counted as active
states: they converge on the existing prepared route. Diagnostics attribution
is plumbing over the same semantic choices, not another execution authority.

## Decision And Ownership Spread

The consumer counts below describe inspected semantic or plumbing roles, not a
complexity score.

| Decision | Owner | Semantic consumers | Plumbing consumers | Cross-owner switch sites |
| --- | --- | ---: | ---: | ---: |
| Exact aggregate shape is admissible | SQL aggregate lowering / `AggregateShapeFacts` | exact session preparation | `EXPLAIN` and diagnostics | 1 session projection through command methods |
| One complete accepted index is selected | query-plan pipeline under accepted visibility | exact session and terminal | compiled-plan cache | 1 selection site; executor only validates identity |
| Exact payload kind | compiled global-aggregate plan owner | exact aggregate session and terminal | three owner-local cache projections | 1 session dispatch plus terminal cardinality dispatch |
| Leading-value evidence is synchronized | `IndexPrefixCardinality` | distinct, range, prefix and numeric exact consumers | commit, replay, fold and recovery maintenance | 0 downstream freshness classifiers |
| Ordered group seek is admitted | covering query planner | covering projection executor | physical-work attribution | 0 runtime eligibility classifiers |
| Exact evidence falls back | exact aggregate session | prepared aggregate executor | shared plan-cache attribution | 1 canonical fallback branch |

The 697-line exact-aggregate session module imports executor, index, query-plan,
schema, session-cache/projection and SQL-lowering contracts because it is the
intentional convergence adapter for this route family. Inspection found no
policy authority in its module root and no second cache owner. Its size is a
review signal, not current ownership debt.

## Extension Rehearsals

### 1. Add another integer kind to exact `SUM` / `AVG`

- expected owner: existing exact numeric aggregate contract and index-owned
  canonical numeric decoder/fold
- semantic modules: aggregate lowering, exact plan payload, exact terminal,
  index metadata fold and SQL numeric finalization
- layers crossed: lowering/session, planner/cache, executor, index owner,
  focused integration proof
- current blocker: the exact payload carries an `IndexId` but no admitted
  numeric-kind contract; the fold is deliberately `Int32`-specific and proves
  canonical `Int64` payloads fit `i32`
- assessment: do not add a sibling type-specific variant or loop. First define
  one owner-carried numeric contract and reuse the shared bounded metadata
  envelope, with absolute instruction and raw-Wasm gates.

### 2. Add bounded indexed `SUM` / `AVG`

- expected owner: structural range planning plus the existing exact metadata
  aggregate owner
- semantic modules: range-bound lowering, cached exact payload, exact terminal
  and `IndexPrefixCardinality`
- layers crossed: planner, session/cache, executor, index metadata and proof
- current blocker: range cardinality owns canonical bounds while numeric
  folding owns value decoding and arithmetic in a separate unbounded loop
- assessment: this is the clearest FCD-002 trigger. A viable design must carry
  one immutable bounded numeric target and share traversal/freshness handling;
  another range-numeric variant with copied envelopes is rejected.

### 3. Derive grouped aggregates from leading-value metadata

- expected owner: grouped planner and grouped result/resource contracts, not
  the current global-aggregate adapter
- semantic modules: grouped admission, metadata projection, grouped budgets,
  ordering/paging and output projection
- layers crossed: lowering, grouped planner/executor, index metadata and
  integration proof
- current blocker: one metadata entry becomes one output group, introducing
  ordering, cardinality, retained-state and pagination semantics absent from the
  global `(count, sum)` result
- assessment: not an adjacent cleanup and not evidence for a generic aggregate
  framework. Require current workload measurements and a separate design; do
  not widen 0.244 through apparent metadata similarity.

These probes are extension-cost tests, not roadmap proposals.

## Findings

| ID | Debt family | Risk | Owner | Evidence | Present friction | Disposition | Trigger |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CTD-002 | `DuplicatedFlowDebt` | `MEDIUM` | first-component metadata and exact-terminal owners | FCD-002 identifies duplicated generation, identity-range, stop-after, multiplicity, work-charge and fallback envelopes in range cardinality and numeric folding; the two adjacent extension rehearsals cross the same lowering, cache, session, executor, index and proof owners | the next aggregate/type would either copy the envelope again or introduce an unmeasured generic mechanism, making ordinary extension disproportionate | `ACCEPT UNTIL TRIGGER` | before authorizing another exact first-component aggregate, numeric kind, or shared metadata freshness/budget change, require one owner-local physical envelope and paired performance/raw-Wasm evidence |

No `HIGH` finding exists. CTD-001 from the 2026-08-10 broad baseline concerns
startup recovery ownership outside this affected-owner scope; this run neither
reopens nor resolves it. The report creates no competing active debt ledger.

## Accepted And Not-Debt Signals

- Five exact payloads are not five caches or authorities. They are exhaustive,
  mutually exclusive values in one compiled command slot and each has distinct
  evidence or payload needs.
- The `Prepared` variant is the canonical fallback, not a compatibility mode.
  Optional exact evidence changes performance only.
- The executor's accepted-index identity check is intentional fail-closed
  boundary enforcement and cannot select a schema.
- Ordered `DISTINCT` and exact metadata aggregation have different physical
  contracts and should not be combined to reduce variant or module counts.
- The 6,336-line SQL performance integration target and 4,912-line audit
  canister are large because they share one expensive fixture/build boundary
  and retain cross-version regression gates. No measured compile/runtime or
  repeated ownership conflict currently justifies splitting them.
- Ordinary and diagnostics-enabled paths share semantic helpers while retaining
  attribution plumbing. Do not introduce callbacks, trait objects or generic
  execution frameworks without measured production-Wasm benefit.
- The FCD-003 hard cut resolves stale count-only orchestration vocabulary and
  removes eight net production Rust lines without changing instructions or
  Wasm.

## Complexity Delta

Numerical comparison with the 2026-08-10 broad baseline is `N/A` because the
route family and affected-owner scope differ. Relative to `v0.237.3`, the 15
scoped aggregate, metadata, covering and cache files changed by 0.238-0.244
contain 2,014 added and 887 removed lines, a net increase of 1,127 lines. This
is a discovery signal spanning four measured performance outcomes and direct
count consolidation, not a debt count.

0.244 now retains +233 production Rust lines relative to `v0.243.1` after the
FCD-003 cleanup, versus +241 at its first closeout. Its production SQL Wasm
remains byte-identical after cleanup at 3,287,174 raw and 1,305,929 gzip bytes,
with the same Candid hash and six exports. The slice adds no public, persisted,
configuration, cache-owner or invalidation axis. Implementation structure is
more complex because one measured exact payload is added, but the current
state-space remains singular and explicit.

This CTD run changes documentation only.

## Focused Verification Readout

| Verification | Status | Result |
| --- | --- | --- |
| State-space and owner trace | `PASS` | exact, prepared, metadata-availability, group-seek and request-snapshot axes inspected |
| Extension rehearsals | `PASS` | two adjacent metadata expansions hit the same FCD-002 gate; grouped aggregation remains a separate semantic owner |
| Layer and SQL branch invariants | `PASS` | final worktree authority, mutation, SQL branch, schema-model and post-link gates pass |
| Clippy-first feature matrix | `PASS` | all-feature tests/lib, no-default and SQL-only core configurations pass |
| Focused exact aggregate regressions | `PASS` | 0.244 numeric and inherited 0.240 exact-count families pass with unchanged measurements |
| Maintained production SQL Wasm/Candid | `PASS` | 3,287,174 raw / 1,305,929 gzip; unchanged Candid hash and six exports |
| Production changes by this audit | `PASS` | none |
| Full repository suite | `BLOCKED` | user-owned and prohibited for this audit by `AGENTS.md` |
| New extension implementation | `BLOCKED` | feature probes and CTD-002 are evidence only and lack separate design authorization |

## Follow-Up

No immediate cleanup is warranted. Retain 0.244 and its singular exact-or-
prepared owner. Apply CTD-002 as a hard no-build gate before the next adjacent
exact metadata proposal: measure a broad query/type cohort, identify one shared
physical contract, and reject the work if breadth requires another copied
traversal, cache, authority or unproven generic framework.
