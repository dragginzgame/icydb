# Bounded Application Batch Progress

Status: idea intake only; not design or implementation authority

Recorded: 2026-08-15

Evidence: [GitHub issue 7](https://github.com/dragginzgame/icydb/issues/7)

## Purpose

Record the demonstrated gap between IcyDB's hard request-execution budget and
application entry points that perform repeated, individually valid database
work. This note preserves the candidate outcome and its ownership boundaries
without assigning a minor version, freezing a public API, or authorizing
implementation.

The reported canister update validates and writes a caller-supplied batch. Each
item performs several exact-key reads followed by one typed write under one
`#[icydb::request_execution]` scope. Batches of 16 succeed, while larger inputs
reliably reach E273 around item 24. Moving most exact-key reads from planned
`IN` filters to native `get`/`get_many` reduced instruction cost but did not
move the observed boundary.

The application can therefore discover a safe batch size only empirically.
The item named by the failing read is ordinary data and succeeds in a smaller
batch; it is merely the next item after aggregate request work is exhausted.

## Maintained Current Surface

The current system already has three relevant authorities:

1. The request execution root owns one monotonic multi-resource hard budget.
   E273 remains the safety boundary and carries the exhausted resource, limit,
   attempted usage, scope, lane, and normalized shape facts.
2. Scalar query page work uses an engine-owned envelope to return successful
   bounded progress before the hard execution budget is exhausted.
3. Durable mutation jobs own replayable progress for catalog-native fixed
   updates admitted from SQL. They deliberately do not provide a generic
   business-workflow engine or persist arbitrary Rust callbacks.

The public session does not expose request-budget consumption or headroom.
`RequestDiagnostics` reports query-shape observations, not authoritative
budget admission. Public `Error` preserves E273 facts, but its compact
`Display` representation renders only the error code.

## Candidate Outcome

The long-term candidate is one engine-owned bounded application-batch progress
contract inside the existing request hard budget:

```text
request execution root: monotonic hard safety budget
  -> batch progress coordinator: conservative soft envelope
     -> preflight one complete declared database item
        -> admitted: execute and commit the complete item
        -> yield: touch none of the item and return prior progress
```

The application describes supported database work structurally. It does not
supply internal resource counts, instruction limits, or a guessed batch size.
IcyDB derives a conservative reservation from the operation descriptors,
accepted schema bounds, and current charging rules. A candidate first slice
should remain narrow enough for a complete proof, such as bounded exact-key
reads followed by one staged accepted typed or structural mutation.

The normal result shape should distinguish complete from partial success and
return completed-item progress plus an application-owned continuation. Exact
names and wire shapes remain a promotion-time decision. Reaching the soft
envelope is successful progress, not E273.

Hard-budget accounting remains monotonic and never refunds work. Only unused
soft admission reservation is released after the item's observed database work
is committed to the progress envelope. E273 remains the final safety boundary
for unbounded work, an incorrect engine reservation, or an indivisible unit
that cannot fit its maintained contract.

## Canonical Authority And Ownership

| Concern | Canonical owner |
| --- | --- |
| Aggregate hard limits and E273 facts | Existing request execution root |
| Soft per-request progress admission | Candidate batch progress coordinator |
| Per-operation worst-case reservation | Existing operation owner plus accepted schema authority |
| Row/index mutation semantics and atomicity | Existing accepted structural mutation and commit pipeline |
| Business validation meaning | Application code |
| Business continuation and idempotency | Application protocol |
| Fixed collection-wide update intent | Existing durable mutation-job runtime |

The coordinator must extend existing budget and mutation authorities rather
than introducing a second counter set, write path, transaction protocol, or
schema source. Application work outside IcyDB remains outside IcyDB's execution
budget guarantee.

## Smallest Supported Item Boundary

A promotion audit must identify an item boundary whose database work can be
known before the item begins. The initial candidate should prefer:

- a bounded set of exact-key read descriptors;
- accepted-schema-derived maximum row and decode work;
- pure application validation over already loaded values;
- a staged typed or structural mutation committed only after validation; and
- no unlisted nested database calls inside the admitted item.

If the application must issue arbitrary queries or intermediate writes while
validating one item, the engine cannot prove that a raw headroom observation
will cover the remaining work. That broader shape is not implicitly admitted
by this note.

## No-Build And Alternatives Gate

### Demonstrated need

The stable 23-24-item E273 boundary proves that a real application batch cannot
derive clean progress from the maintained request contract. A fixed application
maximum works only as an empirical deployment convention and may silently
become invalid when database work changes.

### Simpler alternatives

- **Raise the hard ceiling:** rejected. It moves the failure and weakens the
  request safety boundary without providing resumable progress.
- **Expose raw remaining budget:** useful diagnostics, but insufficient as a
  correctness protocol. The budget is multi-resource and the next item's cost
  is data- and operation-dependent.
- **Use E273 as loop control:** rejected. Hard-budget attempts remain charged,
  and E273 intentionally returns no successful partial database result.
- **Keep a fixed application batch size:** acceptable as a temporary
  workaround, but it retains the demonstrated silent-rot problem.
- **Use the existing durable mutation job:** correct only when the work can be
  lowered to its catalog-native fixed-update intent. It does not cover
  arbitrary application validation plus typed writes.
- **Persist an application closure or add a generic workflow engine:** rejected.
  It would add callback identity, versioning, recovery, and authorization state
  outside existing catalog authority.

### Simplest candidate

Reuse the request root, page-style soft admission, exact-key read owners, and
accepted mutation pipeline for one narrowly declared application item. Add no
caller-tunable budget and no IcyDB-owned business continuation.

## State-Space Delta

The candidate may add one public complete-or-partial batch result and one
volatile admitted-item state owned by the synchronous coordinator. It should
add no persisted phase, engine continuation format, configurable budget
profile, planner route, legacy compatibility path, or second mutation flow.

Promotion must define how the result combines with public/trusted policy,
typed versus structural frontends, item validation failure, schema drift,
lost responses, and application retries. Invalid combinations should be
rejected at admission rather than discovered after partial item work.

## Relationship To Provisional 0.239

The query-capability roadmap's provisional 0.239 owns only a possible bounded
idempotent append-only ingestion primitive. This note may become evidence for
that line only if a current-surface audit narrows the reported workload to the
same catalog-native ingestion semantics.

General application validation followed by typed mutation must not broaden
0.239 incidentally. The immediate programme through 0.230 is now closed, so
that predecessor condition is satisfied. If the maintained gap remains
broader, this note still requires a separate future roadmap disposition. No
minor number is assigned and no implementation is authorized here.

## Promotion Questions

Before promotion, answer:

1. Which exact database operation descriptors make one item preflightable?
2. Can the existing scalar page-work machinery be generalized without making
   query and mutation accounting share an artificial abstraction?
3. What soft envelope leaves hard-budget failure and response headroom intact?
4. How is a staged mutation prevented from committing when its item did not
   complete?
5. What does a lost response require from application idempotency, and what
   must IcyDB prove independently?
6. Does a narrower prepared mutation batch or application-side `get_many` plus
   structural batch remove the measured gap without a new public protocol?
7. Is the workload append-only ingestion owned by provisional 0.239, or a
   distinct application-batch progress capability?
8. What are the raw non-gzipped Wasm, instruction, file/line, and complexity
   deltas for the smallest complete implementation?

## Promotion Gate

The predecessor condition is satisfied by the completed 0.230 closeout. This
note remains non-authoritative until the remaining gates are satisfied:

1. a current-surface audit reproduces the gap and identifies the exhausted
   E273 resource from typed facts;
2. the no-build alternatives above are measured against the real workload;
3. one focused numbered design and status tracker define a practical initial
   set of substantive landing patches; and
4. the user explicitly authorizes that minor-version line.
