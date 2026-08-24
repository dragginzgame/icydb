# Query Capability Disposition And Dependency Roadmap

Status: historical provisional roadmap; assignments from 0.233 through 0.235
were superseded by released work and remaining candidates are unnumbered unless
a current design explicitly promotes them

Roadmap cut: 2026-08-19

Evidence intake:
[production-ledger query capability intake](production-ledger-query-capability-intake.md)

Immediate programme predecessor:
[post-0.224 design programme](post-0.224-design-programme.md)

At its cut this roadmap disposed every numbered intake candidate exactly once,
with 0.231 complete and 0.232 still proposed. That release state is historical.
The provisional 0.233, 0.234, and 0.235 assignments below were not adopted;
those released lines contain different maintained work. They remain historical
intake dispositions rather than promises that the named capabilities shipped.
0.236 is released under the focused
[exact-cardinality planner tie-break design](../0.236-exact-cardinality-planner-tiebreak/0.236-design.md).
The independent
[accepted-schema runtime observation and cold-root scaling design](../0.237-accepted-schema-runtime-observation-and-cold-root-scaling/0.237-design.md),
which was not an intake candidate from this historical roadmap, closed as a
measured unreleased no-build. The provisional 0.237 component/time-expression
assignment was not adopted and is now unnumbered.
The current 0.237 authority is the separately assigned
[SQL query performance hotspot rediscovery design](../0.237-sql-query-performance-hotspot-rediscovery/0.237-design.md),
which is an evidence audit rather than a capability promotion from this
roadmap.
The current [0.240 singular global-aggregate compiled plan cache
design](../0.240-singular-global-aggregate-plan-cache/0.240-design.md) is an
independent runtime-convergence cleanup. It does not adopt the historical
durable aggregate-job assignment recorded later in this document; that intake
candidate remains unnumbered and unpromoted.
Every other unreleased candidate remains unnumbered until a later
current-surface audit and explicit user authorization promotes it.

## Historical Audit Decisions At The Roadmap Cut

### Candidate 1: maintained external gap confirmed

IcyDB has internal value-independent prepared-plan reuse through
`PreparedQueryParameterContract`, whose module explicitly excludes public
prepared-statement APIs and SQL placeholder syntax. The maintained facade SQL
entrypoint accepts only `&str`, and the generated SQL endpoint accepts a SQL
string. No typed external binding envelope was found across the facade,
generated endpoint, Candid, or CLI surface.

The provisional 0.233 assignment was not adopted. Any remaining public bound
invocation gap is unnumbered and requires a fresh current-surface audit.

### Candidate 2: engine authority supported; public SQL gap confirmed

Typed and dynamic public reads already return bounded opaque authenticated
continuations. The maintained contract binds the complete query, authority,
ordering, window, and progress and rejects incompatible reuse. Ordinary pages
are intentionally live-state keyset pagination, not snapshot isolation.
Revision-strict exhaustive pages detect source change and require restart; they
do not retain a multi-version snapshot.

The maintained SQL facade and generated endpoint accept only SQL text. Scalar
SQL results expose no resumable page input, while grouped SQL can return a
`next_cursor` but the parsed `SELECT` and public invocation carry no cursor back
into execution. Therefore the cursor engine exists, but a bound public SQL
request/continuation surface does not.

The provisional 0.233 assignment was not adopted. Any remaining public SQL
continuation gap is unnumbered and must reuse the existing authenticated live
continuation authority. A true multi-version snapshot expansion remains
rejected absent a separate demonstrated workload and bounded storage model.

### Candidate 8: maintained exact-aggregate gap confirmed

NatBig storage, ordering, predicates, output values, and Candid transport are
maintained. Aggregate `SUM`/`AVG`, however, use the shared `ValueReducerState`
and coerce numeric inputs into bounded `Decimal`; final `SUM` is a Decimal.
Values outside that domain cannot supply exact NatBig accumulation.

The gap remains intake evidence but is now unnumbered. Any future promotion
must define exact NatBig aggregate accumulator/output semantics and bounds
without widening all numeric operations or using lossy conversion.

## Exact Candidate Disposition

| Candidate | One disposition | Current owner or outcome |
| --- | --- | --- |
| 1. Public Parameterized And Prepared SQL | Unnumbered after the provisional assignment was not adopted | Requires a fresh current-surface audit; current internal parameterization is predecessor evidence, not a public substitute |
| 2. Authenticated, Snapshot-Safe SQL Pagination | Unnumbered after the provisional assignment was not adopted; true snapshot expansion remains rejected | Any public SQL gap must reuse existing authenticated live/exhaustive continuation authority and add no MVCC cursor |
| 3. Explicit Covering Index Payloads | Unnumbered after the provisional assignment was not adopted | Requires a fresh physical representation and workload audit |
| 4. Statistics-Aware Index Selection | Narrowed current design, not implementation authority | [0.236 exact-cardinality planner tie-breaking](../0.236-exact-cardinality-planner-tiebreak/0.236-design.md); no new persisted statistics |
| 5. Index And Plan Observability | Maintained surfaces plus one narrowed 0.236 extension | Existing explain/execution diagnostics remain authority; 0.236 adds exact tied-candidate evidence only |
| 6. Account Component Expressions | Unnumbered intake candidate | Requires a fresh current-surface audit |
| 7. Nanosecond Time And Bucket Expressions | Unnumbered intake candidate | Requires a fresh current-surface audit |
| 8. Exact NatBig Aggregation Semantics | Unnumbered intake candidate; maintained gap was confirmed at the roadmap cut | Requires a fresh current-surface audit before promotion |
| 9. Bounded Append-Only Ingestion Primitive | Unnumbered; promotion blocked on application-first evidence | Promote only if repeated benchmarks isolate an engine gap |
| 10. Incremental Rollups Or Maintained Materializations | Unnumbered intake candidate | Requires a fresh current-surface audit |
| 11. Durable Resumable Aggregate Jobs | Unnumbered intake candidate | Requires a fresh current-surface audit |
| 12. Range Partitioning And Archive-Aware History | Unnumbered intake candidate | Requires a fresh current-surface audit |
| 13. Explicit Nullable Unique-Index Semantics | Completed maintained outcome | [0.231 explicit nullable unique-index contracts](../0.231-explicit-nullable-unique-index-contracts/0.231-design.md); implementation, independent closeout, and measurement closure are complete |
| 14. Specific Bounded Public SQL Diagnostics | Completed maintained outcome | [0.232 typed public SQL diagnostics](../0.232-typed-public-sql-diagnostics/0.232-design.md): one bounded query-field context through core, facade, Candid, and CLI |
| 15. Bounded Indexed Relation Traversal | Unnumbered after the provisional assignment was not adopted | Requires a fresh current-surface audit |

No numbered candidate appears in another disposition category.

## Candidate 5 Current Disposition

The former physical/planner version split was not adopted. Current authority is:

| Maintained physical/execution evidence | Narrow 0.236 planner extension |
| --- | --- |
| existing index-entry contracts and artifact measurements | exact counts for tied Prefix/MultiLookup/BranchSet candidates |
| existing execution trace actuals | one `exact_cardinality_tiebreak` reason |
| existing chosen/alternative/rejected explain projection | bounded evidence availability/fallback state |

The remaining broad physical costing, covering-payload, histogram, warning,
and DDL-estimate ideas are unnumbered. They are not part of released 0.235 or
the current 0.236 design.

## Secondary Finding Disposition

| Intake finding | Disposition |
| --- | --- |
| Explorer least-privilege SQL/schema authority, evidenced on Canic fleets | Promoted out of the provisional roadmap into the framework-neutral [0.226 application-scoped read authority](../0.226-application-scoped-sql-and-schema-read-authority/0.226-design.md). Canic remains incident evidence and an independent downstream adapter, not an IcyDB dependency or promotion gate. This is not a numbered intake candidate and does not duplicate Candidate 1 or 2. |
| Fixed-length blobs | Deferred. Do not absorb into another expression slice. Promotion requires a separate schema-type audit proving that max-length Blob plus checks cannot coherently express the invariant. |
| Multivalue collection indexes | Deferred pending workload, storage, write-amplification, planner, and boundedness audits. `COLLECTION_CONTAINS` remains a residual predicate, not an index claim. |
| Unique-secondary-index expansion | Deferred pending a current proposal/DDL/accepted-catalog/mutation/execution gap audit. Maintained `CREATE UNIQUE INDEX` support means historical absence is not evidence. |
| Included fields, entry shape, predicate detail, physical DDL estimates | Unnumbered; released 0.235 did not implement this provisional assignment. |
| Estimate/actual comparison, rejected routes, stale evidence, warnings | Existing explain/trace remains authority; 0.236 adds only exact tied-prefix evidence. |
| Generated/stored owner, subaccount, or bucket projections | An unnumbered promotion question; they are not automatically added if canonical expressions suffice. |
| Unknown-field detail | [0.232](../0.232-typed-public-sql-diagnostics/0.232-design.md); the field context is bounded, typed, and shared by equivalent SQL and structural planner failures. |
| Symbolic runtime-boundary documentation | Separate documentation-only follow-up; it has no dependency on rejected-field propagation and is not part of 0.232. |
| Concise human enum rendering | Deferred outside 0.232 because it changes successful result presentation rather than failed-query diagnostics; structured values remain canonical. |
| reference application no-op/upsert observations | Unnumbered evidence only after application-side replay and lookup costs are removed and benchmarks are repeated. |

## Historical Proposed Version Map

This table preserves the original 2026-08-19 ordering for audit history. It is
not a current version assignment; even the linked 0.236 line is now released.

| Order | Line | Candidate input | Maintained outcome or concise promotion question |
| --- | --- | --- | --- |
| 0.231 | [Explicit Nullable Unique-Index Contracts](../0.231-explicit-nullable-unique-index-contracts/0.231-design.md) | 13 | Complete: accepted unique indexes with omit-capable top-level sources require exact non-null guards, while physical null omission and present-entry uniqueness remain unchanged. |
| 0.232 | [Typed Public SQL Diagnostics](../0.232-typed-public-sql-diagnostics/0.232-design.md) | 14 | Reconciled for review: one optional post-normalization resolver field of at most 256 UTF-8 bytes plus a closed semantic role, carried through the shared query error flow without string facts, schema suggestions, producer identity, or a second error model. |
| 0.233 | Bound SQL Invocation And Authenticated Continuation | 1 and 2, narrowed to current facade/generated gaps | What typed request and continuation envelope is externally missing, and how does it bind values to the existing cache, policy, 0.226 read-authority boundary, and authenticated live cursor without a second authorization owner, cursor, or snapshot model? |
| 0.234 | Bounded Indexed Relation Semi-Joins | 15 | Can one indexed `IN`/`EXISTS` semi-join, or one evidenced fixed second bridge, solve the reference application relation hop under existing intermediate/result budgets? |
| 0.235 | Covering Index Payloads And Physical Index Evidence | 3 and Candidate 5 physical fields | Which admitted projections still load base rows, and do included payloads justify their exact entry bytes, write amplification, rebuild cost, and schema-transition surface? |
| 0.236 | [Exact-Cardinality Planner Tie-Breaking](../0.236-exact-cardinality-planner-tiebreak/0.236-design.md) | Narrowed Candidate 4 and maintained Candidate 5 surface | Design audit complete: reuse exact 0.230 prefix counts only for final equal-candidate ties, with no new persisted statistics or optimizer mode. |
| Historical 0.237 assignment, not adopted | Canonical Component And Time-Bucket Expressions | 6 and 7 | Unnumbered: which owner/subaccount and UTC nanosecond bucket operations have one canonical typed meaning across predicates, grouping, ordering, projection, and optional indexes? |
| 0.238 | Exact NatBig Aggregate Semantics | 8 | What exact accumulator and result types preserve NatBig `SUM` and define bounded `AVG` behavior without Decimal narrowing, floating point, or silent saturation? |
| 0.239 | Bounded Idempotent Ingestion | 9 | After reference application removes full-cache replay, unchanged replacements, and repeated lookups, does per-new-row or identical-replay engine cost still grow enough to justify one catalog-native idempotent batch primitive? |
| 0.240 | Durable Resumable Aggregate Jobs | 11 | Which opt-in aggregate state can reuse durable-job identity/lifecycle principles while keeping accumulator, checkpoint, result, expiry, ownership, and schema drift bounded? |
| 0.241 | Incrementally Maintained Rollups | 10 | Which deliberately small deterministic aggregate class can have one atomic or typed-lag replay/repair model after exact scalar, aggregate, ingestion, and job semantics settle? |
| 0.242 | Partitioned And Archive-Aware History | 12 | What accepted catalog authority can prove complete/pruned/unavailable ranges across planning, continuations, jobs, statistics, and archives without silent omission? |

At the roadmap cut, a promotion audit could remove or contract a line before
implementation. The current rule is stricter: all unnumbered candidates above
need a new audit against the released surface and explicit minor authorization.

## Historical Dependency Order

The following was the provisional order at the 2026-08-19 cut. It is retained
as intake history, not current release order:

```text
0.230 exact cardinality generation authority
  -> 0.231 nullable unique-index contracts
  -> 0.232 typed SQL diagnostics
  -> 0.233 bound SQL invocation over existing authenticated continuation
  -> 0.234 bounded indexed semi-joins
  -> 0.235 covering payloads and physical evidence
  -> 0.236 planner statistics/observability extending 0.230
  -> unnumbered component/time-bucket expressions (historically assigned 0.237)
  -> 0.238 exact NatBig aggregates
  -> 0.239 bounded idempotent ingestion, only if evidence promotes it
  -> 0.240 durable aggregate jobs
  -> 0.241 incremental rollups
  -> 0.242 partitioned/archive-aware history
```

Historical semantic dependency assumptions were:

- 0.236 extends 0.230 and consumes 0.235's final physical index evidence;
- 0.233 reuses the maintained continuation authority, may consume 0.232's typed
  diagnostic path, and must extend rather than replace 0.226's read-authority
  boundary;
- the historical 0.241 rollup candidate follows exact expression and aggregate
  semantics, the disposition of the ingestion candidate, and durable-job
  lifecycle evidence; and
- 0.242 remains last because it changes completeness relationships for
  planning, continuations, jobs, statistics, and archive routing.

## Roadmap Promotion Gate

No line is implementation-ready from this file. Released 0.236 and historical
no-build 0.237 are owned by their own design/status trackers, not this
historical roadmap. Promotion of any intake candidate requires:

1. the current minor has a reported ready/complete closeout and the user
   explicitly names the next minor;
2. maintained code/contracts/tests prove a concrete remaining gap;
3. rejected and already-supported behavior is removed from proposed scope;
4. one canonical owner, hard cuts, invariants, measurements, and 1-12 landing
   patches are fully designed; and
5. the user reviews that design and explicitly authorizes that minor.
