# Query Capability Disposition And Dependency Roadmap

Status: provisional roadmap; not design or implementation authority

Roadmap cut: 2026-08-13

Evidence intake:
[production-ledger query capability intake](production-ledger-query-capability-intake.md)

Immediate programme predecessor:
[post-0.224 design programme](post-0.224-design-programme.md)

This roadmap disposes every numbered intake candidate exactly once. Version
labels from 0.230 onward are provisional ordering slots, not accepted minor
scope, landing trackers, release targets, or permission to implement. Promotion
requires a current-surface audit, one focused design/status line, 1-12
substantive landing patches, a completed predecessor closeout, and
explicit user authorization.

## Current-Surface Audit Decisions Required By The Intake

### Candidate 1: maintained external gap confirmed

IcyDB has internal value-independent prepared-plan reuse through
`PreparedQueryParameterContract`, whose module explicitly excludes public
prepared-statement APIs and SQL placeholder syntax. The maintained facade SQL
entrypoint accepts only `&str`, and the generated SQL endpoint accepts a SQL
string. No typed external binding envelope was found across the facade,
generated endpoint, Candid, or CLI surface.

Disposition: provisional 0.232, limited to the missing typed bound invocation
surface and direct binding into existing accepted authority, policy, cache, and
continuation identity.

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

Disposition: provisional 0.232, limited to reusing the existing authenticated
live continuation authority and binding Candidate 1 values into its identity.
It must not create a second cursor or advertise snapshot isolation. A true
multi-version snapshot expansion is rejected absent a separate demonstrated
workload and bounded storage model.

### Candidate 8: maintained exact-aggregate gap confirmed

NatBig storage, ordering, predicates, output values, and Candid transport are
maintained. Aggregate `SUM`/`AVG`, however, use the shared `ValueReducerState`
and coerce numeric inputs into bounded `Decimal`; final `SUM` is a Decimal.
Values outside that domain cannot supply exact NatBig accumulation.

Disposition: provisional 0.237 remains in the roadmap. It must define exact
NatBig aggregate accumulator/output semantics and bounds without widening all
numeric operations or using lossy conversion.

## Exact Candidate Disposition

| Candidate | One disposition | Provisional owner or maintained outcome |
| --- | --- | --- |
| 1. Public Parameterized And Prepared SQL | Provisional future slice | 0.232 typed bound SQL invocation; current internal parameterization is predecessor evidence, not a public substitute |
| 2. Authenticated, Snapshot-Safe SQL Pagination | Provisional future slice; engine support exists but the public SQL request gap is confirmed, and true snapshot expansion is rejected | 0.232 reuses the existing authenticated live/exhaustive continuation authority and adds no MVCC cursor |
| 3. Explicit Covering Index Payloads | Provisional future slice | 0.234 covering payloads and physical index evidence |
| 4. Statistics-Aware Index Selection | Provisional future slice | 0.235 deterministic planner statistics extending 0.229 generations |
| 5. Index And Plan Observability | Provisional future slice, split once by fact ownership | Physical facts go only to 0.234; planner facts go only to 0.235, as enumerated below |
| 6. Account Component Expressions | Provisional future slice | 0.236 canonical component expressions |
| 7. Nanosecond Time And Bucket Expressions | Provisional future slice | 0.236 canonical time-bucket expressions |
| 8. Exact NatBig Aggregation Semantics | Provisional future slice; maintained gap confirmed | 0.237 exact NatBig aggregate semantics |
| 9. Bounded Append-Only Ingestion Primitive | Provisional future slice, promotion blocked on application-first evidence | 0.238 bounded idempotent ingestion only if repeated benchmarks isolate an engine gap |
| 10. Incremental Rollups Or Maintained Materializations | Provisional future slice | 0.240 incrementally maintained rollups |
| 11. Durable Resumable Aggregate Jobs | Provisional future slice | 0.239 durable resumable aggregate jobs |
| 12. Range Partitioning And Archive-Aware History | Provisional future slice | 0.241 partitioned/archive-aware history |
| 13. Explicit Nullable Unique-Index Semantics | Provisional future slice | 0.230 explicit nullable unique-index contracts |
| 14. Specific Bounded Public SQL Diagnostics | Provisional future slice | 0.231 typed public SQL diagnostics plus direct bounded CLI/documentation fallout |
| 15. Bounded Indexed Relation Traversal | Provisional future slice | 0.233 bounded indexed relation semi-joins |

No numbered candidate appears in another disposition category.

## Candidate 5 Non-Duplicating Split

Candidate 5 is one provisional disposition whose evidence fields have two
disjoint owners:

| 0.234 physical-index evidence only | 0.235 planner evidence only |
| --- | --- |
| storage bytes | candidate-plan comparison |
| write amplification and rebuild cost | estimates and actuals |
| physical entry/key/payload shape | chosen and rejected routes |
| covering status | stale-statistics evidence |
| base-row avoidance/materialization | planner warnings |
| DDL physical cost | plan/instruction/response observations |

0.234 may report which physical route exists and what it costs. 0.235 may
explain why a route won and compare plan evidence. Neither line redefines the
other's facts.

## Secondary Finding Disposition

| Intake finding | Disposition |
| --- | --- |
| Explorer least-privilege SQL/schema authority on Canic fleets | Promoted out of the provisional roadmap into immediate [0.226 application-scoped read authority](../0.226-application-scoped-sql-and-schema-read-authority/0.226-design.md). It is not a numbered intake candidate and does not duplicate Candidate 1 or 2. |
| Fixed-length blobs | Deferred. Do not absorb into 0.236. Promotion requires a separate schema-type audit proving that max-length Blob plus checks cannot coherently express the invariant. |
| Multivalue collection indexes | Deferred pending workload, storage, write-amplification, planner, and boundedness audits. `COLLECTION_CONTAINS` remains a residual predicate, not an index claim. |
| Unique-secondary-index expansion | Deferred pending a current proposal/DDL/accepted-catalog/mutation/execution gap audit. Maintained `CREATE UNIQUE INDEX` support means historical absence is not evidence. |
| Included fields, entry shape, predicate detail, physical DDL estimates | 0.234 only. |
| Estimate/actual comparison, rejected routes, stale evidence, warnings | 0.235 only. |
| Generated/stored owner, subaccount, or bucket projections | A 0.236 promotion question; they are not automatically added if canonical expressions suffice. |
| Unknown-field detail, symbolic runtime-boundary documentation, concise human enum rendering | 0.231 only when direct bounded propagation/rendering fallout from Candidate 14; structured values remain canonical. |
| Toko no-op/upsert observations | 0.238 evidence only after application-side replay and lookup costs are removed and benchmarks are repeated. |

## Provisional Lines And Promotion Questions

| Order | Provisional line | Candidate input | Concise promotion question |
| --- | --- | --- | --- |
| 0.230 | Explicit Nullable Unique-Index Contracts | 13 | Can current partial-index semantics make nullable membership and uniqueness explicit before acceptance without adding encoded-null keys? |
| 0.231 | Typed Public SQL Diagnostics | 14 | What smallest bounded typed detail preserves rejected field and clause identity through core, facade, Candid, CLI, and docs without a second error model? |
| 0.232 | Bound SQL Invocation And Authenticated Continuation | 1 and 2, narrowed to current facade/generated gaps | What typed request and continuation envelope is externally missing, and how does it bind values to the existing cache, policy, 0.226 read-authority boundary, and authenticated live cursor without a second authorization owner, cursor, or snapshot model? |
| 0.233 | Bounded Indexed Relation Semi-Joins | 15 | Can one indexed `IN`/`EXISTS` semi-join, or one evidenced fixed second bridge, solve the Toko relation hop under existing intermediate/result budgets? |
| 0.234 | Covering Index Payloads And Physical Index Evidence | 3 and Candidate 5 physical fields | Which admitted projections still load base rows, and do included payloads justify their exact entry bytes, write amplification, rebuild cost, and schema-transition surface? |
| 0.235 | Deterministic Planner Statistics And Plan Observability | 4 and Candidate 5 planner fields | What smallest deterministic advisory evidence improves measured choices while extending 0.229's generation/build/staleness authority and keeping observability bounded? |
| 0.236 | Canonical Component And Time-Bucket Expressions | 6 and 7 | Which owner/subaccount and UTC nanosecond bucket operations have one canonical typed meaning across predicates, grouping, ordering, projection, and optional indexes? |
| 0.237 | Exact NatBig Aggregate Semantics | 8 | What exact accumulator and result types preserve NatBig `SUM` and define bounded `AVG` behavior without Decimal narrowing, floating point, or silent saturation? |
| 0.238 | Bounded Idempotent Ingestion | 9 | After Toko removes full-cache replay, unchanged replacements, and repeated lookups, does per-new-row or identical-replay engine cost still grow enough to justify one catalog-native idempotent batch primitive? |
| 0.239 | Durable Resumable Aggregate Jobs | 11 | Which opt-in aggregate state can reuse durable-job identity/lifecycle principles while keeping accumulator, checkpoint, result, expiry, ownership, and schema drift bounded? |
| 0.240 | Incrementally Maintained Rollups | 10 | Which deliberately small deterministic aggregate class can have one atomic or typed-lag replay/repair model after exact scalar, aggregate, ingestion, and job semantics settle? |
| 0.241 | Partitioned And Archive-Aware History | 12 | What accepted catalog authority can prove complete/pruned/unavailable ranges across planning, continuations, jobs, statistics, and archives without silent omission? |

If a promotion audit finds no maintained gap, that line is removed or
contracted before a full design is written. Candidate 2 is already contracted
to public SQL integration over the maintained cursor engine; snapshot isolation
is not promoted. The 0.237 gap is currently confirmed, so that line remains.

## Final Dependency Order

The governance order is numeric and sequential after a completed 0.229:

```text
0.229 exact cardinality generation authority
  -> 0.230 nullable unique-index contracts
  -> 0.231 typed SQL diagnostics
  -> 0.232 bound SQL invocation over existing authenticated continuation
  -> 0.233 bounded indexed semi-joins
  -> 0.234 covering payloads and physical evidence
  -> 0.235 planner statistics/observability extending 0.229
  -> 0.236 component/time-bucket expressions
  -> 0.237 exact NatBig aggregates
  -> 0.238 bounded idempotent ingestion, only if evidence promotes it
  -> 0.239 durable aggregate jobs
  -> 0.240 incremental rollups
  -> 0.241 partitioned/archive-aware history
```

Semantic dependency constraints within that order are:

- 0.235 extends 0.229 and consumes 0.234's final physical index evidence;
- 0.232 reuses the maintained continuation authority, may consume 0.231's typed
  diagnostic path, and must extend rather than replace 0.226's read-authority
  boundary;
- 0.240 follows 0.236 exact expression semantics, 0.237 exact aggregate
  semantics, the disposition of 0.238 ingestion, and 0.239 durable job
  lifecycle evidence; and
- 0.241 remains last because it changes completeness relationships for
  planning, continuations, jobs, statistics, and archive routing.

## Roadmap Promotion Gate

No line from 0.230 onward is implementation-ready from this file. Promotion of
one line requires:

1. all earlier numeric lines have a reported ready/complete closeout, or the
   roadmap is explicitly renumbered after a removed line;
2. maintained code/contracts/tests prove a concrete remaining gap;
3. rejected and already-supported behavior is removed from proposed scope;
4. one canonical owner, hard cuts, invariants, measurements, and 1-12 landing
   patches are fully designed; and
5. the user reviews that design and explicitly authorizes that minor.
