# 0.187 Second Query-Engine Mega Audit Reminder

Status: parking note. Do not implement from this file directly.

This note exists so the follow-up audit is not forgotten after 0.184, 0.185,
and 0.186. It is intentionally blunt.

## Do Not Run This Too Early

Run this second mega audit after:

1. 0.184 query-engine audit cleanup is closed.
2. 0.185 branch-aware query routing revisit is complete or deliberately
   deferred.
3. 0.186 shared query filter authority is either implemented, rejected, or
   explicitly rescheduled.

Do not run the second audit immediately after 0.184 unless a production issue
requires it. Running it too early will mostly rediscover known deferred work
instead of finding new structural duplication.

## Why 0.187 Exists

0.184 removed many known duplicate flows, but it was not proof that every
duplicate flow is gone. It was a targeted audit cleanup.

0.185 and 0.186 are expected to change query routing and filter authority
again. A second audit after those lines should give a cleaner signal:

- which special-case `IN` / branch paths survived 0.185;
- whether SQL and fluent still diverge after 0.186;
- whether count, aggregate, write, EXPLAIN, and diagnostics paths rebuilt
  duplicate local authority while 184-186 were landing.

## Intended Scope

Audit for duplicate or competing query-engine flows across:

- SQL versus fluent filter lowering;
- query intent versus planner predicate extraction;
- branch-aware route selection versus generic `IN` / multi-lookup routes;
- scalar page, retained-slot page, covering page, and aggregate row-sink
  execution setup;
- direct `COUNT(*)`, prepared COUNT, EXISTS, and prefix-cardinality metadata
  paths;
- global aggregate versus grouped/singleton aggregate execution;
- typed DELETE, SQL DELETE count, SQL DELETE RETURNING, UPDATE, and INSERT
  SELECT row collection;
- EXPLAIN descriptor, verbose diagnostics, attribution, and cache identity
  construction;
- generated canister endpoints versus session/library surfaces.

## Questions The Audit Must Answer

1. Does each user-visible query shape have one semantic authority?
2. Are SQL and fluent equivalent shapes guaranteed to reach the same planner
   facts when they should?
3. Are any parser, generated-model, DTO, or diagnostics surfaces making runtime
   access decisions they should not own?
4. Are residual filter presence, predicate pushdown, and access-proven
   predicates still derived from planner-owned contracts?
5. Are count/cardinality shortcuts consuming the same predicate/filter proof as
   page execution?
6. Are branch-aware routes still special-cased, or are they now regular access
   routes with clear contracts?
7. Are EXPLAIN and cache fingerprints projections of runtime authority, or are
   they carrying a parallel interpretation?
8. Are any `allow` / stale `expect` lint suppressions hiding code that should be
   deleted or feature-gated?
9. Are perf hotspots caused by real execution work, or by repeated compile,
   lower, bind, plan, or diagnostics derivation?
10. What can be deleted without widening public behavior?

## Required Inputs

Before starting 0.187, gather:

- latest 0.184 closeout status;
- 0.185 branch-aware routing design/status;
- 0.186 shared query filter authority decision;
- latest SQL and fluent perf matrix reports;
- latest EXPLAIN/diagnostics snapshots;
- source-invariant script results;
- generated canister matrix results.

## Suggested Audit Method

1. Start with source maps, not refactors.
2. List every duplicate-looking flow with file/function names.
3. Classify each as:
   - real duplicate to remove;
   - deliberate specialization;
   - diagnostics-only projection;
   - cache/fingerprint identity path;
   - deferred architecture item.
4. For real duplicates, require a parity test before cleanup.
5. For performance claims, require attribution before optimization.
6. For architecture changes, write a small design note before code.

## Deliverable

Produce a table:

| Area | Suspected Duplicate | Evidence | Risk | Recommendation | Release Target |
| --- | --- | --- | --- | --- | --- |

Each recommendation should be one of:

- delete now;
- consolidate behind existing contract;
- add invariant test;
- benchmark first;
- keep as deliberate specialization;
- defer to a named future design.

## Explicit Deferrals From Earlier Lines

Carry these into 0.187 only if they still matter after 0.185 and 0.186:

- first-class aggregate operator DTO;
- full operator-level physical plan;
- cost/selectivity-aware planner;
- true chunked durable mutation commits;
- retained-slot late-materialization lane;
- broader expression-analysis artifact with type inference;
- shared SQL/fluent filter authority, if 0.186 did not finish it.

## Morning Checklist

If you are reading this later and trying to remember the plan:

1. Do not move 0.185.
2. Keep 0.185 branch-aware.
3. Keep 0.186 shared filter authority.
4. Run the second mega audit as 0.187 after those have changed the code.
5. The goal of 0.187 is not to keep refactoring forever. The goal is to find
   remaining duplicate authority and either delete it, prove it is deliberate,
   or defer it with a named reason.
