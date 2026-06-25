# 0.186 Shared Query Filter Authority - Tentative Design

Status: tentative. Requires peer review before implementation.

This is a parking design, not an accepted roadmap. It records the likely next
architecture after the 0.184 query-engine audit and the planned 0.185
branch-aware routing revisit. Do not treat this document as permission to
start a broad filter rewrite without a focused review of scope, invariants,
and migration risk.

## Context

0.184 reduced several duplicate query-engine flows. The filter-contract work
made SQL lowering safer by keeping the visible SQL truth expression and the
predicate-pushdown subset together, then moved planner residual expression,
residual predicate, and runtime filter program into one residual-filter
contract.

That was intentionally narrower than the full architecture. SQL and fluent
filters can still enter the query engine through different frontend adapters
before they converge on query intent, planning, and execution.

0.185 should stay focused on branch-aware query routing. It should confirm
that SQL and fluent `IN` / branch-heavy query shapes feed the same planner
path, but it should not absorb this full shared-filter-authority rewrite.

## Problem

IcyDB has multiple representations of one user-visible filtering idea:

- SQL visible boolean expression;
- SQL-derived predicate subset;
- fluent predicate/filter expression;
- access-planning predicate;
- residual expression after access-path satisfaction;
- residual predicate after access-path satisfaction;
- compiled runtime filter program;
- EXPLAIN and cache identity projections.

These are not all redundant, but they should not be independent authorities.
The drift risk is that SQL, fluent, planner, executor, count/cardinality, and
diagnostics can each interpret "the filter" slightly differently.

The concrete failure modes are:

- SQL truth semantics, especially `NULL`, `IN`, and `NOT IN`, diverge from
  predicate pushdown.
- Fluent and SQL build equivalent filters but produce different route,
  residual, cache, or EXPLAIN identity.
- Count/cardinality shortcuts prove a predicate shape that does not match the
  residual runtime filter.
- Branch-aware routing optimizes one frontend but not the other.
- Diagnostics describe the rendered filter string instead of the planner-owned
  proof that execution actually uses.

## Tentative Goal

Introduce one frontend-neutral query-filter contract that becomes the shared
authority after frontend-specific lowering.

SQL and fluent should become adapters into this contract:

```text
SQL WHERE
  -> SQL truth/literal/alias lowering
  -> shared query filter contract

Fluent filter
  -> typed/filter builder lowering
  -> shared query filter contract

Shared query filter contract
  -> predicate subset
  -> access route candidates
  -> residual filter contract
  -> count/cardinality eligibility
  -> cache identity
  -> EXPLAIN diagnostics
```

The contract should not encode SQL parser details. SQL-specific work belongs
before the handoff.

## Proposed Contract Shape

The exact type names are not accepted yet, but the shared object likely needs
to carry:

- canonical visible boolean expression, when one exists;
- canonical predicate subset, when extractable;
- proof of whether the predicate subset fully covers the visible expression;
- reason when no subset can be extracted;
- source kind for diagnostics only, not for runtime decisions;
- schema-binding facts needed for literal/coercion stability;
- stable identity/fingerprint input.

Post-access planning should then derive:

- access-proven predicate terms;
- residual expression;
- residual predicate subset;
- residual runtime program;
- pushdown outcome and reason diagnostics.

0.184 already created part of the post-access side through
`ResidualFilterContract`. 0.186 would decide whether the pre-access side also
needs a first-class shared contract.

## Non-Goals

- Do not change SQL semantics.
- Do not broaden SQL admission.
- Do not change fluent public API behavior.
- Do not change branch-aware route choice as part of this design.
- Do not make EXPLAIN snapshots the authority for runtime behavior.
- Do not keep backwards-compatibility aliases before 1.0.0 if the accepted
  design hard-cuts an internal contract.
- Do not merge this with cursor redesign, cardinality metadata, or cost-based
  route selection.

## Relationship To 0.184

0.184 remains the query-engine audit cleanup line. It should continue to accept
narrow drift-reduction work, especially where behavior is unchanged and tests
prove existing semantics.

0.184 should not attempt the full frontend-neutral filter contract. That would
turn an audit cleanup line into a broad architecture migration.

The narrow 0.184-compatible work is:

- keep SQL visible expression and predicate subset paired;
- derive predicate facts from the schema-bound SQL expression where safe;
- keep residual-filter facts grouped in planner-owned contracts;
- improve diagnostics to consume planner-owned facts.

## Relationship To 0.185

0.185 should stay branch-aware:

- branch-set route representation;
- branch merge cursors;
- small versus large `IN` route choice;
- SQL and fluent branch-heavy queries feeding the same planner path;
- branch-tree reuse versus special-case `IN` flows.

0.185 may expose pressure that motivates this 0.186 design, but should not
implement it unless peer review decides branch-aware correctness cannot be
finished without it.

## Review Questions

Peer review should answer these before implementation:

1. Is the current `NormalizedFilter` close enough to become the shared
   pre-access contract, or should a new type own that boundary?
2. Which layer should own predicate extraction: frontend lowering, query
   intent, or planner preparation?
3. Can fluent filters always produce a canonical visible expression, or do
   predicate-only fluent filters remain valid first-class inputs?
4. Should SQL-specific coercion normalization remain a pre-handoff adapter, or
   should the shared contract carry frontend-specific coercion facts?
5. Which cache identity surfaces are allowed to change?
6. What EXPLAIN snapshots are semantic contracts versus implementation detail?
7. What proof is needed that count/cardinality shortcuts consume the same
   filter authority as page execution?
8. How does this interact with branch-aware `IN` routing in 0.185?

## Suggested First Slice If Accepted

If peer review accepts the direction, start smaller than the full design:

1. Document the current shared and divergent filter entry points.
2. Add source-audit tests proving SQL and fluent equivalent filters converge at
   the query-intent/planner boundary for representative shapes:
   equality, range, `IN`, `NOT IN`, `NULL`, text prefix, boolean composition,
   branch-aware `IN`, and count/cardinality candidates.
3. Move only one frontend-neutral fact into the shared contract.
4. Prove cache, EXPLAIN, route, residual, and count behavior are unchanged.
5. Delete one duplicate derivation path.

No implementation slice should change both the pre-access contract and the
post-access residual contract at the same time.

## Acceptance Gates

Before this becomes a release-line implementation, require:

- SQL and fluent parity tests for equivalent filters.
- SQL truth-semantics tests for `NULL`, `IN`, and `NOT IN`.
- Route parity tests for indexed, full-scan, branch-aware, and count/cardinality
  shapes.
- EXPLAIN identity tests that distinguish semantic identity from diagnostics
  presentation.
- Perf attribution showing no regression on common SQL and fluent filters.
- Source invariant tests preventing new frontend-owned predicate shortcuts from
  bypassing the shared contract.

## Current Recommendation

Keep this as 0.186 tentative design.

Do not move 0.185. Keep 0.185 dedicated to branch-aware routing. Revisit this
document after the 0.185 branch-aware planner work has clarified exactly where
SQL and fluent still diverge.

After 0.185 and this 0.186 decision are complete, run the second query-engine
mega audit parked in
`docs/design/0.187-second-query-engine-audit/second-mega-audit-reminder.md`.
That audit should look for duplicate flows that survive the branch-aware and
shared-filter work, not rediscover work that is already intentionally pending.
