# Shared Aggregate Operator

## Purpose

D1 / F3 identified the dedicated SQL global aggregate lane as a competing
semantic path beside grouped aggregate execution. The safety-net work now proves
the dedicated global lane against the grouped singleton lane, but the long-term
shape should still be one aggregate contract with multiple physical
implementations.

This note scopes the migration before any broad rewrite.

## Current State

SQL global aggregate execution has three useful pieces that should survive:

- a constrained lowering lane for aggregate-only singleton results;
- prepared scalar aggregate strategies that normalize COUNT/SUM/AVG/MIN/MAX,
  target fields, expression inputs, DISTINCT, and FILTER expressions;
- a direct `COUNT(*)` prefix-cardinality fast path that can avoid building and
  executing a full shared prepared plan.

Grouped execution has the broader aggregate semantics:

- group keys and grouped result pagination;
- HAVING and post-aggregate projection behavior;
- ordered/hash grouped execution strategy selection;
- a structural result format shared by SQL and fluent grouped surfaces.

The problem is not the existence of the global fast path. The problem is that
global aggregate execution is still a separate SQL session adapter with its own
direct-count cache/probe path and scalar terminal dispatch. That makes it easy
for future aggregate semantics to land in one lane but not the other.

## Target Contract

Introduce one shared aggregate operator contract before changing execution
behavior. The contract should be able to represent both:

- singleton aggregate over one base query, with no group keys;
- grouped aggregate over one base query, with one or more group keys.

This is not a replacement for the existing executor-level
`StructuralAggregateRequest`. That request already carries scalar aggregate
terminals, projection, HAVING, and schema info after the base query has been
planned. The missing contract is one level higher: an operator envelope that
ties the aggregate request to base query identity, cache/explain identity, and
the physical implementation choice.

The contract should own:

- base structural query identity;
- aggregate terminal list after SQL/fluent semantic preparation;
- optional aggregate FILTER per terminal;
- optional HAVING/post-aggregate filter;
- output projection over aggregate slots and group keys;
- output ordering/windowing expectations;
- execution family: singleton or grouped;
- diagnostics identity for EXPLAIN and cache attribution.

The direct count optimization should become a physical implementation choice
for the singleton aggregate contract, not a separate semantic lane.

## Migration Order

1. Add a small aggregate operator DTO that is executor-neutral and can be built
   from the existing global aggregate command without changing behavior.
2. Adapt the global aggregate session path to consume that DTO internally while
   preserving the current direct-count cardinality probe and shared-plan
   fallback.
3. Add parity tests that the DTO for singleton global aggregate and grouped
   singleton carry equivalent terminal, HAVING, and projection semantics.
4. Move EXPLAIN/cache diagnostics to name the shared aggregate contract and the
   chosen physical implementation separately.
5. Only then decide whether grouped singleton execution can reuse more of the
   direct global path or whether the direct global fast path should remain as a
   singleton physical plan.

## Invariants

- `COUNT(*)` over a covered indexed prefix must still be able to use exact
  prefix-cardinality metadata without row reads.
- A global aggregate with `HAVING` must keep the same truth semantics as the
  grouped singleton parity tests.
- Aggregate FILTER expressions must be validated once before execution and must
  not be reinterpreted differently by global and grouped paths.
- Output projection remapping must be slot-based, not label/string based.
- The shared contract must not force grouped pagination fields onto singleton
  results.
- EXPLAIN must distinguish semantic contract from physical implementation:
  direct prefix-cardinality count, scalar aggregate terminal, hash grouped, or
  ordered grouped.

## Non-Goals For The First Code Slice

- Do not remove the direct `COUNT(*)` fast path.
- Do not make grouped execution pay for singleton-only result shaping.
- Do not introduce a broad physical operator framework.
- Do not change SQL aggregate admission rules.
- Do not add cost-based aggregate selection.

## First Code Slice

The first code slice added a private `PreparedAggregateRequestBundle`
assembled from `SqlGlobalAggregateCommand`, then made
`execute_global_aggregate_with_prepared_plan` consume that bundle instead of
re-reading strategies, projection labels, fixed scales, HAVING, and schema info
independently.

That operator should build or borrow the existing `StructuralAggregateRequest`;
it should not introduce a parallel terminal/projection request type.

The slice deletes session-local reconstruction without changing route
selection, direct-count metadata, shared-plan fallback, or grouped execution.

## Next Code Slice Candidate

EXPLAIN execution now names the aggregate semantic contract and chosen physical
implementation for singleton scalar aggregate terminals and grouped
hash/ordered materialization nodes.

Direct prefix-cardinality COUNT EXPLAIN now reuses the conservative
planning-only prefix-spec derivation to report metadata eligibility and prefix
count. It does not execute metadata lookups and does not claim the shortcut is
guaranteed when metadata is stale; runtime attribution remains the exact source
for `scalar_aggregate.sink_mode=IndexPrefixCardinality`.

The next design question is whether cache/explain identity should carry a
first-class aggregate operator DTO shared by singleton and grouped explain
assembly, or whether these descriptor properties are enough until a runtime
execution merge is justified.

A follow-up cleanup moved singleton direct `COUNT(*)` and direct
prefix-cardinality candidate proof into `AggregateShapeFacts`. Runtime
execution, compiled execution, diagnostics fallback, and EXPLAIN now consume
that precomputed fact set instead of reconstructing the same
strategy/projection/HAVING shape in the session adapter. This still does not
meet the DTO gate by itself; it is local DRY work inside the existing singleton
lane.

## Deferred DTO Gate

Do not add the first-class aggregate operator DTO just to make the design look
complete. Add it only when it deletes a real duplicate consumer or becomes a
shared runtime/explain handoff.

Accept the DTO when at least one of these is true:

- it deletes duplicate logic from both global and grouped aggregate paths;
- it becomes the single input to EXPLAIN assembly for singleton and grouped
  aggregates;
- it carries cache/fingerprint identity that prevents a real misclassification
  risk;
- it becomes the runtime handoff for both singleton and grouped execution.

Until one of those gates is met, keep the current descriptor properties and
runtime attribution as the lightweight aggregate contract surface.

## 2026-06-24 Guard-Mode Checkpoint

A follow-up scan after the singleton aggregate EXPLAIN/diagnostics cleanup did
not meet the DTO gate. The useful cleanup was local: global aggregate
direct-count probing now classifies the probe target once for normal and
diagnostics execution, and global aggregate execution EXPLAIN derives the base
query explain once before rendering terminal descriptors.

Keep the first-class aggregate operator DTO deferred until it removes logic
from both global and grouped aggregate paths, becomes a shared runtime/EXPLAIN
handoff, or prevents a real cache/fingerprint identity bug.
