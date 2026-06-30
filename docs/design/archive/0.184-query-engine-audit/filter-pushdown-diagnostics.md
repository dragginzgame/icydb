# Filter Pushdown Diagnostics

## Purpose

This note narrows the remaining F2 / D3 filter-contract work to diagnostics.
Runtime filtering and access-route selection should not change in this slice.

The current risk is that EXPLAIN can infer predicate pushdown labels by looking
at rendered predicate/access shapes after planning has already happened. That
keeps the user-facing label separate from the planner facts that actually made
the decision.

The target is a planner-owned diagnostics contract consumed by EXPLAIN.

## Implementation Status

The first implementation slice is complete. The finalized static planning
contract now stores `PredicatePushdownDiagnostics` beside
`ResidualFilterContract`, and verbose EXPLAIN renders that planner-owned label.

The initial implementation intentionally preserved existing labels and derived
the reason from existing predicate/access facts only.

The second implementation slice adds explicit verbose
`predicate_pushdown_outcome` and `predicate_pushdown_reason` fields from the
same planner-owned contract. This covers no-filter, expression-only filters
with no predicate subset, access-applied predicates, partial residual
predicates, and full-scan fallback causes while preserving the legacy compact
`predicate_pushdown` label.

Strict index-prefilter observability remains route-level for now through
`diag.r.predicate_stage=index_prefilter(strict_all_or_none)`, because that
stage depends on executor route preparation and covering compatibility. The
stage selection is now centralized in the route-owned
`PredicateStageObservability` contract so descriptor children and verbose
diagnostics cannot drift while the planner-owned predicate-pushdown outcome
contract remains separate.

## Current Problem

The engine already carries these facts, but not as one diagnostics contract:

- visible filter expression;
- derived predicate subset;
- whether the predicate subset fully covers the expression;
- selected access route;
- residual filter shape after access-path satisfaction;
- strict index-predicate capability;
- fallback labels for unsupported predicate pushdown.

Some verbose EXPLAIN labels are currently derived late by scanning the
diagnostic predicate tree for known unsupported shapes such as non-strict
coercion, `IS NULL`, empty `STARTS WITH`, and text scan operators. That is
useful but not authoritative.

## Proposed Vocabulary

Add a small planner-owned pushdown diagnostics artifact with stable labels.
The names can change during implementation, but the concepts should stay small.

`FilterPushdownOutcome`:

- `None`: no filter was present;
- `Full`: the predicate/access plan proves the full filter before residual
  execution;
- `Partial`: some predicate work was pushed down, but residual filtering
  remains;
- `Fallback`: a filter exists, but predicate pushdown was not available.

`FilterPushdownReason`:

- `NoFilter`;
- `NoPredicateSubset`;
- `PredicateSubsetDoesNotCoverExpression`;
- `AccessPathProvesPredicate`;
- `StrictIndexPredicatePrefilter`;
- `ResidualAfterAccess`;
- `NonStrictCompareCoercion`;
- `IsNullRequiresFullScan`;
- `StartsWithEmptyPrefix`;
- `TextOperatorRequiresFullScan`;
- `UnsupportedPredicateShape`;
- `FullScanAccess`.

The labels should be explicit methods on the enum, not ad hoc strings in
EXPLAIN rendering.

## Authority Boundaries

SQL lowering and fluent intent own:

- the visible boolean expression;
- the predicate subset;
- whether the predicate subset fully covers the expression.

Access planning owns:

- which predicate terms are proven by the chosen access route;
- which residual expression/predicate survives;
- whether strict index-predicate prefiltering is available;
- the final pushdown outcome/reason.

EXPLAIN owns:

- rendering the planner-owned outcome/reason labels;
- preserving existing text/JSON field names until a separate compatibility
  decision says otherwise.

EXPLAIN should not continue to classify fallback causes by recursively
inspecting rendered predicates once the planner contract carries the reason.

## First Implementation Slice

Keep the public EXPLAIN shape stable.

1. Add the enum/artifact beside `ResidualFilterContract`.
2. Derive it from existing planner facts without changing route selection.
3. Add accessors on the planned query/static planning contract.
4. Switch verbose predicate-pushdown diagnostics to consume the artifact.
5. Keep existing labels where practical:
   - `none`;
   - `applied(index_prefix)`;
   - `applied(index_range)`;
   - `fallback(non_strict_compare_coercion)`;
   - `fallback(starts_with_empty_prefix)`;
   - `fallback(is_null_full_scan)`;
   - `fallback(text_operator_full_scan)`.

If the first slice cannot prove one existing fallback reason from planner facts,
keep that reason as `UnsupportedPredicateShape` and leave the old inferred
label behind a focused TODO only for that reason. Do not grow the inference
surface.

## Non-Goals

- Do not change which filters are pushed down.
- Do not change access-route ranking.
- Do not introduce a cost model.
- Do not broaden SQL predicate extraction.
- Do not change EXPLAIN field names.
- Do not make generated models authoritative for runtime decisions.

## Tests

Add focused tests for:

- no-filter outcome;
- full predicate pushdown with no residual filter;
- partial predicate pushdown with residual filter;
- fallback for non-strict compare coercion;
- fallback for `IS NULL`;
- fallback for empty `STARTS WITH`;
- fallback for text scan operators;
- JSON/text EXPLAIN label parity.

Snapshot churn is acceptable only when the value is now planner-owned and the
visible label remains intentional.
