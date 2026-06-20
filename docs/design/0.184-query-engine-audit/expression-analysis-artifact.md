# Expression Analysis Artifact

## Purpose

The 0.184 query-engine audit is converging SQL lowering onto one analyzed
expression artifact before larger filter, aggregate, and operator rewrites.

This artifact is intentionally smaller than a full binder product. It records
facts that are already proven during lowering and that multiple SQL lowering
lanes currently rediscover by walking the same `Expr` tree again.

## Current Contract

`AnalyzedLoweredExpr` owns:

- the lowered planner `Expr`;
- the `LoweredExprAnalysis` derived from that exact expression.

`LoweredExprAnalysis` currently records:

- aggregate leaves in left-to-right lowered expression order;
- direct field roots outside aggregate-owned subtrees;
- whether any field path was referenced;
- the first unknown field diagnostic.

Aggregate nodes are traversal leaves for the outer expression analysis. This is
intentional: fields inside aggregate inputs or aggregate filters are owned by
the aggregate semantic path and must not be counted as outer direct-field
leakage.

## First Reuse

Global aggregate projection lowering consumes the analysis-owned aggregate
leaves when interning executable aggregate terminals. That removes a second
aggregate-only walk after the expression has already been analyzed.

## Deferred

Do not grow this into a broad binder casually. The next extensions need an
explicit shape for:

- type inference results;
- ORDER BY expression facts;
- predicate-derivation inputs;
- aggregate input/filter validation facts;
- phase ownership for scalar, grouped, post-aggregate, and write filters.

