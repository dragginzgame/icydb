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

## Order-Term Reuse

Lowered SQL `ORDER BY` terms now carry `LoweredExprAnalysis` for the lowered
planner expression. DISTINCT projection validation consumes that analysis to
prove whether an order term references only projected direct fields, instead of
walking the lowered order expression again through `Expr::references_only_fields`.

This is still intentionally narrow: the order term owns only the same field and
aggregate facts as other analyzed lowered expressions. Type/orderability
inference and richer symbolic derivation remain out of scope.

## Having Reuse

Lowered SQL `HAVING` clauses now carry an `AnalyzedLoweredExpr` artifact. The
grouped aggregate path consumes its aggregate references to resolve projection
aggregate slots, and the global aggregate path consumes the same analysis to
reject direct field leakage and intern HAVING-only aggregate terminals.

This removes the previous post-lowering aggregate/direct-field expression walks
without changing the admitted grouped or global HAVING families.

## Grouped Aggregate Reuse

Grouped SELECT lowering now converts the stable parser aggregate-call list into
validated planner-owned `AggregateExpr`s before HAVING binding. `HAVING`
aggregate-slot resolution compares against those lowered aggregates directly,
and the later query-application phase consumes the same planner expressions
instead of lowering parser aggregate calls again.

The parser-call collection still owns SQL-order discovery for projection and
HAVING clauses. This slice only moves the post-discovery artifact across the
lowering/apply boundary.

## Aggregate Input/Filter Reuse

The local aggregate-call lowering shape now carries analyzed aggregate input and
`FILTER` expressions. Grouped aggregate validation consumes those facts before
the final `AggregateExpr` is built, so alias/unknown-field diagnostics can use
the recorded field-root order without rewalking the lowered input or filter
expression trees.

This is deliberately limited to grouped aggregate lowering. Global aggregate
terminal preparation still validates expression inputs through its existing
typed preparation seam until that path gets its own terminal-preparation
artifact.

## Global Terminal Interning

Global aggregate terminal collection now keeps each retained terminal's semantic
key beside the terminal. Projection and HAVING terminal interning compare
against the retained key list directly instead of rebuilding semantic keys from
all retained terminals on each insert.

This is not the full global terminal-preparation artifact. Expression-input and
filter validation for global terminals remains on the typed preparation seam.

## Deferred

Do not grow this into a broad binder casually. The next extensions need an
explicit shape for:

- type inference results;
- additional ORDER BY expression facts;
- predicate-derivation inputs;
- global aggregate terminal input/filter validation facts;
- phase ownership for scalar, grouped, post-aggregate, and write filters.
