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

## Global Terminal Preparation

Lowered global aggregate terminals now carry their semantic key beside the
analyzed aggregate input and analyzed aggregate-local `FILTER` expression. The
model-bound strategy seam consumes that lowered terminal artifact, validates
expression inputs and filters from the existing analysis proof, and only then
projects the terminal into the executor-neutral prepared strategy.

This keeps terminal de-duplication, input validation, and filter validation on
one SQL-lowering contract instead of rebuilding terminal meaning from a raw
terminal DTO during typed binding.

## Projection Source Capability Reuse

Lowered SQL projection expressions now keep their `LoweredExprAnalysis` beside
the SQL-local projection wrapper until the accepted-schema SELECT binding seam.
`LoweredExprAnalysis` records direct field references and field-path references
in expression traversal order, and projection source-field capability validation
consumes those recorded references instead of walking the projection expression
tree again.

The reuse is deliberately SQL-local. Core `ProjectionSelection` still carries
only the planner projection contract consumed by query planning and execution.

## Grouped Projection Aggregate Reuse

Grouped SELECT projection lowering now returns a SQL-local grouped projection
artifact that owns both the lowered projection and the first-seen unique SQL
aggregate-call list derived from that same pass. HAVING clauses still extend
that aggregate list afterward, preserving the existing grouped reducer slot
ordering and identity-projection fast-path gate.

This removes the previous grouped projection collector pass that validated and
analyzed projection items only to rediscover aggregate calls before the
projection builder analyzed the same items again.

## Global Aggregate Output Ordering Reuse

Global aggregate terminal lowering now records the projection expressions and
aliases that identify singleton aggregate output columns. The global aggregate
command uses those recorded targets to drop inert `ORDER BY` terms over the
single output row, instead of asking a separate ORDER BY helper to lower and
analyze the projection before terminal lowering analyzes it again.

Base-row ORDER BY terms still lower through the normal base-query tail after
the inert output targets are filtered out.

## Scalar Projection Phase Ownership

Scalar projection lowering no longer pre-scans parser SELECT items for
aggregate leaves. Aggregate rejection for scalar projections is owned by
`SqlExprPhase::Scalar` at the shared SQL-expression lowering seam, so SELECT
projection admission does not duplicate the parser aggregate walk before the
lowered expression is analyzed.

## Deferred

Do not grow this into a broad binder casually. The next extensions need an
explicit shape for:

- type inference results;
- additional ORDER BY expression facts;
- predicate-derivation inputs;
- phase ownership for scalar, grouped, post-aggregate, and write filters.

## Next Extension Gate

The next extension should delete a concrete duplicate consumer, not just add a
larger analysis object.

Safe boundaries:

- type inference remains planner-owned and schema-bound. Do not add an optional
  inferred type to every `AnalyzedLoweredExpr`; introduce a separate
  schema-bound typed artifact only at seams that already hold `SchemaInfo` and
  already call planner type inference;
- ORDER BY facts should stay clause-owned. The current artifact can prove
  direct-field derivability for DISTINCT. Richer ORDER BY facts should be added
  only when a second ORDER BY consumer can reuse them without re-inferring
  expression type or route orderability;
- predicate-derivation inputs belong beside `LoweredSqlFilter`, because filter
  lowering owns the visible SQL truth expression and pushdown predicate subset;
- projection source-field capability validation is the model for the next
  cleanup: it became a real win only when projection lowering carried analyzed
  expression facts across the schema-binding seam and deleted the old
  validation-local expression walk.

Preferred next code slice:

1. Pick one schema-bound consumer that currently walks an expression tree after
   SQL lowering.
2. Carry the already-lowered expression plus the exact fact that consumer needs
   through a narrow artifact.
3. Delete the old consumer-local walk in the same slice.
4. Add a guard test proving the consumer receives the analyzed fact rather than
   reconstructing it from the raw expression.
