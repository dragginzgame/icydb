# Filter Contract

## Purpose

The audit finding F2 / D3 identifies SQL filtering as a semantic drift risk:
the visible SQL truth expression, predicate pushdown subset, residual runtime
program, and diagnostics can be derived in different places.

The target architecture is one filter contract that carries these facts
together from SQL lowering through planning and execution.

## SQL-Lowering Slice

This slice stays in SQL lowering. `LoweredSqlFilter` now owns the construction
policy for the visible SQL truth expression and its predicate-pushdown subset.

The named construction paths are:

- scalar SELECT WHERE: scalar visible expression plus optional pushdown subset;
- grouped SELECT WHERE: grouped/base visible expression plus optional pushdown
  subset;
- global aggregate WHERE: visible expression plus required pushdown subset;
- DELETE WHERE: scalar visible expression plus `TRUE` fallback when no pushdown
  subset is extractable;
- UPDATE WHERE: scalar visible expression plus required strict SQL predicate
  subset.

This did not change planner or executor behavior. It removed duplicated
caller-side derivation so future planner/executor contract work has a single
SQL-lowering owner to consume.

## Planner Residual Slice

The next narrow slice keeps execution behavior unchanged while making finalized
planner state harder to misread. `StaticExecutionPlanningContract` now carries
one `ResidualFilterContract` that owns:

- visible residual expression after access-path satisfaction;
- residual predicate subset after access-path satisfaction;
- compiled runtime filter program derived from that residual shape.

Existing `AccessPlannedQuery` accessors still expose the same logical facts to
executor, explain, and test callers. The important ownership change is that
finalized static planning no longer stores these as three loose sibling fields.
The contract documents that they are one post-access filter artifact.

## Diagnostics Slice

`ResidualFilterContract` now derives a compact `ResidualFilterShape` for
diagnostics:

- `none`;
- `predicate`;
- `expression`;
- `expression_and_predicate`.

Execution EXPLAIN residual nodes and verbose route diagnostics consume this
planner-owned shape instead of inferring residual-filter kind from rendered
expression or predicate strings.

## Deferred

The larger filter contract still needs to carry richer proof/fallback facts:

- visible SQL truth expression;
- pushdown predicate subset;
- pushdown coverage proof and fallback reason for diagnostics.

Do not merge the remaining work with route planning in one pass. The next safe
step is to design the coverage/fallback vocabulary without changing
access-route selection or runtime filter evaluation.
