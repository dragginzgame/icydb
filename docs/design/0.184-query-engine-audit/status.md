# 0.184 Status

Status: active.

## Completed

- C1 / D4 / F2: SQL `IN` / `NOT IN` with `NULL` lowers through canonical
  boolean-expression semantics before predicate pushdown extraction.
- C2 / C3 / D2 / F1: SQL NULL boolean-composition behavior and
  preview-evaluator versus `CompiledExpr` parity are covered before evaluator
  convergence work.
- C6 / D5 / F4: ready index routes are compared against forced full-scan
  fallbacks for predicate and branch-set query shapes.
- H1 / D8: shared query-plan cache miss classification now walks warmed cache
  keys once while preserving the existing miss-reason priority.
- C4 / D10: grouped pagination now has explicit tests for order-prefix ties
  failing closed and full group-key tie-breakers paging deterministically.
- F5 / D6 / H8: materialized scalar pages and streaming aggregate row sinks now
  share scalar route hinting, continuation validation, trace setup, plan
  metrics, stats capture, and `ExecutionInputs` assembly.
- C5 / D1 / F3: dedicated global aggregates are now cross-checked against the
  grouped singleton lane for HAVING aliases, searched CASE, filtered
  aggregates, and post-aggregate projection expressions.
- C7 / D7 / F6: write residual filters now compare SELECT targets, UPDATE
  RETURNING targets, DELETE RETURNING targets, and non-returning DELETE counts
  across CASE/NULL/OR/AND predicates.
- C8 / F8 / H10: compiled `INSERT ... SELECT` commands now carry the bound
  source query artifact, so execution reuses the compiled source instead of
  preparing and binding the SELECT source again.
- H5: runtime expression-reader dispatch now has an ignored native
  microbenchmark. The first cleaned measurement showed borrowed callback
  dispatch roughly level with direct slice access, mixed `Cow` callback dispatch
  about 15% over direct access, and owned callback dispatch about 5% over direct
  access for a synthetic expression-heavy retained-row loop, so specialization
  is deferred until broader profiling makes it a top hotspot.
- F1 / D2 / H4 / C3: builder preview projection evaluation now compiles to the
  shared `ScalarProjectionExpr` / `CompiledExpr` path and evaluates through a
  one-slot preview reader, while compact projection error reasons from shared
  function evaluation are preserved.
- H3 / F7 first slice: lowered SQL expression analysis now gathers aggregate
  presence, direct field roots, field-path presence, and unknown-field
  diagnostics in one pass. Grouped projection validation and grouped aggregate
  collection consume that analysis for grouped-field authority instead of
  walking the expression tree again.
- H3 / F7 second slice: lowered SELECT items that also need expression facts
  now flow through an `AnalyzedLoweredExpr` artifact, so grouped/global
  projection consumers receive the lowered expression and its aggregate/field
  proof as one contract instead of loose adjacent values.

## Current Slice

- Ready for the next audit slice.

## Next Candidates

- H3 / F7: extend the analyzed artifact only after a narrow design for type
  inference, aggregate references, ORDER BY facts, and predicate-derivation
  inputs.
