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

## Current Slice

- Ready for the next audit slice.

## Next Candidates

- C8 / F8 / H10: INSERT SELECT source-preparation parity and reuse audit.
