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
- H2 / D9: runtime-visible accepted indexes now cache a sorted reduced semantic
  candidate list in `VisibleIndexes`, and planning/rerank/explain finalization
  consume that list instead of rebuilding semantic index contracts per query.
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
- H3 / F7 third slice: expression analysis now records aggregate leaves in
  lowered expression order, and global aggregate projection lowering consumes
  those analysis-owned leaves instead of walking the same expression again to
  intern executable terminals.
- H3 / F7 fourth slice: lowered SQL `ORDER BY` terms now carry
  `LoweredExprAnalysis`, and DISTINCT projection validation consumes that
  order-term analysis instead of rewalking lowered order expressions for
  direct-field proof.
- H3 / F7 fifth slice: lowered SQL `HAVING` clauses now carry
  `AnalyzedLoweredExpr`; grouped HAVING aggregate-slot resolution and global
  HAVING aggregate/direct-field validation consume that artifact instead of
  rewalking lowered HAVING expressions.
- H3 / F7 sixth slice: grouped SELECT artifacts now carry validated
  planner-owned `AggregateExpr`s, so HAVING aggregate-slot resolution and query
  application reuse the same lowered aggregate list instead of relowering
  parser aggregate calls.
- H3 / F7 seventh slice: aggregate-call lowering shapes now carry analyzed
  grouped aggregate input/FILTER expressions, and grouped aggregate validation
  consumes those facts before building the final `AggregateExpr`.
- H3 / F7 eighth slice: global aggregate terminal collection now stores
  retained semantic keys beside terminals, so projection/HAVING terminal
  interning compares retained keys directly instead of rebuilding them.
- H3 / F7 ninth slice: lowered global aggregate terminals now carry their
  semantic key, analyzed expression input, and analyzed `FILTER` expression
  together through model binding. Strategy preparation validates those
  analyzed facts directly, and global aggregate filters now reject unknown
  fields before execution.
- F2 / D3 first slice: the SQL-lowering filter contract is documented, and
  `LoweredSqlFilter` now owns the current visible-expression plus
  predicate-pushdown construction policies for scalar SELECT, grouped SELECT,
  global aggregate, DELETE, and UPDATE filters.
- F2 / D3 second slice: finalized static planning now stores the residual
  expression, residual predicate subset, and compiled runtime filter program in
  one `ResidualFilterContract` while preserving the existing plan accessors and
  executor behavior.
- F2 / D3 third slice: `ResidualFilterContract` now owns the compact
  diagnostics shape for absent, predicate-only, expression-only, and
  expression-plus-predicate residual filters; execution EXPLAIN nodes and
  verbose route diagnostics consume that shape.
- F2 / D3 fourth slice: the remaining pushdown coverage/fallback diagnostics
  vocabulary is scoped in `filter-pushdown-diagnostics.md`, with EXPLAIN
  intended to consume planner-owned outcome/reason labels in the next code
  slice.
- F4 / D5 / H9 / C6 first slice: compiled encoded index predicates are now
  compared against the canonical runtime predicate program for strict compare
  trees, `IN`, `NOT IN`, large sorted `IN`, text-prefix bounds, and
  conservative `AND` prefilters.
- F4 / D5 / H9 / C6 second slice: an ignored native threshold benchmark now
  compares linear and sorted encoded membership evaluation, and the sorted
  membership cutoff moved from 32 to 16 candidates based on the local signal.
- F2 / D3 fifth slice: verbose predicate-pushdown diagnostics now consume a
  planner-owned `PredicatePushdownDiagnostics` contract stored beside the
  residual-filter contract, preserving existing labels while moving fallback
  reason ownership out of late EXPLAIN predicate-tree scans.
- F2 / D3 sixth slice: verbose predicate-pushdown diagnostics now expose
  planner-owned `outcome` and `reason` fields, covering no-filter,
  expression-only/no-subset, access-applied, residual-after-access, and
  full-scan fallback cases without changing the legacy compact label.
- F2 / D3 seventh slice: strict index-prefilter observability remains
  route-owned but now flows through one `PredicateStageObservability` contract
  shared by execution-descriptor children and verbose diagnostics.

## Current Slice

- H6 / D7 / F6 first slice: SQL write metrics now carry staged-row counts
  beside matched, mutated, and RETURNING row counts, and broad write-shape
  guards plus a SQL perf-matrix hook cover INSERT SELECT, UPDATE, UPDATE
  RETURNING, DELETE, and DELETE RETURNING materialization pressure before any
  streaming/chunked mutation redesign. The local sandbox could not complete
  the live PocketIC run because the PocketIC child exited during startup.

## Next Candidates

- H6 / D7 / F6: run the live PocketIC SQL write materialization matrix in a
  healthy PocketIC environment, record the heap/journaled deltas, and only then
  decide whether chunked mutation preparation needs a separate design slice.
- H3 / F7: extend the analyzed artifact only after a narrow design for type
  inference, additional ORDER BY facts beyond the current field proof, and
  predicate-derivation inputs.
