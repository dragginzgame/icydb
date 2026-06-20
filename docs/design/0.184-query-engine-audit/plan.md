Yes. Treat D / F / H / C as different views of the same work, not separate backlogs:

D = named divergence findings
F = duplicate/competing flows
H = performance hot paths
C = correctness risk tests

The best order is correctness locks → low-risk hot paths → duplicate-flow consolidation → larger architecture.

Recommended full order
0. Establish the safety net first

Do these before production refactors:

0.1 C1 / D4 / F2
Done in 0.184.0:
Add IN / NOT IN with NULL tests and route membership through canonical SQL
boolean lowering before predicate extraction.

0.2 C2 / C3 / D2 / F1
Done in 0.184.0:
Add SQL NULL boolean-composition and expression-evaluator parity tests.

0.3 C6 / D5 / F4
Done in 0.184.0:
Add index route vs forced full-scan equivalence tests, including the
branch-aware composite route.

0.4 C4 / D10
Done in 0.184.0:
Add grouped ORDER BY tie / pagination stability tests.

0.5 C5 / D1 / F3
Done in 0.184.0:
Add global aggregate lane equivalence tests. The larger shared aggregate
operator remains deferred to Phase 6.

0.6 C7 / D7 / F6
Done in 0.184.0:
Add DELETE / UPDATE / RETURNING residual-filter parity tests. The larger
mutation-candidate operator work remains deferred to Phase 5.

This matters because the audit’s main warning is semantic drift between parallel lanes: global aggregates vs general SELECT, compiled expressions vs preview evaluation, predicate subsets vs visible SQL filters, and materialized vs streaming scalar execution.

Phase 1 — Easy wins with low semantic risk
1. H1 / D8: single-pass cache miss classification

Done in 0.184.0.

Finding:
H1 / D8

Why first:
- Low risk
- Localized
- Performance cleanup
- Does not change query semantics

Target:
crates/icydb-core/src/db/session/query/cache.rs

2. H2 / D9: cache sorted semantic index contracts

Finding:
H2 / D9

Why second:
- Still mostly planner-side
- Improves compile/planning overhead
- Slightly more risk than H1 because index ordering and visibility matter

Needs:
- planner determinism tests
- EXPLAIN stability checks if present

Result:
Done after H1 and the planner determinism checks. `VisibleIndexes` now stores
one sorted reduced semantic candidate list for accepted runtime-visible indexes.
Runtime access planning, residual-burden reranking, and accepted EXPLAIN
access-choice finalization consume that list instead of rebuilding semantic
index contracts from accepted field-path/expression metadata per query. The
accepted field-path contracts remain available for the order-only fallback path
that still needs field-path-specific order proof.

Phase 2 — Remove low-level duplicate flows
3. F5 / D6 / H8: share scalar route preparation — done
Finding:
F5 / D6 / H8

Why here:
- Duplicated materialized vs streaming setup is a maintainability drift risk
- It may also normalize page-fetch hints
- It should not change row semantics if done carefully

Target:
crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/materialized.rs
crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/streaming.rs

Implemented as a private scalar execution helper that owns shared route hinting,
continuation validation, trace setup, plan metrics, stats capture, and
`ExecutionInputs` assembly. Materialized pages keep their page-specific
branch/multi-lookup fetch hint as a route-adjustment closure; aggregate row
sinks use the same shared setup with no extra page hint.

4. H5: benchmark runtime expression reader dispatch

Do not optimize this yet.

Finding:
H5

Action:
Benchmark first.

Why:
Trait-object / RefCell reader dispatch may be hot, but it may also be lost in storage/materialization cost. Measure before specializing.

If it is not clearly hot, leave it alone.

Result:
Done in 0.184.0 as an ignored native microbenchmark beside the executor scalar
reader adapters. The cleaned run measured direct slice access at roughly 97us
per iteration, borrowed callback reader dispatch at roughly 97us, mixed `Cow`
callback dispatch at roughly 112us, and owned callback dispatch at roughly
102us over 512 rows and 1,024 iterations. The overhead is measurable in the
mixed/owned callback paths but not high enough to justify a second production
evaluator path before the semantic convergence items.

Phase 3 — Expression convergence
5. F1 / D2 / H4 / C3: make preview evaluation reuse CompiledExpr
Finding:
F1 / D2 / H4 / C3

Why now:
- This removes a real semantic duplicate
- It is medium risk
- It should be protected by parity tests from Phase 0

Target:
CompiledExpr becomes the only executable expression IR.

This is one of the highest-value medium slices. The audit explicitly identifies duplicate scalar evaluators: CompiledExpr::evaluate and direct preview evaluation.

Good implementation strategy:

Step 1:
Add parity tests.

Step 2:
Create tiny preview reader compatible with CompiledExpr.

Step 3:
Route preview evaluation through compilation.

Step 4:
Delete or heavily shrink the direct recursive evaluator.

Result:
Done in 0.184.0. Builder preview expressions now compile into the shared
`ScalarProjectionExpr` / `CompiledExpr` path and evaluate through a one-slot
preview reader. The old recursive preview evaluator was removed, while the
shared function evaluator remains the source of compact projection error
reasons.
6. H3 / F7: typed/analyzed expression pass
Finding:
H3 / F7

Why after D2:
Once expression execution is centralized, centralize expression analysis/lowering.

Goal:
Reduce repeated expression walking across projection, aggregate collection, HAVING, ORDER BY, and predicate derivation.

This should be design-first, not a casual refactor. It starts moving toward a real binder artifact.

First slice:
Lowered SQL expression analysis now performs one traversal for aggregate
presence, direct field roots, field-path presence, and unknown-field
diagnostics. Grouped projection validation and grouped aggregate collection
consume the analysis proof for grouped-field authority instead of walking the
expression tree again.

Second slice:
Lowered SELECT-item consumers that need the expression and its facts now use an
`AnalyzedLoweredExpr` artifact. This keeps grouped/global projection lowering
from treating the lowered expression and its aggregate/field proof as separate
loose values, without broadening the artifact into a full binder product yet.

Third slice:
The short expression artifact note is recorded in
`docs/design/0.184-query-engine-audit/expression-analysis-artifact.md`.
`LoweredExprAnalysis` now records aggregate leaves in left-to-right lowered
expression order, and global aggregate projection lowering consumes those
analysis-owned leaves when interning executable aggregate terminals instead of
walking the same expression tree again.

Deferred:
The broader typed/analyzed expression artifact still needs a short design before
it carries type inference, aggregate input/filter validation facts, ORDER BY
facts, and predicate derivation inputs.

Phase 4 — Filter and predicate contract
7. F2 / D3: introduce unified filter contract
Finding:
F2 / D3

Why here:
- Bigger than D2
- Needs NULL, IN, full-scan/index equivalence tests first
- Core semantic risk area

Goal:
One object carrying:
- visible SQL truth expression
- pushdown predicate subset
- residual runtime program
- coverage proof / reason

This is probably the most important architectural correction in the whole audit, but it should not be first. It touches the planner/executor boundary.

First slice:
The SQL-lowering side is documented in
`docs/design/0.184-query-engine-audit/filter-contract.md`.
`LoweredSqlFilter` now owns the current construction policies for pairing the
visible SQL truth expression with its predicate-pushdown subset across scalar
SELECT, grouped SELECT, global aggregate, DELETE, and UPDATE filters. Planner
and executor residual-filter behavior is intentionally unchanged.

Second slice:
Finalized static planning now carries one `ResidualFilterContract` for the
post-access residual expression, residual predicate subset, and compiled
runtime filter program. Existing plan accessors still present the same facts to
executor and explain callers, but the finalized planning artifact no longer
stores the residual filter as loose sibling fields.

Third slice:
`ResidualFilterContract` now derives the compact diagnostics shape for
absent, predicate-only, expression-only, and expression-plus-predicate residual
filters. Execution EXPLAIN residual nodes and verbose route diagnostics consume
that planner-owned shape instead of inferring residual kind from rendered
strings.

Deferred:
Richer coverage proof and fallback/explain reason should move onto the same
contract only after a narrow diagnostics vocabulary design. Do not fold this
into route planning or access-choice ranking in the same slice.

8. F2 / D4 / C1: route membership through canonical boolean lowering
Finding:
D4 / C1

Status:
Done in the first 0.184.0 audit slice. This was safe before the full filter
contract because it removed a parser-level bypass and kept predicate pushdown
as an extraction from the canonical lowered boolean expression.

Why after D3:
The top-level membership shortcut should be removed only after the unified filter contract exists or at least after the NULL/IN test suite is strong.

Goal:
`IN` / `NOT IN` remains SQL-truth-correct first, pushdown-optimized second.

Do not let predicate optimization define SQL semantics.

9. F4 / D5 / H9 / C6: prove and tune encoded index predicate evaluator
Finding:
F4 / D5 / H9 / C6

Order:
1. property equivalence tests
2. benchmark IN thresholds
3. tune encoded path only if data supports it

Keep the encoded fast path. Mature engines absolutely use encoded/index-specific predicate evaluation. The key is proving it equivalent to canonical predicate semantics.

Phase 5 — Write-path materialization
10. H6 / D7 / F6 / C7 / C8: write candidate bounds and benchmarks
Finding:
H6 / D7 / F6 / C7 / C8

Why not earlier:
- High impact
- Higher semantic risk
- Touches mutation atomicity, RETURNING, ordering, and failure behavior

First step:
Add bounds, metrics, and benchmarks. Do not stream/chunk yet.

Start with observability and limits:

- large DELETE count
- DELETE RETURNING
- UPDATE broad selector
- UPDATE with RETURNING
- INSERT SELECT large source
- failure atomicity tests

Only after that should you design chunked mutation preparation.

11. F8 / H10: reuse compiled SELECT for INSERT SELECT
Finding:
F8 / H10

Done in 0.184.0.

Why after write-path tests:
It removes reparse/rebind duplication, but it affects write execution.

Goal:
INSERT SELECT should reuse the already lowered/compiled source SELECT artifact.

This is medium-risk but valuable. Do it before chunked mutation work.

12. Streaming/chunked mutation pipeline
Finding:
D7 / H6 / larger roadmap

Why late:
This is a real architecture change.

Preconditions:
- bounds decided
- atomicity tests
- RETURNING tests
- selector parity tests
- INSERT SELECT tests

This should be its own design doc or major slice.

Phase 6 — Aggregate architecture
13. F3 / D1 / C5: shared aggregate logical/physical operator
Finding:
F3 / D1 / C5

Why late:
The global aggregate lane may be a performance fast path, but it must become a specialization of shared aggregate semantics.

Goal:
Global aggregate should not be a competing semantic lane.

Do this after aggregate equivalence tests. Preserve the fast path if it is useful, but derive it from the same aggregate contract.

Phase 7 — Larger planner/operator architecture
14. Operator-level physical plan
Finding:
Best-practice gap / larger architectural work

Why late:
This is foundational but invasive.

Goal:
Expose scan/filter/project/sort/aggregate/limit/mutation operators cleanly.

Do not start here. Use the earlier slices to discover what the operator model actually needs.

15. Cost/selectivity-aware planning
Finding:
Longer-term planner improvement

Why last:
Stats and selectivity are only useful after deterministic planning, index contract caching, and route semantics are stable.

This is a later maturity step, not a current cleanup step.

Compact master order
0. Test safety net:
   C1, C2, C3, C4, C5, C6, C7, C8

1. H1 / D8:
   single-pass cache miss classification

2. H2 / D9:
   cache sorted semantic index contracts

3. F5 / D6 / H8:
   shared scalar route preparation

4. H5:
   benchmark expression reader dispatch before optimizing

5. F1 / D2 / H4:
   preview evaluation through CompiledExpr

6. H3 / F7:
   typed/analyzed expression pass

7. F2 / D3:
   unified filter contract

8. D4 / C1:
   canonical membership lowering, no shortcut semantics

9. F4 / D5 / H9:
   index predicate equivalence tests, then tune encoded IN threshold

10. H6 / D7 / F6:
    write candidate bounds, metrics, benchmarks

11. F8 / H10:
    reuse compiled SELECT for INSERT SELECT

12. D7 / H6:
    chunked mutation pipeline design

13. F3 / D1:
    shared aggregate operator

14. physical operator plan / EXPLAIN expansion

15. cost/selectivity-aware planner
What I would actually give Codex next
Use the query-engine audit as the source of truth.

Create a PR-sized implementation roadmap that orders all D, F, H, and C findings into dependency-safe slices.

Rules:
- Do not implement code yet.
- Tests must precede semantic refactors.
- Low-risk localized performance fixes should precede architectural rewrites.
- Do not merge correctness fixes with performance refactors unless unavoidable.
- Explicitly map every D, F, H, and C item to at least one slice.
- Identify prerequisites, affected files, expected behavior changes, tests, benchmarks, and rollback strategy for each slice.
- Mark any item that should remain benchmark-only until evidence proves it is hot.

Preferred ordering:
1. semantic regression tests
2. single-pass cache miss classification
3. cached sorted index contracts
4. shared scalar route preparation
5. expression evaluator convergence
6. typed expression analysis
7. unified filter contract
8. canonical membership lowering
9. index predicate equivalence and tuning
10. write-path bounds and benchmarks
11. INSERT SELECT source reuse
12. chunked mutation design
13. shared aggregate operator
14. operator-level physical plan
15. cost/selectivity planning

Output a table:
| Slice | Items Covered | Purpose | Prerequisites | Files Likely Touched | Tests | Benchmarks | Risk | Stop Condition |

My instinct: start with tests, then H1, then D6/F5, then D2/F1. That gives you fast confidence, removes real duplicate flow, and avoids getting trapped in the big D1 aggregate refactor too early.
