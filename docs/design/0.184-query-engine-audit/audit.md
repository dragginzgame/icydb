# 0.184 Audit

You are auditing the IcyDB query engine.

Goal:
Perform a full read-only audit of the query engine and identify bad practices, architectural divergence, duplicated logic, avoidable query hot paths, correctness risks, and the highest-value improvements. Treat this as a database-engine-quality audit, not a superficial code review.

Do not make code changes yet unless explicitly asked later. First produce a structured audit report with evidence, file paths, specific functions/types/modules, and prioritized recommendations.

Context:
IcyDB is a database system with a complex query engine. Recent audits have found repeated hotspots in queries. We want to confirm whether the current direction is sound and identify improvements aligned with best practices common to mature database engines, especially SQLite-like principles:
- clear separation between parsing, binding/name resolution, planning, optimization, and execution
- stable internal representations for expressions, predicates, plans, rows, values, cursors, and transactions
- predictable semantics for NULL, type coercion, collations, ordering, grouping, limits, aggregates, joins, and error propagation
- cost-conscious query planning and index selection
- minimal duplicate execution paths
- reusable expression evaluation and predicate logic
- bounded memory use, streaming where possible, and avoidance of unnecessary materialization
- deterministic behavior, testable semantics, and benchmarkable performance

Audit scope:
Inspect the entire query engine and all adjacent components that influence query execution, including but not limited to:
- SQL parser / query parser
- AST or query IR
- binder / resolver / semantic analyzer
- expression representation and expression evaluator
- predicate handling
- query planner
- optimizer rules
- physical plan generation
- executor / VM / iterator pipeline
- scan, filter, projection, join, aggregate, sort, limit, insert, update, delete flows
- index selection and index access paths
- storage-layer boundaries used by queries
- transaction/concurrency interactions visible to queries
- error handling and diagnostics
- tests, benchmarks, fixtures, and golden outputs related to queries

First pass: map the architecture.
1. Identify the main query-engine modules and produce a concise architecture map.
2. Describe the query lifecycle from input SQL/API call to final result.
3. List the major intermediate representations used across the lifecycle.
4. Identify whether each stage has a clear ownership boundary or whether responsibilities are mixed.
5. Identify places where the same concept is represented multiple ways.

Second pass: find divergence and duplicate flows.
Look for cases where similar logic exists in multiple places, especially:
- expression evaluation duplicated across planner, optimizer, executor, indexes, filters, constraints, or tests
- predicate normalization duplicated or inconsistently implemented
- type coercion / comparison / NULL handling duplicated
- row/value encoding or decoding duplicated
- ORDER BY / GROUP BY / DISTINCT / LIMIT behavior duplicated
- scan/filter/projection paths implemented separately for different query types
- separate SELECT/UPDATE/DELETE logic that should share planning or execution machinery
- special-case paths that bypass the planner or executor
- index and non-index execution paths with different semantics
- tests encoding behavior that differs from production code

For each duplicate or divergent flow, report:
- exact files/functions/types involved
- what behavior is duplicated or divergent
- why it is risky
- whether it is a correctness risk, performance risk, maintainability risk, or testability risk
- recommended consolidation target

Third pass: identify query hotspots and performance risks.
Look for:
- repeated AST walking
- repeated expression parsing or binding
- repeated allocation/cloning in hot loops
- row-by-row dynamic dispatch where batching/iterator reuse would help
- unnecessary materialization of intermediate rows
- full scans where indexes could be used
- poor predicate pushdown
- late filtering that could occur earlier
- sort or aggregate materialization that could be avoided
- repeated schema lookups
- repeated name resolution
- repeated type coercion
- inefficient joins
- nested loops without selectivity checks
- poor LIMIT handling
- poor projection pruning
- lack of prepared-plan caching where applicable
- excessive string allocation or string-based dispatch
- unnecessary conversion between row/value formats
- lock/transaction overhead inside per-row loops
- poor memory ownership patterns
- unbounded intermediate collections

For each hotspot:
- provide evidence from code
- identify the hot path
- explain why it is likely hot
- estimate likely impact: low / medium / high
- propose a benchmark that would expose it
- propose the smallest safe improvement
- note any correctness risks from changing it

Fourth pass: compare against database-engine best practices.
Use mature database-engine design principles as a reference point, especially SQLite-style discipline, without assuming IcyDB must copy SQLite internals exactly.

Evaluate whether IcyDB has:
- a clean logical plan representation
- a clean physical plan representation
- stable expression/value semantics
- centralized comparison and NULL behavior
- centralized type affinity/coercion logic
- centralized error propagation
- deterministic planner decisions
- reusable scan/filter/project/join/aggregate operators
- clear iterator/cursor/executor abstractions
- a planner that can reason about indexes
- predicate pushdown
- projection pruning
- limit pushdown where safe
- ordering/index exploitation
- clear distinction between compile-time query analysis and runtime execution
- explain/debug output sufficient to understand plans
- query benchmarks that isolate planner and executor regressions

For each missing or weak area:
- state the current state based on code evidence
- explain the expected best-practice direction
- recommend an incremental migration path
- avoid large rewrites unless clearly justified

Fifth pass: correctness and semantic risk audit.
Search for subtle query correctness issues involving:
- NULL comparisons and three-valued logic
- NOT / AND / OR simplification
- operator precedence
- constant folding
- type conversions
- integer/float/text/blob comparison behavior
- collation or string comparison assumptions
- alias resolution
- ambiguous column references
- table qualification
- wildcard expansion
- aggregate semantics
- GROUP BY semantics
- HAVING vs WHERE
- DISTINCT behavior
- ORDER BY stability and expression references
- LIMIT/OFFSET edge cases
- joins and outer join null-extension, if implemented
- subqueries, if implemented
- indexes returning stale or semantically different results
- transaction visibility during scans
- errors swallowed or converted into empty results
- panic/exception paths in query execution

For each issue:
- provide file/function evidence
- give a minimal SQL or API example that may expose the problem
- classify as confirmed bug, likely bug, or risk requiring test
- recommend a regression test

Sixth pass: test and benchmark audit.
Inspect query tests and benchmarks.
Identify:
- missing semantic coverage
- duplicated tests that mask architectural duplication
- lack of negative tests
- lack of randomized/differential testing
- lack of plan-shape tests
- lack of EXPLAIN-style assertions, if relevant
- lack of benchmarks for known hotspots
- benchmarks that do not isolate planner vs executor vs storage cost
- lack of regression tests for index/non-index equivalence

Recommend:
- top 10 regression tests to add
- top 10 benchmarks to add
- any differential testing strategy against SQLite for shared SQL semantics where feasible
- any property-based test strategy for expression evaluation, predicates, and index equivalence

Seventh pass: prioritize improvements.
Create a prioritized roadmap with three horizons:

A. Immediate low-risk improvements
These should be small changes that reduce bugs or hotspots without changing architecture heavily.

B. Medium-sized refactors
These should consolidate duplicate flows, centralize semantics, or improve planner/executor boundaries.

C. Larger architectural improvements
These may include new IRs, iterator abstractions, plan caching, cost model improvements, optimizer framework, or EXPLAIN output.

For each recommendation include:
- title
- severity: critical / high / medium / low
- category: correctness / performance / architecture / maintainability / testing
- affected files/modules
- evidence
- rationale
- proposed fix
- expected impact
- risk of change
- suggested tests
- suggested benchmarks
- whether it should be done before or after other recommendations

Output format:
Produce the audit report in this structure:

# IcyDB Query Engine Audit

## 1. Executive Summary
- Overall assessment
- Biggest correctness risks
- Biggest performance hotspots
- Biggest architectural risks
- Highest ROI improvements

## 2. Query Engine Architecture Map
- Lifecycle diagram in text form
- Major modules
- Major IRs/data structures
- Ownership boundaries
- Boundary violations

## 3. Confirmed Bad Practices and Divergence
Use a table:
| ID | Severity | Category | Location | Finding | Evidence | Recommendation |

## 4. Duplicate or Competing Flows
Use a table:
| ID | Concept | Flow A | Flow B | Risk | Consolidation Target |

## 5. Hot Path and Performance Findings
Use a table:
| ID | Hot Path | Why Hot | Evidence | Impact | Smallest Safe Fix | Benchmark |

## 6. Correctness and SQL Semantics Risks
Use a table:
| ID | Area | Risk | Example Query/API | Evidence | Test Needed | Fix Direction |

## 7. Best-Practice Gaps Compared With Mature DB Engines
Use a table:
| Area | Current State | Best-Practice Direction | Recommended Migration |

## 8. Test and Benchmark Gaps
- Missing regression tests
- Missing benchmark scenarios
- Suggested SQLite differential tests where applicable
- Suggested property-based tests

## 9. Prioritized Roadmap
### Immediate
### Medium-Term
### Larger Architectural Work

## 10. Top 10 Recommended Next Actions
Rank these by expected value and safety.

Rules:
- Be specific. Do not give generic advice.
- Every important claim must cite concrete files/functions/types from the repository.
- Do not assume implementation details without confirming them in code.
- Distinguish confirmed findings from hypotheses.
- Prefer incremental improvements over rewrites.
- Identify places where the current design is already good and should be preserved.
- Do not change files in this audit pass.
- If repository search is incomplete, state exactly what could not be inspected.