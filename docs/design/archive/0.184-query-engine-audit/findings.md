# 0.184 Design

• # IcyDB Query Engine Audit

  ## 1. Executive Summary

  Overall, IcyDB’s query engine is moving in a sound direction. The strongest parts are
  the explicit staged modules, the frozen execution contract, normalized predicate
  planning, shared query-plan caching, deterministic index candidate ordering, and the
  CompiledExpr pipeline. The codebase already shows database-engine discipline in
  several places, especially around accepted-schema authority, plan cache identity,
  EXPLAIN descriptors, index/full-scan convergence tests, and SQL NULL tests.

  The biggest risk is not that the system lacks structure; it is that several parallel
  lanes are now powerful enough to drift: SQL global aggregates vs general SELECT/
  grouped execution, compiled-expression evaluation vs preview expression evaluation,
  predicate subsets vs visible SQL filter expressions, and materialized vs streaming
  scalar execution setup.

  The highest ROI work is to consolidate duplicate execution/evaluation paths without a
  rewrite: make preview expression evaluation reuse CompiledExpr, share scalar route-
  preparation code, add NULL/IN regression tests, improve query-plan cache miss
  accounting, and add index/full-scan property tests.

  No files were changed during this audit.

  ## 2. Query Engine Architecture Map

  Lifecycle:

  SQL string
    -> parser/tokenizer
    -> SQL AST
    -> SQL lowering / binding / capability validation
    -> StructuralQuery / LoweredSqlCommand
    -> query-plan cache key
    -> access planning / predicate normalization / index selection
    -> logical query plan
    -> static execution planning contract
    -> execution preparation
    -> route runtime / key stream / index prefilter
    -> residual filter / projection / grouping / sort / limit
    -> SQL rows, structural rows, mutation rows, or aggregate result

  Major modules:

   Stage           Parser
   Main locations  crates/icydb-core/src/db/sql/parser/mod.rs:78
   Notes           Parses one SQL statement, handles semicolon rules and attribution.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           SQL lowering / binding
   Main locations  crates/icydb-core/src/db/sql/lowering/mod.rs, crates/icydb-core/src/
                   db/sql/lowering/select/mod.rs:227, crates/icydb-core/src/db/sql/
                   lowering/predicate/mod.rs:31
   Notes           Lowers SQL into structural query shapes and predicate subsets.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Expression IR
   Main locations  crates/icydb-core/src/db/query/plan/expr/mod.rs:1, crates/icydb-core/
                   src/db/query/plan/expr/compiled_expr/mod.rs:565
   Notes           CompiledExpr is the intended scalar-expression runtime IR.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Predicate semantics
   Main locations  crates/icydb-core/src/db/predicate/semantics.rs:28
   Notes           Central predicate comparison behavior, including explicit predicate-
                   level Null == Null.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Access planning
   Main locations  crates/icydb-core/src/db/query/plan/access_planner.rs:73, crates/
                   icydb-core/src/db/query/plan/planner/mod.rs:261
   Notes           Normalizes predicates, selects key/index/full-scan routes
                   deterministically.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Logical planning
   Main locations  crates/icydb-core/src/db/query/plan/logical_builder.rs:118
   Notes           Builds scalar/grouped logical query state.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Static execution contract
   Main locations  crates/icydb-core/src/db/query/plan/access_plan.rs:182
   Notes           Freezes slot maps, filters, projections, grouping, ordering, and
                   index compile targets.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Plan cache
   Main locations  crates/icydb-core/src/db/session/query/cache.rs:74
   Notes           Cache identity includes method, schema version/fingerprint,
                   visibility, and structural query.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Execution preparation
   Main locations  crates/icydb-core/src/db/executor/planning/preparation.rs:59
   Notes           Builds runtime/index predicate programs from plan or runtime filter
                   source.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Scalar execution
   Main locations  crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/
                   materialized.rs:47, crates/icydb-core/src/db/executor/pipeline/
                   entrypoints/scalar/streaming.rs:39
   Notes           Two similar scalar route setup paths.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           Kernel / terminal materialization
   Main locations  crates/icydb-core/src/db/executor/kernel/mod.rs:31, crates/icydb-
                   core/src/db/executor/terminal/page/mod.rs:195
   Notes           Handles route attempts, residual retry, key streams, rows, cursors.
  ──────────────────────────────────────────────────────────────────────────────────────
   Stage           SQL execution routing
   Main locations  crates/icydb-core/src/db/session/sql/execute/mod.rs:391
   Notes           Dispatches SELECT, DELETE, global aggregate, EXPLAIN, INSERT, UPDATE.

  Major IRs / data structures:

   IR / type                             Location            Role
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   LoweredSqlCommand, LoweredSqlQuery    crates/icydb-       SQL frontend output.
                                         core/src/db/sql/
                                         lowering/mod.rs
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   LoweredSelectShape                    crates/icydb-       SELECT shape: projection,
                                         core/src/db/sql/    grouping, distinct,
                                         lowering/select/    having, filter, order,
                                         mod.rs:128          limit, offset.
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   LoweredSqlFilter                      crates/icydb-       Carries visible SQL
                                         core/src/db/sql/    expression plus predicate
                                         lowering/select/    subset.
                                         mod.rs:65
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   Predicate                             crates/icydb-       Pushdown/index/filter
                                         core/src/db/sql/    representation.
                                         lowering/
                                         predicate/
                                         mod.rs:31,
                                         crates/icydb-
                                         core/src/db/
                                         predicate/
                                         semantics.rs:28
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   CompiledExpr                          crates/icydb-       Runtime scalar expression
                                         core/src/db/        IR.
                                         query/plan/expr/
                                         compiled_expr/
                                         mod.rs:565
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   LogicalQuery                          crates/icydb-       Logical scalar/grouped
                                         core/src/db/        plan state.
                                         query/plan/
                                         logical_builder.
                                         rs:67
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   PlannedAccessSelection                crates/icydb-       Chosen access route and
                                         core/src/db/        residual metadata.
                                         query/plan/
                                         planner/
                                         mod.rs:50
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   StaticExecutionPlanningContract       crates/icydb-       Stable bridge from
                                         core/src/db/        planning to execution.
                                         query/plan/
                                         access_plan.rs:1
                                         82
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   EffectiveRuntimeFilterProgram         crates/icydb-       Runtime filter as
                                         core/src/db/        predicate program or
                                         query/plan/         compiled expression.
                                         access_plan.rs:2
                                         36
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   ExecutionPreparation                  crates/icydb-       Prepared execution/index
                                         core/src/db/        predicate programs.
                                         executor/
                                         planning/
                                         preparation.rs:2
                                         0
  ────────────────────────────────────  ──────────────────  ────────────────────────────
   KernelRow                             crates/icydb-       Dense or retained row
                                         core/src/db/        representation for
                                         executor/           terminal execution.
                                         terminal/page/
                                         mod.rs:67

  Ownership boundary assessment:

   Boundary                        Assessment
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Parser vs lowering              Mostly clear. Parser still owns some frontend shape
                                   constraints, but not deep semantics.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   Binding/lowering vs planning    Mostly clear, but SQL lowering performs capability
                                   validation, predicate derivation, aggregate
                                   classification, and expression rewriting in one
                                   layer.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   Planner vs executor             Stronger than average.
                                   StaticExecutionPlanningContract is a good boundary.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   Expression evaluation           Intended boundary is clear, but preview evaluation
                                   bypasses it.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   Predicate/index semantics       Central predicate semantics exist, but index
                                   predicate evaluation has an encoded-byte evaluator
                                   that must be continuously proven equivalent.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   SQL aggregate lanes             Weak boundary. Dedicated global aggregate execution
                                   competes with general SELECT/grouped aggregate
                                   machinery.
  ──────────────────────────────  ──────────────────────────────────────────────────────
   Write queries                   UPDATE/DELETE/INSERT SELECT reuse some SELECT
                                   machinery, but still have separate materialization-
                                   heavy write paths.

  ## 3. Confirmed Bad Practices and Divergence

   ID              D1
   Severity        High
   Category        Architecture
   Location        crates/icydb-core/src/db/sql/lowering/aggregate/mod.rs:41, crates/
                   icydb-core/src/db/session/sql/execute/global_aggregate.rs:38, crates/
                   icydb-core/src/db/sql/lowering/select/mod.rs:227
   Finding         Dedicated SQL global aggregate lane competes with general SELECT/
                   grouped aggregate flow.
   Evidence        is_global_aggregate_lane_shape classifies special cases, then SQL
                   execution dispatches GlobalAggregate separately from Select.
   Recommendation  Preserve the fast path, but generate it from a shared aggregate
                   logical/physical operator contract.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D2
   Severity        Medium
   Category        Correctness / Maintainability
   Location        crates/icydb-core/src/db/query/plan/expr/compiled_expr/mod.rs:1,
                   crates/icydb-core/src/db/query/plan/expr/projection_eval.rs:112
   Finding         There are two scalar expression evaluators.
   Evidence        CompiledExpr::evaluate is the declared runtime path, but
                   eval_builder_expr_for_value_preview recursively evaluates Expr
                   directly and duplicates boolean/comparison logic.
   Recommendation  Make preview evaluation compile to CompiledExpr over a tiny reader,
                   or add mandatory parity tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D3
   Severity        Medium
   Category        Correctness / Architecture
   Location        crates/icydb-core/src/db/sql/lowering/select/mod.rs:65, crates/icydb-
                   core/src/db/query/plan/access_plan.rs:182, crates/icydb-core/src/db/
                   query/plan/access_plan.rs:236
   Finding         SQL filter intent is represented as visible expression, predicate
                   subset, residual predicate, residual expr, and effective runtime
                   program.
   Evidence        LoweredSqlFilter carries both visible_expr and predicate_subset;
                   execution later carries residual_filter_expr,
                   residual_filter_predicate, compiled predicate, and
                   EffectiveRuntimeFilterProgram.
   Recommendation  Introduce one filter contract object with explicit fields: pushdown
                   subset, runtime truth expression, and coverage proof.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D4
   Severity        Medium
   Category        Correctness / Maintainability
   Location        crates/icydb-core/src/db/sql/lowering/predicate/mod.rs:42
   Finding         Predicate-only membership path bypasses the normal boolean-expression
                   lowering path.
   Evidence        derive_sql_where_expr_predicate_subset has a top-level membership
                   shortcut before normal bool expression derivation.
   Recommendation  Route all SQL WHERE through canonical boolean expression lowering
                   first; treat membership pushdown as a later extraction.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D5
   Severity        Medium
   Category        Correctness
   Location        crates/icydb-core/src/db/index/predicate/mod.rs:85, crates/icydb-
                   core/src/db/index/predicate/compile.rs:64
   Finding         Index-only predicate evaluation duplicates runtime predicate
                   semantics over encoded bytes.
   Evidence        IndexPredicateProgram has its own compare ops and literal bytes.
   Recommendation  Keep the encoded fast path, but add property tests proving
                   equivalence to full predicate evaluation across NULL, text, numeric,
                   and membership cases.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D6
   Severity        Medium
   Category        Maintainability / Performance
   Location        crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/
                   materialized.rs:47, crates/icydb-core/src/db/executor/pipeline/
                   entrypoints/scalar/streaming.rs:39
   Finding         Scalar materialized and streaming route setup are mostly duplicated.
   Evidence        Both prepare route state, continuation, trace profile, and execution
                   inputs. The materialized path also applies an index-set page-fetch
                   hint.
   Recommendation  Factor shared route-preparation/input assembly. Then intentionally
                   specialize only result collection.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D7
   Severity        High
   Category        Performance / Memory
   Location        crates/icydb-core/src/db/executor/delete/runtime.rs:144, crates/
                   icydb-core/src/db/executor/delete/structural_projection.rs:51,
                   crates/icydb-core/src/db/session/sql/execute/write/update.rs:149,
                   crates/icydb-core/src/db/session/sql/execute/write/insert.rs:275
   Finding         Write flows materialize full candidate/source row sets before
                   mutation.
   Evidence        DELETE collects candidate rows into Vec; UPDATE collects selected PK
                   rows and clones patches per row; INSERT SELECT materializes projected
                   rows.
   Recommendation  Add bounds and benchmarks first; then move toward chunked mutation
                   preparation where atomicity allows.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D8
   Severity        Medium
   Category        Performance
   Location        crates/icydb-core/src/db/session/query/cache.rs:155
   Finding         Cache miss classification scans cache keys repeatedly.
   Evidence        shared_query_plan_cache_miss_reason performs multiple
                   cache.keys().any(...) passes.
   Recommendation  Replace with one pass or auxiliary metadata counters.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D9
   Severity        Medium
   Category        Performance
   Location        crates/icydb-core/src/db/query/plan/planner/mod.rs:234, crates/icydb-
                   core/src/db/query/plan/planner/mod.rs:261
   Finding         Planner rebuilds and sorts semantic index contracts per plan miss.
   Evidence        Candidate vector construction and sorted_index_contracts happen
                   during planning.
   Recommendation  Cache sorted semantic index views in schema/index visibility state.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID              D10
   Severity        Low
   Category        Correctness / Testability
   Location        crates/icydb-core/src/db/query/plan/logical_builder.rs:183
   Finding         Scalar ordering gets primary-key tie-breaks; grouped ordering does
                   not.
   Evidence        build_scalar_effective_order appends PK tie-breaks; grouped explicit
                   order is returned as-is.
   Recommendation  Add explicit grouped pagination stability tests; consider
                   deterministic group-key tie-break where semantically safe.

  ## 4. Duplicate or Competing Flows

   ID                    F1
   Concept               Scalar expression evaluation
   Flow A                CompiledExpr::evaluate in crates/icydb-core/src/db/query/plan/
                         expr/compiled_expr/evaluate.rs:23
   Flow B                Direct Expr preview evaluator in crates/icydb-core/src/db/
                         query/plan/expr/projection_eval.rs:112
   Risk                  NULL, function, numeric, and boolean semantics can drift.
   Consolidation Target  CompiledExpr should be the only executable expression IR.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F2
   Concept               SQL filter representation
   Flow A                visible_expr plus derived predicate in crates/icydb-core/src/
                         db/sql/lowering/select/mod.rs:65
   Flow B                Runtime predicate/expr program in crates/icydb-core/src/db/
                         query/plan/access_plan.rs:236
   Risk                  Predicate coverage mistakes can admit/reject rows incorrectly.
   Consolidation Target  One filter contract with pushdown subset plus runtime truth
                         program.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F3
   Concept               Global aggregate SELECT
   Flow A                Dedicated global aggregate lowering/execution in crates/icydb-
                         core/src/db/sql/lowering/aggregate/mod.rs:41 and crates/icydb-
                         core/src/db/session/sql/execute/global_aggregate.rs:80
   Flow B                General SELECT/grouped lowering in crates/icydb-core/src/db/
                         sql/lowering/select/mod.rs:227
   Risk                  HAVING, LIMIT, aliases, cache identity, and diagnostics can
                         diverge.
   Consolidation Target  Shared aggregate logical plan and physical terminal adapter.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F4
   Concept               Predicate semantics
   Flow A                Runtime predicate semantics in crates/icydb-core/src/db/
                         predicate/semantics.rs:28
   Flow B                Encoded index predicate semantics in crates/icydb-core/src/db/
                         index/predicate/mod.rs:85
   Risk                  Index and full-scan paths may disagree.
   Consolidation Target  Property-tested equivalence layer.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F5
   Concept               Scalar route setup
   Flow A                Materialized scalar path in crates/icydb-core/src/db/executor/
                         pipeline/entrypoints/scalar/materialized.rs:47
   Flow B                Streaming row-sink path in crates/icydb-core/src/db/executor/
                         pipeline/entrypoints/scalar/streaming.rs:39
   Risk                  Hints, tracing, continuation, and retry behavior can drift.
   Consolidation Target  Shared route-preparation builder.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F6
   Concept               Write selection
   Flow A                UPDATE selects PKs through structural SELECT in crates/icydb-
                         core/src/db/session/sql/execute/write/update.rs:68
   Flow B                DELETE uses delete-specific candidate collection in crates/
                         icydb-core/src/db/executor/delete/runtime.rs:99
   Risk                  Boundedness and projection behavior differ across write types.
   Consolidation Target  Shared mutation-candidate operator.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F7
   Concept               SQL literal canonicalization
   Flow A                Predicate canonicalization in crates/icydb-core/src/db/sql/
                         lowering/select/binding.rs:17
   Flow B                Expression-shell canonicalization in crates/icydb-core/src/db/
                         sql/lowering/select/binding.rs:54
   Risk                  Predicate and expression shells can get out of sync.
   Consolidation Target  Single typed expression/binder artifact with referenced
                         predicate subset.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                    F8
   Concept               INSERT SELECT preparation
   Flow A                Re-prepares source SELECT in crates/icydb-core/src/db/session/
                         sql/execute/write/insert.rs:245
   Flow B                Normal SQL compile/lower path in crates/icydb-core/src/db/
                         session/sql/execute/mod.rs:577
   Risk                  Extra parse/bind work and semantic drift risk.
   Consolidation Target  Reuse compiled/lowered source SELECT artifact.

  ## 5. Hot Path and Performance Findings

   ID                 H1
   Hot Path           Plan-cache miss reason
   Why Hot            Runs on every cache miss and scans keys multiple times.
   Evidence           crates/icydb-core/src/db/session/query/cache.rs:155
   Impact             Medium
   Smallest Safe Fix  Single-pass miss classification.
   Benchmark          Many distinct SQL queries with warm cache sizes 10/100/1000.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H2
   Hot Path           Planner candidate preparation
   Why Hot            Rebuilds/sorts index contracts on plan misses.
   Evidence           crates/icydb-core/src/db/query/plan/planner/mod.rs:234
   Impact             Medium
   Smallest Safe Fix  Cache sorted semantic index contracts per visible schema/index
                      set.
   Benchmark          Compile 1000 ad hoc predicates on schema with many indexes.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H3
   Hot Path           SELECT lowering walks expressions repeatedly
   Why Hot            Projection, aggregate collection, HAVING, ORDER, and predicate
                      lowering revisit expression trees.
   Evidence           crates/icydb-core/src/db/sql/lowering/select/mod.rs:227
   Impact             Medium
   Smallest Safe Fix  Add one typed/analyzed expression pass carrying aggregate refs and
                      field refs.
   Benchmark          Complex SELECT with nested CASE, HAVING, ORDER BY, 20 projections.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H4
   Hot Path           Preview expression evaluation
   Why Hot            Recursive AST evaluator clones values and allocates function arg
                      vectors.
   Evidence           crates/icydb-core/src/db/query/plan/expr/projection_eval.rs:112
   Impact             Low/Medium
   Smallest Safe Fix  Route preview through compiled expression when used repeatedly.
   Benchmark          Projection preview loop over computed expressions/functions.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H5
   Hot Path           Runtime expression reader dispatch
   Why Hot            CompiledExpr::evaluate uses trait object readers; scalar readers
                      include RefCell callback paths.
   Evidence           crates/icydb-core/src/db/query/plan/expr/compiled_expr/
                      evaluate.rs:23, crates/icydb-core/src/db/executor/projection/eval/
                      scalar.rs:34
   Impact             Medium
   Smallest Safe Fix  Benchmark first; specialize common dense-row readers if needed.
   Benchmark          Projection-heavy scan with arithmetic and text functions over
                      large row count.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H6
   Hot Path           Write candidate materialization
   Why Hot            DELETE/UPDATE/INSERT SELECT build full vectors before commit.
   Evidence           crates/icydb-core/src/db/executor/delete/runtime.rs:144, crates/
                      icydb-core/src/db/session/sql/execute/write/update.rs:149, crates/
                      icydb-core/src/db/session/sql/execute/write/insert.rs:306
   Impact             High
   Smallest Safe Fix  Enforce/measure bounds; introduce chunked candidate preparation
                      later.
   Benchmark          Large DELETE RETURNING, UPDATE, and INSERT SELECT over 10k+ rows.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H7
   Hot Path           Late materialization after route selection
   Why Hot            Key streams are efficient, but rows are still materialized for
                      residual filters/order/window in several terminal paths.
   Evidence           crates/icydb-core/src/db/executor/terminal/page/mod.rs:195,
                      crates/icydb-core/src/db/executor/pipeline/operators/post_access/
                      coordinator/runtime/phases.rs:45
   Impact             High for broad scans
   Smallest Safe Fix  Push projection pruning and LIMIT/order exploitation where safe.
   Benchmark          Low-selectivity WHERE plus ORDER BY plus LIMIT with and without
                      supporting index.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H8
   Hot Path           Materialized vs streaming scalar hints
   Why Hot            Page-fetch hint exists in materialized scalar path only.
   Evidence           crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/
                      materialized.rs:68, crates/icydb-core/src/db/executor/pipeline/
                      entrypoints/scalar/streaming.rs:66
   Impact             Low/Medium
   Smallest Safe Fix  Share hint computation and prove whether row-sink routes need it.
   Benchmark          Branch-set/multi-lookup LIMIT query into materialized rows vs
                      aggregate sink.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H9
   Hot Path           Index predicate membership threshold
   Why Hot            Encoded IN search changes behavior at fixed threshold.
   Evidence           crates/icydb-core/src/db/index/predicate/compile.rs:24, crates/
                      icydb-core/src/db/index/predicate/mod.rs:85
   Impact             Low/Medium
   Smallest Safe Fix  Benchmark threshold; tune only with data.
   Benchmark          IN lists of 8, 31, 32, 128, 1024 values.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 H10
   Hot Path           INSERT SELECT reparsing/rebinding
   Why Hot            INSERT SELECT extracts and prepares SELECT separately.
   Evidence           crates/icydb-core/src/db/session/sql/execute/write/insert.rs:245
   Impact             Medium
   Smallest Safe Fix  Reuse compiled source SELECT artifact from SQL compile.
   Benchmark          INSERT SELECT with complex source query repeated many times.

  ## 6. Correctness and SQL Semantics Risks

   ID                 C1
   Area               IN / NOT IN with NULL
   Risk               Risk requiring test: SQL three-valued membership semantics may
                      diverge from predicate subset membership.
   Example Query/API  SELECT * FROM t WHERE x IN (NULL, 1); x NOT IN (NULL, 1)
   Evidence           Membership shortcut in crates/icydb-core/src/db/sql/lowering/
                      predicate/mod.rs:42; comparison NULL exclusion only covers compare
                      lowering at crates/icydb-core/src/db/sql/lowering/predicate/
                      mod.rs:271.
   Test Needed        Add indexed and full-scan tests for nullable and non-null fields.
   Fix Direction      Lower membership through canonical SQL truth expression, then
                      extract safe predicate subset.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C2
   Area               SQL NULL vs predicate NULL
   Risk               Current distinction is intentional but fragile.
   Example Query/API  WHERE x = NULL, direct fluent predicate Compare(x, Eq, Null)
   Evidence           Predicate-level Null == Null in crates/icydb-core/src/db/
                      predicate/semantics.rs:28; SQL tests exist in crates/icydb-core/
                      src/db/session/tests/sql_scalar.rs:803.
   Test Needed        Keep existing tests and add boolean-composition cases.
   Fix Direction      Preserve SQL lowering guardrails; do not let SQL = NULL compile to
                      predicate NULL equality.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C3
   Area               Boolean 3VL duplication
   Risk               Confirmed duplication can drift.
   Example Query/API  WHERE NOT (x = NULL) OR y = 1
   Evidence           Compiled bool logic in crates/icydb-core/src/db/query/plan/expr/
                      compiled_expr/evaluate.rs:540; preview bool logic in crates/icydb-
                      core/src/db/query/plan/expr/projection_eval.rs:475.
   Test Needed        Parity tests over generated boolean expressions.
   Fix Direction      Single evaluator.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C4
   Area               Grouped pagination stability
   Risk               Risk requiring test: ties in grouped ORDER BY may paginate
                      nondeterministically.
   Example Query/API  SELECT age, COUNT(*) FROM t GROUP BY age ORDER BY COUNT(*) DESC
                      LIMIT 1 OFFSET 1
   Evidence           Scalar PK tie-break in crates/icydb-core/src/db/query/plan/
                      logical_builder.rs:183; grouped explicit order returned unchanged
                      at crates/icydb-core/src/db/query/plan/logical_builder.rs:199.
   Test Needed        Grouped ORDER BY tie/cursor tests.
   Fix Direction      Append deterministic group-key tie-break where valid, or document
                      unstable ties.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C5
   Area               Global aggregate lane drift
   Risk               Risk requiring regression tests despite existing coverage.
   Example Query/API  SELECT COUNT(*) AS c FROM t HAVING CASE WHEN c > 0 THEN TRUE END
   Evidence           Dedicated lane in crates/icydb-core/src/db/sql/lowering/aggregate/
                      mod.rs:41 and execution in crates/icydb-core/src/db/session/sql/
                      execute/global_aggregate.rs:80.
   Test Needed        Cross-check dedicated aggregate vs equivalent grouped/singleton
                      cases.
   Fix Direction      Shared aggregate IR/operator.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C6
   Area               Index/full-scan semantic divergence
   Risk               Risk requiring property tests.
   Example Query/API  Queries using numeric widening, text casefold, ranges, IN,
                      nullable/missing fields.
   Evidence           Runtime semantics in crates/icydb-core/src/db/predicate/
                      semantics.rs:28; index encoded evaluator in crates/icydb-core/src/
                      db/index/predicate/mod.rs:85.
   Test Needed        Property tests comparing hidden-index full scan vs index route.
   Fix Direction      Treat index evaluator as optimization proven equivalent to
                      predicate evaluator.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C7
   Area               DELETE residual filter behavior
   Risk               Risk requiring broader SQL NULL tests for mutation filters.
   Example Query/API  DELETE FROM t WHERE CASE WHEN age > 10 THEN TRUE ELSE NULL END
                      RETURNING id
   Evidence           Delete runtime uses ExecutionPreparation::from_runtime_plan at
                      crates/icydb-core/src/db/executor/delete/runtime.rs:40; post-
                      access filter over materialized rows at crates/icydb-core/src/db/
                      executor/pipeline/operators/post_access/coordinator/runtime/
                      phases.rs:45.
   Test Needed        DELETE count vs RETURNING parity with CASE/NULL/OR/AND.
   Fix Direction      Shared filter contract and tests against SELECT result set.
  ──────────────────────────────────────────────────────────────────────────────────────
   ID                 C8
   Area               UPDATE/INSERT SELECT unbounded response/source behavior
   Risk               Correctness risk mostly around resource exhaustion and partial
                      semantics.
   Example Query/API  UPDATE t SET x = 1 WHERE broad_predicate; INSERT INTO t SELECT ...
   Evidence           UPDATE materializes selected PK rows in crates/icydb-core/src/db/
                      session/sql/execute/write/update.rs:149; INSERT SELECT
                      materializes patches in crates/icydb-core/src/db/session/sql/
                      execute/write/insert.rs:306.
   Test Needed        Bound-limit and failure atomicity tests.
   Fix Direction      Add explicit bounds and streaming/chunking only after atomicity
                      design is clear.

  ## 7. Best-Practice Gaps Compared With Mature DB Engines

   Area                     Parsing / binding / planning separation
   Current State            Mostly separated, but SQL lowering also performs capability
                            checks, aggregate classification, predicate extraction, and
                            expression rewriting.
   Best-Practice Direction  Parser produces AST; binder resolves names/types; planner
                            consumes typed IR.
   Recommended Migration    Introduce a typed SQL/binder artifact before structural
                            query lowering.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Logical plan representation
   Current State            LogicalQuery exists and is useful.
   Best-Practice Direction  Logical plan should represent SELECT/aggregate/write
                            candidate selection uniformly.
   Recommended Migration    Extend logical plan to cover aggregate/write candidate
                            operators instead of special lanes.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Physical plan representation
   Current State            StaticExecutionPlanningContract is strong, but not a full
                            operator tree.
   Best-Practice Direction  Physical operators should be explainable and reusable: scan/
                            filter/project/sort/aggregate/limit/write-candidate.
   Recommended Migration    Add explicit physical operator descriptors behind current
                            contract incrementally.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Expression semantics
   Current State            CompiledExpr is strong and well-documented.
   Best-Practice Direction  One executable expression IR.
   Recommended Migration    Remove or wrap direct preview evaluator.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Predicate / NULL behavior
   Current State            Central predicate semantics exist, SQL-specific NULL guards
                            exist, and tests cover = NULL.
   Best-Practice Direction  SQL truth semantics and predicate pushdown should be
                            formally linked by coverage proofs.
   Recommended Migration    Add filter contract and differential/property tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Type coercion / affinity
   Current State            There is schema-aware literal canonicalization, but
                            expression and predicate shells are rewritten separately.
   Best-Practice Direction  Binder owns coercion once.
   Recommended Migration    Move canonicalization into typed expression binding.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Index planning
   Current State            Deterministic and schema-authoritative.
   Best-Practice Direction  Add cost/selectivity awareness and avoid rebuilding static
                            index metadata.
   Recommended Migration    Cache visible index contracts; later add statistics/
                            cardinality estimates.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Predicate pushdown
   Current State            Present through predicate subsets and index predicate
                            compilation.
   Best-Practice Direction  Pushdown must be conservative and explainable.
   Recommended Migration    Add EXPLAIN coverage for pushdown vs residual and property
                            tests for equivalence.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Projection pruning
   Current State            Present in projection specs, but write/materialization paths
                            still collect broad row sets.
   Best-Practice Direction  Only materialize required slots.
   Recommended Migration    Extend pruning through DELETE/UPDATE/INSERT SELECT candidate
                            pipelines.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     LIMIT pushdown
   Current State            Some route/page hints exist.
   Best-Practice Direction  LIMIT should move as early as semantic ordering allows.
   Recommended Migration    Share page-fetch hints and add limit-pushdown plan-shape
                            tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Streaming / bounded memory
   Current State            Key streams exist; row/materialized terminal and write paths
                            still build large vectors.
   Best-Practice Direction  Operators stream unless sort/group/transaction semantics
                            require materialization.
   Recommended Migration    Bound first; then chunk materialization and mutation
                            preparation.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     EXPLAIN / diagnostics
   Current State            Stronger than average; stable descriptor tests exist.
   Best-Practice Direction  EXPLAIN should expose logical and physical choices including
                            residual filters.
   Recommended Migration    Preserve current EXPLAIN vocabulary while adding operator-
                            level details.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                     Benchmarks
   Current State            Integration perf audit exists. No broad isolated benches or
                            SQLite differential tests found.
   Best-Practice Direction  Separate compile/planner/executor/storage/mutation
                            benchmarks.
   Recommended Migration    Add targeted benchmarks listed below.

  ## 8. Test and Benchmark Gaps

  Missing regression tests:

  1. IN and NOT IN with NULL, indexed and full-scan.
  2. Boolean SQL NULL composition: NOT (x = NULL), x = NULL OR TRUE, x = NULL AND TRUE.
  3. Preview evaluator vs CompiledExpr parity for generated scalar expressions.
  4. Index predicate vs full-scan predicate equivalence across text, numeric, ranges,
     IN, nullable fields.

  5. Grouped ORDER BY tie pagination and cursor stability.
  6. Global aggregate lane vs equivalent grouped/singleton aggregate semantics.
  7. DELETE count vs RETURNING parity for CASE/NULL residual filters.
  8. UPDATE selector semantics with residual filters, ORDER/LIMIT/OFFSET, and returning
     bounds.

  9. INSERT SELECT source ordering, generated fields, and large-source behavior.
  10. EXPLAIN plan-shape parity for materialized scalar vs aggregate row-sink routes.

  Missing benchmark scenarios:

  1. Query-plan cache miss classification with large warm caches.
  2. Planner compile cost with many indexes and many ad hoc predicates.
  3. Complex SELECT compile cost with nested expressions, HAVING, ORDER BY.
  4. Projection-heavy full scan expression evaluation.
  5. Low-selectivity residual filter plus ORDER BY plus LIMIT.
  6. Branch-set/multi-lookup LIMIT queries, materialized vs streaming sink.
  7. Large DELETE count and DELETE RETURNING.
  8. Large UPDATE selector plus patch cloning.
  9. Large INSERT SELECT.
  10. Encoded index IN predicate threshold sizes.

  SQLite differential testing should be feasible for shared scalar SELECT semantics:
  NULL logic, comparison, boolean expressions, ORDER/LIMIT/OFFSET, aggregates, HAVING,
  DISTINCT, and simple joins if supported. Do not differential-test IcyDB-specific
  predicate or storage behavior directly.

  Property-based testing should target expression evaluation parity, predicate/index
  equivalence, order stability, pagination cursor invariants, and index-visible vs
  hidden-index result equality.

  ## 9. Prioritized Roadmap

  ### Immediate

   Recommendation          Add SQL IN/NOT IN NULL tests
   Severity                High
   Category                Correctness
   Affected files/modules  SQL predicate lowering/tests
   Fix                     Add indexed/full-scan regression tests.
   Impact                  High confidence around subtle SQL semantics.
   Risk                    Low
   Dependencies            None
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Add grouped ORDER BY tie tests
   Severity                Medium
   Category                Correctness
   Affected files/modules  Logical planning/tests
   Fix                     Lock expected deterministic or documented behavior.
   Impact                  Prevents pagination surprises.
   Risk                    Low
   Dependencies            None
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Single-pass cache miss reason
   Severity                Medium
   Category                Performance
   Affected files/modules  crates/icydb-core/src/db/session/query/cache.rs:155
   Fix                     Replace repeated scans with one pass.
   Impact                  Low-risk compile/cache win.
   Risk                    Low
   Dependencies            None
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Share scalar route preparation
   Severity                Medium
   Category                Maintainability
   Affected files/modules  crates/icydb-core/src/db/executor/pipeline/entrypoints/
                           scalar/materialized.rs:47, crates/icydb-core/src/db/executor/
                           pipeline/entrypoints/scalar/streaming.rs:39
   Fix                     Extract common route/input builder.
   Impact                  Reduces future drift.
   Risk                    Low/Medium
   Dependencies            None
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Add index/full-scan equivalence property tests
   Severity                High
   Category                Correctness
   Affected files/modules  index predicate, session tests
   Fix                     Generate predicates and compare hidden-index vs index route.
   Impact                  High.
   Risk                    Medium
   Dependencies            Test harness design

  ### Medium-Term

   Recommendation          Make preview evaluation use CompiledExpr
   Severity                Medium
   Category                Correctness / Maintainability
   Affected files/modules  expression planner/eval
   Fix                     Replace recursive preview evaluator or wrap it in parity-
                           tested compile path.
   Impact                  Removes semantic duplication.
   Risk                    Medium
   Dependencies            Expression parity tests
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Introduce unified filter contract
   Severity                High
   Category                Architecture / Correctness
   Affected files/modules  SQL lowering, access plan, executor prep
   Fix                     Represent pushdown predicate, runtime truth program, and
                           coverage proof together.
   Impact                  Reduces predicate/SQL drift.
   Risk                    Medium
   Dependencies            NULL and predicate equivalence tests
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Cache visible semantic index contracts
   Severity                Medium
   Category                Performance
   Affected files/modules  planner/schema visibility
   Fix                     Precompute sorted index contract views.
   Impact                  Better ad hoc compile performance.
   Risk                    Medium
   Dependencies            Planner determinism tests
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Reuse compiled SELECT for INSERT SELECT
   Severity                Medium
   Category                Performance / Maintainability
   Affected files/modules  SQL insert execution
   Fix                     Avoid reparsing/repreparing source SELECT.
   Impact                  Better compile path and fewer semantics lanes.
   Risk                    Medium
   Dependencies            INSERT SELECT tests
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Add mutation candidate bounds/metrics
   Severity                High
   Category                Performance / Safety
   Affected files/modules  DELETE/UPDATE/INSERT SELECT
   Fix                     Measure and bound large materialized candidate sets.
   Impact                  Prevents resource surprises.
   Risk                    Medium
   Dependencies            Existing public bound policy

  ### Larger Architectural Work

   Recommendation          Shared aggregate logical/physical operator
   Severity                High
   Category                Architecture
   Affected files/modules  SQL aggregate/select/executor
   Fix                     Make global aggregate a specialization of shared aggregate
                           plan.
   Impact                  Reduces major competing lane.
   Risk                    Medium/High
   Dependencies            Aggregate regression suite
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Typed binder artifact
   Severity                High
   Category                Architecture / Correctness
   Affected files/modules  SQL lowering/binding
   Fix                     Produce resolved names, types, coercions, aggregate refs,
                           field refs once.
   Impact                  Cleaner compile-time/runtime split.
   Risk                    High
   Dependencies            Expression/filter consolidation
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Operator-level physical plan
   Severity                Medium
   Category                Architecture / EXPLAIN
   Affected files/modules  planner/executor/explain
   Fix                     Expose scan/filter/project/sort/aggregate/limit/mutation
                           operators.
   Impact                  Better optimization and debugging.
   Risk                    High
   Dependencies            Current contract stabilization
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Cost/selectivity-aware planning
   Severity                Medium
   Category                Performance
   Affected files/modules  planner/index metadata
   Fix                     Add stats/cardinality estimates after deterministic baseline
                           is preserved.
   Impact                  Better index choice.
   Risk                    Medium/High
   Dependencies            Cached index contracts and benchmarks
  ──────────────────────────────────────────────────────────────────────────────────────
   Recommendation          Streaming/chunked mutation pipeline
   Severity                High
   Category                Performance / Memory
   Affected files/modules  DELETE/UPDATE/INSERT SELECT
   Fix                     Avoid full candidate materialization where transaction
                           semantics allow.
   Impact                  Major memory improvement.
   Risk                    High
   Dependencies            Bounds, atomicity tests, rollback design

  ## 10. Top 10 Recommended Next Actions

  1. Add regression tests for SQL IN/NOT IN with NULL, both index-backed and full-scan.
  2. Add property tests comparing index routes with forced full scans for supported
     predicate subsets.

  3. Replace repeated query-plan cache miss scans with a single-pass implementation.
  4. Extract shared scalar route-preparation/input assembly for materialized and
     streaming scalar execution.

  5. Add grouped ORDER BY tie and cursor stability tests.
  6. Make expression preview evaluation reuse CompiledExpr, or add mandatory parity
     tests before changing behavior.

  7. Introduce a unified filter contract that records pushdown subset, runtime truth
     expression, and coverage proof.

  8. Add benchmarks for planner compile cost with many indexes and complex SQL
     expressions.

  9. Add explicit benchmarks and safety tests for large DELETE, UPDATE, and INSERT
     SELECT materialization.

  10. Start designing a shared aggregate operator so the dedicated global aggregate lane
     becomes an optimization, not a separate semantic path.

  Validation: read-only audit only. I inspected the query-engine, SQL, planner,
  executor, index predicate, write, test, and benchmark-adjacent files with targeted
  repository searches. I did not run the test suite and did not modify files.
