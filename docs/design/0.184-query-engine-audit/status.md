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
- H6 / D7 / F6 first slice: SQL write metrics now carry staged-row counts
  beside matched, mutated, and RETURNING row counts, and broad write-shape
  guards plus a SQL perf-matrix hook cover broad UPDATE, UPDATE RETURNING,
  DELETE, and DELETE RETURNING materialization pressure before any
  streaming/chunked mutation redesign. INSERT SELECT staged-row attribution is
  covered by core SQL write tests because the live heap/journaled perf fixtures
  use explicit Int32 primary keys and reject computed primary-key insertion by
  design. After the staged-row bound guard landed, the live PocketIC rerun
  completed with the pinned local 14.0.0 binary and confirmed broad SQL write
  materialization remains visible rather than failing the endpoint:
  heap UPDATE count/RETURNING at roughly 98.3M/99.1M instructions, heap DELETE
  count/RETURNING at roughly 354.4M/357.5M, journaled UPDATE count/RETURNING at
  roughly 100.3M/100.2M, and journaled DELETE count/RETURNING at roughly
  357.4M/360.0M for the 32-row fixture.
- H6 / D7 / F6 second slice: policy-validated public bounded UPDATE plans now
  carry explicit staged-row execution bounds beside RETURNING bounds, and
  UPDATE execution verifies staged candidate rows before opening the mutation
  commit window.
- H6 / D7 / F6 second slice: structural DELETE RETURNING execution now accepts
  an internal row-bound contract and verifies it after delete preparation but
  before the commit-window bridge; broad SQL DELETE remains unbounded until a
  public DELETE exposure policy exists.
- H6 / D7 / F6 third slice: SQL DELETE has a non-executing exposure-policy
  classifier matching the UPDATE policy split for broad session writes,
  generated query/DDL rejection, public primary-key-only deletes, public
  bounded deterministic deletes, and future admin/bulk routing. UPDATE and
  DELETE now share primary-key WHERE proof, canonical primary-key ordering
  proof, narrow RETURNING classification, and row-bound combination helpers.
- H6 / D7 / F6 fourth slice: policy-validated public DELETE execution adapters
  now consume `SqlPublicPrimaryKeyDeletePlan` and `SqlPublicBoundedDeletePlan`,
  bind parsed DELETE statements through the accepted schema, and pass staged-row
  / RETURNING row bounds into the delete executor before the commit window for
  both count-only and RETURNING deletes.
- H6 / D7 / F6 fifth slice: UPDATE and DELETE policy validation plus DELETE
  execution projection bounds now share bounded-public write checks and
  staged-row / RETURNING row-bound calculation through the common write-policy
  helper while preserving distinct update/delete rejection and execution-bound
  surface types.
- H3 / F7 tenth slice: lowered SQL projection expressions now carry their
  `LoweredExprAnalysis` through the SELECT schema-binding seam, and projection
  source-field capability validation consumes the recorded direct/path source
  references instead of walking projection expressions again.
- H3 / F7 eleventh slice: grouped SELECT projection lowering now produces both
  the SQL-local projection artifact and the stable first-seen aggregate-call
  list from the same analyzed expression pass, removing the separate grouped
  projection aggregate collector.
- H3 / F7 twelfth slice: global aggregate terminal lowering now records the
  aggregate output expressions and aliases that make singleton-result ORDER BY
  terms inert, so output-order stripping no longer re-analyzes the projection.
- H3 / F7 thirteenth slice: scalar SELECT projection lowering now lets the
  shared `SqlExprPhase::Scalar` expression lowerer reject aggregate leaves,
  removing the projection-local parser aggregate pre-scan and a redundant
  DISTINCT empty-projection branch.
- D1 / F3 first aggregate-architecture slice: the shared aggregate operator
  migration is scoped in `shared-aggregate-operator.md`, and the global
  aggregate session adapter now prepares one private structural aggregate
  operator envelope around the existing executor `StructuralAggregateRequest`
  instead of reconstructing terminals, projection labels, fixed scales, HAVING,
  and schema info inline. The direct-count cardinality fast path and grouped
  execution behavior are unchanged.
- D1 / F3 second aggregate-architecture slice: aggregate EXPLAIN execution
  nodes now expose `aggregate_contract` and `aggregate_physical` properties for
  singleton scalar terminals and grouped hash/ordered materialization, so the
  semantic aggregate contract is visible separately from the chosen physical
  implementation. Direct prefix-cardinality COUNT EXPLAIN now also reports
  conservative metadata eligibility and prefix count when the same no-metadata
  planning proof can derive exact prefix specs; runtime scalar-aggregate
  `sink_mode` remains the exact execution attribution.
- D1 / F3 third aggregate-architecture slice: SQL global aggregate command
  facts now own the singleton direct `COUNT(*)` and prefix-cardinality
  metadata-candidate proof, so runtime execution, compiled execution,
  diagnostics fallback, and EXPLAIN consume one precomputed fact set instead of
  rebuilding strategy/projection/HAVING checks in the session adapter.
- H7 first slice: scalar materialization lane metrics now report retained-slot
  layout executions, retained value count, and byte-length-only retained value
  count, giving late-materialization work an execution-owned footprint before
  any new runtime path is introduced.
- H7 second slice: retained-slot footprint now flows through normal
  diagnostics `KernelRowAttribution` and SQL perf-matrix samples, so late
  materialization pressure can be ranked from existing attribution reports.
  The matrix report now ranks retained layout hits, retained slot values, and
  byte-length-only retained values separately, so H7 candidates do not have to
  show up indirectly through broad kernel-row scan rankings.
- Large literal `IN` first slice: SQL membership lowering, predicate bridge
  recovery, truth-set compilation, and scalar evaluation now keep membership as
  a compact `IN_LIST` function instead of expanding into left-deep boolean
  chains. A filtered matrix rerun dropped sparse token
  `collection_id IN (...)` page compile work from about 1.67B instructions to
  about 3.3M total compile / 2.2M lower, and the matching count compile from
  about 846M to about 3.3M total compile / 2.3M lower.
- Large literal `IN` second slice: index multi-lookup routes can now admit
  key-only / index-covered projections into the covering path. The sparse token
  `collection_id IN (...) ORDER BY id LIMIT 50` page still scans 256 index
  entries because the available `(collection_id, stage, id)` index cannot prove
  global primary-key order for a collection-only lookup, but it no longer opens
  row storage for the key-only projection.
- Large literal `IN` third slice: residual access-proof stripping now handles
  identical canonical branch-value sets in linear time, and finalized static
  planning skips compiling preparation predicates when the selected access path
  proves the whole filter. The sparse token page rerun dropped from about
  103.7M to about 40.6M total instructions, with planner work down from about
  78.7M to about 15.8M while retaining zero row-store reads and the same 256
  index-entry scan.
- Large literal `IN` fourth slice: index-prefix-family routes whose consumed
  prefix leaves the primary-key fields as the exact remaining index suffix can
  now prove primary-key `ORDER BY` without a materialized sort. This admits
  order-compatible multi-lookup shapes such as `bucket IN (...) ORDER BY id`
  on `(bucket, id)` while still rejecting sparse collection-only lookups on
  `(collection_id, stage, id)`.
- Large literal `IN` fifth slice: synchronized prefix-cardinality metadata can
  now enumerate bounded exact child prefixes, and scalar multi-lookup execution
  can expand one child slot when that proves the primary-key suffix order. This
  lets full-entity/fluent sparse `collection_id IN (...) ORDER BY id` shapes on
  `(collection_id, stage, id)` stream bounded child-prefix branches without a
  materialized sort. SQL key-only and hybrid covering projections remain on the
  covering lane, which is still a separate follow-up if the same optimization is
  worth carrying there.
- Large literal `IN` sixth slice: SQL key-only/index-covered covering pages now
  reuse the same metadata-backed child-prefix expansion proof when it succeeds
  fail-closed. The sparse token
  `collection_id IN (...) ORDER BY id LIMIT 50` key-only matrix shape dropped
  from about 37.7M total instructions / 256 index-entry reads to about 9.9M
  total instructions / 74 index-entry reads in the focused comparator, while
  staying row-store-free.
- Large literal `IN` sixth slice follow-up: a focused hybrid-covering guard now
  proves `SELECT id, title ... collection_id IN (...) ORDER BY id LIMIT N`
  inherits the same bounded child-prefix expansion, hydrates only returned
  row-backed fields, and keeps missing-prefix pruning bounded.
- Large literal `IN` cleanup follow-up: scalar streaming and SQL covering
  pages now share the executor-local exact child-prefix expansion helper, so
  prefix-cardinality synchronization, cap exhaustion, target-prefix validation,
  and empty child-prefix results stay on one fail-closed runtime contract.
- Covering cleanup first slice: pure and hybrid covering projections now share
  index-backed access admission, lowering, component-index selection, scan
  window construction, and component-row resolution. Their row assembly remains
  separate because pure covering stays row-store-free while hybrid covering
  selectively hydrates row-backed fields after the index-backed window.
- Covering cleanup second slice: aggregate projection and `bytes_by` terminals
  now share the unbounded single-component covering resolver for already
  lowered index-prefix/range specs.
- Covering cleanup third slice: generic covering projection mapping, aggregate
  projection, and `bytes_by` terminals now share one covering row-presence and
  effective-window fold. Terminal-specific value decoding and byte-length
  folding remain local to their terminal lanes.
- F5 / D6 / H8 scalar-spine follow-up: materialized scalar pages and aggregate
  row sinks now share one scalar kernel observability finalizer for scanned
  rows, post-access rows, projected rows, distinct-key counts, and execution
  trace stats. This deletes a small duplicate terminal-owned flow without
  changing route selection, page materialization, aggregate row sinking, or
  attribution field names.
- F5 / D6 / H8 scalar-spine follow-up: initial no-cursor scalar runtime
  preparation now consumes one structural runtime handoff helper shared by
  materialized scalar pages, retained-slot SQL pages, and aggregate row sinks.
  The diagnostics-only path keeps its explicit measured subphases so perf
  attribution remains stable.
- F5 / D6 / H8 scalar-spine follow-up: prepared-load callers now share the
  continuation-signature extraction and scalar runtime handoff step before
  initial runtime preparation, including the SQL retained-slot override path.
- F5 / D6 / H8 scalar-spine follow-up: SQL retained-slot initial page execution
  now delegates continuation setup, projection runtime-mode selection, and
  retained-slot layout selection to shared scalar runtime setup, leaving the SQL
  entrypoint as a thin adapter around the prepared handoff. Initial scalar
  route setup now also uses named surface options for unpaged scalar rows,
  aggregate row sinks, and materialized scalar rows instead of repeating
  boolean policy literals at the entrypoints. The diagnostics-only attribution
  path still measures route-plan lookup separately, but it now calls the same
  runtime helper used by normal initial scalar setup. Resumed scalar page setup
  now goes through a matching runtime helper, so cursor-aware projection and
  validation policy also stay out of the executor adapter. The initial scalar
  runtime setup now uses the same named option contract before and after
  continuation construction instead of carrying a second handoff-only option
  struct.
- F5 / D6 / H8 scalar-spine follow-up: diagnostics-only initial scalar runtime
  preparation now lives beside normal scalar runtime preparation. The public
  attributed scalar entrypoint consumes a runtime-owned measured helper instead
  of reconstructing continuation signatures, scalar handoff, route-plan lookup,
  and runtime-bundle assembly inline, preserving attribution fields while
  reducing drift between measured and unmeasured scalar setup.
- H7 third slice: retained layouts with byte-length-only text/blob slots now
  decode normal retained values and scalar byte lengths through one opened
  structural row reader. Focused guards prove mixed value-mode decoding opens
  one row, and SQL `OCTET_LENGTH(blob)` projections remain slot-only while
  opening each projected row once. The SQL perf matrix now exposes retained
  byte-length hotspots as a first-class ranking for future follow-up.
- H7 focused matrix follow-up: a 54-scenario retained-slot rerun covered the
  documented user retained-slot cases and deterministic blob `OCTET_LENGTH`
  shapes. The highest byte-length cases remained slot-only and bounded; the
  highest non-byte retained-slot cases were field-comparison scans that need row
  facts rather than a separate late-materialization lane.
- H7 focused rerun after sparse-`IN` work: the four highest retained-slot
  candidate families stayed bounded at about 2.1M-2.7M total instructions.
  Blob `OCTET_LENGTH` shapes still retain byte-length-only values instead of
  blob payloads, and user field-comparison shapes still need row facts for
  predicate/order evaluation. The larger cursor-emitting slot-only projection
  idea remains deferred until a workload with cursor emission shows a repeated
  retained/full-row hotspot.
- SQL parser-boundary hardening: parser-local normalization checks, tree
  traversal helpers, aggregate-kind mappings, scalar-function call-shape
  helpers, and order-expression parse helpers now stay visible only inside the
  SQL frontend subtree. This preserves parsed SQL and lowering behavior while
  keeping session/executor code from depending on parser-owned helper methods.
- SQL SELECT lowering boundary hardening: strict-literal predicate/expression
  canonicalization now stays private to SELECT lowering instead of being
  re-exported at the broader `db` boundary.
- SQL branch-ownership invariants now guard the parser frontend boundary and
  SELECT strict-literal canonicalizer ownership so those helpers cannot widen
  back to broader `db` visibility unnoticed.
- F2 / D3 follow-up: `LoweredSqlFilter` now owns final accepted-schema filter
  handoff into `StructuralQuery`, keeping visible-expression, predicate-subset,
  and strict-literal canonicalization policy inside the SQL filter contract.
- SQL compile-boundary cleanup: `CompiledSqlCommand` now owns aggregate,
  mutation, and row-returning shape facts, so `SqlCompileArtifacts`
  construction validates against command-owned classification instead of
  keeping a local mirror match.
- Count-terminal cleanup: SQL direct `COUNT(*)`, prepared aggregate `COUNT`,
  and prepared aggregate `EXISTS` now share the exact prefix-cardinality
  metadata sum helper, and the lowered-plan to durable SQL prefix-spec
  conversion now lives with the executor cardinality helpers. The remaining
  SQL direct-count cache entry still owns durable compiled-prefix specs, while
  fluent/prepared aggregate execution continues to consume the live lowered
  plan.
- SQL global-aggregate direct-count cleanup: normal execution, compiled
  execution, diagnostics execution, and EXPLAIN now share the same
  metadata-fast-path eligibility predicate. Compiled direct-count cache hits
  also construct the probe through one helper, while measured diagnostics keep
  their own timing boundary.
- SQL global-aggregate direct-count cleanup follow-up: normal and compiled
  execution now share direct-count probe execution and fallback-authority
  resolution, and normal/diagnostics execution share cached direct-count
  plan-entry construction. Diagnostics still keep a separate measured path so
  phase attribution remains stable.
- F2 / D3 filter-contract cleanup: executor routing, covering admission,
  aggregate fast paths, scalar pipeline boundaries, and residual-presence tests
  now consume `AccessPlannedQuery::has_any_residual_filter()` instead of
  repeating expression-or-predicate checks. The raw OR remains only inside the
  planner-owned residual contract accessor.
- F2 / D3 filter-contract guardrail: the layer-authority invariant script now
  rejects new residual-filter presence gates that rebuild expression-or-predicate
  checks outside the planner-owned residual contract accessor.

## Current Slice

- H7 remains open for evidence-driven follow-up only. Do not add a new
  materialization lane until retained-slot metrics identify a repeated shape
  that still falls back to full rows, over-retains slots, or performs avoidable
  row-store reads. The current measured cursorless SQL projection cases do not
  meet that threshold.
- Sparse `IN` child-prefix work is now in cleanup/guard mode: scalar and
  covering execution consume the same metadata expansion helper, and empty
  expansions are covered as successful empty pages rather than route failures.
- Prefix-cardinality terminal work is in local DRY mode: SQL and fluent-facing
  count/existence terminals now share metadata summing, while SQL-specific
  compiled cache identity remains separate.
- SQL global aggregate direct-count work is still a lightweight singleton
  fast path, not a shared aggregate operator rewrite. The cleanup now removes
  repeated eligibility/probe mechanics, duplicate non-diagnostic probe
  execution, and session-local direct-count shape reconstruction.
- Filter handoff work is in local DRY mode: residual expression and predicate
  accessors remain available for callers that need the actual artifacts, while
  boolean gating should go through the single plan-owned presence helper.
  CI now guards that split for non-test code.

## Next Candidates

- Sparse literal `IN` follow-up: scalar/full-entity, key-only/index-covered,
  and tested hybrid-covering pages now have the shared prefix-cardinality
  child-expansion path for `(collection_id, stage, id)`. Only tune this further
  if a new sparse `IN` shape shows up as a repeated hotspot.
- D1 / F3: decide whether cache/explain identity should eventually carry a
  first-class aggregate operator DTO shared by singleton global aggregate and
  grouped aggregate explain assembly, or whether the current additive
  descriptor properties are sufficient until a runtime execution merge is
  justified. The DTO gate is recorded in `shared-aggregate-operator.md`: do
  not add it until it deletes duplicate global/grouped logic, becomes the
  shared EXPLAIN/runtime handoff, or prevents a real cache/fingerprint
  misclassification risk.
- H3 / F7: extend the analyzed artifact only after a narrow design for type
  inference, additional ORDER BY facts beyond the current field proof, and
  predicate-derivation inputs.
- H6 / D7 / F6: design chunked mutation preparation separately if broad
  session/admin SQL writes become a product requirement. The current evidence
  supports keeping public writes policy-bounded rather than widening broad
  mutation execution in the query-engine audit line.
