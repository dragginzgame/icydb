# Audit: Query Instruction Footprint

## Purpose

Track runtime instruction drift over time for IcyDB query execution across
session, fluent, typed-query, and reduced-SQL entry surfaces.

This is a runtime execution audit.

It is NOT:

* a wasm-size audit
* a branch-count audit
* a correctness audit
* a pure planner-structure audit

The job of this audit is to measure how many local instructions IcyDB query
surfaces and representative query shapes actually execute, explain where the
hot paths live, and catch regressions before they become shared runtime cost
across common query patterns.

This audit is not permission to remove intended semantics, safety checks,
determinism guarantees, explain fidelity, or fail-closed behavior just to make
instruction numbers smaller.

## Why This Audit Is IcyDB-Specific

IcyDB does not look like a generic HTTP service and should not be audited like
one.

Current IcyDB query/runtime shape includes:

* multiple user-facing query fronts:

  * fluent / typed query execution
  * session query execution
  * reduced SQL compilation + execution
  * explain-only SQL surfaces
* multiple semantically similar but not instruction-identical paths:

  * `query_from_sql(...)`
  * `execute_sql(...)`
  * `execute_sql_projection(...)`
  * grouped / aggregate SQL execution
  * `explain_sql(...)`
* a test-canister-only `sql(...)` endpoint intended for quick ad-hoc SQL checks
* metrics sink/state infrastructure
* recurring structural audit artifacts that already identify shared hub pressure
  in parser, lowering, explain, predicate runtime, session, and executor paths

That means this audit must do more than “run one query and time it”.

It must:

* compare equivalent query intent across different entry surfaces
* cover many different query shapes, not just one happy-path `SELECT *`
* separate compile/lower/plan/explain/execute cost when possible
* capture cursor/paging/order/projection/grouping/aggregate sensitivity
* preserve fail-closed unsupported/rejection paths as first-class scenarios
* map instruction growth back to shared hubs such as parser/lowering/predicate/
  explain/executor, not only leaf APIs

An audit copied from Canic endpoints or from a generic SQL benchmark harness
will miss these properties and will not be comparable.

## Risk Model / Invariant

This is a drift audit, not a correctness invariant audit.

Risk model:

* silent instruction growth on common query paths taxes every caller
* “same result” queries can get more expensive when parser/lowering/planner/
  predicate/executor shared hubs accrete work
* SQL and fluent surfaces can drift apart in cost while staying semantically
  aligned
* unsupported/rejection paths can become unexpectedly expensive
* cursor/paging/order/projection/grouping shapes can hide regressions behind a
  small happy-path sample set
* explain surfaces can become a meaningful runtime tax if they drift in shared
  hubs

Optimization constraint:

* reduce instruction use without removing intended behavior
* do not treat feature removal or fail-open behavior as a perf win
* preserve deterministic explain and fail-closed boundaries
* preserve semantic equivalence across supported surfaces unless the report
  explicitly states a deliberate product change

Invariant:

* important query surfaces and query-shape families should remain measurable,
  comparable, and explainable across runs
* critical flows should either have phase-level attribution or be explicitly
  listed as attribution gaps
* shared-hub regressions should be visible before they become permanent runtime
  tax

## Run This Audit After

* parser changes in `db::sql::parser`
* lowering changes in `db::sql::lowering`
* planner / access-choice / predicate planning refactors
* executor routing / continuation / terminal / mutation changes
* `db::session::query` or `db::session::sql` refactors
* explain / execution-descriptor changes
* metrics sink / state / trace changes
* SQL subset expansion
* projection, grouping, aggregate, cursor, or delete behavior changes
* any PR claiming “performance improvement”, “no runtime impact”, or “just
  internal refactor” on shared query code

## Report Preamble (Required)

Every report generated from this audit must include:

* Scope
* Definition path
* Compared baseline report path
* Code snapshot identifier
* Method tag/version
* Comparability status
* Auditor
* Run timestamp (UTC)
* Branch
* Worktree
* Execution environment (`unit-test`, `PocketIC`, `test-canister`, `mixed`)
* Entity/entities in scope
* Entry surfaces in scope
* Query shapes in scope

## Measurement Model (Mandatory)

Use these terms consistently.

### Canonical Row Model

The audit authority is a normalized row model, not the exact transport or test
output shape of the current release.

Every captured sample must normalize into rows with these semantic fields:

* `subject_kind`
* `subject_label`
* `entry_surface`
* `count`
* `total_local_instructions`
* `avg_local_instructions`
* `scenario_key`
* `scenario_labels`
* `entity_scope`
* `query_shape_key`
* `query_shape_labels`
* `sample_origin` (`instruction_harness`, `checkpoint`, `derived`)
* `phase_kind` when relevant (`compile`, `lower`, `plan`, `execute`, `explain`,
  `delete`, `cursor`, `projection`, `grouped`, `aggregate`)

Minimum expectations:

* direct execution samples normalize to `subject_kind = query_surface`
* phase-attribution samples normalize to `subject_kind = phase`
* checkpoint samples normalize to `subject_kind = checkpoint`

The transport may change.

Possible current transports for this audit may include:

* explicit instruction harness output from tests/benchmarks
* ad-hoc test-canister sampling through the SQL test endpoint
* phase-level log/checkpoint output captured by the harness
* derived normalization from structured test output

Reports must compare canonical row fields, not one concrete test-print format.

If the transport changes but normalized semantics stay the same, the method tag
may remain stable.

If normalized semantics change, the method tag must change.

### Authoritative Signal

The authoritative machine-readable signal is isolated instruction sampling from
a repeatable harness and normalized into the canonical row model.

Interpretation:

* `count` = number of repeated executions for the same scenario
* `total_local_instructions` = accumulated local instructions for that scenario
* `avg_local_instructions = total_local_instructions / count`

### Explain / Structural Artifacts

`EXPLAIN`, `EXPLAIN JSON`, `EXPLAIN EXECUTION`, and structural audit artifacts
are diagnostic aids only.

They are useful for:

* verifying that two scenarios are shape-equivalent
* localizing likely planner/executor hotspots
* explaining why one query shape costs more than another
* spotting which shared hubs probably own the drift

They are NOT instruction totals.

Audit rule:

* do not treat explain text/JSON as instruction counters
* do not compare structural complexity metrics directly to instruction totals
  unless the report explicitly states that the comparison is heuristic only
* use instruction rows for regression tracking
* use explain / structural artifacts for hotspot localization

### Phase Attribution

Phase attribution is diagnostic only unless the method explicitly isolates the
phase.

Preferred phase buckets:

* parse
* lower
* normalize/canonicalize
* plan
* compile runtime
* execute access
* execute post-access
* assemble projection
* grouped aggregate
* global aggregate
* cursor continuation
* explain logical
* explain execution
* rejection mapping

If a run cannot isolate one phase, mark that phase attribution `PARTIAL`.

### Counter Semantics

These counts are local runtime instruction counts for the current process /
canister / harness context.

That means:

* they count local instructions in the measured execution context
* they do not automatically include unrelated external work
* they are not cycle charges
* they are not branch counts
* they are not wasm-size proxies

Do not compare instruction counts directly to wasm-size changes or complexity
table scores without separate evidence.

### Freshness Rule

Comparable samples require one of these:

* fresh process / fresh canister / fresh test topology per scenario group
* documented single-scenario-per-instance runs
* explicit report note that multiple scenarios intentionally share one runtime
  instance

If freshness/isolation is violated, deltas are non-comparable.

## Scope

Measure and report:

* fluent query execution instruction totals
* typed query execution instruction totals
* reduced-SQL execution instruction totals
* projection, grouped, aggregate, delete, and explain surfaces
* cursor/paging/order/projection sensitivity
* failure/rejection/unsupported paths
* phase-level attribution when available
* structural hotspot localization for the most expensive scenarios

### Default Scope

Default code scope:

* `crates/icydb-core/src/db/session/`
* `crates/icydb-core/src/db/sql/`
* `crates/icydb-core/src/db/query/`
* `crates/icydb-core/src/db/executor/`
* `crates/icydb-core/src/db/predicate/`
* `crates/icydb-core/src/metrics/`

Default runtime scope:

* one representative entity with realistic indexes
* one minimal entity
* one entity with:

  * primary key access
  * at least one secondary index
  * at least one composite index if available
  * enough rows to make range/order/page behavior observable

### Default Entry Surfaces

For each supported scenario, sample what exists:

* fluent load query
* fluent delete query
* typed `Query<E>` execution
* `query_from_sql(...)`
* `execute_sql(...)`
* `execute_sql_projection(...)`
* `execute_sql_grouped(...)`
* `execute_sql_aggregate(...)`
* `explain_sql(...)`
* test-canister `sql(...)` endpoint when used for ad-hoc parity checks

### Explain Isolation Rule

Explain samples must never be mixed into execution scenario groups.

Explain samples must document:

* explain mode (`logical`, `json`, `execution`)
* whether the scenario was shape-equivalent to a measured execution query
* whether instruction capture was for explain-only or execute-only
* whether explain sampling shared a runtime instance with execution scenarios

If explain activity cannot be isolated cleanly, explain comparability is
`PARTIAL`.

## Argument Matrix (Mandatory)

For each entry surface sampled, cover as many of these as the surface supports.

### Base Classes

1. minimal valid query
2. representative valid query
3. high-cardinality valid query
4. rejection / unsupported path
5. repeated-call path where caching / shape reuse / warm-store effects matter

### Required IcyDB Query Shape Families

Include lots of different queries.

At minimum, recurring runs should try to cover:

#### Scalar load shapes

* `SELECT *` / whole-row load
* primary-key equality
* primary-key `IN (...)`
* secondary equality
* secondary `IN (...)`
* strict same-field `OR` that should canonicalize to `IN`
* full-scan fallback
* `IS NULL`
* contradictory predicate / constant-false equivalent shape
* `STARTS WITH`
* lower-bound range
* upper-bound range
* bounded `BETWEEN`-equivalent range
* composite prefix + trailing range
* order satisfied by access path
* order requiring extra work
* `LIMIT`
* `OFFSET`
* empty result
* one-row result
* many-row result

#### Cursor / paging shapes

* first page without cursor
* second page with valid cursor
* invalid cursor payload
* query-signature mismatch cursor
* cursor on ordered scalar path
* cursor on grouped path if supported
* page window where continuation is absent
* page window where continuation is required

#### Projection shapes

* scalar field-list projection
* projection preserving primary key
* projection without primary key where allowed
* computed projection
* projection with aliasing if supported
* wide projection vs narrow projection

#### Grouped / aggregate shapes

* one grouped count
* grouped aggregate with multiple groups
* grouped aggregate with empty result
* grouped aggregate with `HAVING`
* grouped aggregate rejection path
* global `COUNT(*)`
* global `COUNT(field)`
* global `SUM(field)`
* global `AVG(field)`
* global `MIN(field)`
* global `MAX(field)`
* aggregate unknown target field rejection

#### Delete shapes

* bounded delete
* ordered delete with limit
* delete empty match
* delete rejection / unsupported shape

#### Explain shapes

* `EXPLAIN` logical
* `EXPLAIN JSON`
* `EXPLAIN EXECUTION`
* explain for grouped query
* explain for global aggregate
* explain rejection for unsupported SQL
* explain rejection for non-EXPLAIN input

#### SQL parser / lowering boundary shapes

* minimal supported SQL
* representative supported SQL
* qualified identifiers
* unsupported SQL feature classes
* grouped SQL requiring grouped execution surface
* projection SQL on entity-shaped execute surface
* SQL statement that parses but fails during lowering / planning / execution

If a class is not applicable, state that explicitly.

### Scenario Identity Tuple

Every measured scenario must have a stable identity tuple.

Minimum tuple:

* `entity`
* `entry_surface`
* `query_family`
* `arg_class`
* `predicate_shape`
* `projection_shape`
* `aggregate_shape`
* `order_shape`
* `page_shape`
* `cursor_state`
* `result_cardinality_class`
* `store_state`
* `index_state`
* `freshness_model`
* `method_tag`

Why this matters:

* “same query” is not enough in IcyDB
* SQL, fluent, and typed surfaces may be semantically aligned but instruction-
  different
* cursor/window/order/projection shape materially changes cost
* store/index cardinality changes can dominate totals
* comparability must be anchored to explicit scenario identity, not memory

## Coverage Scan (Mandatory)

Before capturing instruction data:

1. enumerate entry surfaces in scope
2. enumerate query families in scope
3. scan current observability hooks
4. scan current structural hotspot artifacts
5. list critical flows that still lack phase attribution

Recommended scans:

* `rg -n "query_from_sql|execute_sql|execute_sql_projection|execute_sql_grouped|execute_sql_aggregate|explain_sql" crates/icydb-core/src`
* `rg -n "trace_query|MetricsEvent|with_metrics_sink|sink::record" crates/icydb-core/src`
* `rg -n "EXPLAIN|GROUP BY|COUNT\\(|ORDER BY|LIMIT|cursor|continuation" crates/icydb-core/src/db`
* `rg -n "runtime-metrics.tsv|function-branch-hotspots.tsv|complexity-accretion" docs/audits`

Important:

* if phase checkpoints do not exist for a critical flow, that is a real audit
  result
* instruction capture can still pass while phase-attribution coverage remains
  partial

### Phase Naming Contract

Phase labels should be short, stage-like, and stable.

Preferred examples:

* `parse_sql`
* `lower_sql`
* `normalize_predicate`
* `plan_access`
* `compile_runtime`
* `run_access`
* `post_access`
* `assemble_projection`
* `build_cursor`
* `render_explain`
* `map_rejection`

Avoid:

* prose sentences
* unstable wording
* labels that encode transient values

If phase names or placement change, mark cross-run phase deltas
`N/A (method change)`.

## Decision Rule

* primary regression authority: isolated instruction totals from comparable
  query-surface runs
* secondary diagnostic: average instructions per execution
* tertiary diagnostic: isolated phase attribution when available
* compare only same scenario identity tuple
* if phase names / placement change, mark the affected delta
  `N/A (method change)`

Do not claim improvement from:

* comparing SQL vs fluent as if they were the same scenario unless the report
  explicitly says the comparison is cross-surface
* comparing fresh-instance runs against warm accumulated runs
* comparing explain-only runs against execute-only runs as if they were one
  metric
* comparing different result-cardinality classes as if they were identical

## Required Report Sections

Every report generated from this definition must include:

* `## Query Matrix`
* `## Phase Attribution`
* `## Phase Coverage Gaps`
* `## Structural Hotspots`
* `## Planner / Lowering Pressure`
* `## Executor / Predicate Pressure`
* `## Entry Surface Skew`
* `## Early Warning Signals`
* `## Risk Score`
* `## Verification Readout`

### Query Matrix

Must include:

* entity
* entry surface
* query label
* scenario label
* count
* total instructions
* average instructions per execution
* baseline delta or `N/A`

### Phase Attribution

Must include:

* flow name
* phase names in order
* per-phase instruction deltas when available
* missing-attribution gaps, if any

### Phase Coverage Gaps

Must include:

* critical flows with phase attribution
* critical flows without phase attribution
* proposed first insertion points for uncovered critical flows

### Structural Hotspots

For the highest-cost queries/flows, map cost back to concrete modules and files
with command evidence.

Examples:

* `rg -n "<query method>|<flow function>" crates/icydb-core/src`
* `rg -n "^use " <hot module directory>`
* direct references to likely hotspots such as:

  * `crates/icydb-core/src/db/sql/parser/`
  * `crates/icydb-core/src/db/sql/lowering/`
  * `crates/icydb-core/src/db/session/query.rs`
  * `crates/icydb-core/src/db/session/sql/`
  * `crates/icydb-core/src/db/query/explain/`
  * `crates/icydb-core/src/db/predicate/`
  * `crates/icydb-core/src/db/executor/`

### Planner / Lowering Pressure

For the hottest instruction paths, normalize pressure on:

* `db::sql::parser`
* `db::sql::lowering`
* query planning / access-choice / predicate normalization
* explain-plan rendering for equivalent shapes

Report:

* whether the hotspot is parser-heavy, lowering-heavy, planner-heavy, or mostly
  execution-heavy
* whether the cost is shared across many surfaces
* whether SQL-only overhead is dominating otherwise cheap execution

### Executor / Predicate Pressure

For the hottest execution paths, normalize pressure on:

* predicate runtime / simplify / normalize
* executable-plan construction
* access stream traversal
* continuation/window math
* projection assembly
* grouped / aggregate execution
* explain descriptor generation when sampled

### Entry Surface Skew

For equivalent query intent, report whether instruction cost differs materially
across:

* fluent
* typed query
* SQL execute
* SQL projection
* SQL grouped / aggregate
* explain surfaces

This section exists to catch drift where equivalent semantics stop being
economical through one front door.

### Early Warning Signals

Must call out signals such as:

* new query surfaces entering the audit matrix
* high-growth shared query shapes with unchanged behavior claims
* unsupported/rejection paths approaching or exceeding supported-path cost
* cursor or paging paths inflating unexpectedly
* SQL-only overhead growing faster than execution cost
* critical flows still missing phase attribution
* instruction growth concentrated in shared hubs rather than leaf methods

### Risk Score

Use a normalized `0-10` score.

Rubric:

* `0-2`: shared-runtime regression severity
* `0-2`: concentration in shared parser/lowering/planner/executor hubs
* `0-2`: phase-attribution coverage gaps
* `0-2`: comparability loss or method drift
* `0-2`: surface skew / cursor sensitivity / rejection-path inflation

Report both:

* total score
* one short line per rubric component

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL` with concrete evidence.

1. Entry surfaces in scope were enumerated before measurement.
2. Query shape families were enumerated before measurement.
3. Current observability hooks were scanned.
4. Comparable scenario identity tuples were defined for each sampled scenario.
5. The current capture transport was normalized into the canonical row model.
6. Freshness / isolation strategy was documented.
7. Phase attribution was captured where available.
8. Flows lacking phase attribution were listed explicitly with proposed insertion
   sites where possible.
9. Explain samples, if present, were isolated from execute samples.
10. Baseline path was selected according to baseline policy.
11. Deltas versus baseline were recorded when comparable.
12. Verification readout includes command outcomes with `PASS` / `FAIL` /
    `BLOCKED`.

## Execution Contract

Preferred execution environment:

* repeatable unit/integration test harness with explicit instruction capture
* PocketIC or test-canister execution when canister behavior is part of the
  surface being measured

Use ad-hoc manual test-canister sampling only when the scenario cannot be
represented in the normal harness and the report explains why.

No canonical runner script is assumed.

Until one exists, each report must:

* list the exact commands used
* emit normalized artifacts
* state any manual steps explicitly

Recommended command bundle:

* `rg -n "query_from_sql|execute_sql|execute_sql_projection|execute_sql_grouped|execute_sql_aggregate|explain_sql" crates/icydb-core/src`
* `rg -n "trace_query|MetricsEvent|with_metrics_sink|sink::record" crates/icydb-core/src`
* `rg -n "runtime-metrics.tsv|function-branch-hotspots.tsv|complexity-accretion" docs/audits`
* targeted test runs that exercise:

  * scalar load matrix
  * cursor/paging matrix
  * projection matrix
  * grouped / aggregate matrix
  * explain matrix
  * unsupported / rejection matrix
  * delete matrix
* optional test-canister `sql(...)` checks for quick SQL parity validation

Required capture artifacts:

* `scenario-manifest.json`
* `instruction-rows.tsv` or `instruction-rows.json`
* `phase-attribution.log`
* `verification-readout.md`
* `method.json`
* `environment.json`

The report may additionally attach:

* explain text / JSON / execution outputs
* structural hotspot evidence
* raw harness logs

Those are supporting artifacts, not the audit authority.

## Comparability Rules

Two runs are comparable only if all of these hold:

* same method tag
* same scenario identity tuple
* same phase names and placement for attributed flows
* same freshness / isolation model
* same entity/index/state assumptions

If any item changes, mark the delta `N/A (method change)`.

### Method Change Triggers

The method tag must change when any of these change:

* capture transport shape changed in a way that affects normalization
* canonical subject labels changed
* phase names or placement changed
* harness changed
* freshness / isolation model changed
* default query matrix changed materially
* entity/index fixture changed materially
* risk-score rubric changed

When the method tag changes:

* add a `Method Changes` section to the report
* mark affected deltas as `N/A (method change)`
* keep at least one unchanged anchor metric where possible

## Failure Classification

Use these classifications when results move:

* `PASS`: stable or improved, with coverage intact
* `PARTIAL`: data captured but phase coverage or comparability is incomplete
* `FAIL`: material regression, missing required evidence, or shared-hub growth
  without explanation

## Follow-Up Expectations

If the report identifies a hotspot or regression:

* name the query/flow
* name the entry surface
* name the owning module(s)
* state whether the issue is shared-runtime or surface-specific
* state whether it is parser/lowering/planner/predicate/executor/explain-driven
* propose the next investigation target

Examples of acceptable follow-up actions:

* add phase attribution around SQL parse/lower/plan/execute boundaries
* add first attribution points around cursor continuation assembly
* separate explain-only cost from execute cost in one harness
* reduce repeated predicate normalization or repeated projection assembly
* reduce repeated storage / index traversals for one query family
* split a hotspot module so attribution is less ambiguous
* narrow a scenario matrix where one pathological boundary case is masking the
  general trend
