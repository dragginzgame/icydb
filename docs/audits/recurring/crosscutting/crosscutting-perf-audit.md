# Audit: Query Instruction Footprint

## Purpose

Track runtime instruction drift over time for the current IcyDB query and SQL
execution surfaces.

This is a runtime execution audit.

It is NOT:

* a wasm-size audit
* a branch-count audit
* a correctness audit
* a generic benchmark-harness shootout

The job of this audit is to measure how many local instructions the live query
surfaces actually execute, explain where the hot paths live, and catch
regressions before they become shared runtime cost across common query shapes.

This audit is not permission to remove intended semantics, safety checks,
determinism guarantees, explain fidelity, or fail-closed behavior just to make
instruction numbers smaller.

---

## Current Runtime Topology

This audit must use the current execution topology, not retired helper names.

Primary owners:

* `db/session/query.rs`
  * typed/fluent query planning and execution
  * shared query-plan cache attribution
  * typed/fluent phase attribution
* `db/session/sql/mod.rs`
  * `execute_trusted_sql_query`
  * `execute_trusted_sql_exact_update`
  * `execute_trusted_sql_prefix_update`
  * `prepare_trusted_sql_resumable_update`
  * `resume_trusted_sql_resumable_update`
  * `execute_trusted_sql_query_with_attribution`
  * SQL compile cache attribution
* `db/session/sql/execute/*`
  * `execute_compiled_sql`
  * `execute_compiled_sql_with_phase_attribution`
  * grouped SQL execution routing
* `db/session/sql/explain.rs`
  * explain-only SQL surfaces
* `db/session/sql/projection/runtime/mod.rs`
  * `execute_sql_projection_rows_for_canister`
  * pure-covering / hybrid-covering projection shaping
* `db/sql/lowering/mod.rs`
  * `compile_sql_command`
* `db/executor/pipeline/entrypoints/*`
  * scalar and grouped execution entrypoints

Historical helper names such as:

* `query_from_sql(...)`
* `execute_sql(...)`
* `execute_sql_projection(...)`
* `execute_sql_grouped(...)`
* `execute_sql_aggregate(...)`

are obsolete as primary owner labels. They may still appear in archived reports,
but new audit runs must anchor findings to the current owner boundary above.

### Current Authoritative Harness Lanes

Recurring runs must prefer the dedicated PocketIC perf harnesses over generic
demo-canister dispatch sampling.

Authoritative current lanes:

* SQL lane:
  * `testing/integration/tests/sql_perf_audit.rs`
  * `canisters/audit/sql_perf/src/lib.rs`
  * `schema/audit/sql_perf`
  * covers SQL query, update, explain, repeat/cache, projection, grouped, and
    phase-attribution scenarios
* generated SQL matrix lane:
  * `testing/integration/tests/sql_perf_matrix_audit.rs`
  * reuses the `sql_perf` audit canister, not demo canisters
  * generates broad deterministic SQL query coverage for hotspot discovery;
    typed correctness generation is owned by the dedicated correctness lanes
  * emits ranked JSON/Markdown reports under `artifacts/perf-audit/` by default
* typed/fluent lane:
  * `testing/integration/tests/fluent_perf_audit.rs`
  * covers fluent query/update, repeat/cache, direct-row, grouped, and finalize
    attribution scenarios

Secondary context lane:

* generated dispatch or `demo_rpg` canister sampling may be used only as
  optional end-to-end context
* it must not be the primary comparable baseline unless the dedicated SQL and
  fluent harnesses cannot run, and the report must mark that method shift

SQL and fluent harness rows must be interpreted through the canonical row model
below and stored under
`docs/reports/recurring/YYYY/MM/DD/perf-audit/<run>/artifacts/` when artifacts
are emitted for a recurring run.

---

## Why This Audit Is IcyDB-Specific

IcyDB does not have one query surface or one cost lane.

The live tree includes:

* typed/fluent query execution
* session-owned reduced SQL query execution
* reduced SQL update execution
* explain-only SQL execution
* scalar projection materialization with covering and hybrid-covering fast paths
* grouped execution with grouped fold/finalize attribution
* shared query-plan cache reuse
* SQL compiled-command cache reuse
* generated dispatch shells and canister harnesses that may wrap the same core
  session owners differently

That means this audit must do more than “run one query and time it”.

It must:

* compare equivalent query intent across current entry surfaces
* cover many query shapes, not just one happy-path scalar read
* separate compile, planner, store, executor, and projection cost when the
  current diagnostics surface allows it
* capture cursor, paging, order, projection, grouping, aggregate, explain, and
  rejection sensitivity
* map drift back to shared hubs such as lowering, session SQL execution,
  projection runtime, session query, and executor entrypoints

---

## Risk Model

This is a drift audit, not a correctness invariant audit.

Primary risks:

* silent instruction growth on common query paths taxes every caller
* equivalent typed/fluent and SQL surfaces can drift apart in cost while
  remaining semantically aligned
* compile cache and shared query-plan cache behavior can shift cost between
  compile and execute without obvious user-visible behavior change
* cursor, projection, grouped, and explain paths can regress while scalar happy
  paths stay flat
* rejection and unsupported paths can become unexpectedly expensive

Optimization constraints:

* reduce instruction use without removing intended behavior
* do not count fail-open behavior as a perf win
* preserve deterministic explain and fail-closed boundaries
* preserve semantic parity across supported surfaces unless a report explicitly
  states a deliberate product change

---

## Report Preamble (Required)

Every report generated from this audit must include:

* scope
* definition path
* compared baseline report path
* code snapshot identifier
* method tag/version
* comparability status
* auditor
* run timestamp (UTC)
* branch
* worktree
* execution environment (`unit-test`, `PocketIC`, `test-canister`, `mixed`)
* entities in scope
* entry surfaces in scope
* query shapes in scope

---

## Measurement Model

Use these terms consistently.

### Canonical Row Model

Every captured sample must normalize into rows with these semantic fields:

* `subject_kind`
* `subject_label`
* `entry_surface`
* `scenario_key`
* `entity_scope`
* `query_shape_key`
* `phase_kind`
* `count`
* `total_local_instructions`
* `avg_local_instructions`
* `sample_origin`
* `scenario_labels`
* `query_shape_labels`

Minimum expectations:

* direct execution samples normalize to `subject_kind = query_surface`
* phase-attribution samples normalize to `subject_kind = phase`
* cache or runtime checkpoints normalize to `subject_kind = checkpoint`

If the transport changes but normalized semantics stay the same, the method tag
may remain stable. If normalized semantics change, the method tag must change.

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

They are NOT instruction totals.

### Phase Attribution

Phase attribution is diagnostic unless the method explicitly isolates the phase.

Preferred current phase buckets:

* `compile_sql`
* `compile_sql_cache_key`
* `compile_sql_cache_lookup`
* `parse_sql`
* `prepare_sql`
* `lower_sql`
* `bind_sql`
* `plan_query`
* `run_store`
* `run_executor`
* `assemble_projection`
* `grouped_stream`
* `grouped_fold`
* `grouped_finalize`
* `response_decode`
* `render_explain`
* `cursor_rejection`
* `rejection_mapping`

If a run cannot isolate a phase, mark that attribution `PARTIAL`.

### Freshness Rule

Comparable samples require one of these:

* fresh process / fresh canister / fresh test topology per scenario group
* documented single-scenario-per-instance runs
* explicit report note that multiple scenarios intentionally share one runtime
  instance

If freshness/isolation is violated, deltas are non-comparable.

---

## Scope

Measure and report:

* typed/fluent query execution instruction totals
* reduced SQL query execution instruction totals
* reduced SQL update execution totals when included in the scenario matrix
* explain-only SQL execution
* projection, grouped, aggregate, cursor, and paging sensitivity
* failure, rejection, and unsupported paths
* phase-level attribution when available
* structural hotspot localization for the most expensive scenarios

### Default Code Scope

* `crates/icydb-core/src/db/session/`
* `crates/icydb-core/src/db/sql/`
* `crates/icydb-core/src/db/query/`
* `crates/icydb-core/src/db/executor/`
* `crates/icydb-core/src/db/predicate/`
* diagnostics hooks that report instruction attribution

### Default Entry Surfaces

For each supported scenario, sample what exists:

* checked-in SQL performance profile through the exact P1 discovery and P2
  confirmation shards, as the authoritative comparable SQL read lane
* focused PocketIC SQL checks in `sql_perf_audit` only for write, storage,
  repeat, route, or cache contracts not yet owned by the profile; this target
  has no scheduled baseline or independent repeated-sampling authority
* PocketIC typed/fluent perf harness
  `fluent_perf_audit_harness_reports_instruction_samples`, as the authoritative
  typed/fluent lane
* typed/fluent load query execution
* typed/fluent paged query execution
* `DbSession::execute_trusted_sql_query::<E>(...)`
* `DbSession::execute_trusted_sql_exact_update::<E>(...)`
* `DbSession::execute_trusted_sql_prefix_update::<E>(...)`
* `DbSession::prepare_trusted_sql_resumable_update::<E>(...)`
* `DbSession::resume_trusted_sql_resumable_update::<E>(...)`
* `DbSession::execute_trusted_sql_query_with_attribution::<E>(...)` when diagnostics are
  available
* current explain SQL surface
* generated dispatch or canister shell wrappers, but only when the report maps
  them back to the current owner surface

### Explain Isolation Rule

Explain samples must never be mixed into execution scenario groups.

Explain samples must document:

* explain mode (`logical`, `json`, `execution`)
* whether the scenario was shape-equivalent to a measured execution query
* whether instruction capture was for explain-only or execute-only
* whether explain sampling shared a runtime instance with execution scenarios

If explain activity cannot be isolated cleanly, explain comparability is
`PARTIAL`.

---

## Scenario Matrix (Mandatory)

For each entry surface sampled, cover as many of these as the surface supports.

### Base Classes

1. minimal valid query
2. representative valid query
3. high-cardinality valid query
4. rejection / unsupported path
5. repeated-call path where cache reuse or warm runtime effects matter

### Required Query Shape Families

At minimum, recurring runs should try to cover:

#### Scalar load shapes

* whole-row load
* primary-key equality
* secondary equality
* range query
* ordered query satisfied by access path
* ordered query requiring extra work
* `LIMIT`
* `OFFSET`
* empty result
* one-row result
* many-row result

#### Cursor / paging shapes

* first page without cursor
* second page with valid cursor
* invalid cursor payload
* signature / plan mismatch cursor when available
* ordered scalar cursor path
* grouped cursor path when available

#### Projection shapes

* narrow field-list projection
* wide projection
* computed projection
* covering projection
* hybrid-covering projection

#### Grouped / aggregate shapes

* grouped count
* grouped aggregate with multiple groups
* grouped aggregate with empty result
* grouped aggregate with `HAVING`
* grouped rejection path
* global aggregate

#### Explain shapes

* explain logical
* explain JSON
* explain execution
* explain grouped query
* explain aggregate query
* explain rejection path

#### SQL frontend shapes

* minimal supported SQL
* representative supported SQL
* grouped SQL
* aggregate SQL
* projection-heavy SQL
* parse/lower rejection
* execute-time rejection

#### Optional update shapes

If the run explicitly includes updates, keep them labeled separately from
query-only rows:

* delete
* insert
* update
* batch insert

Do not let update rows dominate a query-footprint headline without saying so
explicitly.

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
* `freshness_model`
* `method_tag`

---

## Coverage Scan (Mandatory)

Before capturing instruction data:

1. enumerate entry surfaces in scope
2. enumerate query families in scope
3. scan current attribution hooks
4. scan current structural hotspot artifacts
5. list critical flows that still lack phase attribution

Recommended current scans:

* `rg -n "PerformanceProfile|p1_shard|p2_candidates|fluent_perf_scenarios|scenario_key|baseline_path" testing/integration/tests/sql_perf_matrix_audit.rs testing/integration/tests/sql_perf_p*.rs testing/integration/tests/sql_perf_audit.rs testing/integration/tests/fluent_perf_audit.rs`
* `rg -n "SqlQueryExecutionAttribution|QueryExecutionAttribution|store_get_calls|grouped_stream_local_instructions" crates/icydb-core/src canisters/audit/sql_perf/src`
* `rg -n "execute_trusted_sql_query|execute_trusted_sql_exact_update|execute_trusted_sql_prefix_update|prepare_trusted_sql_resumable_update|resume_trusted_sql_resumable_update|execute_trusted_sql_query_with_attribution|execute_compiled_sql|execute_compiled_sql_with_phase_attribution" crates/icydb-core/src/db/session`
* `rg -n "compile_sql_command|compile_sql_query|compile_sql_mutation" crates/icydb-core/src/db`
* `rg -n "execute_sql_projection_rows_for_canister|sql_select_prepared_plan|execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src/db`
* `rg -n "EXPLAIN|cursor|continuation|GROUP BY|HAVING|DISTINCT|LIMIT|OFFSET" crates/icydb-core/src/db`
* `rg -n "with_phase_attribution|local_instruction_counter|store_get_calls|cache_hits|cache_misses" crates/icydb-core/src/db`

Important:

* if phase checkpoints do not exist for a critical flow, that is a real audit
  result
* instruction capture can still pass while phase-attribution coverage remains
  partial

---

## Decision Rule

Primary regression authority:

* isolated instruction totals from comparable query-surface runs

Secondary diagnostic evidence:

* phase attribution
* cache hit/miss attribution
* explain or structural hotspot localization

Do not call a regression solely from:

* wasm growth
* branch counts
* line counts
* qualitative “feels heavier” reading

---

## Required Output Format

Produce:

## 0. Run Metadata + Comparability Note

Include the full report preamble plus a short note on what is directly
comparable to the chosen baseline and what is newly added or method-shifted.

## 1. Coverage Table

| Scenario Family | Surfaces Covered | Missing Surfaces | Attribution Depth | Risk |
| --------------- | ---------------- | ---------------- | ----------------- | ---- |

## 2. Current Matrix

| Scenario Key | Entry Surface | Count | Avg | Notes |
| ------------ | ------------- | ----: | ---: | ----- |

## 3. Comparison Highlights

Summarize only the most important deltas versus the comparable baseline.

## 4. Phase Attribution Read

| Scenario Key | Compile | Planner | Store | Executor | Projection/Finalize | Notes |
| ------------ | -------: | ------: | ----: | -------: | ------------------: | ----- |

Mark unavailable columns as `N/A` or `PARTIAL`, not blank.

## 5. Hotspot Localization

List the current shared hubs that most plausibly explain the expensive rows.

## 6. Coverage Gaps

List:

* missing scenario families
* missing attribution hooks
* surfaces present in code but absent from sampling
* new current owners that older reports did not cover

## 7. Overall Read

Provide:

* biggest improvements
* clearest regressions
* unstable or non-comparable areas
* next best focused rerun

Do not turn this into a redesign proposal.

---

## Baseline Verification Commands

Current recurring runs should first verify that the dedicated harnesses are
registered:

* `cargo test -p icydb-testing-integration --test sql_perf_audit -- --list`
* `cargo test -p icydb-testing-integration --test fluent_perf_audit -- --list`

Then verify both harnesses compile:

* `cargo test -p icydb-testing-integration --test sql_perf_audit --no-run`
* `cargo test -p icydb-testing-integration --test fluent_perf_audit --no-run`

The remaining dedicated typed/fluent instruction capture command is:

* `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test fluent_perf_audit fluent_perf_audit_harness_reports_instruction_samples -- --nocapture`

Focused SQL contract and follow-up attribution commands:

* `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_update_warm_persists_compiled_and_shared_cache_across_calls -- --nocapture`
* `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_repeated_query_contracts_keep_compiled_and_shared_cache_path -- --nocapture`
* `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_shared_floor_queries_report_phase_breakdown -- --nocapture`
* `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test fluent_perf_audit fluent_perf_update_warm_persists_query_cache_across_calls -- --nocapture`

Deterministic P1 shard commands:

- run `make build-sql-perf-wasm` once to create the exact subject shared by the
  complete P1, scale, P2, and instrumentation run;
* run `make test-sql-perf-p1-shard P1_SHARD=<index>` once for each index from
  zero through seven;
* run `make test-sql-perf-scale-shard SCALE_SHARD=<index>` once for each index
  from zero through seven; and
* run `make test-sql-perf-p1-merge` only after all eight P1 and all eight scale
  shard artifacts exist.

Each P1 shard runner uses the checked-in performance profile, its deterministic
membership, the fixed top-20 ranking budget, and the same prebuilt `wasm-release`
canister bytes. The merge reads exactly shards zero through seven, validates each
shard against its
serialized outcomes, and is the only authority that emits the complete matrix.
Prefix, first-N, ranking-count, and wasm-profile overrides are intentionally not
supported because they cannot produce comparable Tier D evidence.

The successful P1 merge writes the strict P2 candidate selection. Run
`make test-sql-perf-p2-shard P2_SHARD=<index>` once for each index from zero
through seven, then run `make test-sql-perf-p2-merge`. Each candidate has five
fresh-canister cold observations and five independently update-warmed
observations; cache counters prove each mode, and every stable receipt is
required. Missing, duplicate, tampered, semantically drifting, or unstable
evidence fails the P2 merge.

Every current measured P1, P2, scale, comparison, and calibration artifact
carries the same validated `measurement_coverage` projection. Instruction
attribution, store-operation counts, result cardinality, and projected blob
payload bytes are measured. Peak heap, allocator traffic, and stable-memory byte
volume are explicitly `not_measured`; zero must not stand in for unavailable
evidence.

Run `make test-sql-perf-instrumentation` to write the separate strict
attributed-versus-total-only calibration artifact for the fixed user primary-key
`LIMIT 1` sentinel. It records the complete environment, raw WASM identity, five
isolated samples per path, exact result identity, and median overhead. Until clean
scheduled runs establish a reviewed budget, its disposition is
`observation_only`; it is evidence, not a passing threshold.

Run `make test-sql-perf-baseline` only with explicit `P2_BASELINE_PATH` and
`SCALE_BASELINE_PATH` values naming reviewed artifacts. The command compares
confirmed P2 medians and exact scale totals, normalized costs, and slopes. It
rejects incomparable environments or semantic drift before producing a verdict and
never updates either baseline. Confirmed comparisons retain raw unaccounted and
over-attributed phase residual changes as observation-only metrics until reviewed
residual budgets exist.

The P1 merge accepts exactly one baseline authority. Normal runs require
`P1_BASELINE_PATH` naming a reviewed complete P1 report, derive typed
`baseline_threshold` candidate reasons, and reject dirty, incomplete,
incomparable, or semantically drifting evidence. Before the first reviewed
baseline exists, use `PERF_CALIBRATION_COHORT=<canonical-id>` together with
`PERF_CALIBRATION_RUN=1`, `2`, or `3` to produce one explicitly tagged clean
member of the three-run calibration cohort. Calibration evidence retains no
historical threshold reasons and cannot satisfy the ordinary baseline verdict.

After all three bundles exist, run `make test-sql-perf-calibration-review` with
`PERF_CALIBRATION_RUN_1_DIR`, `PERF_CALIBRATION_RUN_2_DIR`, and
`PERF_CALIBRATION_RUN_3_DIR`. The command rejects mixed cohort identities,
ordinals, comparable environments, raw Wasm subjects, component identities, and
semantic results. Its bounded JSON projection retains P2 metric medians, scale
totals, exact normalized costs, adjacent-cardinality slopes, attribution
overhead, and raw/normalized top-20 scenarios recurring in at least two runs.
This is review evidence only: it cannot update the profile, select thresholds,
or bless a baseline.

Every raw phase ranked by P2 must be observed by the exact P1 profile. A global
non-count aggregate sentinel exercises scalar reducer-fold attribution; global
`COUNT` scenarios alone are insufficient because their fast path bypasses that
runtime. Candidate selection fails closed when any required raw metric remains
unobserved.

The `SQL Performance Evidence` workflow runs nightly and on manual dispatch. It
builds one raw `wasm-release` subject before sharding and distributes that exact
artifact to every measurement job. Normal runs resolve a reviewed workflow-run
artifact from the dispatch input or `ICYDB_SQL_PERF_BASELINE_RUN_ID`, require all
eight P1, scale, and P2 jobs, perform the strict merges, retain the current bundle,
and run the ordinary comparison as a separate fail-closed gate. Manual calibration
dispatches supply the same cohort plus one exact run ordinal; they retain tagged
evidence but skip the inapplicable ordinary comparison. No workflow run blesses a
baseline automatically.

A reviewed previous-source bridge may set `subject_revision` to one exact
40-character commit SHA. Every code-bearing job checks out that immutable
revision while retaining the workflow's strict environment comparison. The
override may identify the exact historical source for an explicit three-run
calibration cohort, allowing the existing strict reviewer to establish the
previous-source side of an environment bridge. It does not relax lockfile
checks or create another baseline-selection path. Reused ordinal-two and
ordinal-three subjects must match the ordinal-one artifact's recorded source
revision and raw Wasm SHA-256 before measurement begins.

The separate manual `SQL Performance Calibration Review` workflow downloads
three explicitly named evidence runs, invokes the same strict reviewer, and
retains only its diagnostic projection. Supplying three run IDs is not itself
authority to change a checked-in budget or baseline.

Deterministic matrix output control:

* `SQL_PERF_WASM_PATH` selects the one shared raw `wasm-release` subject;
* `P1_SHARD_DIR` selects the shard-artifact directory for both make targets; and
* `P1_REPORT_OUT` selects the merged JSON/Markdown report path stem;
* `P1_BASELINE_PATH` selects one reviewed P1 discovery baseline, while
  `PERF_CALIBRATION_COHORT` plus `PERF_CALIBRATION_RUN` explicitly select one
  first-baseline calibration run instead;
* `P2_SELECTION_PATH` selects the strict candidate artifact; and
* `P2_SHARD_DIR` and `P2_REPORT_PATH` select P2 shard and merged artifacts;
* `SCALE_SHARD_DIR` and `SCALE_REPORT_PATH` select scale shard and merged
  artifacts;
* `PERF_INSTRUMENTATION_PATH` selects the calibration artifact; and
* `P2_BASELINE_PATH`, `SCALE_BASELINE_PATH`, and `PERF_COMPARISON_PATH` select
  explicit comparison inputs and output.

When a recurring run emits raw captures or transformed rows, write them below
`docs/reports/recurring/YYYY/MM/DD/perf-audit/<run>/artifacts/`. Previously
captured SQL-only artifacts remain historical context within their owning
migrated report runs; they do not define a current output path.
