# Executor Test Triage

Date: 2026-03-26

Scope: pruned files under `crates/icydb-core/src/db/executor/tests` that are not part of the revived live subset.

## Summary

The revived subset is now:

- `continuation_structure`
- `cursor_validation`
- `lifecycle`
- `live_state`
- `load_structure`
- `metrics`
- `mutation_save`
- `ordering`
- `pagination`
- `post_access`
- `reverse_index`
- `semantics`
- `set_access`
- `stale_secondary`

The still-pruned families are not one homogeneous backlog. They now split into two buckets:

1. Delete wrapper/harness files that only existed to aggregate stale suites.
2. Migrate owner-local tests into the modules that now own the behavior.
3. Leave the remaining very large aggregate matrix family pruned until it can be revived deliberately.

## Delete

These files are not good candidates for direct revival at the executor-root test harness.

- `crates/icydb-core/src/db/executor/tests/paged_builder.rs`
  Reason: this is a 2413-line intent/paging contract suite that leans on `PagingIntentError` and old builder/test-only access-plan seams. The ownership is query-intent and pagination policy, not executor-root orchestration.
  Status: deleted from the executor-root tree in this pass.

- `crates/icydb-core/src/db/executor/tests/aggregate/mod.rs`
  Reason: this is a 1600-line kitchen-sink wrapper for the aggregate matrix family, not a reusable test unit by itself.
  Status: deleted from the executor-root tree in this pass. The underlying aggregate matrix files remain pruned.

- `crates/icydb-core/src/db/executor/tests/pagination/mod.rs`
  Reason: this is a 981-line harness for the pagination matrix family and keeps the old executor-root aggregation pattern alive.
  Status: deleted from the executor-root tree in this pass. The underlying pagination matrix files remain pruned.

- `crates/icydb-core/src/db/executor/tests/route/mod.rs`
  Reason: this is only a route-family harness and depends on stale shared fixtures and compatibility exports.
  Status: deleted from the executor-root tree in this pass. The route-owner matrix family has since been folded into `db/executor/route/tests.rs`.

## Migrate

These suites still test real behavior, but the current location and shared-fixture model are wrong.

- `crates/icydb-core/src/db/executor/tests/route/*.rs`
  Reason: these are route-owner policy tests, not executor-root integration tests.
  Stale dependencies seen: `AccessPlannedQuery::new_typed(...)`, `build_execution_route_plan_for_grouped_plan`, `residual_predicate_pushdown_fetch_cap`, `FieldExtremaIneligibilityReason`, `MUTATION_FAST_PATH_ORDER`.
  Status: complete. `precedence_matrix.rs`, `capability_matrix.rs`, `load_matrix.rs`, `mutation_matrix.rs`, `field_extrema_matrix.rs`, and `aggregate_matrix.rs` are now folded into `db/executor/route/tests.rs`, and the old executor-root `route/` subtree is empty.

- `crates/icydb-core/src/db/executor/tests/semantics.rs`
  Reason: the remaining executor-owned snapshot contracts belonged with the live executor harness once the missing test-only executable-plan snapshot seam and unique-range fixture were restored.
  Status: complete. The reduced execution-pipeline snapshot family is now live again under `db/executor/tests/semantics.rs`, and its expectations have been rebased to the current executor/query explain surface.

## Revive Later

These families are large enough that trying to wire them back in wholesale would recreate the original problem.

- `crates/icydb-core/src/db/executor/tests/aggregate/*.rs`
  Size: 8156 lines across five files.
  Reason: mixes aggregate planner policy, explain descriptors, covering fast paths, session-matrix behavior, and ranking.
  Recommendation: leave pruned until there is a dedicated aggregate-test consolidation pass. Revive by owner slice, not by re-enabling the old family.

## Recommended Order

1. Delete the obsolete wrapper files after their surviving cases are accounted for.
2. Migrate route-owner tests file by file.
3. Split `semantics.rs` into owner-local slices.
4. Only then consider aggregate matrix revival.

## Push Guidance

The current executor test-root revival is in a good state to push after the route-owner migration.

Reason:

- the live subset is real and green
- the route-owner backlog is migrated instead of half-wired
- the remaining aggregate/pagination backlog is now clearly triaged for follow-up work

## Follow-Up Status

- The primary-key set-access runtime contracts for `by_ids` dedup, PK-union dedup, and recursive union execution-descriptor shape now live in `db/executor/tests/set_access.rs`.
- The simple intersection set-access runtime contracts for canonical asc/desc overlap, no-overlap empty result, duplicate suppression, and recursive intersection execution-descriptor shape now live in `db/executor/tests/set_access.rs`.
- The nested composite overlap and descending paged union/intersection continuation contracts now live in `db/executor/tests/set_access.rs`.
- The three-child descending union continuation stress case now also lives in `db/executor/tests/set_access.rs`.
- The post-access filtering contracts for `by_id` plus optional equality and ordered pagination over mixed `IN` + text predicates now live in `db/executor/tests/post_access.rs`.
- The post-access `contains(...)` filtering and filtered delete-limit contracts now also live in `db/executor/tests/post_access.rs`.
- The strong-relation delete guard, reverse-index lifecycle, and reverse-index recovery contracts now live in `db/executor/tests/reverse_index.rs`.
- The stale-secondary missing-row-policy contracts now live in `db/executor/tests/stale_secondary.rs`.
- The optional-order missing-value ordering contract now lives in `db/executor/tests/ordering.rs`.
- The ordered delete-limit runtime contract now also lives in `db/executor/tests/ordering.rs`.
- The small no-cursor limit-window pagination contracts now live in `db/executor/tests/pagination.rs`, and the old `pagination/limit_no_cursor_matrix.rs` file has been deleted.
- The core primary-key cursor resume, next-boundary, and PK fast-vs-fallback pagination parity contracts now also live in `db/executor/tests/pagination.rs`.
- The first secondary index-range pagination parity slice now also lives in `db/executor/tests/pagination.rs`: paged window parity, pushdown-vs-fallback cursor-boundary parity, and shared-boundary resume parity.
- The simple secondary-order pushdown parity contracts now also live in `db/executor/tests/pagination.rs`: canonical order parity, closed-prefix window preservation, and explicit descending rank/id parity.
- The first deeper index-range continuation invariants now also live in `db/executor/tests/pagination.rs`: raw anchor tracking against the last emitted row and cross-shape signature rejection between pushdown and fallback cursor lanes.
- The first composite-budget pagination contracts now also live in `db/executor/tests/pagination.rs`: safe ASC/DESC composite PK-order scan budgeting, cursor-boundary budget disablement, and budgeted-vs-fallback continuation-boundary parity.
- The next composite-budget pagination contracts now also live in `db/executor/tests/pagination.rs`: post-access-sort budget disablement, residual-filter budget disablement, and nested composite safe-shape scan budgeting.
- The mixed-direction composite fallback pagination contract now also lives in `db/executor/tests/pagination.rs`, and the old `pagination/composite_budget_matrix.rs` file has been deleted.
- The first composite range-edge pagination contracts now also live in `db/executor/tests/pagination.rs`: between-equivalent pushdown/fallback parity, min/max rank edge handling, and composite range cursor pagination without duplicates.
- The next composite range-edge contracts now also live in `db/executor/tests/pagination.rs`: strict anchor monotonicity across pages, descending mixed-edge duplicate-group resume, duplicate lower/upper edge boundary exhaustion, and the two trace outcome contracts for accepted secondary-order vs rejected composite index-range pushdown.
- The single-field descending range continuation contracts now also live in `db/executor/tests/pagination.rs`: upper-anchor resume suffix, lower-boundary exhaustion, single-element exhaustion, multi-page no-duplicate/no-omission coverage, and descending mixed-edge duplicate-group resume.
- The explicit single-field half-open range boundary contract now also lives in `db/executor/tests/pagination.rs`: lower-edge duplicate resume and upper-edge terminal exhaustion.
- The concrete single-field edge semantics now also live in `db/executor/tests/pagination.rs`: between-equivalent ordered parity, min/max tag edge handling, and the concrete unique secondary-range paged-vs-unbounded contract with strict raw-key anchor monotonicity.
- The remaining `range_edges_trace_matrix.rs` backlog is gone. Its property matrix and both table-driven parity loops now live in `db/executor/tests/pagination.rs`, and the old file has been deleted.
- The distinct backlog is now gone. Union distinct order/boundary parity, row-distinct DataKey preservation, union distinct boundary-complete resume, distinct DESC secondary boundary-complete resume, distinct DESC secondary fast/fallback parity, distinct DESC primary-key fast/fallback parity, distinct DESC index-range parity, distinct mixed-direction fallback parity, and the distinct offset parity family now all live in `db/executor/tests/pagination.rs`, and the old `pagination/distinct_matrix.rs` file has been deleted.
- The first ordering-permutation backlog tranche is now also live in `db/executor/tests/pagination.rs`: simple union/intersection child-order permutation parity, mixed-direction union/intersection child-order permutation parity, and the secondary-order trace-label contracts for explicit top-n seek vs non-top-n pushdown.
- The mixed-direction fallback-vs-uniform rank-unique parity contract is now also live in `db/executor/tests/pagination.rs`: row order, emitted boundaries, and token resumes stay aligned across the mixed-direction fallback lane and the equivalent uniform-direction lane.
- The mixed-direction resume matrix is now also live in `db/executor/tests/pagination.rs`: resume boundaries are complete across the rank/id and group/rank/id order variants, and paged traversal stays duplicate-free at limits 1, 2, and 3.
- The remaining table-driven union/intersection permutation loops are now also live in `db/executor/tests/pagination.rs`, and the old `pagination/ordering_permutation_matrix.rs` file has been deleted.
- The first `cursor_pk` backlog tranche is now also live in `db/executor/tests/pagination.rs`: stable offset-token bytes for the same plan shape, first-page-to-continuation window semantics across asc/desc plus offset variants, by-ids offset resume completeness, and shared-boundary PK fast-vs-fallback parity.
- The next `cursor_pk` backlog tranche is now also live in `db/executor/tests/pagination.rs`: bounded PK key-range continuation, cursor-past-end empty-page handling, the fail-closed inverted manual key-range invariant, non-top-n PK trace labeling, unsupported PK cursor boundary validation (missing/type/arity mismatch), and the `PhaseEntity` rank-order continuation boundary/signature family.
- The final `cursor_pk` backlog tranche is now also live in `db/executor/tests/pagination.rs`: PK fast-vs-by-ids shape-signature rejection, shape-local resume parity across asc/desc ordered windows, token replay parity with explicit cross-shape rejection, the secondary offset-resume parity cases, and PK fast-path scan-accounting coverage. The old `pagination/cursor_pk_matrix.rs` file has been deleted.
- The full `index_range` backlog is now also live in `db/executor/tests/pagination.rs`: descending index-prefix/by-ids parity, prefix-window terminal exhaustion, single-field/composite/unique full-stream direction symmetry, limit matrices, exact-size and terminal-page cursor suppression, index-range limit-pushdown trace and replay parity, residual-filter retry behavior, and the index-only predicate distinct/range/`IN` families. The old `pagination/index_range_matrix.rs` file has been deleted.
- The fluent explain text/json/verbose adapter contract now lives in `db/session/tests.rs`.
- The session-facing aggregate projection and ranked terminal contracts now also live in `db/session/tests.rs`: execute/projection parity for `values_by`, `values_by_with_ids`, and `distinct_values_by`; `take(k)` prefix parity; deterministic `top_k_by` / `bottom_k_by` ordering; ranked-row projection parity for value and value-with-id terminals; first/last value projection parity; base-order direction invariance across ranked terminals; and ranked insertion-order invariance.
- The typed `DbSession` facade slice now also lives in `db/session/tests.rs`: `select_one()` constant/no-metrics behavior, direct `show_indexes(...)` and `describe_entity(...)` payload coverage for plain and indexed session fixtures, and `trace_query(...)` plan-hash/explain parity plus ordered execution summary coverage. Reviving that slice also flushed out one stale expectation from the dead matrix: `trace_query()` guarantees a human-readable selected access hint, not a hard `Index...` strategy label.
- The typed `DbSession` aggregate-explain slice now also lives in `db/session/tests.rs`: `explain_exists()` standard-route coverage, `explain_not_exists()` alias parity, and `explain_first()` / `explain_last()` order-shape parity on the live session aggregate fixture. Reviving that slice also flushed out a second stale explain assumption from the dead matrix: the stable public contract is the route / execution-mode / node-type family, not legacy `projected_field` or `projection_mode` entries in aggregate execution metadata.
- The typed `DbSession` execution-explain matrix now also lives in `db/session/tests.rs`: strict indexed predicate prefilter staging, residual predicate staging, and `LIMIT 0` execution-shape behavior are now covered on the live session-local indexed fixture instead of the dead aggregate matrix.
- The typed `DbSession` execution-descriptor slice now also lives in `db/session/tests.rs`: by-key vs index-prefix vs index-multi root classification, unordered covering-scan eligibility, and ordered limited descriptor-tree structure are now covered on the live session-local fixtures. Reviving that slice flushed out more stale matrix assumptions: the session-facing descriptor contract does not guarantee the old executor-only metadata keys such as `covering_scan_reason`, `scan_direction`, or `order_satisfied_by_index`.
- The typed `DbSession` seek and execution-surface slice now also lives in `db/session/tests.rs`: indexed `explain_min()` / `explain_max()` seek labels plus fetch contracts, and the strict index-prefix execution text/json surface contract, now both covered on the local indexed aggregate fixture instead of the dead session matrix.
- The session-owned aggregate terminal parity slice now also lives in `db/session/tests.rs`: `min_by("missing_field")` fail-before-scan behavior, numeric `sum_by("rank")` / `avg_by("rank")` execute parity, and the existing identity/new-field aggregate parity now cover the executor-root duplicates that were still stranded in `aggregate/session_matrix.rs`.
- The remaining non-temporal session-path slice now also lives in `db/session/tests.rs`: identity-terminal parity, `exists` / `not_exists` / `is_empty` early-stop scan-budget parity, `primary_key IS NULL` zero-scan lowering, and `primary_key IS NULL OR id = ...` branch parity are now covered on the live session aggregate fixture instead of the executor-root matrix.
- The temporal session-value slice now also lives in `db/session/tests.rs`: entity/value projection typing, grouped temporal keys, distinct temporal projections, first/last scalar projections, value/id pairs, ranked temporal value projections, and ranked row terminals are all now covered on the live `SessionTemporalEntity` fixture. The old `aggregate/session_matrix.rs` file is deleted.
- The first executor-owned aggregate core slice now lives in `db/executor/tests/aggregate_core.rs`: bypassed field-target executor invariants, unknown ranked target fail-closed behavior across all ranked terminal forms, and non-orderable field-target rejection with zero scan-budget consumption.
- The remaining executor-owned ranked and secondary-index extrema slice now also lives in `db/executor/tests/aggregate_core.rs`: deterministic field-target MIN/MAX selection, primary-key ascending tie-break semantics, deterministic `nth_by(...)` positions, `median_by(...)` lower-median policy, `min_max_by(...)` parity, `k=0` empty-window scan-budget parity, and the secondary-index ordered MIN/MAX field-target contracts. The old `aggregate/ranked_matrix.rs` backlog is gone, and `aggregate/core_contract_matrix.rs` has been trimmed down to the still-unique fail-closed cases.
- The bypassed-validation fail-closed slice now also lives in `db/executor/tests/aggregate_core.rs`: planner-bypassed unknown-field and non-orderable field-target MIN rejection now run in the live owner suite instead of the stale `aggregate/core_contract_matrix.rs` backlog.
- The stale duplicate of the planner-bypassed non-extrema invariant test is also gone from `aggregate/core_contract_matrix.rs`, and the old file no longer carries the dead `execute_bypassed_field_target_validation(...)` helper.
- The first executor-owned aggregate projection slice now lives in `db/executor/tests/aggregate_projection.rs`: `count_distinct_by(...)` effective-window parity, non-orderable/list-order stability, residual-retry scan-budget parity, distinct-modifier window tracking, row-level `values_by(...).distinct()` semantics, `distinct_values_by(...)` effective-window and first-observed-dedup parity, covering constant/index projection parity, covering non-leading distinct-order parity, optional-null and missing-field ranked projection parity, and strict missing-row corruption surfacing for both covering constant and covering index projections.
- The old `aggregate/projection_matrix.rs` backlog is now gone. Its remaining executor-owned projection scan-budget matrix now lives in `db/executor/tests/aggregate_projection.rs`, and its session-owned projection contracts were already lifted into `db/session/tests.rs`.
- The old `aggregate/ranked_matrix.rs` backlog is now gone. Its remaining executor-owned ranked/extrema parity and `k=0` scan-budget contracts now live in `db/executor/tests/aggregate_core.rs`, and its session-owned direction/insertion semantics were already lifted into `db/session/tests.rs`.
- The first executor-owned aggregate path slice now lives in `db/executor/tests/aggregate_path.rs`: by-id/by-ids/count/exists window parity, strict missing-row corruption classification, full-scan/key-range/index-range scan-budget bounds, union/intersection path parity, composite direct-vs-fallback scan-accounting, index-range aggregate parity, strict consistency parity, and limit-zero aggregate parity.
- The session-owned aggregate bytes slice now lives in `db/session/tests.rs`: `bytes()` persisted-row parity, `bytes_by("rank")` encoded-value parity, `explain_bytes_by("rank")` terminal metadata and strict-materialized-mode coverage, empty-window zero handling, and unknown-field fail-before-scan / fail-before-planning behavior. Reviving those tests also flushed out one stale assumption from the dead matrix: `bytes_by(...)` counts canonical serialized field values, not stored slot-envelope bytes.
- The executor-owned execution-pipeline snapshot family now lives again in `db/executor/tests/semantics.rs`.
- Reviving that file did not expose a runtime bug. It exposed stale snapshot expectations only: plan-hash, continuation-signature, grouped execution-strategy, and execution-descriptor wording had all drifted with the live executor/query surface.
