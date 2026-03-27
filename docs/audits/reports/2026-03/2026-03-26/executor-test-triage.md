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

The still-pruned families are not one homogeneous backlog. They split into three buckets:

1. Delete wrapper/harness files that only existed to aggregate stale suites.
2. Migrate owner-local tests into the modules that now own the behavior.
3. Leave the very large matrix families pruned until they can be revived deliberately.

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
  Size: 12387 lines across eight files.
  Reason: mixes aggregate planner policy, explain descriptors, covering fast paths, session-matrix behavior, and ranking.
  Recommendation: leave pruned until there is a dedicated aggregate-test consolidation pass. Revive by owner slice, not by re-enabling the old family.

- `crates/icydb-core/src/db/executor/tests/pagination/*.rs`
  Size: 4944 lines across three files.
  Reason: heavy use of stale builder helpers and shared fixtures, plus the remaining deep ordering/cursor/index-range matrices.
  Stale dependencies seen: `AccessPlannedQuery::new_typed(...)`, `cursor_boundary_from_entity(...)`, `execute_paged_with_cursor(...)`, and shared fixture entities from the old root test module.
  Recommendation: continue by owner slice. The small no-cursor limit-window contracts, the core primary-key cursor resume and fast-vs-fallback parity cases, the first index-range pushdown/fallback plus simple secondary-order parity slices, the first index-range anchor/signature contracts, the composite-budget safe-shape / budget-disable / boundary-parity family, the full range-edge continuation family, the single-field and composite table-driven range parity loops, the unique secondary-range property matrix, and the full distinct family are now live in `db/executor/tests/pagination.rs`; the remaining matrix backlog is now the ordering-permutation / cursor-pk / index-range families.

## Recommended Order

1. Delete the obsolete wrapper files after their surviving cases are accounted for.
2. Migrate route-owner tests file by file.
3. Split `semantics.rs` into owner-local slices.
4. Only then consider aggregate or pagination matrix revival.

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
- The fluent explain text/json/verbose adapter contract now lives in `db/session/tests.rs`.
- The executor-owned execution-pipeline snapshot family now lives again in `db/executor/tests/semantics.rs`.
- Reviving that file did not expose a runtime bug. It exposed stale snapshot expectations only: plan-hash, continuation-signature, grouped execution-strategy, and execution-descriptor wording had all drifted with the live executor/query surface.
