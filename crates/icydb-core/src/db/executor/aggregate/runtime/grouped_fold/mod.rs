//! Module: executor::aggregate::runtime::grouped_fold
//! Responsibility: grouped key-stream construction and fold execution mechanics.
//! Does not own: grouped route derivation or grouped output finalization.
//! Boundary: consumes grouped route-stage payload and emits grouped fold-stage payload.

mod bundle;
mod page_finalize;

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

#[cfg(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
))]
use std::cell::RefCell;

use crate::{
    db::{
        contracts::canonical_value_compare,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            ExecutionPreparation,
            aggregate::runtime::grouped_fold::{
                bundle::{GroupedAggregateBundle, GroupedAggregateBundleSpec},
                page_finalize::finalize_grouped_page,
            },
            aggregate::{
                ExecutionContext, GroupError, aggregate_materialized_fold_direction,
                runtime::{
                    grouped_distinct::{
                        execute_global_distinct_field_aggregate,
                        global_distinct_field_target_and_kind, page_global_distinct_grouped_row,
                    },
                    grouped_output::project_grouped_rows_from_projection,
                },
            },
            group::{GroupKey, StableHash, stable_hash_from_digest},
            group::{grouped_budget_observability, grouped_execution_context_from_planner_config},
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntimeAdapter, GroupedCursorPage, GroupedFoldStage,
                GroupedRouteStage, GroupedStreamStage, PageCursor, PreparedExecutionProjection,
                ProjectionMaterializationMode, RowView, StructuralGroupedRowRuntime,
            },
            plan_metrics::record_grouped_plan_metrics,
        },
        index::IndexCompilePolicy,
        query::plan::FieldSlot,
    },
    error::InternalError,
    model::field::FieldKind,
    value::{Value, ValueHashWriter},
};

///
/// GroupedCountFoldMetrics
///
/// GroupedCountFoldMetrics aggregates one test-scoped view of the dedicated
/// grouped `COUNT(*)` fold path inside executor runtime.
/// It lets perf probes separate fold-path row ingestion, bucket lookup,
/// grouped-key insertion, and page finalization work without changing runtime
/// behavior.
///

#[cfg(feature = "structural-read-metrics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GroupedCountFoldMetrics {
    pub fold_stage_runs: u64,
    pub rows_folded: u64,
    pub borrowed_probe_rows: u64,
    pub borrowed_hash_computations: u64,
    pub owned_group_fallback_rows: u64,
    pub owned_key_materializations: u64,
    pub bucket_candidate_checks: u64,
    pub existing_group_hits: u64,
    pub new_group_inserts: u64,
    pub finalize_stage_runs: u64,
    pub finalized_group_count: u64,
    pub window_rows_considered: u64,
    pub having_rows_rejected: u64,
    pub resume_boundary_rows_rejected: u64,
    pub candidate_rows_qualified: u64,
    pub bounded_selection_candidates_seen: u64,
    pub bounded_selection_heap_replacements: u64,
    pub bounded_selection_rows_sorted: u64,
    pub unbounded_selection_rows_sorted: u64,
    pub page_rows_skipped_for_offset: u64,
    pub projection_rows_input: u64,
    pub page_rows_emitted: u64,
    pub cursor_construction_attempts: u64,
    pub next_cursor_emitted: u64,
}

#[cfg(not(feature = "structural-read-metrics"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GroupedCountFoldMetrics {
    pub fold_stage_runs: u64,
    pub rows_folded: u64,
    pub borrowed_probe_rows: u64,
    pub borrowed_hash_computations: u64,
    pub owned_group_fallback_rows: u64,
    pub owned_key_materializations: u64,
    pub bucket_candidate_checks: u64,
    pub existing_group_hits: u64,
    pub new_group_inserts: u64,
    pub finalize_stage_runs: u64,
    pub finalized_group_count: u64,
    pub window_rows_considered: u64,
    pub having_rows_rejected: u64,
    pub resume_boundary_rows_rejected: u64,
    pub candidate_rows_qualified: u64,
    pub bounded_selection_candidates_seen: u64,
    pub bounded_selection_heap_replacements: u64,
    pub bounded_selection_rows_sorted: u64,
    pub unbounded_selection_rows_sorted: u64,
    pub page_rows_skipped_for_offset: u64,
    pub projection_rows_input: u64,
    pub page_rows_emitted: u64,
    pub cursor_construction_attempts: u64,
    pub next_cursor_emitted: u64,
}

#[cfg(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
))]
std::thread_local! {
    static GROUPED_COUNT_FOLD_METRICS: RefCell<Option<GroupedCountFoldMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
))]
fn update_grouped_count_fold_metrics(update: impl FnOnce(&mut GroupedCountFoldMetrics)) {
    GROUPED_COUNT_FOLD_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(not(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
)))]
fn update_grouped_count_fold_metrics(_update: impl FnOnce(&mut GroupedCountFoldMetrics)) {}

/// with_grouped_count_fold_metrics
///
/// Run one closure while collecting dedicated grouped `COUNT(*)` fold metrics
/// on the current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
))]
#[allow(dead_code)]
pub(crate) fn with_grouped_count_fold_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, GroupedCountFoldMetrics) {
    GROUPED_COUNT_FOLD_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "grouped count fold metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(GroupedCountFoldMetrics::default());
    });

    let result = f();
    let metrics =
        GROUPED_COUNT_FOLD_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}

#[cfg(not(any(
    test,
    feature = "structural-read-metrics",
    feature = "perf-attribution"
)))]
#[allow(dead_code)]
pub(crate) fn with_grouped_count_fold_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, GroupedCountFoldMetrics) {
    (f(), GroupedCountFoldMetrics::default())
}

///
/// GroupedCountState
///
/// GroupedCountState keeps the dedicated grouped `COUNT(*)` fold on a
/// borrowed-probe fast path and defers owned `GroupKey` construction until a
/// genuinely new group must be inserted.
///

struct GroupedCountState {
    groups: Vec<(GroupKey, u32)>,
    bucket_index: HashMap<StableHash, GroupedCountBucket>,
}

///
/// GroupedCountBucket
///
/// GroupedCountBucket keeps the common grouped-count hash-bucket case
/// allocation-free by storing one lone group index inline and promoting to a
/// collision vector only after the fold actually observes a stable-hash peer.
///

enum GroupedCountBucket {
    Single(usize),
    Colliding(Vec<usize>),
}

impl GroupedCountBucket {
    // Return the tracked bucket indexes as one shared slice regardless of
    // whether the bucket stayed collision-free or promoted to a vector.
    const fn as_slice(&self) -> &[usize] {
        match self {
            Self::Single(index) => std::slice::from_ref(index),
            Self::Colliding(indexes) => indexes.as_slice(),
        }
    }

    // Insert one grouped-count index, promoting to a vector only when the
    // bucket actually needs to retain more than one peer.
    fn push_index(&mut self, new_index: usize) {
        match self {
            Self::Single(existing_index) => {
                *self = Self::Colliding(vec![*existing_index, new_index]);
            }
            Self::Colliding(indexes) => indexes.push(new_index),
        }
    }

    // Build one fresh collision-free grouped-count bucket.
    const fn single(index: usize) -> Self {
        Self::Single(index)
    }
}

///
/// BoundedGroupedCountCandidate
///
/// BoundedGroupedCountCandidate keeps the largest retained canonical grouped
/// key at the top of the heap so grouped-count finalization can keep only the
/// smallest `selection_bound` rows when pagination bounds are active.
///

#[derive(Eq, PartialEq)]
struct BoundedGroupedCountCandidate {
    group_key: GroupKey,
    count: u32,
    direction: Direction,
}

impl Ord for BoundedGroupedCountCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_grouped_boundary_values(
            self.direction,
            self.group_key.canonical_value(),
            other.group_key.canonical_value(),
        )
    }
}

impl PartialOrd for BoundedGroupedCountCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl GroupedCountState {
    // Build one empty grouped-count state container.
    fn new() -> Self {
        Self {
            groups: Vec::new(),
            bucket_index: HashMap::new(),
        }
    }

    // Increment one grouped count row while keeping the common existing-group
    // path on borrowed row-slot hashing and comparison only.
    fn increment_row(
        &mut self,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        borrowed_group_probe_supported: bool,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.rows_folded = metrics.rows_folded.saturating_add(1);
        });

        // Phase 1: keep the common scalar-like grouped key path allocation-free
        // until we prove the row belongs to a genuinely new group.
        if borrowed_group_probe_supported {
            update_grouped_count_fold_metrics(|metrics| {
                metrics.borrowed_probe_rows = metrics.borrowed_probe_rows.saturating_add(1);
            });
            update_grouped_count_fold_metrics(|metrics| {
                metrics.borrowed_hash_computations =
                    metrics.borrowed_hash_computations.saturating_add(1);
            });
            let group_hash = stable_hash_group_values_from_row_view(row_view, group_fields)?;
            if let Some(existing_index) = find_matching_group_index(
                self.groups.as_slice(),
                self.bucket_index.get(&group_hash),
                row_view,
                group_fields,
            )? {
                let (_, count) = self.groups.get_mut(existing_index).ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "grouped count state missing bucket-indexed group: index={existing_index}",
                    ))
                })?;
                *count = count.saturating_add(1);

                return Ok(());
            }

            let group_key =
                materialize_group_key_from_row_view(row_view, group_fields, Some(group_hash))?;
            debug_assert_eq!(
                group_key.hash(),
                group_hash,
                "borrowed grouped key hash must match owned canonical group key hash",
            );

            return self.finish_new_group_insert(group_hash, group_key, grouped_execution_context);
        }

        // Phase 2: preserve the canonical owned-key fallback for structured
        // grouped values whose equality contract still depends on full
        // canonical materialization.
        let group_key = materialize_group_key_from_row_view(row_view, group_fields, None)?;

        self.increment_owned_group_key(group_key, grouped_execution_context)
    }

    // Increment one grouped count row from one direct single grouped value
    // when the grouped route already proves the single-field identity-canonical
    // fast path is valid.
    fn increment_single_group_value(
        &mut self,
        group_value: Value,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.rows_folded = metrics.rows_folded.saturating_add(1);
            metrics.borrowed_probe_rows = metrics.borrowed_probe_rows.saturating_add(1);
            metrics.borrowed_hash_computations =
                metrics.borrowed_hash_computations.saturating_add(1);
        });
        let group_hash = stable_hash_single_group_value(&group_value)?;
        if let Some(existing_index) = find_matching_single_group_value_index(
            self.groups.as_slice(),
            self.bucket_index.get(&group_hash),
            &group_value,
        )? {
            let (_, count) = self.groups.get_mut(existing_index).ok_or_else(|| {
                InternalError::query_executor_invariant(format!(
                    "grouped count state missing bucket-indexed direct group: index={existing_index}",
                ))
            })?;
            *count = count.saturating_add(1);

            return Ok(());
        }

        update_grouped_count_fold_metrics(|metrics| {
            metrics.owned_key_materializations =
                metrics.owned_key_materializations.saturating_add(1);
        });
        let group_key =
            GroupKey::from_single_canonical_group_value_with_hash(group_value, group_hash);

        self.finish_new_group_insert(group_hash, group_key, grouped_execution_context)
    }

    // Insert one newly observed grouped key after the borrowed fast path has
    // already ruled out an existing canonical group match.
    fn finish_new_group_insert(
        &mut self,
        group_hash: StableHash,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        grouped_execution_context
            .record_new_group(group_count_before_insert, group_capacity_before_insert)
            .map_err(GroupError::into_internal_error)?;
        let new_index = self.groups.len();
        self.groups.push((group_key, 1));
        self.bucket_index
            .entry(group_hash)
            .and_modify(|bucket| bucket.push_index(new_index))
            .or_insert_with(|| GroupedCountBucket::single(new_index));
        update_grouped_count_fold_metrics(|metrics| {
            metrics.new_group_inserts = metrics.new_group_inserts.saturating_add(1);
        });

        Ok(())
    }

    // Increment one grouped count row from an already-owned canonical key.
    fn increment_owned_group_key(
        &mut self,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.owned_group_fallback_rows = metrics.owned_group_fallback_rows.saturating_add(1);
        });

        // Phase 1: reuse the stable-hash side index so owned-key fallback rows
        // still avoid a full scan across every grouped count entry.
        let group_hash = group_key.hash();
        if let Some(bucket) = self.bucket_index.get(&group_hash) {
            for existing_index in bucket.as_slice().iter().copied() {
                update_grouped_count_fold_metrics(|metrics| {
                    metrics.bucket_candidate_checks =
                        metrics.bucket_candidate_checks.saturating_add(1);
                });
                if self
                    .groups
                    .get(existing_index)
                    .is_some_and(|(existing, _)| existing == &group_key)
                {
                    let (_, count) = self.groups.get_mut(existing_index).ok_or_else(|| {
                        InternalError::query_executor_invariant(format!(
                            "grouped count state missing owned-key bucket index: index={existing_index}",
                        ))
                    })?;
                    *count = count.saturating_add(1);
                    update_grouped_count_fold_metrics(|metrics| {
                        metrics.existing_group_hits = metrics.existing_group_hits.saturating_add(1);
                    });

                    return Ok(());
                }
            }
        }

        // Phase 2: admit one new group only after the existing-group lookup
        // misses under canonical owned-key equality.
        self.finish_new_group_insert(group_hash, group_key, grouped_execution_context)
    }

    // Consume this grouped-count state into finalized `(group_key, count)` rows.
    fn into_groups(self) -> Vec<(GroupKey, u32)> {
        self.groups
    }
}

// Materialize one canonical grouped key from row slots when borrowed probing
// cannot satisfy the ingest path or the row opens a genuinely new group.
fn materialize_group_key_from_row_view(
    row_view: &RowView,
    group_fields: &[FieldSlot],
    precomputed_hash: Option<StableHash>,
) -> Result<GroupKey, InternalError> {
    update_grouped_count_fold_metrics(|metrics| {
        metrics.owned_key_materializations = metrics.owned_key_materializations.saturating_add(1);
    });

    if let [field] = group_fields {
        let group_value = row_view.require_slot_ref(field.index())?.clone();
        let identity_canonical_form = field
            .kind()
            .is_some_and(single_group_field_kind_has_identity_canonical_form);

        return match (identity_canonical_form, precomputed_hash) {
            (true, Some(hash)) => Ok(GroupKey::from_single_canonical_group_value_with_hash(
                group_value,
                hash,
            )),
            (true, None) => GroupKey::from_single_canonical_group_value(group_value),
            (false, Some(hash)) => GroupKey::from_single_group_value_with_hash(group_value, hash),
            (false, None) => GroupKey::from_single_group_value(group_value),
        }
        .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error);
    }

    let group_values = row_view.group_values(group_fields)?;
    match precomputed_hash {
        Some(hash) => GroupKey::from_group_values_with_hash(group_values, hash),
        None => GroupKey::from_group_values(group_values),
    }
    .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)
}

// Build one grouped key stream from route-owned grouped execution metadata
// using already-resolved runtime and row-decode boundaries.
pub(in crate::db::executor) fn build_grouped_stream_with_runtime(
    route: &GroupedRouteStage,
    runtime: &ExecutionRuntimeAdapter<'_>,
    execution_preparation: ExecutionPreparation,
    row_runtime: StructuralGroupedRowRuntime,
) -> Result<GroupedStreamStage, InternalError> {
    let execution_inputs = ExecutionInputs::new_prepared(
        runtime,
        route.plan(),
        AccessStreamBindings {
            index_prefix_specs: route.index_prefix_specs(),
            index_range_specs: route.index_range_specs(),
            continuation: AccessScanContinuationInput::new(None, route.direction()),
        },
        &execution_preparation,
        ProjectionMaterializationMode::SharedValidation,
        PreparedExecutionProjection::empty(),
        true,
    );
    record_grouped_plan_metrics(&route.plan().access, route.grouped_execution_mode());
    let resolved = ExecutionKernel::resolve_execution_key_stream_without_distinct(
        &execution_inputs,
        route.grouped_route_plan(),
        IndexCompilePolicy::ConservativeSubset,
    )?;

    Ok(GroupedStreamStage::new(
        row_runtime,
        execution_preparation,
        resolved,
    ))
}

// Execute grouped aggregate folding over one resolved grouped key stream using
// only structural grouped reducer/runtime contracts.
pub(in crate::db::executor) fn execute_group_fold_stage(
    route: &GroupedRouteStage,
    mut stream: GroupedStreamStage,
) -> Result<GroupedFoldStage, InternalError> {
    // Phase 1: initialize grouped fold context, projection contracts, and reducers.
    let mut grouped_execution_context =
        grouped_execution_context_from_planner_config(Some(route.grouped_execution()));
    let grouped_budget = grouped_budget_observability(&grouped_execution_context);
    debug_assert!(
        grouped_budget.max_groups() >= grouped_budget.groups()
            && grouped_budget.max_group_bytes() >= grouped_budget.estimated_bytes()
            && grouped_execution_context
                .config()
                .max_distinct_values_total()
                >= grouped_budget.distinct_values()
            && grouped_budget.aggregate_states() >= grouped_budget.groups(),
        "grouped budget observability invariants must hold at grouped route entry",
    );
    let grouped_projection_spec = route.plan().frozen_projection_spec().clone();

    // Phase 2: route global DISTINCT grouped aggregates through their
    // dedicated grouped execution path when strategy permits it.
    if let Some(folded) = try_execute_global_distinct_grouped_fold_stage(
        route,
        &mut stream,
        &mut grouped_execution_context,
        &grouped_projection_spec,
    )? {
        return Ok(folded);
    }

    // Phase 2B: route the common grouped `COUNT(*)` shape through the
    // planner-carried dedicated fold-path contract instead of re-reading
    // grouped planner strategy inside runtime.
    if route.grouped_fold_path().uses_count_rows_dedicated_fold() {
        return execute_single_grouped_count_fold_stage(
            route,
            &mut stream,
            &mut grouped_execution_context,
            &grouped_projection_spec,
        );
    }

    // Phase 3: initialize grouped engines only for the remaining grouped
    // aggregate families that still use the canonical grouped reducer path.
    let grouped_bundle = build_grouped_bundle(route, &grouped_execution_context)?;

    // Phase 4: retain the canonical generic grouped reducer path for every
    // grouped aggregate shape that is not covered by a dedicated fast path.
    execute_generic_grouped_fold_stage(
        route,
        &mut stream,
        &mut grouped_execution_context,
        grouped_bundle,
        &grouped_projection_spec,
    )
}

// Build the shared grouped aggregate bundle for canonical grouped terminal
// projection layout.
fn build_grouped_bundle(
    route: &GroupedRouteStage,
    grouped_execution_context: &ExecutionContext,
) -> Result<GroupedAggregateBundle, InternalError> {
    if global_distinct_field_target_and_kind(route.grouped_distinct_execution_strategy()).is_some()
    {
        return Ok(GroupedAggregateBundle::new(Vec::new()));
    }

    let grouped_specs = route
        .projection_layout()
        .aggregate_positions()
        .iter()
        .enumerate()
        .map(|(aggregate_index, projection_index)| {
            let aggregate_spec = route
                .grouped_aggregate_execution_specs()
                .get(aggregate_index)
                .ok_or_else(|| {
                    GroupedRouteStage::aggregate_index_out_of_bounds_for_projection_layout(
                        *projection_index,
                        aggregate_index,
                    )
                })?;

            GroupedAggregateBundleSpec::new(
                aggregate_spec.kind(),
                aggregate_materialized_fold_direction(aggregate_spec.kind()),
                aggregate_spec.distinct(),
                aggregate_spec.target_field().cloned(),
                grouped_execution_context
                    .config()
                    .max_distinct_values_per_group(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(GroupedAggregateBundle::new(grouped_specs))
}

// Execute one grouped global-DISTINCT route through the existing dedicated
// grouped distinct aggregate path when that strategy is active.
fn try_execute_global_distinct_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<Option<GroupedFoldStage>, InternalError> {
    if global_distinct_field_target_and_kind(route.grouped_distinct_execution_strategy()).is_none()
    {
        return Ok(None);
    }

    grouped_execution_context
        .record_implicit_single_group()
        .map_err(GroupError::into_internal_error)?;
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let global_row = execute_global_distinct_field_aggregate(
        route.consistency(),
        row_runtime,
        resolved,
        compiled_predicate,
        grouped_execution_context,
        route.grouped_distinct_execution_strategy(),
        (&mut scanned_rows, &mut filtered_rows),
    )?;
    let grouped_window = route.grouped_pagination_window();
    let page_rows = page_global_distinct_grouped_row(
        global_row,
        grouped_window.initial_offset_for_page(),
        grouped_window.limit(),
    );
    update_grouped_count_fold_metrics(|metrics| {
        metrics.projection_rows_input = metrics
            .projection_rows_input
            .saturating_add(u64::try_from(page_rows.len()).unwrap_or(u64::MAX));
    });
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;

    Ok(Some(GroupedFoldStage::from_grouped_stream(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor: None,
        },
        filtered_rows,
        false,
        stream,
        scanned_rows,
    )))
}

// Execute grouped `COUNT(*)` through a dedicated fold path that keeps only one
// canonical grouped-count map instead of the generic grouped reducer stack.
fn execute_single_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    update_grouped_count_fold_metrics(|metrics| {
        metrics.fold_stage_runs = metrics.fold_stage_runs.saturating_add(1);
    });
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let consistency = route.consistency();
    let borrowed_group_probe_supported =
        group_fields_support_borrowed_group_probe(route.group_fields());
    let direct_single_group_field = match (compiled_predicate, route.group_fields()) {
        (None, [field])
            if field
                .kind()
                .is_some_and(single_group_field_kind_has_identity_canonical_form) =>
        {
            Some(field)
        }
        _ => None,
    };
    let mut grouped_counts = GroupedCountState::new();

    // Phase 1: fold grouped source rows directly into one canonical count map.
    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        if let Some(group_field) = direct_single_group_field {
            let Some(group_value) =
                row_runtime.read_single_group_value(consistency, &data_key, group_field.index())?
            else {
                continue;
            };
            scanned_rows = scanned_rows.saturating_add(1);
            filtered_rows = filtered_rows.saturating_add(1);
            grouped_counts.increment_single_group_value(group_value, grouped_execution_context)?;

            continue;
        }

        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            continue;
        };
        scanned_rows = scanned_rows.saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            continue;
        }
        filtered_rows = filtered_rows.saturating_add(1);
        grouped_counts.increment_row(
            &row_view,
            route.group_fields(),
            borrowed_group_probe_supported,
            grouped_execution_context,
        )?;
    }

    // Phase 2: page and project the finalized grouped-count rows directly so
    // this dedicated path does not round-trip through the generic candidate
    // row envelope only to rebuild grouped rows immediately afterwards.
    let (page_rows, next_cursor) =
        finalize_grouped_count_page(route, grouped_projection_spec, grouped_counts.into_groups())?;

    Ok(GroupedFoldStage::from_grouped_stream(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Ingest grouped source rows into the shared grouped bundle while preserving
// grouped budget contracts and borrowed grouped-key fast paths.
fn fold_group_rows_into_bundle(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_bundle: &mut GroupedAggregateBundle,
) -> Result<(usize, usize), InternalError> {
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let consistency = route.consistency();
    let borrowed_group_probe_supported =
        group_fields_support_borrowed_group_probe(route.group_fields());

    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            continue;
        };
        scanned_rows = scanned_rows.saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            continue;
        }
        filtered_rows = filtered_rows.saturating_add(1);

        // Phase 1: preserve the borrowed grouped-key fast path so existing
        // groups stay allocation-free on the hot ingest loop.
        let borrowed_group_hash = if borrowed_group_probe_supported {
            Some(stable_hash_group_values_from_row_view(
                &row_view,
                route.group_fields(),
            )?)
        } else {
            None
        };
        let mut owned_group_key = None;

        // Phase 2: update the shared per-group aggregate-state row instead of
        // routing the row through one engine-owned group map per aggregate.
        grouped_bundle
            .ingest_row(
                grouped_execution_context,
                &data_key,
                &row_view,
                route.group_fields(),
                borrowed_group_hash,
                &mut owned_group_key,
            )
            .map_err(GroupError::into_internal_error)?;
    }

    Ok((scanned_rows, filtered_rows))
}

// Execute the canonical grouped reducer/finalize path for every grouped shape
// that does not use a dedicated grouped fast path.
fn execute_generic_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    mut grouped_bundle: GroupedAggregateBundle,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let (scanned_rows, filtered_rows) = fold_group_rows_into_bundle(
        route,
        stream,
        grouped_execution_context,
        &mut grouped_bundle,
    )?;
    let (page_rows, next_cursor) = finalize_grouped_page(
        route,
        grouped_projection_spec,
        grouped_bundle,
        route.grouped_pagination_window(),
    )?;

    Ok(GroupedFoldStage::from_grouped_stream(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Return true when one grouped value can be hashed and compared from the
// borrowed row slot without first allocating a canonical owned value tree.
#[cfg(test)]
fn group_value_supports_borrowed_group_probe(value: &Value) -> bool {
    match value {
        Value::List(_) | Value::Map(_) | Value::Unit => false,
        Value::Enum(value_enum) => value_enum
            .payload()
            .is_none_or(group_value_supports_borrowed_group_probe),
        _ => true,
    }
}

// Return true when one single grouped field kind already arrives in canonical
// grouped-equality form, so owned single-field key materialization can skip
// the recursive normalization pass entirely.
const fn single_group_field_kind_has_identity_canonical_form(kind: FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Account
            | FieldKind::Blob
            | FieldKind::Bool
            | FieldKind::Date
            | FieldKind::Duration
            | FieldKind::Float32
            | FieldKind::Float64
            | FieldKind::Int
            | FieldKind::Int128
            | FieldKind::IntBig
            | FieldKind::Principal
            | FieldKind::Subaccount
            | FieldKind::Text
            | FieldKind::Timestamp
            | FieldKind::Uint
            | FieldKind::Uint128
            | FieldKind::UintBig
            | FieldKind::Ulid
    )
}

// Return true when one planner-frozen grouped field kind can stay on the
// borrowed grouped-key probe path without owned canonical materialization.
fn group_field_kind_supports_borrowed_group_probe(kind: FieldKind) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid => true,
        FieldKind::Enum { variants, .. } => variants.iter().all(|variant| {
            variant.payload_kind().is_none_or(|payload_kind| {
                group_field_kind_supports_borrowed_group_probe(*payload_kind)
            })
        }),
        FieldKind::Relation { key_kind, .. } => {
            group_field_kind_supports_borrowed_group_probe(*key_kind)
        }
        FieldKind::List(_)
        | FieldKind::Set(_)
        | FieldKind::Map { .. }
        | FieldKind::Structured { .. }
        | FieldKind::Unit => false,
    }
}

// Return true when every planner-frozen grouped slot kind supports the
// borrowed grouped-key probe path for this grouped route.
fn group_fields_support_borrowed_group_probe(group_fields: &[FieldSlot]) -> bool {
    group_fields.iter().all(|field| {
        field
            .kind()
            .is_some_and(group_field_kind_supports_borrowed_group_probe)
    })
}

// Return true when every grouped slot on this row can use the borrowed count
// fast path without falling back to owned canonical materialization.
#[cfg(test)]
fn supports_borrowed_group_probe(
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    for field in group_fields {
        if !group_value_supports_borrowed_group_probe(row_view.require_slot_ref(field.index())?) {
            return Ok(false);
        }
    }

    Ok(true)
}

// Hash one virtual grouped key list directly from borrowed row slots so the
// grouped `COUNT(*)` fast path does not allocate `Vec<Value>` on lookups.
fn stable_hash_group_values_from_row_view(
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<StableHash, InternalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(group_fields.len());

    for field in group_fields {
        hash_writer.write_list_value(row_view.require_slot_ref(field.index())?)?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

// Hash one canonical single grouped value through the same one-element list
// framing used by grouped-count key materialization.
fn stable_hash_single_group_value(group_value: &Value) -> Result<StableHash, InternalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(1);
    hash_writer.write_list_value(group_value)?;

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

// Return true when one canonical grouped key matches one direct single grouped
// value under the grouped-count single-field identity-canonical fast path.
fn single_group_key_matches_value(
    group_key: &GroupKey,
    group_value: &Value,
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = group_key.canonical_value() else {
        return Err(InternalError::query_executor_invariant(
            "grouped count key must remain a canonical Value::List".to_string(),
        ));
    };
    let [canonical_group_value] = canonical_group_values.as_slice() else {
        return Err(InternalError::query_executor_invariant(format!(
            "single-field grouped count key must retain exactly one canonical value: len={}",
            canonical_group_values.len(),
        )));
    };

    Ok(canonical_value_compare(group_value, canonical_group_value) == Ordering::Equal)
}

// Return true when one canonical grouped key matches this row's grouped slot
// values under the borrowed grouped-count fast-path equality contract.
fn canonical_group_value_matches_row_view(
    canonical_group_value: &Value,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = canonical_group_value else {
        return Err(InternalError::query_executor_invariant(
            "grouped count key must remain a canonical Value::List".to_string(),
        ));
    };
    if canonical_group_values.len() != group_fields.len() {
        return Err(InternalError::query_executor_invariant(format!(
            "grouped count key field count drifted from route group fields: key_len={} group_fields_len={}",
            canonical_group_values.len(),
            group_fields.len(),
        )));
    }

    for (field, canonical_group_value) in group_fields.iter().zip(canonical_group_values) {
        if canonical_value_compare(
            row_view.require_slot_ref(field.index())?,
            canonical_group_value,
        ) != Ordering::Equal
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn group_key_matches_row_view(
    group_key: &GroupKey,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    canonical_group_value_matches_row_view(group_key.canonical_value(), row_view, group_fields)
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches the current borrowed grouped slot values.
fn find_matching_group_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupedCountBucket>,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<Option<usize>, InternalError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    for group_index in bucket.as_slice() {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.bucket_candidate_checks = metrics.bucket_candidate_checks.saturating_add(1);
        });
        let Some((group_key, _)) = grouped_counts.get(*group_index) else {
            return Err(InternalError::query_executor_invariant(format!(
                "grouped count bucket index out of bounds: index={} len={}",
                group_index,
                grouped_counts.len(),
            )));
        };
        if group_key_matches_row_view(group_key, row_view, group_fields)? {
            update_grouped_count_fold_metrics(|metrics| {
                metrics.existing_group_hits = metrics.existing_group_hits.saturating_add(1);
            });
            return Ok(Some(*group_index));
        }
    }

    Ok(None)
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches one direct single grouped value.
fn find_matching_single_group_value_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupedCountBucket>,
    group_value: &Value,
) -> Result<Option<usize>, InternalError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    for group_index in bucket.as_slice() {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.bucket_candidate_checks = metrics.bucket_candidate_checks.saturating_add(1);
        });
        let Some((group_key, _)) = grouped_counts.get(*group_index) else {
            return Err(InternalError::query_executor_invariant(format!(
                "grouped count bucket index out of bounds: index={} len={}",
                group_index,
                grouped_counts.len(),
            )));
        };
        if single_group_key_matches_value(group_key, group_value)? {
            update_grouped_count_fold_metrics(|metrics| {
                metrics.existing_group_hits = metrics.existing_group_hits.saturating_add(1);
            });
            return Ok(Some(*group_index));
        }
    }

    Ok(None)
}

// Finalize grouped count buckets into grouped rows plus optional next cursor
// without routing the dedicated count path back through the generic candidate
// row envelope.
fn finalize_grouped_count_page(
    route: &GroupedRouteStage,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
    grouped_counts: Vec<(GroupKey, u32)>,
) -> Result<(Vec<crate::db::GroupedRow>, Option<PageCursor>), InternalError> {
    update_grouped_count_fold_metrics(|metrics| {
        metrics.finalize_stage_runs = metrics.finalize_stage_runs.saturating_add(1);
    });
    update_grouped_count_fold_metrics(|metrics| {
        metrics.finalized_group_count = u64::try_from(grouped_counts.len()).unwrap_or(u64::MAX);
    });
    let grouped_pagination_window = route.grouped_pagination_window();
    let limit = grouped_pagination_window.limit();
    let initial_offset_for_page = grouped_pagination_window.initial_offset_for_page();
    let selection_bound = route.grouped_selection_bound();
    let resume_boundary = route.grouped_resume_boundary();
    let mut page_rows = Vec::<crate::db::GroupedRow>::new();
    let mut groups_skipped_for_offset = 0usize;
    let mut has_more = false;

    // Phase 1: walk finalized grouped counts in canonical grouped-key order.
    for (group_key, count) in
        select_grouped_count_candidates(route, grouped_counts, selection_bound, resume_boundary)?
    {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.candidate_rows_qualified = metrics.candidate_rows_qualified.saturating_add(1);
        });
        let aggregate_value = Value::Uint(u64::from(count));
        if groups_skipped_for_offset < initial_offset_for_page {
            groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
            update_grouped_count_fold_metrics(|metrics| {
                metrics.page_rows_skipped_for_offset =
                    metrics.page_rows_skipped_for_offset.saturating_add(1);
            });
            continue;
        }
        if let Some(limit) = limit
            && page_rows.len() >= limit
        {
            has_more = true;
            break;
        }

        let emitted_group_key = match group_key.into_canonical_value() {
            Value::List(values) => values,
            value => {
                return Err(GroupedRouteStage::canonical_group_key_must_be_list(&value));
            }
        };
        page_rows.push(crate::db::GroupedRow::new(
            emitted_group_key,
            vec![aggregate_value],
        ));
        update_grouped_count_fold_metrics(|metrics| {
            metrics.page_rows_emitted = metrics.page_rows_emitted.saturating_add(1);
        });
    }

    // Phase 2: preserve grouped projection ownership, including the identity
    // projection fast path that returns the rows unchanged.
    update_grouped_count_fold_metrics(|metrics| {
        metrics.projection_rows_input = metrics
            .projection_rows_input
            .saturating_add(u64::try_from(page_rows.len()).unwrap_or(u64::MAX));
    });
    let next_cursor_boundary = has_more
        .then(|| page_rows.last().map(|row| row.group_key().to_vec()))
        .flatten();
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;
    let next_cursor = if has_more {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.cursor_construction_attempts =
                metrics.cursor_construction_attempts.saturating_add(1);
        });
        update_grouped_count_fold_metrics(|metrics| {
            metrics.next_cursor_emitted = metrics.next_cursor_emitted.saturating_add(1);
        });
        next_cursor_boundary
            .as_ref()
            .map(|last_group_key| route.grouped_next_cursor(last_group_key.clone()))
            .transpose()?
    } else {
        None
    };

    Ok((page_rows, next_cursor))
}

// Select grouped-count candidates after HAVING/resume filtering, using a
// bounded top-k heap when the grouped continuation window exposes one.
fn select_grouped_count_candidates(
    route: &GroupedRouteStage,
    grouped_counts: Vec<(GroupKey, u32)>,
    selection_bound: Option<usize>,
    resume_boundary: Option<&Value>,
) -> Result<Vec<(GroupKey, u32)>, InternalError> {
    if let Some(selection_bound) = selection_bound {
        return select_bounded_grouped_count_candidates(
            route,
            grouped_counts,
            selection_bound,
            resume_boundary,
        );
    }

    select_unbounded_grouped_count_candidates(route, grouped_counts, resume_boundary)
}

// Select the smallest canonical grouped-count rows needed for one bounded page
// window so grouped `LIMIT/OFFSET` does not sort every qualifying group.
fn select_bounded_grouped_count_candidates(
    route: &GroupedRouteStage,
    grouped_counts: Vec<(GroupKey, u32)>,
    selection_bound: usize,
    resume_boundary: Option<&Value>,
) -> Result<Vec<(GroupKey, u32)>, InternalError> {
    let mut qualifying = Vec::new();
    for (group_key, count) in grouped_counts {
        if !grouped_count_row_matches_window(route, &group_key, count, resume_boundary)? {
            continue;
        }
        qualifying.push((group_key, count));
    }

    Ok(retain_smallest_grouped_count_candidates(
        qualifying,
        selection_bound,
        route.direction(),
    ))
}

// Retain only the smallest canonical grouped-count rows needed for one
// bounded page window.
fn retain_smallest_grouped_count_candidates(
    grouped_counts: Vec<(GroupKey, u32)>,
    selection_bound: usize,
    direction: Direction,
) -> Vec<(GroupKey, u32)> {
    let mut retained = BinaryHeap::<BoundedGroupedCountCandidate>::new();

    // Phase 1: keep only the smallest `selection_bound` qualifying groups in
    // a max-heap so the grouped count fast path pays `O(G log K)` instead of
    // sorting every qualifying group when pagination bounds are active.
    for (group_key, count) in grouped_counts {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.bounded_selection_candidates_seen =
                metrics.bounded_selection_candidates_seen.saturating_add(1);
        });
        let candidate = BoundedGroupedCountCandidate {
            group_key,
            count,
            direction,
        };
        if retained.len() < selection_bound {
            retained.push(candidate);
            continue;
        }

        if retained
            .peek()
            .is_some_and(|largest_retained| candidate.cmp(largest_retained).is_lt())
        {
            retained.pop();
            retained.push(candidate);
            update_grouped_count_fold_metrics(|metrics| {
                metrics.bounded_selection_heap_replacements = metrics
                    .bounded_selection_heap_replacements
                    .saturating_add(1);
            });
        }
    }

    // Phase 2: restore grouped-key order across the retained window only,
    // respecting the active grouped execution direction.
    let mut out: Vec<(GroupKey, u32)> = retained
        .into_vec()
        .into_iter()
        .map(|candidate| (candidate.group_key, candidate.count))
        .collect::<Vec<_>>();
    update_grouped_count_fold_metrics(|metrics| {
        metrics.bounded_selection_rows_sorted = metrics
            .bounded_selection_rows_sorted
            .saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
    });
    out.sort_by(|(left_key, _), (right_key, _)| {
        compare_grouped_boundary_values(
            direction,
            left_key.canonical_value(),
            right_key.canonical_value(),
        )
    });

    out
}

// Select every qualifying grouped-count row and restore canonical order when
// no bounded grouped page window is active.
fn select_unbounded_grouped_count_candidates(
    route: &GroupedRouteStage,
    grouped_counts: Vec<(GroupKey, u32)>,
    resume_boundary: Option<&Value>,
) -> Result<Vec<(GroupKey, u32)>, InternalError> {
    let mut out = Vec::with_capacity(grouped_counts.len());

    // Phase 1: apply grouped HAVING and continuation-resume filters before
    // materializing the final canonical grouped-count row set.
    for (group_key, count) in grouped_counts {
        if grouped_count_row_matches_window(route, &group_key, count, resume_boundary)? {
            out.push((group_key, count));
        }
    }

    // Phase 2: restore canonical grouped-key order across every qualifying
    // row when the grouped page window is not bounded by `offset + limit + 1`.
    update_grouped_count_fold_metrics(|metrics| {
        metrics.unbounded_selection_rows_sorted = metrics
            .unbounded_selection_rows_sorted
            .saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
    });
    out.sort_by(|(left_key, _), (right_key, _)| {
        compare_grouped_boundary_values(
            route.direction(),
            left_key.canonical_value(),
            right_key.canonical_value(),
        )
    });

    Ok(out)
}

// Return true when one grouped count row survives grouped HAVING and resume
// boundary filters and should participate in candidate selection.
fn grouped_count_row_matches_window(
    route: &GroupedRouteStage,
    group_key: &GroupKey,
    count: u32,
    resume_boundary: Option<&Value>,
) -> Result<bool, InternalError> {
    update_grouped_count_fold_metrics(|metrics| {
        metrics.window_rows_considered = metrics.window_rows_considered.saturating_add(1);
    });
    let aggregate_value = Value::Uint(u64::from(count));
    if let Some(grouped_having) = route.grouped_having()
        && !crate::db::executor::aggregate::runtime::group_matches_having(
            grouped_having,
            route.group_fields(),
            group_key.canonical_value(),
            std::slice::from_ref(&aggregate_value),
        )?
    {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.having_rows_rejected = metrics.having_rows_rejected.saturating_add(1);
        });
        return Ok(false);
    }
    if let Some(resume_boundary) = resume_boundary
        && !grouped_resume_boundary_allows_candidate(
            route.direction(),
            group_key.canonical_value(),
            resume_boundary,
        )
    {
        update_grouped_count_fold_metrics(|metrics| {
            metrics.resume_boundary_rows_rejected =
                metrics.resume_boundary_rows_rejected.saturating_add(1);
        });
        return Ok(false);
    }

    Ok(true)
}

// Compare grouped boundary values in the active grouped execution direction.
fn compare_grouped_boundary_values(direction: Direction, left: &Value, right: &Value) -> Ordering {
    match direction {
        Direction::Asc => canonical_value_compare(left, right),
        Direction::Desc => canonical_value_compare(right, left),
    }
}

// Return true when one candidate remains beyond the grouped continuation
// boundary in the active grouped execution direction.
fn grouped_resume_boundary_allows_candidate(
    direction: Direction,
    candidate_key: &Value,
    resume_boundary: &Value,
) -> bool {
    compare_grouped_boundary_values(direction, candidate_key, resume_boundary).is_gt()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        GroupedCountState, retain_smallest_grouped_count_candidates,
        stable_hash_group_values_from_row_view, supports_borrowed_group_probe,
    };
    use crate::{
        db::{
            executor::{
                aggregate::{ExecutionConfig, ExecutionContext},
                pipeline::contracts::RowView,
            },
            query::plan::FieldSlot,
        },
        types::Decimal,
        value::{Value, with_test_hash_override},
    };

    fn group_fields(indices: &[usize]) -> Vec<FieldSlot> {
        indices
            .iter()
            .map(|index| FieldSlot::from_parts_for_test(*index, format!("field_{index}")))
            .collect()
    }

    #[test]
    fn grouped_count_fast_path_hash_matches_owned_group_key_hash() {
        let row_view = RowView::new(vec![
            Some(Value::Decimal(Decimal::new(100, 2))),
            Some(Value::Text("alpha".to_string())),
        ]);
        let group_fields = group_fields(&[0, 1]);

        assert!(
            supports_borrowed_group_probe(&row_view, &group_fields).expect("borrowed probe"),
            "scalar grouped values should stay on the borrowed grouped-count fast path",
        );

        let borrowed_hash =
            stable_hash_group_values_from_row_view(&row_view, &group_fields).expect("hash");
        let owned_group_key = crate::db::executor::group::GroupKey::from_group_values(
            row_view.group_values(&group_fields).expect("group values"),
        )
        .expect("owned group key");

        assert_eq!(
            borrowed_hash,
            owned_group_key.hash(),
            "borrowed grouped-count hashing must stay aligned with owned canonical group-key hashing",
        );
    }

    #[test]
    fn grouped_count_fast_path_rejects_structured_group_values() {
        let row_view = RowView::new(vec![Some(Value::List(vec![Value::Uint(7)]))]);
        let group_fields = group_fields(&[0]);

        assert!(
            !supports_borrowed_group_probe(&row_view, &group_fields).expect("borrowed probe"),
            "structured grouped values must fall back to owned canonical key materialization",
        );
    }

    #[test]
    fn grouped_count_fast_path_handles_hash_collisions_without_merging_groups() {
        with_test_hash_override([0xAB; 16], || {
            let mut grouped_execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
            let group_fields = group_fields(&[0]);
            let alpha = RowView::new(vec![Some(Value::Text("alpha".to_string()))]);
            let beta = RowView::new(vec![Some(Value::Text("beta".to_string()))]);
            let borrowed_group_probe_supported = true;
            let mut grouped_counts = GroupedCountState::new();

            grouped_counts
                .increment_row(
                    &alpha,
                    &group_fields,
                    borrowed_group_probe_supported,
                    &mut grouped_execution_context,
                )
                .expect("alpha insert");
            grouped_counts
                .increment_row(
                    &beta,
                    &group_fields,
                    borrowed_group_probe_supported,
                    &mut grouped_execution_context,
                )
                .expect("beta insert");
            grouped_counts
                .increment_row(
                    &alpha,
                    &group_fields,
                    borrowed_group_probe_supported,
                    &mut grouped_execution_context,
                )
                .expect("alpha increment");

            let mut rows = grouped_counts.into_groups();
            rows.sort_by(|(left_key, _), (right_key, _)| {
                crate::db::contracts::canonical_value_compare(
                    left_key.canonical_value(),
                    right_key.canonical_value(),
                )
            });
            assert_eq!(
                rows,
                vec![
                    (
                        crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                            "alpha".to_string(),
                        )])
                        .expect("alpha key"),
                        2,
                    ),
                    (
                        crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                            "beta".to_string(),
                        )])
                        .expect("beta key"),
                        1,
                    ),
                ],
                "same-hash grouped count rows must remain distinct under canonical grouped equality",
            );
        });
    }

    #[test]
    fn grouped_count_bounded_candidate_selection_keeps_smallest_canonical_window() {
        let rows = vec![
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(9)])
                    .expect("group key"),
                9,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(2)])
                    .expect("group key"),
                2,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(5)])
                    .expect("group key"),
                5,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(1)])
                    .expect("group key"),
                1,
            ),
        ];

        let selected =
            retain_smallest_grouped_count_candidates(rows, 3, crate::db::direction::Direction::Asc);

        assert_eq!(
            selected
                .into_iter()
                .map(|(group_key, count)| (group_key.into_canonical_value(), count))
                .collect::<Vec<_>>(),
            vec![
                (Value::List(vec![Value::Uint(1)]), 1),
                (Value::List(vec![Value::Uint(2)]), 2),
                (Value::List(vec![Value::Uint(5)]), 5),
            ],
            "bounded grouped count selection should retain the smallest canonical grouped-key window only",
        );
    }
}
