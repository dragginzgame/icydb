//! Module: executor::aggregate::runtime::grouped_fold
//! Responsibility: grouped key-stream construction and fold execution mechanics.
//! Does not own: grouped route derivation or grouped output finalization.
//! Boundary: consumes grouped route-stage payload and emits grouped fold-stage payload.

mod candidate_rows;
mod engine_init;
mod ingest;
mod page_finalize;

use std::collections::BTreeMap;

use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            ExecutionPreparation,
            aggregate::runtime::grouped_fold::{
                candidate_rows::collect_grouped_candidate_rows, engine_init::build_grouped_engines,
                ingest::fold_group_rows_into_engines, page_finalize::finalize_grouped_page,
            },
            aggregate::{
                ExecutionContext, GroupError, GroupedAggregateEngine,
                runtime::{
                    grouped_distinct::{
                        execute_global_distinct_field_aggregate,
                        global_distinct_field_target_and_kind, page_global_distinct_grouped_row,
                    },
                    grouped_output::project_grouped_rows_from_projection,
                },
            },
            group::{GroupKey, StableHash, canonical_group_key_equals},
            group::{grouped_budget_observability, grouped_execution_context_from_planner_config},
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntime, GroupedCursorPage, GroupedFoldStage,
                GroupedRouteStage, GroupedRowRuntime, GroupedStreamStage, PageCursor,
                ProjectionMaterializationMode,
            },
            plan_metrics::record_grouped_plan_metrics,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};

// Build one grouped key stream from route-owned grouped execution metadata
// using already-resolved runtime and row-decode boundaries.
pub(in crate::db::executor) fn build_grouped_stream_with_runtime<'a>(
    route: &GroupedRouteStage,
    runtime: &dyn ExecutionRuntime,
    entity_model: &'static EntityModel,
    slot_map: Option<Vec<usize>>,
    row_runtime: Box<dyn GroupedRowRuntime + 'a>,
) -> Result<GroupedStreamStage<'a>, InternalError> {
    // Grouped runtime only consumes the compiled row predicate plus the
    // conservative index predicate used during key-stream resolution. It does
    // not read explain-only capability snapshots or strict pushdown state.
    let execution_preparation =
        ExecutionPreparation::from_runtime_plan(entity_model, route.plan(), slot_map);
    let execution_inputs = ExecutionInputs::new(
        entity_model,
        runtime,
        route.plan(),
        AccessStreamBindings {
            index_prefix_specs: route.index_prefix_specs(),
            index_range_specs: route.index_range_specs(),
            continuation: AccessScanContinuationInput::new(None, route.direction()),
        },
        &execution_preparation,
        ProjectionMaterializationMode::SharedValidation,
        true,
    )?;
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
    mut stream: GroupedStreamStage<'_>,
) -> Result<GroupedFoldStage, InternalError> {
    // Phase 1: initialize grouped fold context, projection contracts, and reducers.
    let mut grouped_execution_context =
        grouped_execution_context_from_planner_config(Some(route.grouped_execution()));
    let max_groups_bound =
        usize::try_from(grouped_execution_context.config().max_groups()).unwrap_or(usize::MAX);
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
    let aggregate_count = route.projection_layout().aggregate_positions().len();
    let grouped_projection_spec = route.plan().projection_spec(route.entity_model());

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
    let (grouped_engines, short_circuit_keys) =
        build_grouped_engines(route, &grouped_execution_context)?;

    // Phase 4: retain the canonical generic grouped reducer path for every
    // grouped aggregate shape that is not covered by a dedicated fast path.
    execute_generic_grouped_fold_stage(
        route,
        &mut stream,
        &mut grouped_execution_context,
        (grouped_engines, short_circuit_keys, aggregate_count),
        max_groups_bound,
        &grouped_projection_spec,
    )
}

// Execute one grouped global-DISTINCT route through the existing dedicated
// grouped distinct aggregate path when that strategy is active.
fn try_execute_global_distinct_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage<'_>,
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
        route.entity_model(),
        route.grouped_distinct_execution_strategy(),
        (&mut scanned_rows, &mut filtered_rows),
    )?;
    let grouped_window = route.grouped_pagination_window();
    let page_rows = page_global_distinct_grouped_row(
        global_row,
        grouped_window.initial_offset_for_page(),
        grouped_window.limit(),
    );
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
    stream: &mut GroupedStreamStage<'_>,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let consistency = route.consistency();
    let mut grouped_counts = BTreeMap::<StableHash, Vec<(GroupKey, u32)>>::new();

    // Phase 1: fold grouped source rows directly into one canonical count map.
    let mut on_key = |data_key: crate::db::data::DataKey| -> Result<
        crate::db::executor::KeyStreamLoopControl,
        InternalError,
    > {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            return Ok(crate::db::executor::KeyStreamLoopControl::Emit);
        };
        scanned_rows = scanned_rows.saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            return Ok(crate::db::executor::KeyStreamLoopControl::Emit);
        }
        filtered_rows = filtered_rows.saturating_add(1);
        let group_values = row_view.group_values(route.group_fields())?;
        let group_key = GroupKey::from_group_values(group_values)
            .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)?;
        increment_grouped_count(
            &mut grouped_counts,
            grouped_execution_context,
            group_key,
        )?;

        Ok(crate::db::executor::KeyStreamLoopControl::Emit)
    };
    crate::db::executor::drive_key_stream_with_control_flow(
        resolved.key_stream_mut(),
        &mut || crate::db::executor::KeyStreamLoopControl::Emit,
        &mut on_key,
    )?;

    // Phase 2: page and project the finalized grouped-count rows directly so
    // this dedicated path does not round-trip through the generic candidate
    // row envelope only to rebuild grouped rows immediately afterwards.
    let (page_rows, next_cursor) =
        finalize_grouped_count_page(route, grouped_projection_spec, grouped_counts)?;

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

// Execute the canonical grouped reducer/finalize path for every grouped shape
// that does not use a dedicated grouped fast path.
fn execute_generic_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage<'_>,
    grouped_execution_context: &mut ExecutionContext,
    reducers: (Vec<Box<dyn GroupedAggregateEngine>>, Vec<Vec<Value>>, usize),
    max_groups_bound: usize,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let (mut grouped_engines, mut short_circuit_keys, aggregate_count) = reducers;
    let (scanned_rows, filtered_rows) = fold_group_rows_into_engines(
        route,
        stream,
        grouped_execution_context,
        grouped_engines.as_mut_slice(),
        short_circuit_keys.as_mut_slice(),
        max_groups_bound,
    )?;
    let grouped_pagination_window = route.grouped_pagination_window().clone();
    let grouped_candidate_rows = collect_grouped_candidate_rows(
        route,
        grouped_engines,
        aggregate_count,
        max_groups_bound,
        &grouped_pagination_window,
    )?;
    let (page_rows, next_cursor) = finalize_grouped_page(
        route,
        grouped_projection_spec,
        grouped_candidate_rows,
        &grouped_pagination_window,
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

// Increment one canonical grouped count bucket while preserving grouped budget
// accounting and deterministic bucket collision handling.
fn increment_grouped_count(
    grouped_counts: &mut BTreeMap<StableHash, Vec<(GroupKey, u32)>>,
    grouped_execution_context: &mut ExecutionContext,
    group_key: GroupKey,
) -> Result<(), InternalError> {
    let hash = group_key.hash();
    if let Some(bucket) = grouped_counts.get_mut(&hash) {
        if let Some((_, count)) = bucket
            .iter_mut()
            .find(|(existing_key, _)| canonical_group_key_equals(existing_key, &group_key))
        {
            *count = count.saturating_add(1);

            return Ok(());
        }

        grouped_execution_context
            .record_new_group(&group_key, false, bucket.len(), bucket.capacity())
            .map_err(GroupError::into_internal_error)?;
        bucket.push((group_key, 1));

        return Ok(());
    }

    grouped_execution_context
        .record_new_group(&group_key, true, 0, 0)
        .map_err(GroupError::into_internal_error)?;
    grouped_counts.insert(hash, vec![(group_key, 1)]);

    Ok(())
}

// Finalize grouped count buckets into grouped rows plus optional next cursor
// without routing the dedicated count path back through the generic candidate
// row envelope.
fn finalize_grouped_count_page(
    route: &GroupedRouteStage,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
    grouped_counts: BTreeMap<StableHash, Vec<(GroupKey, u32)>>,
) -> Result<(Vec<crate::db::GroupedRow>, Option<PageCursor>), InternalError> {
    let grouped_pagination_window = route.grouped_pagination_window();
    let limit = grouped_pagination_window.limit();
    let initial_offset_for_page = grouped_pagination_window.initial_offset_for_page();
    let resume_boundary = route
        .grouped_continuation_capabilities()
        .resume_boundary_applied()
        .then(|| grouped_pagination_window.resume_boundary())
        .flatten();
    let mut page_rows = Vec::<crate::db::GroupedRow>::new();
    let mut groups_skipped_for_offset = 0usize;
    let mut has_more = false;

    // Phase 1: walk finalized grouped counts in canonical grouped-key order.
    for (group_key, count) in flatten_grouped_count_rows(grouped_counts) {
        let aggregate_value = Value::Uint(u64::from(count));
        if let Some(grouped_having) = route.grouped_having()
            && !crate::db::executor::aggregate::runtime::grouped_having::group_matches_having(
                grouped_having,
                route.group_fields(),
                group_key.canonical_value(),
                std::slice::from_ref(&aggregate_value),
            )?
        {
            continue;
        }
        if let Some(resume_boundary) = resume_boundary
            && !canonical_value_compare(group_key.canonical_value(), resume_boundary).is_gt()
        {
            continue;
        }
        if groups_skipped_for_offset < initial_offset_for_page {
            groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
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
    }

    // Phase 2: preserve grouped projection ownership, including the identity
    // projection fast path that returns the rows unchanged.
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;
    let next_cursor = if has_more {
        page_rows
            .last()
            .map(|row| route.grouped_next_cursor(row.group_key().to_vec()))
            .transpose()?
    } else {
        None
    };

    Ok((page_rows, next_cursor))
}

// Flatten grouped count buckets into canonical `(group_key, count)` rows in
// canonical grouped-key order for dedicated grouped-count page finalization.
fn flatten_grouped_count_rows(
    grouped_counts: BTreeMap<StableHash, Vec<(GroupKey, u32)>>,
) -> Vec<(GroupKey, u32)> {
    let expected_output_count = grouped_counts
        .values()
        .fold(0usize, |count, bucket| count.saturating_add(bucket.len()));
    let mut out = Vec::with_capacity(expected_output_count);

    // Phase 1: collect all finalized grouped-count rows while consuming the
    // stable-hash bucket structure.
    for (_, mut bucket) in grouped_counts {
        // Phase 2: break collision ties by canonical grouped-key value inside
        // each collision bucket before flattening.
        bucket.sort_by(|(left_key, _), (right_key, _)| {
            canonical_value_compare(left_key.canonical_value(), right_key.canonical_value())
        });

        // Phase 3: move finalized grouped-count rows out of the collision bucket.
        for (group_key, count) in bucket {
            out.push((group_key, count));
        }
    }

    // Phase 4: restore canonical grouped-key order across the full flattened
    // row set so grouped page emission preserves logical grouped ordering.
    out.sort_by(|(left_key, _), (right_key, _)| {
        canonical_value_compare(left_key.canonical_value(), right_key.canonical_value())
    });

    out
}
