//! Module: executor::aggregate::runtime::grouped_fold::candidate_rows
//! Responsibility: grouped fold candidate buffering/ranking sinks for pagination windows.
//! Does not own: grouped planner policy semantics or aggregate contract derivation.
//! Boundary: selects and applies grouped candidate retention strategy during fold execution.

mod sink;

use std::cmp::Ordering;

use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            GroupedContinuationCapabilities, GroupedPaginationWindow,
            aggregate::GroupedAggregateEngine,
            aggregate::runtime::grouped_having::group_matches_having,
            pipeline::contracts::GroupedRouteStage,
        },
        query::plan::{FieldSlot, GroupHavingSpec},
    },
    error::InternalError,
    value::Value,
};

use crate::db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink::GroupedCandidateSink;

// Finalize grouped reducers into deterministic candidate rows before paging.
pub(super) fn collect_grouped_candidate_rows(
    route: &GroupedRouteStage,
    grouped_engines: Vec<Box<dyn GroupedAggregateEngine>>,
    aggregate_count: usize,
    max_groups_bound: usize,
    pagination_window: &GroupedPaginationWindow,
) -> Result<Vec<(Value, Vec<Value>)>, InternalError> {
    // Phase 1: route the common single-aggregate grouped shape away from the
    // multi-aggregate sibling-alignment machinery.
    if aggregate_count == 1 {
        let primary_engine = grouped_engines
            .into_iter()
            .next()
            .ok_or_else(GroupedRouteStage::missing_primary_aggregate_iterator)?;

        return collect_single_aggregate_candidate_rows_from_finalized(
            route.group_fields(),
            route.grouped_having(),
            route.grouped_continuation_capabilities(),
            pagination_window,
            primary_engine.finalize()?.into_iter(),
            max_groups_bound,
        );
    }

    // Phase 2: finalize typed aggregate engines into canonical `(group_key, value)` iterators.
    let finalized_iters = finalize_grouped_iterators(grouped_engines)?;

    // Phase 3: execute shared candidate-row selection/runtime filtering once.
    collect_grouped_candidate_rows_from_finalized(
        aggregate_count,
        route.group_fields(),
        route.grouped_having(),
        route.grouped_continuation_capabilities(),
        pagination_window,
        finalized_iters,
        max_groups_bound,
    )
}

// Execute candidate-row selection for the common single-aggregate grouped
// shape without paying sibling-iterator alignment costs.
pub(super) fn collect_single_aggregate_candidate_rows_from_finalized<I>(
    group_fields: &[FieldSlot],
    grouped_having: Option<&GroupHavingSpec>,
    continuation_capabilities: GroupedContinuationCapabilities,
    pagination_window: &GroupedPaginationWindow,
    primary_iter: I,
    max_groups_bound: usize,
) -> Result<Vec<(Value, Vec<Value>)>, InternalError>
where
    I: Iterator<Item = (Value, Value)>,
{
    // Phase 1: project continuation/window contracts that define candidate selection.
    let limit = pagination_window.limit();
    let selection_bound = if continuation_capabilities.selection_bound_applied() {
        pagination_window.selection_bound()
    } else {
        None
    };
    let resume_boundary = if continuation_capabilities.resume_boundary_applied() {
        pagination_window.resume_boundary()
    } else {
        None
    };
    let mut grouped_candidate_sink = GroupedCandidateSink::new(selection_bound, max_groups_bound);

    // Phase 2: collect one candidate row per finalized grouped aggregate output.
    if limit.is_none_or(|limit| limit != 0) {
        for (group_key_value, aggregate_value) in primary_iter {
            if let Some(grouped_having) = grouped_having
                && !group_matches_having(
                    grouped_having,
                    group_fields,
                    &group_key_value,
                    std::slice::from_ref(&aggregate_value),
                )?
            {
                continue;
            }
            if let Some(resume_boundary) = resume_boundary
                && !canonical_value_compare(&group_key_value, resume_boundary).is_gt()
            {
                continue;
            }

            if grouped_candidate_sink.push_candidate_from_slice(
                group_key_value,
                std::slice::from_ref(&aggregate_value),
            )? {
                break;
            }
        }
    }

    Ok(grouped_candidate_sink.into_rows())
}

// Finalize typed grouped aggregate engines into canonical iterator payloads.
fn finalize_grouped_iterators(
    grouped_engines: Vec<Box<dyn GroupedAggregateEngine>>,
) -> Result<Vec<std::vec::IntoIter<(Value, Value)>>, InternalError> {
    grouped_engines
        .into_iter()
        .map(|engine| engine.finalize().map(std::iter::IntoIterator::into_iter))
        .collect()
}

// Execute grouped candidate row selection/alignment over finalized aggregate
// iterators without any entity-typed runtime dependencies.
fn collect_grouped_candidate_rows_from_finalized(
    aggregate_count: usize,
    group_fields: &[FieldSlot],
    grouped_having: Option<&GroupHavingSpec>,
    continuation_capabilities: GroupedContinuationCapabilities,
    pagination_window: &GroupedPaginationWindow,
    mut finalized_iters: Vec<std::vec::IntoIter<(Value, Value)>>,
    max_groups_bound: usize,
) -> Result<Vec<(Value, Vec<Value>)>, InternalError> {
    // Phase 1: project continuation/window contracts that define candidate selection.
    let limit = pagination_window.limit();
    let selection_bound = if continuation_capabilities.selection_bound_applied() {
        pagination_window.selection_bound()
    } else {
        None
    };
    let resume_boundary = if continuation_capabilities.resume_boundary_applied() {
        pagination_window.resume_boundary()
    } else {
        None
    };
    if aggregate_count == 0 {
        return Err(GroupedRouteStage::aggregate_terminal_required());
    }

    // Phase 2: align sibling iterators by canonical key and collect candidates.
    let mut primary_iter = finalized_iters
        .drain(..1)
        .next()
        .ok_or_else(GroupedRouteStage::missing_primary_aggregate_iterator)?;
    let mut grouped_candidate_sink = GroupedCandidateSink::new(selection_bound, max_groups_bound);
    let mut aggregate_values = Vec::with_capacity(aggregate_count);
    let mut selection_saturated = false;

    if limit.is_none_or(|limit| limit != 0) {
        for (group_key_value, primary_value) in primary_iter.by_ref() {
            // Reuse one aggregate scratch buffer across groups so sibling
            // alignment does not allocate a fresh `Vec` before HAVING/resume
            // filters reject the row.
            aggregate_values.clear();
            aggregate_values.push(primary_value);
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                let (sibling_group_key, sibling_value) = sibling_iter.next().ok_or_else(|| {
                    GroupedRouteStage::missing_sibling_aggregate_row(sibling_index)
                })?;
                if canonical_value_compare(&sibling_group_key, &group_key_value) != Ordering::Equal
                {
                    return Err(GroupedRouteStage::sibling_aggregate_key_mismatch(
                        sibling_index,
                        &group_key_value,
                        &sibling_group_key,
                    ));
                }
                aggregate_values.push(sibling_value);
            }
            debug_assert_eq!(
                aggregate_values.len(),
                aggregate_count,
                "grouped aggregate value alignment must preserve declared aggregate count",
            );
            if let Some(grouped_having) = grouped_having
                && !group_matches_having(
                    grouped_having,
                    group_fields,
                    &group_key_value,
                    aggregate_values.as_slice(),
                )?
            {
                continue;
            }
            if let Some(resume_boundary) = resume_boundary
                && !canonical_value_compare(&group_key_value, resume_boundary).is_gt()
            {
                continue;
            }

            if grouped_candidate_sink
                .push_candidate_from_slice(group_key_value, aggregate_values.as_slice())?
            {
                selection_saturated = true;
                break;
            }
        }
        if !selection_saturated {
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                if sibling_iter.next().is_some() {
                    return Err(GroupedRouteStage::trailing_sibling_aggregate_rows(
                        sibling_index,
                    ));
                }
            }
        }
    }

    Ok(grouped_candidate_sink.into_rows())
}
