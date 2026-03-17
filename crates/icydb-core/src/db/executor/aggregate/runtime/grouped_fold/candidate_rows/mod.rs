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
            aggregate::runtime::{
                grouped_having::group_matches_having, grouped_output::aggregate_output_to_value,
            },
            aggregate::{AggregateEngine, AggregateExecutionMode, AggregateFinalizeAdapter},
            pipeline::contracts::GroupedRouteStage,
        },
        query::plan::{FieldSlot, GroupHavingSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

use crate::db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink::GroupedCandidateSink;

// Finalize grouped reducers into deterministic candidate rows before paging.
pub(super) fn collect_grouped_candidate_rows<E>(
    route: &GroupedRouteStage<E>,
    grouped_engines: Vec<AggregateEngine<E>>,
    aggregate_count: usize,
    max_groups_bound: usize,
    pagination_window: &GroupedPaginationWindow,
) -> Result<Vec<(Value, Vec<Value>)>, InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: finalize typed aggregate engines into canonical `(group_key, value)` iterators.
    let finalized_iters = finalize_grouped_iterators(grouped_engines)?;

    // Phase 2: execute shared candidate-row selection/runtime filtering once.
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

// Finalize typed grouped aggregate engines into canonical iterator payloads.
fn finalize_grouped_iterators<E>(
    grouped_engines: Vec<AggregateEngine<E>>,
) -> Result<Vec<std::vec::IntoIter<(Value, Value)>>, InternalError>
where
    E: EntityKind + EntityValue,
{
    grouped_engines
        .into_iter()
        .map(|engine| {
            AggregateFinalizeAdapter::from_execution_mode(AggregateExecutionMode::Grouped)
                .finalize(engine)?
                .into_grouped()
                .map(|outputs| {
                    outputs
                        .into_iter()
                        .map(|output| {
                            (
                                output.group_key().canonical_value().clone(),
                                aggregate_output_to_value(output.output()),
                            )
                        })
                        .collect::<Vec<_>>()
                        .into_iter()
                })
        })
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
        return Err(crate::db::error::query_executor_invariant(
            "grouped execution requires at least one aggregate terminal",
        ));
    }

    // Phase 2: align sibling iterators by canonical key and collect candidates.
    let mut primary_iter = finalized_iters.drain(..1).next().ok_or_else(|| {
        crate::db::error::query_executor_invariant("missing grouped primary iterator")
    })?;
    let mut grouped_candidate_sink = GroupedCandidateSink::new(selection_bound, max_groups_bound);

    if limit.is_none_or(|limit| limit != 0) {
        for (group_key_value, primary_value) in primary_iter.by_ref() {
            let mut aggregate_values = Vec::with_capacity(aggregate_count);
            aggregate_values.push(primary_value);
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                let (sibling_group_key, sibling_value) = sibling_iter.next().ok_or_else(|| {
                    crate::db::error::query_executor_invariant(format!(
                        "grouped finalize alignment missing sibling aggregate row: sibling_index={sibling_index}"
                    ))
                })?;
                if canonical_value_compare(&sibling_group_key, &group_key_value) != Ordering::Equal
                {
                    return Err(crate::db::error::query_executor_invariant(format!(
                        "grouped finalize alignment mismatch at sibling_index={sibling_index}: primary_key={group_key_value:?}, sibling_key={sibling_group_key:?}"
                    )));
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

            grouped_candidate_sink.push_candidate(group_key_value, aggregate_values)?;
        }
        for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
            if sibling_iter.next().is_some() {
                return Err(crate::db::error::query_executor_invariant(format!(
                    "grouped finalize alignment has trailing sibling rows: sibling_index={sibling_index}"
                )));
            }
        }
    }

    Ok(grouped_candidate_sink.into_rows())
}
