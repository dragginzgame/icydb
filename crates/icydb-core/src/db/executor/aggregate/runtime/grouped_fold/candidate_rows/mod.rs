//! Module: executor::aggregate::runtime::grouped_fold::candidate_rows
//! Responsibility: grouped fold candidate buffering/ranking sinks for pagination windows.
//! Does not own: grouped planner policy semantics or aggregate contract derivation.
//! Boundary: selects and applies grouped candidate retention strategy during fold execution.

mod sink;

use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            GroupedContinuationCapabilities, GroupedPaginationWindow,
            aggregate::runtime::{
                grouped_fold::bundle::GroupedAggregateBundle, grouped_having::group_matches_having,
            },
            pipeline::contracts::GroupedRouteStage,
        },
        query::plan::GroupHavingSpec,
    },
    error::InternalError,
};

use crate::db::executor::aggregate::runtime::grouped_fold::candidate_rows::sink::GroupedCandidateSink;

// Finalize the shared grouped bundle into deterministic candidate rows before paging.
pub(super) fn collect_grouped_candidate_rows(
    route: &GroupedRouteStage,
    grouped_bundle: GroupedAggregateBundle,
    max_groups_bound: usize,
    pagination_window: &GroupedPaginationWindow,
) -> Result<Vec<(crate::value::Value, Vec<crate::value::Value>)>, InternalError> {
    collect_grouped_candidate_rows_from_finalized(
        route.grouped_having(),
        route.grouped_continuation_capabilities(),
        pagination_window,
        grouped_bundle.finalize().into_iter(),
        route.group_fields(),
        max_groups_bound,
    )
}

// Execute grouped candidate row selection over finalized grouped rows without
// any sibling-iterator alignment or per-engine finalize buffers.
fn collect_grouped_candidate_rows_from_finalized<I>(
    grouped_having: Option<&GroupHavingSpec>,
    continuation_capabilities: GroupedContinuationCapabilities,
    pagination_window: &GroupedPaginationWindow,
    finalized_rows: I,
    group_fields: &[crate::db::query::plan::FieldSlot],
    max_groups_bound: usize,
) -> Result<Vec<(crate::value::Value, Vec<crate::value::Value>)>, InternalError>
where
    I: Iterator<Item = (crate::value::Value, Vec<crate::value::Value>)>,
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

    // Phase 2: walk finalized grouped rows once and retain only the rows that
    // survive HAVING and continuation/window filtering.
    if limit.is_none_or(|limit| limit != 0) {
        for (group_key_value, aggregate_values) in finalized_rows {
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
                break;
            }
        }
    }

    Ok(grouped_candidate_sink.into_rows())
}
