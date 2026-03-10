//! Module: executor::load::grouped_fold::candidate_rows
//! Responsibility: grouped fold candidate buffering/ranking sinks for pagination windows.
//! Does not own: grouped planner policy semantics or aggregate contract derivation.
//! Boundary: selects and applies grouped candidate retention strategy during fold execution.

mod sink;

use std::cmp::Ordering;

use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            aggregate::AggregateEngine,
            load::{GroupedPaginationWindow, GroupedRouteStageProjection, LoadExecutor, invariant},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

use crate::db::executor::load::grouped_fold::candidate_rows::sink::GroupedCandidateSink;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Derive grouped pagination contracts once from grouped plan + cursor state.
    pub(super) fn grouped_pagination_window<R>(route: &R) -> GroupedPaginationWindow
    where
        R: GroupedRouteStageProjection<E>,
    {
        route.grouped_pagination_window().clone()
    }

    // Finalize grouped reducers into deterministic candidate rows before paging.
    pub(super) fn collect_grouped_candidate_rows<R>(
        route: &R,
        grouped_engines: Vec<AggregateEngine<E>>,
        aggregate_count: usize,
        max_groups_bound: usize,
        pagination_window: &GroupedPaginationWindow,
    ) -> Result<Vec<(Value, Vec<Value>)>, InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        // Phase 1: project continuation/window contracts that define candidate selection.
        let limit = pagination_window.limit();
        let continuation_capabilities = route.grouped_continuation_capabilities();
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
            return Err(invariant(
                "grouped execution requires at least one aggregate terminal",
            ));
        }

        // Phase 2: finalize aggregate engines and align sibling iterators by canonical key.
        let mut finalized_iters = grouped_engines
            .into_iter()
            .map(|engine| engine.finalize_grouped().map(Vec::into_iter))
            .collect::<Result<Vec<_>, _>>()?;
        let mut primary_iter = finalized_iters
            .drain(..1)
            .next()
            .ok_or_else(|| invariant("missing grouped primary iterator"))?;
        let mut grouped_candidate_sink =
            GroupedCandidateSink::new(selection_bound, max_groups_bound);

        // Phase 3: materialize aligned grouped outputs into the strategy-selected sink.
        if limit.is_none_or(|limit| limit != 0) {
            for primary_output in primary_iter.by_ref() {
                let group_key_value = primary_output.group_key().canonical_value().clone();
                let mut aggregate_values = Vec::with_capacity(aggregate_count);
                aggregate_values.push(Self::aggregate_output_to_value(primary_output.output()));
                for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                    let sibling_output = sibling_iter.next().ok_or_else(|| {
                        invariant(format!(
                            "grouped finalize alignment missing sibling aggregate row: sibling_index={sibling_index}"
                        ))
                    })?;
                    let sibling_group_key = sibling_output.group_key().canonical_value();
                    if canonical_value_compare(sibling_group_key, &group_key_value)
                        != Ordering::Equal
                    {
                        return Err(invariant(format!(
                            "grouped finalize alignment mismatch at sibling_index={sibling_index}: primary_key={group_key_value:?}, sibling_key={sibling_group_key:?}"
                        )));
                    }
                    aggregate_values.push(Self::aggregate_output_to_value(sibling_output.output()));
                }
                debug_assert_eq!(
                    aggregate_values.len(),
                    aggregate_count,
                    "grouped aggregate value alignment must preserve declared aggregate count",
                );
                if let Some(grouped_having) = route.grouped_having()
                    && !Self::group_matches_having(
                        grouped_having,
                        route.group_fields(),
                        &group_key_value,
                        aggregate_values.as_slice(),
                    )?
                {
                    continue;
                }
                if let Some(resume_boundary) = resume_boundary
                    && canonical_value_compare(&group_key_value, resume_boundary)
                        != Ordering::Greater
                {
                    continue;
                }

                grouped_candidate_sink.push_candidate(group_key_value, aggregate_values)?;
            }
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                if sibling_iter.next().is_some() {
                    return Err(invariant(format!(
                        "grouped finalize alignment has trailing sibling rows: sibling_index={sibling_index}"
                    )));
                }
            }
        }

        Ok(grouped_candidate_sink.into_rows())
    }
}
