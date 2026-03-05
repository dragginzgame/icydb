use std::cmp::Ordering;

use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            aggregate::AggregateEngine,
            load::{GroupedPaginationWindow, GroupedRouteStageProjection, LoadExecutor},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

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
    #[expect(clippy::too_many_lines)]
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
        let limit = pagination_window.limit();
        let selection_bound = pagination_window.selection_bound();
        let resume_boundary = pagination_window.resume_boundary();
        if aggregate_count == 0 {
            return Err(crate::db::executor::load::invariant(
                "grouped execution requires at least one aggregate terminal",
            ));
        }
        let mut finalized_iters = grouped_engines
            .into_iter()
            .map(|engine| engine.finalize_grouped().map(Vec::into_iter))
            .collect::<Result<Vec<_>, _>>()?;
        let mut primary_iter = finalized_iters.drain(..1).next().ok_or_else(|| {
            crate::db::executor::load::invariant("missing grouped primary iterator")
        })?;
        let mut grouped_candidate_rows = Vec::<(Value, Vec<Value>)>::new();

        if limit.is_none_or(|limit| limit != 0) {
            for primary_output in primary_iter.by_ref() {
                let group_key_value = primary_output.group_key().canonical_value().clone();
                let mut aggregate_values = Vec::with_capacity(aggregate_count);
                aggregate_values.push(Self::aggregate_output_to_value(primary_output.output()));
                for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                    let sibling_output = sibling_iter.next().ok_or_else(|| {
                        crate::db::executor::load::invariant(format!(
                            "grouped finalize alignment missing sibling aggregate row: sibling_index={sibling_index}"
                        ))
                    })?;
                    let sibling_group_key = sibling_output.group_key().canonical_value();
                    if canonical_value_compare(sibling_group_key, &group_key_value)
                        != Ordering::Equal
                    {
                        return Err(crate::db::executor::load::invariant(format!(
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

                // Keep only the smallest `offset + limit + 1` canonical grouped keys when
                // paging is bounded so grouped LIMIT does not require one full grouped buffer.
                if let Some(selection_bound) = selection_bound {
                    match grouped_candidate_rows.binary_search_by(|(existing_key, _)| {
                        canonical_value_compare(existing_key, &group_key_value)
                    }) {
                        Ok(_) => {
                            return Err(crate::db::executor::load::invariant(format!(
                                "grouped finalize produced duplicate canonical group key: {group_key_value:?}"
                            )));
                        }
                        Err(insert_index) => {
                            grouped_candidate_rows
                                .insert(insert_index, (group_key_value, aggregate_values));
                            if grouped_candidate_rows.len() > selection_bound {
                                let _ = grouped_candidate_rows.pop();
                            }
                            debug_assert!(
                                grouped_candidate_rows.len() <= selection_bound,
                                "bounded grouped candidate rows must stay <= selection_bound",
                            );
                        }
                    }
                } else {
                    grouped_candidate_rows.push((group_key_value, aggregate_values));
                    debug_assert!(
                        grouped_candidate_rows.len() <= max_groups_bound,
                        "grouped candidate rows must stay bounded by max_groups",
                    );
                }
            }
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                if sibling_iter.next().is_some() {
                    return Err(crate::db::executor::load::invariant(format!(
                        "grouped finalize alignment has trailing sibling rows: sibling_index={sibling_index}"
                    )));
                }
            }
            if selection_bound.is_none() {
                grouped_candidate_rows
                    .sort_by(|(left, _), (right, _)| canonical_value_compare(left, right));
            }
        }
        if let Some(selection_bound) = selection_bound {
            debug_assert!(
                grouped_candidate_rows.len() <= selection_bound,
                "grouped candidate rows must remain bounded by selection_bound",
            );
        } else {
            debug_assert!(
                grouped_candidate_rows.len() <= max_groups_bound,
                "grouped candidate rows must remain bounded by max_groups",
            );
        }

        Ok(grouped_candidate_rows)
    }
}
