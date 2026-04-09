//! Module: db::executor::aggregate::runtime::grouped_fold::page_finalize
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::page_finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    db::{
        GroupedRow,
        contracts::canonical_value_compare,
        executor::{
            GroupedPaginationWindow,
            aggregate::runtime::{
                grouped_fold::bundle::GroupedAggregateBundle, grouped_having::group_matches_having,
                grouped_output::project_grouped_rows_from_projection,
            },
            group::GroupKey,
            pipeline::contracts::{GroupedRouteStage, PageCursor},
        },
        query::plan::{GroupHavingSpec, expr::ProjectionSpec},
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedPageCandidate
///
/// GroupedPageCandidate keeps one finalized grouped row payload in a form that
/// can still participate in canonical grouped-key ordering before the public
/// `GroupedRow` boundary is materialized.
///

#[derive(Eq, PartialEq)]
struct GroupedPageCandidate {
    group_key: GroupKey,
    aggregate_values: Vec<Value>,
}

impl Ord for GroupedPageCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        canonical_value_compare(
            self.group_key.canonical_value(),
            other.group_key.canonical_value(),
        )
    }
}

impl PartialOrd for GroupedPageCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl GroupedPageCandidate {
    // Finalize one single-aggregate grouped state bundle into one candidate row.
    fn from_single(
        finalized_group: crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup,
    ) -> Self {
        let (group_key, aggregate_value) = finalized_group.finalize_single();

        Self {
            group_key,
            aggregate_values: vec![aggregate_value],
        }
    }

    // Finalize one multi-aggregate grouped state bundle into one candidate row.
    fn from_many(
        finalized_group: crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup,
        aggregate_count: usize,
    ) -> Self {
        let (group_key, aggregate_values) = finalized_group.finalize(aggregate_count);

        Self {
            group_key,
            aggregate_values,
        }
    }

    // Return true when this finalized grouped row survives grouped HAVING and
    // continuation resume-boundary filtering.
    fn matches_window(
        &self,
        grouped_having: Option<&GroupHavingSpec>,
        group_fields: &[crate::db::query::plan::FieldSlot],
        resume_boundary: Option<&Value>,
    ) -> Result<bool, InternalError> {
        if let Some(grouped_having) = grouped_having
            && !group_matches_having(
                grouped_having,
                group_fields,
                self.group_key.canonical_value(),
                self.aggregate_values.as_slice(),
            )?
        {
            return Ok(false);
        }
        if let Some(resume_boundary) = resume_boundary
            && !canonical_value_compare(self.group_key.canonical_value(), resume_boundary).is_gt()
        {
            return Ok(false);
        }

        Ok(true)
    }

    // Consume this finalized grouped payload into the public grouped row DTO.
    fn into_row(self) -> Result<GroupedRow, InternalError> {
        let emitted_group_key = match self.group_key.into_canonical_value() {
            Value::List(values) => values,
            value => {
                return Err(GroupedRouteStage::canonical_group_key_must_be_list(&value));
            }
        };

        Ok(GroupedRow::new(emitted_group_key, self.aggregate_values))
    }
}

// Apply grouped finalize, filtering, paging, and projection over the shared
// grouped bundle without round-tripping through a candidate row buffer.
pub(super) fn finalize_grouped_page(
    route: &GroupedRouteStage,
    grouped_projection_spec: &ProjectionSpec,
    grouped_bundle: GroupedAggregateBundle,
    pagination_window: &GroupedPaginationWindow,
) -> Result<(Vec<GroupedRow>, Option<PageCursor>), InternalError> {
    let grouped_having = route.grouped_having();
    let continuation_capabilities = route.grouped_continuation_capabilities();
    let group_fields = route.group_fields();
    let selection_bound = continuation_capabilities
        .selection_bound_applied()
        .then(|| pagination_window.selection_bound())
        .flatten();
    let resume_boundary = continuation_capabilities
        .resume_boundary_applied()
        .then(|| pagination_window.resume_boundary())
        .flatten();
    let (page_rows, next_cursor_boundary) = if let Some(selection_bound) = selection_bound {
        finalize_bounded_grouped_page_rows(
            grouped_having,
            grouped_bundle,
            group_fields,
            pagination_window,
            resume_boundary,
            selection_bound,
        )?
    } else {
        finalize_unbounded_grouped_page_rows(
            grouped_having,
            grouped_bundle,
            group_fields,
            pagination_window,
            resume_boundary,
        )?
    };
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;
    let next_cursor = next_cursor_boundary
        .map(|last_group_key| route.grouped_next_cursor(last_group_key))
        .transpose()?;

    Ok((page_rows, next_cursor))
}

// Apply grouped candidate selection, filtering, offset, and limit over one
// bounded grouped page window without sorting every finalized group first.
fn finalize_bounded_grouped_page_rows(
    grouped_having: Option<&GroupHavingSpec>,
    grouped_bundle: GroupedAggregateBundle,
    group_fields: &[crate::db::query::plan::FieldSlot],
    pagination_window: &GroupedPaginationWindow,
    resume_boundary: Option<&Value>,
    selection_bound: usize,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError> {
    let selected_candidates = if grouped_bundle.has_single_aggregate() {
        retain_smallest_grouped_page_candidates(
            grouped_bundle
                .into_groups()
                .map(GroupedPageCandidate::from_single),
            grouped_having,
            group_fields,
            resume_boundary,
            selection_bound,
        )?
    } else {
        let aggregate_count = grouped_bundle.aggregate_count();

        retain_smallest_grouped_page_candidates(
            grouped_bundle.into_groups().map(|finalized_group| {
                GroupedPageCandidate::from_many(finalized_group, aggregate_count)
            }),
            grouped_having,
            group_fields,
            resume_boundary,
            selection_bound,
        )?
    };

    page_rows_from_candidates(
        selected_candidates.into_iter(),
        pagination_window.limit(),
        pagination_window.initial_offset_for_page(),
    )
}

// Apply grouped filtering, offset, and limit over the common grouped shape
// when no bounded grouped page window is active.
fn finalize_unbounded_grouped_page_rows(
    grouped_having: Option<&GroupHavingSpec>,
    grouped_bundle: GroupedAggregateBundle,
    group_fields: &[crate::db::query::plan::FieldSlot],
    pagination_window: &GroupedPaginationWindow,
    resume_boundary: Option<&Value>,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError> {
    let finalized_candidates = if grouped_bundle.has_single_aggregate() {
        grouped_bundle
            .into_sorted_groups()
            .into_iter()
            .map(GroupedPageCandidate::from_single)
            .collect::<Vec<_>>()
    } else {
        let aggregate_count = grouped_bundle.aggregate_count();

        grouped_bundle
            .into_sorted_groups()
            .into_iter()
            .map(|finalized_group| {
                GroupedPageCandidate::from_many(finalized_group, aggregate_count)
            })
            .collect::<Vec<_>>()
    };

    let filtered_candidates = finalized_candidates
        .into_iter()
        .filter_map(|candidate| {
            let keep = candidate.matches_window(grouped_having, group_fields, resume_boundary);
            match keep {
                Ok(true) => Some(Ok(candidate)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    page_rows_from_candidates(
        filtered_candidates.into_iter(),
        pagination_window.limit(),
        pagination_window.initial_offset_for_page(),
    )
}

// Retain only the smallest canonical grouped rows needed for one bounded page
// window after grouped HAVING and resume filtering.
fn retain_smallest_grouped_page_candidates<I>(
    finalized_candidates: I,
    grouped_having: Option<&GroupHavingSpec>,
    group_fields: &[crate::db::query::plan::FieldSlot],
    resume_boundary: Option<&Value>,
    selection_bound: usize,
) -> Result<Vec<GroupedPageCandidate>, InternalError>
where
    I: Iterator<Item = GroupedPageCandidate>,
{
    let mut retained = BinaryHeap::<GroupedPageCandidate>::new();

    // Phase 1: keep only the smallest `selection_bound` qualifying groups so
    // bounded grouped pages do not sort every finalized group up front.
    for candidate in finalized_candidates {
        if !candidate.matches_window(grouped_having, group_fields, resume_boundary)? {
            continue;
        }
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
        }
    }

    // Phase 2: restore ascending canonical grouped-key order across the
    // retained bounded window only.
    let mut out = retained.into_vec();
    out.sort();

    Ok(out)
}

// Apply grouped page offset/limit semantics over already-selected grouped
// candidates and materialize the public grouped row DTOs only for emitted rows.
fn page_rows_from_candidates<I>(
    selected_candidates: I,
    limit: Option<usize>,
    initial_offset_for_page: usize,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError>
where
    I: Iterator<Item = GroupedPageCandidate>,
{
    let mut page_rows = Vec::<GroupedRow>::new();
    let mut has_more = false;
    let mut groups_skipped_for_offset = 0usize;

    // Phase 1: apply offset and limit only after the candidate set is already
    // filtered into ascending canonical grouped-key order.
    for candidate in selected_candidates {
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

        page_rows.push(candidate.into_row()?);
    }

    let next_cursor_boundary = if has_more {
        page_rows.last().map(|row| row.group_key().to_vec())
    } else {
        None
    };

    Ok((page_rows, next_cursor_boundary))
}
