//! Module: db::executor::aggregate::runtime::grouped_fold::page_finalize
//! Finalizes grouped-fold candidate streams into grouped result pages.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    db::{
        GroupedRow,
        contracts::canonical_value_compare,
        direction::Direction,
        executor::{
            GroupedPaginationWindow,
            aggregate::runtime::{
                group_matches_having, group_matches_having_expr,
                grouped_fold::bundle::GroupedAggregateBundle,
                grouped_output::project_grouped_values_from_compiled_projection,
            },
            group::GroupKey,
            pipeline::contracts::{GroupedRouteStage, PageCursor},
            projection::{
                CompiledGroupedProjectionPlan, compile_grouped_projection_plan_if_needed,
            },
        },
        query::plan::{GroupHavingExpr, GroupHavingSpec, expr::ProjectionSpec},
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
    direction: Direction,
}

impl Ord for GroupedPageCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_grouped_boundary_values(
            self.direction,
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
    // Finalize one grouped state bundle into one candidate row while
    // preserving the single-aggregate fast path's scalar finalize contract.
    fn from_finalized(
        finalized_group: crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup,
        aggregate_count: usize,
        direction: Direction,
    ) -> Self {
        let (group_key, aggregate_values) = if aggregate_count == 1 {
            let (group_key, aggregate_value) = finalized_group.finalize_single();

            (group_key, vec![aggregate_value])
        } else {
            finalized_group.finalize(aggregate_count)
        };

        Self {
            group_key,
            aggregate_values,
            direction,
        }
    }

    // Borrow the grouped key payload in grouped-row declaration order without
    // first materializing the public grouped DTO.
    fn group_key_values(&self) -> Result<&[Value], InternalError> {
        let Value::List(values) = self.group_key.canonical_value() else {
            return Err(GroupedRouteStage::canonical_group_key_must_be_list(
                self.group_key.canonical_value(),
            ));
        };

        Ok(values.as_slice())
    }

    // Return true when this finalized grouped row survives grouped HAVING and
    // continuation resume-boundary filtering.
    fn matches_window(
        &self,
        grouped_having: Option<&GroupHavingSpec>,
        grouped_having_expr: Option<&GroupHavingExpr>,
        group_fields: &[crate::db::query::plan::FieldSlot],
        resume_boundary: Option<&Value>,
    ) -> Result<bool, InternalError> {
        if let Some(grouped_having_expr) = grouped_having_expr
            && !group_matches_having_expr(
                grouped_having_expr,
                group_fields,
                self.group_key.canonical_value(),
                self.aggregate_values.as_slice(),
            )?
        {
            return Ok(false);
        }
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
            && !grouped_resume_boundary_allows_candidate(
                self.direction,
                self.group_key.canonical_value(),
                resume_boundary,
            )
        {
            return Ok(false);
        }

        Ok(true)
    }

    // Consume this finalized grouped payload into the public grouped row DTO.
    fn into_row(self) -> Result<GroupedRow, InternalError> {
        let emitted_group_key = Self::into_group_key_values(self.group_key)?;

        Ok(GroupedRow::new(emitted_group_key, self.aggregate_values))
    }

    // Consume one canonical group key into the grouped response key vector.
    fn into_group_key_values(group_key: GroupKey) -> Result<Vec<Value>, InternalError> {
        match group_key.into_canonical_value() {
            Value::List(values) => Ok(values),
            value => Err(GroupedRouteStage::canonical_group_key_must_be_list(&value)),
        }
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
    let grouped_having_expr = route.grouped_having_expr();
    let group_fields = route.group_fields();
    let compiled_projection = compile_grouped_projection_plan_if_needed(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
    )?;
    let selection_bound = route.grouped_selection_bound();
    let resume_boundary = route.grouped_resume_boundary();
    let (page_rows, next_cursor_boundary) = if let Some(selection_bound) = selection_bound {
        finalize_bounded_grouped_page_rows(
            route.direction(),
            grouped_having,
            grouped_having_expr,
            grouped_bundle,
            group_fields,
            pagination_window,
            resume_boundary,
            selection_bound,
            compiled_projection,
        )?
    } else {
        finalize_unbounded_grouped_page_rows(
            route.direction(),
            grouped_having,
            grouped_having_expr,
            grouped_bundle,
            group_fields,
            pagination_window,
            resume_boundary,
            compiled_projection,
        )?
    };
    let next_cursor = next_cursor_boundary
        .map(|last_group_key| route.grouped_next_cursor(last_group_key))
        .transpose()?;

    Ok((page_rows, next_cursor))
}

// Build one finalized grouped candidate iterator from the grouped bundle
// without changing the single-aggregate versus multi-aggregate execution
// contract.
fn into_grouped_page_candidates(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
    direction: Direction,
) -> Vec<GroupedPageCandidate> {
    let aggregate_count = grouped_bundle.aggregate_count();
    into_finalize_groups(grouped_bundle, sorted)
        .into_iter()
        .map(|finalized_group| {
            GroupedPageCandidate::from_finalized(finalized_group, aggregate_count, direction)
        })
        .collect()
}

// Materialize grouped finalize entries in either canonical key order or
// hash-table iteration order without duplicating the bundle extraction path.
fn into_finalize_groups(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
) -> Vec<crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup> {
    if sorted {
        grouped_bundle.into_sorted_groups()
    } else {
        grouped_bundle.into_groups()
    }
}

// Apply grouped candidate selection, filtering, offset, and limit over one
// bounded grouped page window without sorting every finalized group first.
//
// This helper intentionally carries the full bounded grouped page-finalize
// contract in one place so direction-aware selection, HAVING filtering,
// continuation boundaries, and optional projection shaping stay synchronized.
#[expect(clippy::too_many_arguments)]
fn finalize_bounded_grouped_page_rows(
    direction: Direction,
    grouped_having: Option<&GroupHavingSpec>,
    grouped_having_expr: Option<&GroupHavingExpr>,
    grouped_bundle: GroupedAggregateBundle,
    group_fields: &[crate::db::query::plan::FieldSlot],
    pagination_window: &GroupedPaginationWindow,
    resume_boundary: Option<&Value>,
    selection_bound: usize,
    compiled_projection: Option<CompiledGroupedProjectionPlan<'_>>,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError> {
    let selected_candidates = retain_smallest_grouped_page_candidates(
        into_grouped_page_candidates(grouped_bundle, false, direction).into_iter(),
        grouped_having,
        grouped_having_expr,
        group_fields,
        resume_boundary,
        selection_bound,
    )?;

    finalize_grouped_page_rows_from_candidates(
        selected_candidates.into_iter(),
        pagination_window.limit(),
        pagination_window.initial_offset_for_page(),
        |_| Ok(true),
        compiled_projection,
    )
}

// Apply grouped filtering, offset, and limit over the common grouped shape
// when no bounded grouped page window is active.
fn finalize_unbounded_grouped_page_rows(
    direction: Direction,
    grouped_having: Option<&GroupHavingSpec>,
    grouped_having_expr: Option<&GroupHavingExpr>,
    grouped_bundle: GroupedAggregateBundle,
    group_fields: &[crate::db::query::plan::FieldSlot],
    pagination_window: &GroupedPaginationWindow,
    resume_boundary: Option<&Value>,
    compiled_projection: Option<CompiledGroupedProjectionPlan<'_>>,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError> {
    finalize_grouped_page_rows_from_candidates(
        into_grouped_page_candidates(grouped_bundle, true, direction).into_iter(),
        pagination_window.limit(),
        pagination_window.initial_offset_for_page(),
        |candidate| {
            candidate.matches_window(
                grouped_having,
                grouped_having_expr,
                group_fields,
                resume_boundary,
            )
        },
        compiled_projection,
    )
}

// Retain only the smallest canonical grouped rows needed for one bounded page
// window after grouped HAVING and resume filtering.
fn retain_smallest_grouped_page_candidates<I>(
    finalized_candidates: I,
    grouped_having: Option<&GroupHavingSpec>,
    grouped_having_expr: Option<&GroupHavingExpr>,
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
        if !candidate.matches_window(
            grouped_having,
            grouped_having_expr,
            group_fields,
            resume_boundary,
        )? {
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

    // Phase 2: restore grouped-key order across the retained bounded window
    // only, respecting the active grouped execution direction.
    let mut out = retained.into_vec();
    out.sort();

    Ok(out)
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

// Apply grouped filtering, offset/limit, and final row shaping over one
// ordered grouped-candidate stream in a single pass.
fn finalize_grouped_page_rows_from_candidates<I, FilterFn>(
    selected_candidates: I,
    limit: Option<usize>,
    initial_offset_for_page: usize,
    mut filter_candidate: FilterFn,
    compiled_projection: Option<CompiledGroupedProjectionPlan<'_>>,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError>
where
    I: Iterator<Item = GroupedPageCandidate>,
    FilterFn: FnMut(&GroupedPageCandidate) -> Result<bool, InternalError>,
{
    if let Some(compiled_projection) = compiled_projection {
        return finalize_grouped_page_rows_with_shaper(
            selected_candidates,
            limit,
            initial_offset_for_page,
            &mut filter_candidate,
            |candidate| {
                project_grouped_values_from_compiled_projection(
                    &compiled_projection,
                    candidate.group_key_values()?,
                    candidate.aggregate_values.as_slice(),
                )
            },
        );
    }

    finalize_grouped_page_rows_with_shaper(
        selected_candidates,
        limit,
        initial_offset_for_page,
        filter_candidate,
        GroupedPageCandidate::into_row,
    )
}

// Accumulate one grouped page directly from one ordered candidate stream using
// a caller-selected row shaper so the loop body stays single-purpose.
fn finalize_grouped_page_rows_with_shaper<I, FilterFn, ShapeFn>(
    selected_candidates: I,
    limit: Option<usize>,
    initial_offset_for_page: usize,
    mut filter_candidate: FilterFn,
    mut shape_row: ShapeFn,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError>
where
    I: Iterator<Item = GroupedPageCandidate>,
    FilterFn: FnMut(&GroupedPageCandidate) -> Result<bool, InternalError>,
    ShapeFn: FnMut(GroupedPageCandidate) -> Result<GroupedRow, InternalError>,
{
    let mut page_rows = Vec::<GroupedRow>::new();
    let mut has_more = false;
    let mut groups_skipped_for_offset = 0usize;

    // Phase 1: filter, offset, limit, and shape rows in one ordered pass.
    for candidate in selected_candidates {
        if !filter_candidate(&candidate)? {
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

        page_rows.push(shape_row(candidate)?);
    }

    let next_cursor_boundary = if has_more {
        page_rows.last().map(|row| row.group_key().to_vec())
    } else {
        None
    };

    Ok((page_rows, next_cursor_boundary))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{GroupedPageCandidate, finalize_grouped_page_rows_from_candidates};
    use crate::{
        db::{
            GroupedRow,
            executor::{
                group::GroupKey,
                projection::{CompiledGroupedProjectionPlan, compile_grouped_projection_plan},
            },
            query::{
                builder::aggregate::{count, max_by},
                plan::{
                    AggregateKind, FieldSlot, GroupedAggregateExecutionSpec,
                    PlannedProjectionLayout,
                    expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
                },
            },
        },
        value::Value,
    };

    #[test]
    fn finalize_grouped_page_rows_from_candidates_projects_directly_from_candidates() {
        let projection = ProjectionSpec::from_fields_for_test(vec![
            ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("age")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(max_by("score")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(count()),
                alias: None,
            },
        ]);
        let projection_layout = PlannedProjectionLayout {
            group_field_positions: vec![0],
            aggregate_positions: vec![1, 2],
        };
        let group_fields = [FieldSlot::from_parts_for_test(0, "age")];
        let aggregate_execution_specs = [
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Count,
                None,
                None,
                false,
            ),
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Max,
                Some(FieldSlot::from_parts_for_test(1, "score")),
                Some("score"),
                false,
            ),
        ];
        let compiled_projection = compile_grouped_projection_plan(
            &projection,
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
        )
        .expect("grouped projection should compile");
        let grouped_projection = CompiledGroupedProjectionPlan::from_parts_for_test(
            compiled_projection,
            &projection_layout,
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
        );
        let candidates = vec![GroupedPageCandidate {
            group_key: GroupKey::from_group_values(vec![Value::Uint(21)])
                .expect("candidate group key"),
            aggregate_values: vec![Value::Uint(2), Value::Uint(90)],
            direction: crate::db::direction::Direction::Asc,
        }];

        let (rows, next_cursor_boundary) = finalize_grouped_page_rows_from_candidates(
            candidates.into_iter(),
            None,
            0,
            |_| Ok(true),
            Some(grouped_projection),
        )
        .expect("candidate projection should succeed");

        assert_eq!(
            rows,
            vec![GroupedRow::new(
                vec![Value::Uint(21)],
                vec![Value::Uint(90), Value::Uint(2)],
            )]
        );
        assert_eq!(next_cursor_boundary, None);
    }
}
