//! Module: db::executor::aggregate::runtime::grouped_fold::page_finalize
//! Finalizes grouped-fold candidate streams into grouped result pages.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    db::executor::projection::ProjectionEvalError,
    db::{
        direction::Direction,
        executor::projection::GroupedRowView,
        executor::{
            GroupedPaginationWindow, RuntimeGroupedRow,
            aggregate::runtime::{
                group_matches_having_expr,
                grouped_fold::{
                    bundle::GroupedAggregateBundle,
                    utils::{
                        compare_grouped_boundary_values, grouped_next_cursor_boundary,
                        grouped_resume_boundary_allows_candidate,
                    },
                },
                grouped_output::project_grouped_values_from_compiled_projection,
            },
            group::GroupKey,
            pipeline::contracts::{GroupedRouteStage, PageCursor},
            projection::{
                CompiledGroupedProjectionPlan, GroupedProjectionExpr,
                compile_grouped_projection_expr, compile_grouped_projection_plan_if_needed,
                eval_grouped_projection_expr,
            },
        },
        numeric::canonical_value_compare,
        query::plan::{
            OrderDirection,
            expr::{Expr, ProjectionSpec},
        },
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
    ranking: GroupedPageCandidateRanking,
}

#[derive(Eq, PartialEq)]
enum GroupedPageCandidateRanking {
    Canonical {
        direction: Direction,
    },
    TopK {
        order_values: Vec<Value>,
        directions: Vec<OrderDirection>,
    },
}

struct CompiledGroupedTopKOrder {
    terms: Vec<CompiledGroupedTopKOrderTerm>,
}

struct CompiledGroupedTopKOrderTerm {
    expr: GroupedProjectionExpr,
    direction: OrderDirection,
}

impl Ord for GroupedPageCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_grouped_page_candidate_order(self, other)
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
        ranking: GroupedPageCandidateRanking,
    ) -> Result<Self, InternalError> {
        let (group_key, aggregate_values) = if aggregate_count == 1 {
            let (group_key, aggregate_value) = finalized_group.finalize_single()?;

            (group_key, vec![aggregate_value])
        } else {
            finalized_group.finalize(aggregate_count)?
        };

        Ok(Self {
            group_key,
            aggregate_values,
            ranking,
        })
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
        compiled_having_expr: Option<&GroupedProjectionExpr>,
        group_fields: &[crate::db::query::plan::FieldSlot],
        resume_boundary: Option<&Value>,
    ) -> Result<bool, InternalError> {
        if let Some(compiled_having_expr) = compiled_having_expr
            && !group_matches_having_expr(
                compiled_having_expr,
                &GroupedRowView::new(
                    self.group_key_values()?,
                    self.aggregate_values.as_slice(),
                    group_fields,
                    &[],
                ),
            )?
        {
            return Ok(false);
        }
        if let Some(resume_boundary) = resume_boundary
            && !grouped_resume_boundary_allows_candidate(
                self.canonical_direction(),
                self.group_key.canonical_value(),
                resume_boundary,
            )
        {
            return Ok(false);
        }

        Ok(true)
    }

    const fn canonical_direction(&self) -> Direction {
        match self.ranking {
            GroupedPageCandidateRanking::Canonical { direction } => direction,
            GroupedPageCandidateRanking::TopK { .. } => Direction::Asc,
        }
    }

    // Consume this finalized grouped payload into the public grouped row DTO.
    fn into_row(self) -> Result<RuntimeGroupedRow, InternalError> {
        let emitted_group_key = Self::into_group_key_values(self.group_key)?;

        Ok(RuntimeGroupedRow::new(
            emitted_group_key,
            self.aggregate_values,
        ))
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
) -> Result<(Vec<RuntimeGroupedRow>, Option<PageCursor>), InternalError> {
    let compiled_projection = compile_grouped_projection_plan_if_needed(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
    )?;
    let compiled_having_expr = route
        .grouped_having_expr()
        .map(|expr| {
            compile_grouped_projection_expr(
                expr,
                route.group_fields(),
                route.grouped_aggregate_execution_specs(),
            )
            .map_err(ProjectionEvalError::into_grouped_projection_internal_error)
        })
        .transpose()?;
    let selection = GroupedPageFinalizeSelection::new(
        route,
        pagination_window,
        compiled_projection,
        compiled_having_expr,
    )?;
    let (page_rows, next_cursor_boundary) =
        if let Some(selection_bound) = route.grouped_selection_bound() {
            selection.finalize_bounded(grouped_bundle, selection_bound)?
        } else {
            selection.finalize_unbounded(grouped_bundle)?
        };
    let next_cursor = if route.uses_top_k_group_selection() {
        None
    } else {
        next_cursor_boundary
            .map(|last_group_key| route.grouped_next_cursor(last_group_key))
            .transpose()?
    };

    Ok((page_rows, next_cursor))
}

// Build one finalized grouped candidate iterator from the grouped bundle
// without changing the single-aggregate versus multi-aggregate execution
// contract.
fn into_grouped_page_candidates(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
    direction: Direction,
    compiled_top_k_order: Option<&CompiledGroupedTopKOrder>,
    group_fields: &[crate::db::query::plan::FieldSlot],
) -> Result<Vec<GroupedPageCandidate>, InternalError> {
    let aggregate_count = grouped_bundle.aggregate_count();
    let candidates = into_finalize_groups(grouped_bundle, sorted)
        .into_iter()
        .map(|finalized_group| {
            let mut candidate = GroupedPageCandidate::from_finalized(
                finalized_group,
                aggregate_count,
                GroupedPageCandidateRanking::Canonical { direction },
            )?;

            if let Some(compiled_order) = compiled_top_k_order {
                candidate.ranking = compile_grouped_page_candidate_top_k_ranking(
                    &candidate,
                    compiled_order,
                    group_fields,
                )
                .expect("grouped Top-K order values must compile from finalized groups");
            }

            Ok(candidate)
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    Ok(candidates)
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

fn compile_grouped_top_k_order(
    route: &GroupedRouteStage,
) -> Result<Option<CompiledGroupedTopKOrder>, InternalError> {
    if !route.uses_top_k_group_selection() {
        return Ok(None);
    }

    let order = route.plan().scalar_plan().order.as_ref().ok_or_else(|| {
        InternalError::query_invalid_logical_plan(
            "grouped Top-K strategy requires explicit grouped ORDER BY terms",
        )
    })?;
    let mut terms = Vec::with_capacity(order.fields.len());

    for term in &order.fields {
        let expr = match term.expr() {
            Expr::Field(_)
            | Expr::Aggregate(_)
            | Expr::Literal(_)
            | Expr::FunctionCall { .. }
            | Expr::Case { .. }
            | Expr::Binary { .. }
            | Expr::Unary { .. } => term.expr().clone(),
            #[cfg(test)]
            Expr::Alias { .. } => term.expr().clone(),
        };
        let compiled = match compile_grouped_projection_expr(
            &expr,
            route.group_fields(),
            route.grouped_aggregate_execution_specs(),
        ) {
            Ok(compiled) => compiled,
            Err(ProjectionEvalError::UnknownField { .. }) => continue,
            Err(err) => {
                return Err(ProjectionEvalError::into_grouped_projection_internal_error(
                    err,
                ));
            }
        };
        terms.push(CompiledGroupedTopKOrderTerm {
            expr: compiled,
            direction: term.direction(),
        });
    }

    if terms.is_empty() {
        return Err(InternalError::query_invalid_logical_plan(
            "grouped Top-K order did not retain any grouped-row-visible order terms",
        ));
    }

    Ok(Some(CompiledGroupedTopKOrder { terms }))
}

fn compile_grouped_page_candidate_top_k_ranking(
    candidate: &GroupedPageCandidate,
    compiled_order: &CompiledGroupedTopKOrder,
    group_fields: &[crate::db::query::plan::FieldSlot],
) -> Result<GroupedPageCandidateRanking, InternalError> {
    let Value::List(group_key_values) = candidate.group_key.canonical_value() else {
        return Err(GroupedRouteStage::canonical_group_key_must_be_list(
            candidate.group_key.canonical_value(),
        ));
    };
    let grouped_row = GroupedRowView::new(
        group_key_values.as_slice(),
        candidate.aggregate_values.as_slice(),
        group_fields,
        &[],
    );
    let mut order_values = Vec::with_capacity(compiled_order.terms.len());
    let mut directions = Vec::with_capacity(compiled_order.terms.len());

    for term in &compiled_order.terms {
        order_values.push(
            eval_grouped_projection_expr(&term.expr, &grouped_row)
                .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?,
        );
        directions.push(term.direction);
    }

    Ok(GroupedPageCandidateRanking::TopK {
        order_values,
        directions,
    })
}

///
/// GroupedPageFinalizeSelection
///
/// GroupedPageFinalizeSelection freezes the route-owned finalize-time page
/// selection contract for one grouped output page.
/// It keeps direction, HAVING filters, continuation boundary, pagination, and
/// optional compiled projection under one local owner so bounded and
/// unbounded finalize paths stop rethreading the same inputs separately.
///

struct GroupedPageFinalizeSelection<'a> {
    direction: Direction,
    compiled_having_expr: Option<GroupedProjectionExpr>,
    compiled_top_k_order: Option<CompiledGroupedTopKOrder>,
    group_fields: &'a [crate::db::query::plan::FieldSlot],
    pagination_window: &'a GroupedPaginationWindow,
    resume_boundary: Option<&'a Value>,
    compiled_projection: Option<CompiledGroupedProjectionPlan<'a>>,
}

impl<'a> GroupedPageFinalizeSelection<'a> {
    // Build one grouped page-finalize selection contract from the grouped
    // route and one already-resolved grouped projection plan.
    fn new(
        route: &'a GroupedRouteStage,
        pagination_window: &'a GroupedPaginationWindow,
        compiled_projection: Option<CompiledGroupedProjectionPlan<'a>>,
        compiled_having_expr: Option<GroupedProjectionExpr>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            direction: route.direction(),
            compiled_having_expr,
            compiled_top_k_order: compile_grouped_top_k_order(route)?,
            group_fields: route.group_fields(),
            pagination_window,
            resume_boundary: route.grouped_resume_boundary(),
            compiled_projection,
        })
    }

    // Finalize one bounded grouped page window without sorting every
    // finalized group up front.
    fn finalize_bounded(
        &self,
        grouped_bundle: GroupedAggregateBundle,
        selection_bound: usize,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<Vec<Value>>), InternalError> {
        let selected_candidates = self.retain_smallest_candidates(
            into_grouped_page_candidates(
                grouped_bundle,
                false,
                self.direction,
                self.compiled_top_k_order.as_ref(),
                self.group_fields,
            )?
            .into_iter(),
            selection_bound,
        )?;

        self.finalize_rows_from_candidates(selected_candidates.into_iter(), |_| Ok(true))
    }

    // Finalize the common grouped page shape when no bounded grouped window
    // is active.
    fn finalize_unbounded(
        &self,
        grouped_bundle: GroupedAggregateBundle,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<Vec<Value>>), InternalError> {
        self.finalize_rows_from_candidates(
            into_grouped_page_candidates(
                grouped_bundle,
                true,
                self.direction,
                self.compiled_top_k_order.as_ref(),
                self.group_fields,
            )?
            .into_iter(),
            |candidate| self.matches_window(candidate),
        )
    }

    // Return true when one finalized grouped candidate survives grouped
    // HAVING and continuation resume-boundary filtering.
    fn matches_window(&self, candidate: &GroupedPageCandidate) -> Result<bool, InternalError> {
        candidate.matches_window(
            self.compiled_having_expr.as_ref(),
            self.group_fields,
            self.resume_boundary,
        )
    }

    // Retain only the smallest canonical grouped rows needed for one bounded
    // page window after grouped HAVING and resume filtering.
    fn retain_smallest_candidates<I>(
        &self,
        finalized_candidates: I,
        selection_bound: usize,
    ) -> Result<Vec<GroupedPageCandidate>, InternalError>
    where
        I: Iterator<Item = GroupedPageCandidate>,
    {
        let mut retained = BinaryHeap::<GroupedPageCandidate>::new();

        // Phase 1: keep only the smallest `selection_bound` qualifying groups
        // so bounded grouped pages do not sort every finalized group up front.
        for candidate in finalized_candidates {
            if !self.matches_window(&candidate)? {
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

        // Phase 2: restore grouped-key order across the retained bounded
        // window only, respecting the active grouped execution direction.
        let mut out = retained.into_vec();
        out.sort();

        Ok(out)
    }

    // Apply grouped filtering, offset/limit, and final row shaping over one
    // ordered grouped-candidate stream in a single pass.
    fn finalize_rows_from_candidates<I, FilterFn>(
        &self,
        selected_candidates: I,
        mut filter_candidate: FilterFn,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<Vec<Value>>), InternalError>
    where
        I: Iterator<Item = GroupedPageCandidate>,
        FilterFn: FnMut(&GroupedPageCandidate) -> Result<bool, InternalError>,
    {
        if let Some(compiled_projection) = &self.compiled_projection {
            return finalize_grouped_page_rows_with_shaper(
                selected_candidates,
                self.pagination_window.limit(),
                self.pagination_window.initial_offset_for_page(),
                &mut filter_candidate,
                |candidate| {
                    project_grouped_values_from_compiled_projection(
                        compiled_projection,
                        candidate.group_key_values()?,
                        candidate.aggregate_values.as_slice(),
                    )
                },
            );
        }

        finalize_grouped_page_rows_with_shaper(
            selected_candidates,
            self.pagination_window.limit(),
            self.pagination_window.initial_offset_for_page(),
            filter_candidate,
            GroupedPageCandidate::into_row,
        )
    }
}

fn compare_grouped_page_candidate_order(
    left: &GroupedPageCandidate,
    right: &GroupedPageCandidate,
) -> Ordering {
    match (&left.ranking, &right.ranking) {
        (
            GroupedPageCandidateRanking::Canonical {
                direction: left_direction,
            },
            GroupedPageCandidateRanking::Canonical {
                direction: right_direction,
            },
        ) if left_direction == right_direction => compare_grouped_boundary_values(
            *left_direction,
            left.group_key.canonical_value(),
            right.group_key.canonical_value(),
        ),
        (
            GroupedPageCandidateRanking::TopK {
                order_values: left_values,
                directions,
            },
            GroupedPageCandidateRanking::TopK {
                order_values: right_values,
                directions: right_directions,
            },
        ) if directions == right_directions => {
            for ((left_value, right_value), direction) in left_values
                .iter()
                .zip(right_values.iter())
                .zip(directions.iter())
            {
                let cmp = match direction {
                    OrderDirection::Asc => canonical_value_compare(left_value, right_value),
                    OrderDirection::Desc => canonical_value_compare(right_value, left_value),
                };
                if !cmp.is_eq() {
                    return cmp;
                }
            }

            canonical_value_compare(
                left.group_key.canonical_value(),
                right.group_key.canonical_value(),
            )
        }
        _ => canonical_value_compare(
            left.group_key.canonical_value(),
            right.group_key.canonical_value(),
        ),
    }
}

// Accumulate one grouped page directly from one ordered candidate stream using
// a caller-selected row shaper so the loop body stays single-purpose.
fn finalize_grouped_page_rows_with_shaper<I, FilterFn, ShapeFn>(
    selected_candidates: I,
    limit: Option<usize>,
    initial_offset_for_page: usize,
    mut filter_candidate: FilterFn,
    mut shape_row: ShapeFn,
) -> Result<(Vec<RuntimeGroupedRow>, Option<Vec<Value>>), InternalError>
where
    I: Iterator<Item = GroupedPageCandidate>,
    FilterFn: FnMut(&GroupedPageCandidate) -> Result<bool, InternalError>,
    ShapeFn: FnMut(GroupedPageCandidate) -> Result<RuntimeGroupedRow, InternalError>,
{
    let mut page_rows = Vec::<RuntimeGroupedRow>::new();
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
        page_rows
            .last()
            .map(|row| grouped_next_cursor_boundary(row.group_key()))
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
    use super::{GroupedPageCandidate, finalize_grouped_page_rows_with_shaper};
    use crate::{
        db::{
            executor::{
                RuntimeGroupedRow,
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
            ranking: super::GroupedPageCandidateRanking::Canonical {
                direction: crate::db::direction::Direction::Asc,
            },
        }];

        let (rows, next_cursor_boundary) = finalize_grouped_page_rows_with_shaper(
            candidates.into_iter(),
            None,
            0,
            |_| Ok(true),
            |candidate| {
                crate::db::executor::aggregate::runtime::grouped_output::project_grouped_values_from_compiled_projection(
                    &grouped_projection,
                    candidate.group_key_values()?,
                    candidate.aggregate_values.as_slice(),
                )
            },
        )
        .expect("candidate projection should succeed");

        assert_eq!(
            rows,
            vec![RuntimeGroupedRow::new(
                vec![Value::Uint(21)],
                vec![Value::Uint(90), Value::Uint(2)],
            )]
        );
        assert_eq!(next_cursor_boundary, None);
    }
}
