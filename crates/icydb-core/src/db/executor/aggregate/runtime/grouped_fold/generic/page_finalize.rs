//! Module: db::executor::aggregate::runtime::grouped_fold::generic::page_finalize
//! Finalizes grouped-fold candidate streams into grouped result pages.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::projection::ProjectionEvalError,
    db::{
        cursor::GroupedContinuationToken,
        direction::Direction,
        executor::projection::GroupedRowView,
        executor::{
            GroupedPaginationWindow, RuntimeGroupedRow,
            aggregate::{
                CompiledExpr, OrderDirection, ProjectionSpec,
                runtime::{
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
            },
            budget::{charge_current_execution_budget, charge_sort_work, runtime_value_work},
            group::GroupKey,
            pipeline::contracts::GroupedRouteStage,
            projection::{
                CompiledGroupedProjectionPlan, compile_grouped_projection_expr,
                compile_grouped_projection_plan_if_needed,
            },
        },
        numeric::canonical_value_compare,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{borrow::Cow, cmp::Ordering, collections::BinaryHeap};

///
/// OrderedGroupedPageSelection
///
/// OrderedGroupedPageSelection consumes closed groups in already-proven final
/// key order. It reuses the canonical grouped finalization, HAVING, cursor,
/// offset, projection, and lookahead rules while retaining only response rows.
///

pub(in crate::db::executor::aggregate::runtime::grouped_fold) struct OrderedGroupedPageSelection<'a>
{
    selection: GroupedPageFinalizeSelection<'a>,
    aggregate_count: usize,
    page_rows: Vec<RuntimeGroupedRow>,
    groups_skipped_for_offset: usize,
    has_more: bool,
}

impl<'a> OrderedGroupedPageSelection<'a> {
    /// Build one incremental page selector for a canonical ordered grouped route.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn new(
        route: &'a GroupedRouteStage,
        grouped_projection_spec: &'a ProjectionSpec,
        aggregate_count: usize,
    ) -> Result<Self, InternalError> {
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
                .map_err(ProjectionEvalError::into_internal_error)
            })
            .transpose()?;
        let selection = GroupedPageFinalizeSelection::new(
            route,
            route.grouped_pagination_window(),
            compiled_projection,
            compiled_having_expr,
        )?;
        if selection.compiled_top_k_order.is_some() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(Self {
            selection,
            aggregate_count,
            page_rows: Vec::new(),
            groups_skipped_for_offset: 0,
            has_more: false,
        })
    }

    /// Consume one closed group and return whether lookahead proves the page complete.
    pub(super) fn push_closed_group(
        &mut self,
        finalized_group: crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup,
    ) -> Result<bool, InternalError> {
        let candidate = GroupedPageCandidate::from_finalized(
            finalized_group,
            self.aggregate_count,
            GroupedPageCandidateRanking::Canonical {
                direction: self.selection.direction,
            },
        )?;

        self.push_candidate(candidate)
    }

    /// Consume one already-finalized specialized group through the canonical page owner.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn push_finalized_values(
        &mut self,
        group_key: GroupKey,
        aggregate_values: Vec<Value>,
    ) -> Result<bool, InternalError> {
        if aggregate_values.len() != self.aggregate_count {
            return Err(InternalError::query_executor_invariant());
        }
        let candidate = GroupedPageCandidate {
            group_key,
            aggregate_values,
            ranking: GroupedPageCandidateRanking::Canonical {
                direction: self.selection.direction,
            },
        };

        self.push_candidate(candidate)
    }

    // Apply HAVING, continuation, offset, limit, lookahead, and projection to
    // one candidate whose aggregate values are already final.
    fn push_candidate(&mut self, candidate: GroupedPageCandidate) -> Result<bool, InternalError> {
        if !self.selection.matches_window(&candidate)? {
            return Ok(false);
        }
        if self.groups_skipped_for_offset
            < self.selection.pagination_window.initial_offset_for_page()
        {
            self.groups_skipped_for_offset = self.groups_skipped_for_offset.saturating_add(1);
            return Ok(false);
        }
        if self
            .selection
            .pagination_window
            .limit()
            .is_some_and(|limit| self.page_rows.len() >= limit)
        {
            self.has_more = true;
            return Ok(true);
        }

        self.page_rows
            .push(self.selection.shape_candidate(candidate)?);

        Ok(false)
    }

    /// Complete the incremental selection and construct its canonical cursor page.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn finish(
        self,
        route: &GroupedRouteStage,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<GroupedContinuationToken>), InternalError> {
        let next_cursor = if self.has_more {
            self.page_rows
                .last()
                .map(|row| grouped_next_cursor_boundary(row.group_key()))
                .map(|last_group_key| route.grouped_next_cursor(last_group_key))
                .transpose()?
        } else {
            None
        };

        Ok((self.page_rows, next_cursor))
    }
}

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
        terms: Vec<ResolvedGroupedTopKOrderTerm>,
    },
}

/// One evaluated grouped top-k order value paired with its comparison direction.
#[derive(Eq, PartialEq)]
struct ResolvedGroupedTopKOrderTerm {
    value: Value,
    direction: OrderDirection,
}

struct CompiledGroupedTopKOrder {
    terms: Vec<CompiledGroupedTopKOrderTerm>,
}

struct CompiledGroupedTopKOrderTerm {
    expr: CompiledExpr,
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
    // Estimate all owned value backing retained by this candidate. Top-K order
    // terms are separate owned values and therefore remain chargeable even
    // when they are equal to one group or aggregate output value.
    fn estimated_backing_bytes(&self) -> u64 {
        let group_key_bytes = runtime_value_work(self.group_key.canonical_value()).0;
        let aggregate_bytes = self.aggregate_values.iter().fold(0_u64, |total, value| {
            total.saturating_add(runtime_value_work(value).0)
        });
        let ranking_bytes = match &self.ranking {
            GroupedPageCandidateRanking::Canonical { .. } => 0,
            GroupedPageCandidateRanking::TopK { terms } => {
                terms.iter().fold(0_u64, |total, term| {
                    total.saturating_add(runtime_value_work(&term.value).0)
                })
            }
        };

        group_key_bytes
            .saturating_add(aggregate_bytes)
            .saturating_add(ranking_bytes)
    }

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
        compiled_having_expr: Option<&CompiledExpr>,
        resume_boundary: Option<&Value>,
    ) -> Result<bool, InternalError> {
        if let Some(compiled_having_expr) = compiled_having_expr
            && !group_matches_having_expr(
                compiled_having_expr,
                &GroupedRowView::new(self.group_key_values()?, self.aggregate_values.as_slice()),
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
) -> Result<(Vec<RuntimeGroupedRow>, Option<GroupedContinuationToken>), InternalError> {
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
            .map_err(ProjectionEvalError::into_internal_error)
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
fn for_each_grouped_page_candidate(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
    direction: Direction,
    compiled_top_k_order: Option<&CompiledGroupedTopKOrder>,
    mut visit_candidate: impl FnMut(GroupedPageCandidate) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let aggregate_count = grouped_bundle.aggregate_count();

    for finalized_group in into_finalize_groups(grouped_bundle, sorted)? {
        let mut candidate = GroupedPageCandidate::from_finalized(
            finalized_group,
            aggregate_count,
            GroupedPageCandidateRanking::Canonical { direction },
        )?;

        if let Some(compiled_order) = compiled_top_k_order {
            candidate.ranking =
                compile_grouped_page_candidate_top_k_ranking(&candidate, compiled_order)?;
        }

        visit_candidate(candidate)?;
    }

    Ok(())
}

// Materialize finalized grouped page candidates only for execution modes that
// genuinely require a full ordered candidate set.
fn collect_grouped_page_candidates(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
    direction: Direction,
    compiled_top_k_order: Option<&CompiledGroupedTopKOrder>,
) -> Result<Vec<GroupedPageCandidate>, InternalError> {
    let mut candidates = Vec::new();

    for_each_grouped_page_candidate(
        grouped_bundle,
        sorted,
        direction,
        compiled_top_k_order,
        |candidate| {
            candidates.push(candidate);
            Ok(())
        },
    )?;

    Ok(candidates)
}

// Materialize grouped finalize entries in either canonical key order or
// hash-table iteration order without duplicating the bundle extraction path.
fn into_finalize_groups(
    grouped_bundle: GroupedAggregateBundle,
    sorted: bool,
) -> Result<
    Vec<crate::db::executor::aggregate::runtime::grouped_fold::bundle::GroupedFinalizeGroup>,
    InternalError,
> {
    if sorted {
        grouped_bundle.into_sorted_groups()
    } else {
        Ok(grouped_bundle.into_groups())
    }
}

fn compile_grouped_top_k_order(
    route: &GroupedRouteStage,
) -> Result<Option<CompiledGroupedTopKOrder>, InternalError> {
    if !route.uses_top_k_group_selection() {
        return Ok(None);
    }

    let order = route
        .plan()
        .scalar_plan()
        .order
        .as_ref()
        .ok_or_else(InternalError::query_invalid_logical_plan)?;
    let mut terms = Vec::with_capacity(order.fields.len());

    for term in &order.fields {
        let expr = term.expr().clone();
        let compiled = match compile_grouped_projection_expr(
            &expr,
            route.group_fields(),
            route.grouped_aggregate_execution_specs(),
        ) {
            Ok(compiled) => compiled,
            Err(ProjectionEvalError::UnknownField { .. }) => continue,
            Err(err) => {
                return Err(ProjectionEvalError::into_internal_error(err));
            }
        };
        terms.push(CompiledGroupedTopKOrderTerm {
            expr: compiled,
            direction: term.direction(),
        });
    }

    if terms.is_empty() {
        return Err(InternalError::query_invalid_logical_plan());
    }

    Ok(Some(CompiledGroupedTopKOrder { terms }))
}

fn compile_grouped_page_candidate_top_k_ranking(
    candidate: &GroupedPageCandidate,
    compiled_order: &CompiledGroupedTopKOrder,
) -> Result<GroupedPageCandidateRanking, InternalError> {
    let Value::List(group_key_values) = candidate.group_key.canonical_value() else {
        return Err(GroupedRouteStage::canonical_group_key_must_be_list(
            candidate.group_key.canonical_value(),
        ));
    };
    let grouped_row = GroupedRowView::new(
        group_key_values.as_slice(),
        candidate.aggregate_values.as_slice(),
    );
    let mut terms = Vec::with_capacity(compiled_order.terms.len());

    for term in &compiled_order.terms {
        terms.push(ResolvedGroupedTopKOrderTerm {
            value: term
                .expr
                .evaluate(&grouped_row)
                .map(Cow::into_owned)
                .map_err(ProjectionEvalError::into_internal_error)?,
            direction: term.direction,
        });
    }

    Ok(GroupedPageCandidateRanking::TopK { terms })
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
    compiled_having_expr: Option<CompiledExpr>,
    compiled_top_k_order: Option<CompiledGroupedTopKOrder>,
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
        compiled_having_expr: Option<CompiledExpr>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            direction: route.direction(),
            compiled_having_expr,
            compiled_top_k_order: compile_grouped_top_k_order(route)?,
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
        let selected_candidates =
            self.retain_smallest_candidates_from_bundle(grouped_bundle, selection_bound)?;

        self.finalize_rows_from_candidates(selected_candidates.into_iter(), |_| Ok(true))
    }

    // Finalize the common grouped page shape when no bounded grouped window
    // is active.
    fn finalize_unbounded(
        &self,
        grouped_bundle: GroupedAggregateBundle,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<Vec<Value>>), InternalError> {
        self.finalize_rows_from_candidates(
            collect_grouped_page_candidates(
                grouped_bundle,
                true,
                self.direction,
                self.compiled_top_k_order.as_ref(),
            )?
            .into_iter(),
            |candidate| self.matches_window(candidate),
        )
    }

    // Return true when one finalized grouped candidate survives grouped
    // HAVING and continuation resume-boundary filtering.
    fn matches_window(&self, candidate: &GroupedPageCandidate) -> Result<bool, InternalError> {
        candidate.matches_window(self.compiled_having_expr.as_ref(), self.resume_boundary)
    }

    // Shape one selected candidate through the canonical grouped projection boundary.
    fn shape_candidate(
        &self,
        candidate: GroupedPageCandidate,
    ) -> Result<RuntimeGroupedRow, InternalError> {
        let Some(compiled_projection) = &self.compiled_projection else {
            return candidate.into_row();
        };

        project_grouped_values_from_compiled_projection(
            compiled_projection,
            candidate.group_key_values()?,
            candidate.aggregate_values.as_slice(),
        )
    }

    // Retain only the smallest canonical grouped rows needed for one bounded
    // page window after grouped HAVING and resume filtering.
    fn retain_smallest_candidates_from_bundle(
        &self,
        grouped_bundle: GroupedAggregateBundle,
        selection_bound: usize,
    ) -> Result<Vec<GroupedPageCandidate>, InternalError> {
        let mut retained = BinaryHeap::<GroupedPageCandidate>::new();

        // Phase 1: keep only the smallest `selection_bound` qualifying groups
        // so bounded grouped pages do not sort every finalized group up front.
        for_each_grouped_page_candidate(
            grouped_bundle,
            false,
            self.direction,
            self.compiled_top_k_order.as_ref(),
            |candidate| {
                if !self.matches_window(&candidate)? {
                    return Ok(());
                }
                charge_grouped_top_k_candidate::<GroupedPageCandidate>(
                    retained.len(),
                    selection_bound,
                    candidate.estimated_backing_bytes(),
                )?;
                if retained.len() < selection_bound {
                    retained.push(candidate);
                    return Ok(());
                }

                if retained
                    .peek()
                    .is_some_and(|largest_retained| candidate.cmp(largest_retained).is_lt())
                {
                    retained.pop();
                    retained.push(candidate);
                }

                Ok(())
            },
        )?;

        // Phase 2: restore grouped-key order across the retained bounded
        // window only, respecting the active grouped execution direction.
        let mut out = retained.into_vec();
        charge_sort_work::<GroupedPageCandidate>(out.len())?;
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

fn charge_grouped_top_k_candidate<R>(
    retained_count: usize,
    selection_bound: usize,
    retained_backing_bytes: u64,
) -> Result<(), InternalError> {
    let comparisons = if retained_count == 0 {
        0
    } else if retained_count < selection_bound {
        1
    } else {
        retained_count.saturating_add(1)
    };
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::SortEntries, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        u64::try_from(comparisons).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortTemporaryBytes,
        u64::try_from(std::mem::size_of::<R>())
            .unwrap_or(u64::MAX)
            .saturating_add(retained_backing_bytes),
    )
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
            GroupedPageCandidateRanking::TopK { terms: left_terms },
            GroupedPageCandidateRanking::TopK { terms: right_terms },
        ) if left_terms.len() == right_terms.len()
            && left_terms
                .iter()
                .zip(right_terms)
                .all(|(left, right)| left.direction == right.direction) =>
        {
            for (left, right) in left_terms.iter().zip(right_terms) {
                let cmp = match left.direction {
                    OrderDirection::Asc => canonical_value_compare(&left.value, &right.value),
                    OrderDirection::Desc => canonical_value_compare(&right.value, &left.value),
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
    use super::{
        GroupedPageCandidate, GroupedPageCandidateRanking, ResolvedGroupedTopKOrderTerm,
        finalize_grouped_page_rows_with_shaper,
    };
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
        let group_fields =
            crate::db::query::plan::GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(
                0, "age",
            )]);
        let aggregate_execution_specs = [
            GroupedAggregateExecutionSpec::from_test_inputs(
                AggregateKind::Count,
                None,
                None,
                false,
            ),
            GroupedAggregateExecutionSpec::from_test_inputs(
                AggregateKind::Max,
                Some(FieldSlot::from_test_slot(1, "score")),
                Some("score"),
                false,
            ),
        ];
        let compiled_projection = compile_grouped_projection_plan(
            &projection,
            &group_fields,
            aggregate_execution_specs.as_slice(),
        )
        .expect("grouped projection should compile");
        let grouped_projection = CompiledGroupedProjectionPlan::from_test_inputs(
            compiled_projection,
            &projection_layout,
        );
        let candidates = vec![GroupedPageCandidate {
            group_key: GroupKey::from_group_values(vec![Value::Nat64(21)])
                .expect("candidate group key"),
            aggregate_values: vec![Value::Nat64(2), Value::Nat64(90)],
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
                vec![Value::Nat64(21)],
                vec![Value::Nat64(90), Value::Nat64(2)],
            )]
        );
        assert_eq!(next_cursor_boundary, None);
    }

    #[test]
    fn grouped_top_k_candidate_counts_owned_order_value_backing() {
        let candidate = GroupedPageCandidate {
            group_key: GroupKey::from_group_values(vec![Value::Text("group".repeat(64))])
                .expect("candidate group key"),
            aggregate_values: vec![Value::Text("aggregate".repeat(64))],
            ranking: GroupedPageCandidateRanking::TopK {
                terms: vec![ResolvedGroupedTopKOrderTerm {
                    value: Value::Text("order".repeat(64)),
                    direction: crate::db::query::plan::OrderDirection::Asc,
                }],
            },
        };

        assert!(candidate.estimated_backing_bytes() > 1_000);
    }
}
