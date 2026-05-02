//! Module: executor::aggregate::runtime::grouped_fold::count::window
//! Responsibility: grouped `COUNT(*)` page-window selection.
//! Boundary: owns HAVING, resume-boundary, bounded selection, and cursor payloads.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    db::{
        direction::Direction,
        executor::{
            RuntimeGroupedRow,
            aggregate::{
                reducer_core::finalize_count,
                runtime::{
                    group_matches_having_expr,
                    grouped_fold::{
                        metrics,
                        utils::{compare_grouped_boundary_values, grouped_next_cursor_boundary},
                    },
                    grouped_output::project_grouped_rows_from_projection,
                },
            },
            group::GroupKey,
            pipeline::contracts::{GroupedRouteStage, PageCursor},
            projection::{GroupedRowView, ProjectionEvalError, compile_grouped_projection_expr},
        },
        query::plan::expr::CompiledExpr,
    },
    error::InternalError,
    value::Value,
};

///
/// BoundedGroupedCountCandidate
///
/// BoundedGroupedCountCandidate keeps the largest retained canonical grouped
/// key at the top of the heap so grouped-count finalization can keep only the
/// smallest `selection_bound` rows when pagination bounds are active.
///

#[derive(Eq, PartialEq)]
struct BoundedGroupedCountCandidate {
    group_key: GroupKey,
    count: u32,
    direction: Direction,
}

impl Ord for BoundedGroupedCountCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_grouped_boundary_values(
            self.direction,
            self.group_key.canonical_value(),
            other.group_key.canonical_value(),
        )
    }
}

impl PartialOrd for BoundedGroupedCountCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

///
/// GroupedCountWindowSelection
///
/// GroupedCountWindowSelection freezes the grouped-count page-window policy
/// for one grouped route.
/// It keeps bounded selection, HAVING filtering, and resume-boundary filtering
/// under one local owner instead of rethreading raw route-derived values
/// through several sibling helpers.
///

pub(super) struct GroupedCountWindowSelection<'a> {
    route: &'a GroupedRouteStage,
    selection_bound: Option<usize>,
    resume_boundary: Option<&'a Value>,
    compiled_having_expr: Option<CompiledExpr>,
}

impl<'a> GroupedCountWindowSelection<'a> {
    // Build one grouped-count window selector from one grouped route stage.
    pub(super) fn new(route: &'a GroupedRouteStage) -> Result<Self, InternalError> {
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

        Ok(Self {
            route,
            selection_bound: route.grouped_selection_bound(),
            resume_boundary: route.grouped_resume_boundary(),
            compiled_having_expr,
        })
    }

    // Select grouped-count candidates after HAVING and resume filtering,
    // using a bounded top-k heap only when the grouped page window exposes one.
    fn select_candidates(
        &self,
        grouped_counts: Vec<(GroupKey, u32)>,
    ) -> Result<Vec<(GroupKey, u32)>, InternalError> {
        if let Some(selection_bound) = self.selection_bound {
            return self.select_bounded_candidates(grouped_counts, selection_bound);
        }

        self.select_unbounded_candidates(grouped_counts)
    }

    // Select and page grouped-count rows before grouped projection runs, so
    // grouped-count finalization keeps row-window policy and payload shaping
    // under one local owner.
    pub(super) fn select_page_rows(
        &self,
        grouped_counts: Vec<(GroupKey, u32)>,
    ) -> Result<GroupedCountPageRows, InternalError> {
        let grouped_pagination_window = self.route.grouped_pagination_window();
        let limit = grouped_pagination_window.limit();
        let initial_offset_for_page = grouped_pagination_window.initial_offset_for_page();
        let mut page_rows = Vec::<RuntimeGroupedRow>::new();
        let mut groups_skipped_for_offset = 0usize;
        let mut has_more = false;

        // Walk finalized grouped counts in canonical grouped-key order and
        // stop as soon as the current grouped page window proves another row
        // exists beyond the emitted page.
        for (group_key, count) in self.select_candidates(grouped_counts)? {
            metrics::record_candidate_row_qualified();
            let aggregate_value = finalize_count(u64::from(count));
            if groups_skipped_for_offset < initial_offset_for_page {
                groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
                metrics::record_page_row_skipped_for_offset();
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
            page_rows.push(RuntimeGroupedRow::new(
                emitted_group_key,
                vec![aggregate_value],
            ));
            metrics::record_page_row_emitted();
        }

        Ok(GroupedCountPageRows::new(page_rows, has_more))
    }

    // Select the smallest canonical grouped-count rows needed for one bounded
    // page window so grouped `LIMIT/OFFSET` does not sort every qualifying group.
    fn select_bounded_candidates(
        &self,
        grouped_counts: Vec<(GroupKey, u32)>,
        selection_bound: usize,
    ) -> Result<Vec<(GroupKey, u32)>, InternalError> {
        let mut retained = BinaryHeap::<BoundedGroupedCountCandidate>::new();

        // Stream HAVING/resume-qualified groups directly into the bounded heap
        // instead of staging every qualifying candidate in a second vector.
        for (group_key, count) in grouped_counts {
            if !self.row_matches_window(&group_key, count)? {
                continue;
            }
            self.retain_bounded_candidate(&mut retained, group_key, count, selection_bound);
        }

        Ok(self.finish_retained_bounded_candidates(retained))
    }

    // Select every qualifying grouped-count row and restore canonical order
    // when no bounded grouped page window is active.
    fn select_unbounded_candidates(
        &self,
        grouped_counts: Vec<(GroupKey, u32)>,
    ) -> Result<Vec<(GroupKey, u32)>, InternalError> {
        let mut out = Vec::with_capacity(grouped_counts.len());

        // Phase 1: apply grouped HAVING and continuation-resume filters before
        // materializing the final canonical grouped-count row set.
        for (group_key, count) in grouped_counts {
            if self.row_matches_window(&group_key, count)? {
                out.push((group_key, count));
            }
        }

        // Phase 2: restore canonical grouped-key order across every qualifying
        // row when the grouped page window is not bounded by `offset + limit + 1`.
        metrics::record_unbounded_selection_rows_sorted(out.len());
        out.sort_by(|(left_key, _), (right_key, _)| {
            compare_grouped_boundary_values(
                self.route.direction(),
                left_key.canonical_value(),
                right_key.canonical_value(),
            )
        });

        Ok(out)
    }

    // Return true when one grouped count row survives grouped HAVING and
    // resume-boundary filtering and should participate in candidate selection.
    fn row_matches_window(&self, group_key: &GroupKey, count: u32) -> Result<bool, InternalError> {
        metrics::record_window_row_considered();
        let aggregate_value = finalize_count(u64::from(count));
        let Value::List(group_key_values) = group_key.canonical_value() else {
            return Err(GroupedRouteStage::canonical_group_key_must_be_list(
                group_key.canonical_value(),
            ));
        };
        let grouped_row = GroupedRowView::new(
            group_key_values.as_slice(),
            std::slice::from_ref(&aggregate_value),
            self.route.group_fields(),
            &[],
        );
        if let Some(compiled_having_expr) = self.compiled_having_expr.as_ref()
            && !group_matches_having_expr(compiled_having_expr, &grouped_row)?
        {
            metrics::record_having_row_rejected();
            return Ok(false);
        }
        if let Some(resume_boundary) = self.resume_boundary
            && !crate::db::executor::aggregate::runtime::grouped_fold::utils::grouped_resume_boundary_allows_candidate(
                self.route.direction(),
                group_key.canonical_value(),
                resume_boundary,
            )
        {
            metrics::record_resume_boundary_row_rejected();
            return Ok(false);
        }

        Ok(true)
    }

    // Retain only the smallest canonical grouped-count rows needed for one
    // bounded grouped page window so selection does not sort every qualifying
    // group.
    #[cfg(test)]
    pub(super) fn retain_smallest_candidates(
        &self,
        grouped_counts: Vec<(GroupKey, u32)>,
        selection_bound: usize,
    ) -> Vec<(GroupKey, u32)> {
        let mut retained = BinaryHeap::<BoundedGroupedCountCandidate>::new();

        // Phase 1: keep only the smallest `selection_bound` qualifying groups
        // in a max-heap so the grouped count fast path pays `O(G log K)`
        // instead of sorting every qualifying group when pagination bounds
        // are active.
        for (group_key, count) in grouped_counts {
            self.retain_bounded_candidate(&mut retained, group_key, count, selection_bound);
        }

        self.finish_retained_bounded_candidates(retained)
    }

    // Insert one already-qualified grouped-count candidate into the retained
    // bounded heap, preserving the old max-heap replacement semantics while
    // allowing callers to avoid staging all qualifying rows first.
    fn retain_bounded_candidate(
        &self,
        retained: &mut BinaryHeap<BoundedGroupedCountCandidate>,
        group_key: GroupKey,
        count: u32,
        selection_bound: usize,
    ) {
        metrics::record_bounded_selection_candidate_seen();
        let candidate = BoundedGroupedCountCandidate {
            group_key,
            count,
            direction: self.route.direction(),
        };
        if retained.len() < selection_bound {
            retained.push(candidate);
            return;
        }

        if retained
            .peek()
            .is_some_and(|largest_retained| candidate.cmp(largest_retained).is_lt())
        {
            retained.pop();
            retained.push(candidate);
            metrics::record_bounded_selection_heap_replacement();
        }
    }

    // Convert the retained bounded heap back to canonical grouped-key order so
    // downstream pagination and projection see the same row order as before.
    fn finish_retained_bounded_candidates(
        &self,
        retained: BinaryHeap<BoundedGroupedCountCandidate>,
    ) -> Vec<(GroupKey, u32)> {
        // Phase 2: restore grouped-key order across the retained window only,
        // respecting the active grouped execution direction.
        let mut out: Vec<(GroupKey, u32)> = retained
            .into_vec()
            .into_iter()
            .map(|candidate| (candidate.group_key, candidate.count))
            .collect::<Vec<_>>();
        metrics::record_bounded_selection_rows_sorted(out.len());
        out.sort_by(|(left_key, _), (right_key, _)| {
            compare_grouped_boundary_values(
                self.route.direction(),
                left_key.canonical_value(),
                right_key.canonical_value(),
            )
        });

        out
    }
}

///
/// GroupedCountPageRows
///
/// GroupedCountPageRows keeps the grouped-count page rows selected before
/// grouped projection runs.
/// It owns the projection and next-cursor tail for the dedicated grouped
/// count path so row-window selection and final page shaping stay aligned.
///

pub(super) struct GroupedCountPageRows {
    rows: Vec<RuntimeGroupedRow>,
    has_more: bool,
}

impl GroupedCountPageRows {
    // Build one grouped-count page-row bundle before grouped projection and
    // next-cursor shaping run.
    const fn new(rows: Vec<RuntimeGroupedRow>, has_more: bool) -> Self {
        Self { rows, has_more }
    }

    // Apply grouped projection plus optional next-cursor construction to one
    // already paged grouped-count row bundle.
    pub(super) fn project_and_build_cursor(
        self,
        route: &GroupedRouteStage,
        grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
    ) -> Result<(Vec<RuntimeGroupedRow>, Option<PageCursor>), InternalError> {
        metrics::record_projection_rows_input(self.rows.len());
        let next_cursor_boundary = self
            .has_more
            .then(|| {
                self.rows
                    .last()
                    .map(|row| grouped_next_cursor_boundary(row.group_key()))
            })
            .flatten();
        let page_rows = project_grouped_rows_from_projection(
            grouped_projection_spec,
            route.projection_is_identity(),
            route.projection_layout(),
            route.group_fields(),
            route.grouped_aggregate_execution_specs(),
            self.rows,
        )?;
        let next_cursor = if self.has_more {
            metrics::record_cursor_construction_attempt();
            metrics::record_next_cursor_emitted();
            next_cursor_boundary
                .map(|last_group_key| route.grouped_next_cursor(last_group_key))
                .transpose()?
        } else {
            None
        };

        Ok((page_rows, next_cursor))
    }
}
