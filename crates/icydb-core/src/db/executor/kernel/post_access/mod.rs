//! Kernel-owned post-access execution semantics for planned queries.

mod order_cursor;
#[cfg(test)]
mod tests;
mod window;

use crate::db::executor::kernel::post_access::order_cursor::{
    apply_order_spec as apply_post_access_order_spec,
    apply_order_spec_bounded as apply_post_access_order_spec_bounded,
};
#[cfg(test)]
use crate::{
    db::executor::route::{derive_budget_safety_flags, streaming_access_shape_safe},
    traits::EntitySchema,
};
use crate::{
    db::{
        cursor::{
            ContinuationToken, CursorBoundary,
            next_cursor_for_materialized_rows as derive_next_materialized_cursor,
        },
        executor::{ExecutionKernel, ScalarContinuationBindings},
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::ops::Deref;

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

///
/// PlanRow
/// Row abstraction for applying plan semantics to executor rows.
///

pub(crate) trait PlanRow<E: EntityKind> {
    fn entity(&self) -> &E;
}

impl<E: EntityKind> PlanRow<E> for (Id<E>, E) {
    fn entity(&self) -> &E {
        &self.1
    }
}

///
/// PostAccessStats
///
/// Post-access execution statistics.
///
/// Runtime currently consumes only:
/// - `rows_after_cursor` for continuation decisions
/// - `delete_was_limited` for delete diagnostics
///
/// Additional phase-level fields are compiled in tests for structural assertions.
///

#[cfg_attr(test, expect(dead_code, clippy::struct_excessive_bools))]
pub(crate) struct PostAccessStats {
    pub(crate) delete_was_limited: bool,
    pub(crate) rows_after_cursor: usize,
    #[cfg(test)]
    pub(crate) filtered: bool,
    #[cfg(test)]
    pub(crate) ordered: bool,
    #[cfg(test)]
    pub(crate) paged: bool,
    #[cfg(test)]
    pub(crate) rows_after_filter: usize,
    #[cfg(test)]
    pub(crate) rows_after_order: usize,
    #[cfg(test)]
    pub(crate) rows_after_page: usize,
    #[cfg(test)]
    pub(crate) rows_after_delete_limit: usize,
}

///
/// BudgetSafetyMetadata
///
/// Executor-facing plan metadata for guarded scan-budget eligibility checks.
/// This metadata keeps budget-safety predicates explicit at the plan boundary.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct BudgetSafetyMetadata {
    pub(crate) has_residual_filter: bool,
    pub(crate) access_order_satisfied_by_path: bool,
    pub(crate) requires_post_access_sort: bool,
}

///
/// PostAccessPlan
///
/// Executor-owned post-access operation wrapper over one plan contract.
///

struct PostAccessPlan<'a, K> {
    plan: &'a AccessPlannedQuery<K>,
}

impl<'a, K> PostAccessPlan<'a, K> {
    const fn new(plan: &'a AccessPlannedQuery<K>) -> Self {
        Self { plan }
    }
}

impl<K> Deref for PostAccessPlan<'_, K> {
    type Target = AccessPlannedQuery<K>;

    fn deref(&self) -> &Self::Target {
        self.plan
    }
}

impl ExecutionKernel {
    pub(crate) fn apply_post_access_with_compiled_predicate<E, R, K>(
        plan: &AccessPlannedQuery<K>,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        PostAccessPlan::new(plan)
            .apply_post_access_with_compiled_predicate::<E, R>(rows, compiled_predicate)
    }

    pub(crate) fn apply_post_access_with_cursor_and_compiled_predicate<E, R, K>(
        plan: &AccessPlannedQuery<K>,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        PostAccessPlan::new(plan).apply_post_access_with_cursor_and_compiled_predicate::<E, R>(
            rows,
            cursor,
            compiled_predicate,
        )
    }

    pub(in crate::db::executor) fn next_cursor_for_materialized_rows<E>(
        plan: &AccessPlannedQuery<E::Key>,
        rows: &[(Id<E>, E)],
        stats: &PostAccessStats,
        continuation: ScalarContinuationBindings<'_>,
    ) -> Result<Option<ContinuationToken>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        derive_next_materialized_cursor(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            plan.scalar_plan().page.as_ref(),
            rows,
            stats.rows_after_cursor,
            continuation.post_access_cursor_boundary(),
            continuation.previous_index_range_anchor(),
            continuation.direction(),
            continuation.continuation_signature(),
        )
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn budget_safety_metadata<E, K>(plan: &AccessPlannedQuery<K>) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        PostAccessPlan::new(plan).budget_safety_metadata::<E>()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_streaming_access_shape_safe<E, K>(plan: &AccessPlannedQuery<K>) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        PostAccessPlan::new(plan).is_streaming_access_shape_safe::<E>()
    }
}

impl<K> PostAccessPlan<'_, K> {
    /// Apply predicate, ordering, and pagination in plan order with one precompiled predicate.
    fn apply_post_access_with_compiled_predicate<E, R>(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        self.apply_post_access_with_cursor_and_compiled_predicate::<E, R>(
            rows,
            None,
            compiled_predicate,
        )
    }

    /// Apply predicate, ordering, cursor boundary, and pagination with a precompiled predicate.
    fn apply_post_access_with_cursor_and_compiled_predicate<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        self.validate_cursor_mode(cursor)?;

        // Phase 1: predicate filtering.
        let (filtered, rows_after_filter) =
            self.apply_filter_phase::<E, R>(rows, compiled_predicate)?;

        // Phase 2: ordering.
        let (ordered, rows_after_order) = self.apply_order_phase::<E, R>(rows, cursor, filtered)?;

        // Phase 3: continuation boundary.
        let (_cursor_skipped, rows_after_cursor) = ExecutionKernel::apply_cursor_boundary_phase::<
            K,
            E,
            R,
        >(
            self, rows, cursor, ordered, rows_after_order
        )?;

        // Phase 4: load pagination.
        let (paged, rows_after_page) = self.apply_page_phase(rows, ordered, cursor)?;

        // Phase 5: delete limiting.
        let (delete_was_limited, rows_after_delete_limit) =
            self.apply_delete_limit_phase(rows, ordered)?;

        #[cfg(not(test))]
        let _ = rows_after_filter;
        #[cfg(not(test))]
        let _ = paged;
        #[cfg(not(test))]
        let _ = rows_after_page;
        #[cfg(not(test))]
        let _ = rows_after_delete_limit;

        Ok(PostAccessStats {
            delete_was_limited,
            rows_after_cursor,
            #[cfg(test)]
            filtered,
            #[cfg(test)]
            ordered,
            #[cfg(test)]
            paged,
            #[cfg(test)]
            rows_after_filter,
            #[cfg(test)]
            rows_after_order,
            #[cfg(test)]
            rows_after_page,
            #[cfg(test)]
            rows_after_delete_limit,
        })
    }

    // Enforce load/delete cursor compatibility before execution phases.
    fn validate_cursor_mode(&self, cursor: Option<&CursorBoundary>) -> Result<(), InternalError> {
        if cursor.is_some() && !self.plan.scalar_plan().mode.is_load() {
            return Err(InternalError::query_invalid_logical_plan(
                "delete plans must not carry cursor boundaries",
            ));
        }

        Ok(())
    }

    // Predicate phase (already normalized and validated during planning).
    fn apply_filter_phase<E, R>(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        let filtered = if self.plan.scalar_plan().predicate.is_some() {
            let Some(compiled_predicate) = compiled_predicate else {
                return Err(invariant(
                    "post-access filtering requires precompiled predicate slots",
                ));
            };

            rows.retain(|row| compiled_predicate.eval(row.entity()));
            true
        } else {
            false
        };

        Ok((filtered, rows.len()))
    }

    // Ordering phase with bounded-load optimization for first-page load paths.
    fn apply_order_phase<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        filtered: bool,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        let bounded_order_keep = ExecutionKernel::bounded_order_keep_count(self, cursor);
        let logical = self.plan.scalar_plan();
        if let Some(order) = &logical.order
            && !order.fields.is_empty()
        {
            if logical.predicate.is_some() && !filtered {
                return Err(invariant("ordering must run after filtering"));
            }

            let ordered_total = rows.len();
            if rows.len() > 1 {
                if let Some(keep_count) = bounded_order_keep {
                    apply_post_access_order_spec_bounded::<E, R>(rows, order, keep_count);
                } else {
                    apply_post_access_order_spec::<E, R>(rows, order);
                }
            }

            // Keep logical post-order cardinality even when bounded ordering
            // trims the working set for load-page execution.
            return Ok((true, ordered_total));
        }

        Ok((false, rows.len()))
    }

    // Load pagination phase (offset/limit).
    fn apply_page_phase<R>(
        &self,
        rows: &mut Vec<R>,
        ordered: bool,
        cursor: Option<&CursorBoundary>,
    ) -> Result<(bool, usize), InternalError> {
        let logical = self.plan.scalar_plan();
        let paged = if logical.mode.is_load()
            && let Some(page) = &logical.page
        {
            if logical.order.is_some() && !ordered {
                return Err(invariant("pagination must run after ordering"));
            }
            window::apply_pagination(
                rows,
                ExecutionKernel::effective_page_offset(self, cursor),
                page.limit,
            );
            true
        } else {
            false
        };

        Ok((paged, rows.len()))
    }

    // Delete-limit phase (after ordering).
    fn apply_delete_limit_phase<R>(
        &self,
        rows: &mut Vec<R>,
        ordered: bool,
    ) -> Result<(bool, usize), InternalError> {
        let logical = self.plan.scalar_plan();
        let delete_was_limited = if logical.mode.is_delete()
            && let Some(limit) = &logical.delete_limit
        {
            if logical.order.is_some() && !ordered {
                return Err(invariant("delete limit must run after ordering"));
            }
            window::apply_delete_limit(rows, limit.max_rows);
            true
        } else {
            false
        };

        Ok((delete_was_limited, rows.len()))
    }

    /// Build budget-safety metadata used by guarded execution scan budgeting.
    #[must_use]
    #[cfg(test)]
    fn budget_safety_metadata<E>(&self) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        let (has_residual_filter, access_order_satisfied_by_path, requires_post_access_sort) =
            derive_budget_safety_flags::<E, _>(self.plan);

        BudgetSafetyMetadata {
            has_residual_filter,
            access_order_satisfied_by_path,
            requires_post_access_sort,
        }
    }

    // Shared streaming eligibility gate for execution paths that consume
    // the resolved ordered key stream directly without post-access filtering/sorting.
    #[must_use]
    #[cfg(test)]
    fn is_streaming_access_shape_safe<E>(&self) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        streaming_access_shape_safe::<E, _>(self.plan)
    }
}
