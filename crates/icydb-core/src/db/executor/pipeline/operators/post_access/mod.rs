//! Module: executor::pipeline::operators::post_access
//! Responsibility: post-access execution operators for planned query materialization.
//! Does not own: planner validation semantics or access-path route selection.
//! Boundary: applies post-access ordering/window behavior over materialized rows.

mod order_cursor;
#[cfg(test)]
mod tests;
mod window;

use crate::db::executor::pipeline::operators::post_access::order_cursor::{
    apply_order_spec as apply_post_access_order_spec,
    apply_order_spec_bounded as apply_post_access_order_spec_bounded,
};
#[cfg(test)]
use crate::{
    db::executor::route::{derive_budget_safety_flags, stream_order_contract_safe},
    traits::EntitySchema,
};
use crate::{
    db::{
        cursor::{
            ContinuationToken, CursorBoundary,
            next_cursor_for_materialized_rows as derive_next_materialized_cursor,
        },
        executor::{
            ExecutionKernel, ScalarContinuationBindings,
            route::access_order_satisfied_by_route_contract,
        },
        predicate::PredicateProgram,
        query::plan::{AccessPlannedQuery, DeleteLimitSpec, OrderSpec, PageSpec, QueryMode},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

///
/// PlanRow
/// Row abstraction for applying plan semantics to executor rows.
///

pub(in crate::db::executor) trait PlanRow<E: EntityKind> {
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
pub(in crate::db::executor) struct PostAccessStats {
    pub(in crate::db::executor) delete_was_limited: bool,
    pub(in crate::db::executor) rows_after_cursor: usize,
    #[cfg(test)]
    pub(in crate::db::executor) filtered: bool,
    #[cfg(test)]
    pub(in crate::db::executor) ordered: bool,
    #[cfg(test)]
    pub(in crate::db::executor) paged: bool,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_filter: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_order: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_page: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_delete_limit: usize,
}

///
/// BudgetSafetyMetadata
///
/// Executor-facing plan metadata for guarded scan-budget eligibility checks.
/// This metadata keeps budget-safety predicates explicit at the plan boundary.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::db::executor) struct BudgetSafetyMetadata {
    pub(in crate::db::executor) has_residual_filter: bool,
    pub(in crate::db::executor) access_order_satisfied_by_path: bool,
    pub(in crate::db::executor) requires_post_access_sort: bool,
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

    // Project the plan mode through one post-access boundary accessor.
    const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    // Project ORDER BY semantics through one post-access boundary accessor.
    const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    // Project page-window semantics through one post-access boundary accessor.
    const fn page_spec(&self) -> Option<&PageSpec> {
        self.plan.scalar_plan().page.as_ref()
    }

    // Project delete-limit semantics through one post-access boundary accessor.
    const fn delete_limit_spec(&self) -> Option<&DeleteLimitSpec> {
        self.plan.scalar_plan().delete_limit.as_ref()
    }

    // Project residual predicate presence through one post-access boundary accessor.
    const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }
}

impl ExecutionKernel {
    pub(in crate::db::executor) fn apply_post_access_with_compiled_predicate<E, R, K>(
        plan: &AccessPlannedQuery<K>,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        PostAccessPlan::new(plan)
            .apply_post_access_with_compiled_predicate::<E, R>(rows, compiled_predicate)
    }

    pub(in crate::db::executor) fn apply_post_access_with_cursor_and_compiled_predicate<E, R, K>(
        plan: &AccessPlannedQuery<K>,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
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
        let post_access = PostAccessPlan::new(plan);

        derive_next_materialized_cursor(
            &plan.access,
            post_access.order_spec(),
            post_access.page_spec(),
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
    pub(in crate::db::executor) fn budget_safety_metadata<E, K>(
        plan: &AccessPlannedQuery<K>,
    ) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        PostAccessPlan::new(plan).budget_safety_metadata::<E>()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) fn is_stream_order_contract_safe<E, K>(
        plan: &AccessPlannedQuery<K>,
    ) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        PostAccessPlan::new(plan).is_stream_order_contract_safe::<E>()
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
        E: EntityKind<Key = K> + EntityValue,
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
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        self.validate_cursor_mode(cursor)?;

        // Phase 1: predicate filtering.
        let (filtered, rows_after_filter) =
            self.apply_filter_phase::<E, R>(rows, compiled_predicate)?;

        // Phase 2: ordering.
        let (ordered, rows_after_order) = self.apply_order_phase::<E, R>(rows, cursor, filtered)?;

        // Phase 3: continuation boundary.
        let (_cursor_skipped, rows_after_cursor) =
            ExecutionKernel::apply_cursor_boundary_phase::<K, E, R>(
                self.plan,
                rows,
                cursor,
                ordered,
                rows_after_order,
            )?;

        // Phase 4: load pagination.
        let (paged, rows_after_page) = self.apply_page_phase(rows, ordered, cursor)?;

        // Phase 5: delete limiting.
        let (delete_was_limited, rows_after_delete_limit) =
            self.apply_delete_limit_phase(rows, ordered)?;

        #[cfg(not(test))]
        let _ = (
            rows_after_filter,
            paged,
            rows_after_page,
            rows_after_delete_limit,
        );

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
        if cursor.is_some() && !self.mode().is_load() {
            return Err(crate::db::error::query_invalid_logical_plan(
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
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        let filtered = if self.has_predicate() {
            let Some(compiled_predicate) = compiled_predicate else {
                return Err(crate::db::error::query_executor_invariant(
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
        E: EntityKind<Key = K> + EntityValue,
        R: PlanRow<E>,
    {
        let bounded_order_keep = ExecutionKernel::bounded_order_keep_count(self.plan, cursor);
        if let Some(order) = self.order_spec()
            && !order.fields.is_empty()
        {
            if self.has_predicate() && !filtered {
                return Err(crate::db::error::query_executor_invariant(
                    "ordering must run after filtering",
                ));
            }

            // If access traversal already satisfies requested ORDER BY
            // semantics, preserve stream order and skip in-memory sorting.
            if self.order_satisfied_by_access_path::<E>() {
                return Ok((true, rows.len()));
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

    // Return whether the resolved access stream order already satisfies this
    // ORDER BY contract under planner logical pushdown policy gates.
    fn order_satisfied_by_access_path<E>(&self) -> bool
    where
        E: EntityKind<Key = K> + EntityValue,
    {
        access_order_satisfied_by_route_contract::<E, _>(self.plan)
    }

    // Load pagination phase (offset/limit).
    fn apply_page_phase<R>(
        &self,
        rows: &mut Vec<R>,
        ordered: bool,
        cursor: Option<&CursorBoundary>,
    ) -> Result<(bool, usize), InternalError> {
        let paged = if self.mode().is_load()
            && let Some(page) = self.page_spec()
        {
            if self.order_spec().is_some() && !ordered {
                return Err(crate::db::error::query_executor_invariant(
                    "pagination must run after ordering",
                ));
            }
            window::apply_pagination(
                rows,
                ExecutionKernel::effective_page_offset(self.plan, cursor),
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
        let delete_was_limited = if self.mode().is_delete()
            && let Some(limit) = self.delete_limit_spec()
        {
            if self.order_spec().is_some() && !ordered {
                return Err(crate::db::error::query_executor_invariant(
                    "delete limit must run after ordering",
                ));
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
    fn is_stream_order_contract_safe<E>(&self) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        stream_order_contract_safe::<E, _>(self.plan)
    }
}
