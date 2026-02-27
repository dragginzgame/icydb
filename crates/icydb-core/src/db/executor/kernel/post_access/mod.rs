//! Kernel-owned post-access execution semantics for planned queries.

mod order_cursor;
mod window;

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary,
            next_cursor_for_materialized_rows as derive_next_materialized_cursor,
        },
        direction::Direction,
        executor::{
            ExecutionKernel, predicate_runtime::PredicateFieldSlots,
            predicate_runtime::eval_with_slots as eval_predicate_with_slots,
        },
        policy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntitySchema, EntityValue},
    types::Id,
};
use std::ops::Deref;

use crate::db::executor::kernel::post_access::order_cursor::{
    apply_order_spec as apply_post_access_order_spec,
    apply_order_spec_bounded as apply_post_access_order_spec_bounded,
};

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
        compiled_predicate: Option<&PredicateFieldSlots>,
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
        compiled_predicate: Option<&PredicateFieldSlots>,
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
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        signature: ContinuationSignature,
    ) -> Result<Option<ContinuationToken>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        derive_next_materialized_cursor(
            plan,
            rows,
            stats.rows_after_cursor,
            cursor_boundary,
            direction,
            signature,
        )
    }

    #[must_use]
    pub(crate) fn budget_safety_metadata<E, K>(plan: &AccessPlannedQuery<K>) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        PostAccessPlan::new(plan).budget_safety_metadata::<E>()
    }

    #[must_use]
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
        compiled_predicate: Option<&PredicateFieldSlots>,
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
        compiled_predicate: Option<&PredicateFieldSlots>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        self.validate_post_access_invariants()?;
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

    // Guard post-access execution with internal plan-shape invariants.
    // Planner owns user-facing validation; this only catches internal misuse.
    fn validate_post_access_invariants(&self) -> Result<(), InternalError> {
        policy::validate_plan_shape(&self.plan.logical)
            .map_err(InternalError::plan_invariant_violation)
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
        compiled_predicate: Option<&PredicateFieldSlots>,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        let filtered = if self.plan.scalar_plan().predicate.is_some() {
            let Some(compiled_predicate) = compiled_predicate else {
                return Err(InternalError::query_executor_invariant(
                    "post-access filtering requires precompiled predicate slots",
                ));
            };

            rows.retain(|row| eval_predicate_with_slots(row.entity(), compiled_predicate));
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
                return Err(InternalError::query_executor_invariant(
                    "ordering must run after filtering",
                ));
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
                return Err(InternalError::query_executor_invariant(
                    "pagination must run after ordering",
                ));
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
                return Err(InternalError::query_executor_invariant(
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
    fn budget_safety_metadata<E>(&self) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        let logical = self.plan.scalar_plan();
        let has_residual_filter = logical.predicate.is_some();
        let access_order_satisfied_by_path = self.is_access_order_satisfied_by_path::<E>();
        let has_order = logical
            .order
            .as_ref()
            .is_some_and(|order| !order.fields.is_empty());
        let requires_post_access_sort = has_order && !access_order_satisfied_by_path;

        BudgetSafetyMetadata {
            has_residual_filter,
            access_order_satisfied_by_path,
            requires_post_access_sort,
        }
    }

    // Shared streaming eligibility gate for execution paths that consume
    // the resolved ordered key stream directly without post-access filtering/sorting.
    #[must_use]
    fn is_streaming_access_shape_safe<E>(&self) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        if !self.plan.scalar_plan().mode.is_load() {
            return false;
        }

        let metadata = self.budget_safety_metadata::<E>();
        if metadata.has_residual_filter {
            return false;
        }
        if metadata.requires_post_access_sort {
            return false;
        }

        true
    }

    // Return true when access-phase key ordering already matches canonical
    // executor ordering for the current plan order spec.
    fn is_access_order_satisfied_by_path<E>(&self) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        let Some(order) = self.plan.scalar_plan().order.as_ref() else {
            return false;
        };
        if order.fields.len() != 1 {
            return false;
        }
        if order.fields[0].0 != E::MODEL.primary_key.name {
            return false;
        }

        Self::access_stream_is_pk_ordered(&self.plan.access)
    }

    // Composite access order is valid only when every child preserves canonical
    // primary-key ordering.
    fn access_stream_is_pk_ordered(access: &AccessPlan<K>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::access_path_is_pk_ordered(path),
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                children.iter().all(Self::access_stream_is_pk_ordered)
            }
        }
    }

    // Current access-path producers normalize emitted `DataKey` values into
    // canonical order before stream composition.
    const fn access_path_is_pk_ordered(path: &AccessPath<K>) -> bool {
        match path {
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::IndexPrefix { .. }
            | AccessPath::IndexRange { .. }
            | AccessPath::FullScan => true,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        access::AccessPath,
        contracts::Predicate,
        cursor::CursorBoundary,
        query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
    };
    use crate::{db::ReadConsistency, model::field::FieldKind, types::Ulid};

    crate::test_entity! {
        ident = BudgetMetadataEntity,
        id = Ulid,
        entity_name = "BudgetMetadataEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("rank", FieldKind::Uint),
        ],
        indexes = [],
    }

    #[test]
    fn bounded_order_keep_count_includes_offset_for_non_cursor_page() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<u64>::FullScan, ReadConsistency::MissingOk);
        plan.page = Some(PageSpec {
            limit: Some(5),
            offset: 3,
        });

        assert_eq!(
            crate::db::executor::ExecutionKernel::bounded_order_keep_count(&plan, None),
            Some(9),
            "bounded ordering should keep offset + limit + 1 rows"
        );
    }

    #[test]
    fn bounded_order_keep_count_disabled_when_cursor_present() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<u64>::FullScan, ReadConsistency::MissingOk);
        plan.page = Some(PageSpec {
            limit: Some(5),
            offset: 0,
        });
        let cursor = CursorBoundary { slots: Vec::new() };

        assert_eq!(
            crate::db::executor::ExecutionKernel::bounded_order_keep_count(&plan, Some(&cursor),),
            None,
            "bounded ordering should be disabled for continuation requests"
        );
    }

    #[test]
    fn budget_safety_metadata_marks_pk_order_plan_as_access_order_satisfied() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });

        let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
            BudgetMetadataEntity,
            _,
        >(&plan);
        assert!(
            metadata.access_order_satisfied_by_path,
            "single-field PK ordering should be marked access-order-satisfied"
        );
        assert!(
            !metadata.has_residual_filter,
            "plan without predicate should not report residual filtering"
        );
        assert!(
            !metadata.requires_post_access_sort,
            "access-order-satisfied plan should not require post-access sorting"
        );
    }

    #[test]
    fn budget_safety_metadata_marks_residual_filter_plan_as_unsafe() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.predicate = Some(Predicate::True);

        let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
            BudgetMetadataEntity,
            _,
        >(&plan);
        assert!(
            metadata.has_residual_filter,
            "predicate-bearing plan must report residual filtering"
        );
        assert!(
            metadata.access_order_satisfied_by_path,
            "residual filter should not hide access-order satisfaction result"
        );
        assert!(
            !metadata.requires_post_access_sort,
            "residual filtering alone should not imply post-access sorting"
        );
    }
}
