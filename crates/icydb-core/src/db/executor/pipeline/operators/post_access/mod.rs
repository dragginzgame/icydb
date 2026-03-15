//! Module: executor::pipeline::operators::post_access
//! Responsibility: post-access execution operators for planned query materialization.
//! Does not own: planner validation semantics or access-path route selection.
//! Boundary: applies post-access ordering/window behavior over materialized rows.

mod contracts;
mod coordinator;
mod order_cursor;
mod terminal;
#[cfg(test)]
mod tests;
mod window;

#[cfg(test)]
use crate::traits::EntitySchema;
use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, pipeline::operators::post_access::coordinator::PostAccessPlan,
        },
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

#[cfg(test)]
pub(in crate::db::executor) use contracts::BudgetSafetyMetadata;
pub(in crate::db::executor) use contracts::{PlanRow, PostAccessStats};

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
