//! Module: executor::pipeline::operators::post_access
//! Responsibility: post-access execution operators for planned query materialization.
//! Does not own: planner validation semantics or access-path route selection.
//! Boundary: applies post-access ordering/window behavior over materialized rows.

mod contracts;
mod coordinator;
mod order_cursor;
mod terminal;
mod window;

use crate::{
    db::{
        executor::{
            ExecutionKernel, OrderReadableRow,
            pipeline::{
                contracts::PostAccessContract, operators::post_access::coordinator::PostAccessPlan,
            },
        },
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::EntityKind,
};

pub(in crate::db::executor) use contracts::PostAccessStats;

impl ExecutionKernel {
    pub(in crate::db::executor) fn apply_delete_post_access_with_compiled_predicate<E, R, K>(
        plan: &AccessPlannedQuery,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind<Key = K>,
        R: OrderReadableRow,
    {
        PostAccessPlan::new(PostAccessContract::new(plan))
            .apply_delete_post_access_with_compiled_predicate::<E, R>(rows, compiled_predicate)
    }
}
