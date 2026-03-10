//! Module: db::executor::load::terminal::ranking::take
//! Responsibility: module-local ownership and contracts for db::executor::load::terminal::ranking::take.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutablePlan, load::LoadExecutor},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one `take(k)` terminal over the canonical load response.
    pub(in crate::db) fn take(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_take_terminal(plan, take_count)
    }

    // Execute one row-terminal take (`take(k)`) via canonical materialized
    // response semantics.
    fn execute_take_terminal(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let response = self.execute(plan)?;
        let mut rows = response.rows();
        let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
        if rows.len() > take_len {
            rows.truncate(take_len);
        }

        Ok(EntityResponse::new(rows))
    }
}
