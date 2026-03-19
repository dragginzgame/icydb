//! Module: db::executor::terminal::ranking::take
//! Responsibility: module-local ownership and contracts for db::executor::terminal::ranking::take.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutablePlan, pipeline::contracts::LoadExecutor,
            terminal::ranking::RankingTerminalBoundaryRequest,
        },
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
        self.execute_ranking_terminal_boundary(
            plan,
            RankingTerminalBoundaryRequest::Take { take_count },
        )?
        .into_rows()
    }
}
