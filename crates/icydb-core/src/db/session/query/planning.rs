//! Module: db::session::query::planning
//! Responsibility: session-owned planned/compiled query adapter surfaces.
//! Does not own: cache internals, executor dispatch, explain rendering, or query intent construction.
//! Boundary: maps cached lower access plans into query-owned plan DTOs.

use crate::{
    db::{
        DbSession, Query, QueryError,
        query::intent::{CompiledQuery, PlannedQuery},
    },
    traits::{CanisterKind, EntityKind},
};

impl<C: CanisterKind> DbSession<C> {
    /// Compile one typed query using only indexes currently visible for the recovered store.
    pub(in crate::db) fn compile_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<CompiledQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, _) = self.cached_shared_query_plan_for_entity::<E>(query)?;

        Ok(CompiledQuery::from_plan(
            prepared_plan.logical_plan().clone(),
        ))
    }

    /// Build one logical planned-query shell using only indexes currently visible for the recovered store.
    pub(in crate::db) fn planned_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<PlannedQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, _) = self.cached_shared_query_plan_for_entity::<E>(query)?;

        Ok(PlannedQuery::from_plan(
            prepared_plan.logical_plan().clone(),
        ))
    }
}
