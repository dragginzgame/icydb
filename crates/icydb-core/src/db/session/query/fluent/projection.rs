//! Module: db::session::query::fluent::projection
//! Responsibility: session adapters for fluent value/projection terminals.
//! Does not own: field-slot resolution, projection expression planning, or executor strategy.
//! Boundary: maps projection terminal strategies into scalar projection executor requests.

use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest},
        query::{
            builder::{
                CountDistinctBySlotTerminal, DistinctValuesBySlotTerminal,
                FirstValueBySlotTerminal, LastValueBySlotTerminal, ValuesBySlotTerminal,
                ValuesBySlotWithIdsTerminal,
            },
            plan::{AggregateKind, FieldSlot, expr::Expr},
        },
        session::{
            AcceptedExecutionOutput, AcceptedIdValuesOutput, AcceptedOptionalValueOutput,
            AcceptedValuesOutput,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Execute one projection terminal boundary and keep field projection
    // executor details out of fluent query modules.
    fn execute_scalar_projection_boundary<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<AcceptedExecutionOutput<ScalarProjectionBoundaryOutput>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan_and_catalog(query, move |load, plan| {
            load.execute_scalar_projection_boundary(plan, target_field, request)
        })
    }

    // Execute and decode one projection terminal boundary so value/count/id
    // decoding policy stays in the session adapter instead of each fluent method.
    fn execute_scalar_projection_value<E, T>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        request: ScalarProjectionBoundaryRequest,
        decode: impl FnOnce(ScalarProjectionBoundaryOutput) -> Result<T, InternalError>,
    ) -> Result<AcceptedExecutionOutput<T>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (output, enum_catalog) = self
            .execute_scalar_projection_boundary(query, target_field, request)?
            .into_parts();
        let value = decode(output).map_err(QueryError::execute)?;

        Ok(AcceptedExecutionOutput::new(value, enum_catalog))
    }

    // Execute one fluent `values_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_values_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: ValuesBySlotTerminal,
    ) -> Result<AcceptedValuesOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_values,
        )
    }

    // Execute one fluent `project_values(projection)` terminal with the bounded
    // projection expression carried into the executor projection boundary.
    pub(in crate::db) fn execute_fluent_project_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        projection: Expr,
    ) -> Result<AcceptedValuesOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_projection_value(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedValues { projection },
            ScalarProjectionBoundaryOutput::into_values,
        )
    }

    // Execute one fluent `distinct_values_by(field)` terminal through its
    // concrete session boundary.
    pub(in crate::db) fn execute_fluent_distinct_values_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: DistinctValuesBySlotTerminal,
    ) -> Result<AcceptedValuesOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_values,
        )
    }

    // Execute one fluent `count_distinct_by(field)` terminal through its
    // concrete session boundary.
    pub(in crate::db) fn execute_fluent_count_distinct_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: CountDistinctBySlotTerminal,
    ) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_count,
        )
        .map(AcceptedExecutionOutput::into_value)
    }

    // Execute one fluent `values_by_with_ids(field)` terminal through its
    // concrete session boundary.
    pub(in crate::db) fn execute_fluent_values_by_with_ids_slot<E>(
        &self,
        query: &Query<E>,
        strategy: ValuesBySlotWithIdsTerminal,
    ) -> Result<AcceptedIdValuesOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_values_with_ids::<E>,
        )
    }

    // Execute one fluent `project_values_with_ids(projection)` terminal with
    // projection fused into executor-side value extraction.
    pub(in crate::db) fn execute_fluent_project_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        projection: Expr,
    ) -> Result<AcceptedIdValuesOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_projection_value(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedValuesWithIds { projection },
            ScalarProjectionBoundaryOutput::into_values_with_ids::<E>,
        )
    }

    // Execute one fluent `first_value_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_first_value_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: FirstValueBySlotTerminal,
    ) -> Result<AcceptedOptionalValueOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_terminal_value,
        )
    }

    // Execute one fluent `last_value_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_last_value_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: LastValueBySlotTerminal,
    ) -> Result<AcceptedOptionalValueOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        self.execute_scalar_projection_value(
            query,
            target_field,
            request,
            ScalarProjectionBoundaryOutput::into_terminal_value,
        )
    }

    // Execute one fluent first/last projected-value terminal with projection
    // applied after selected-row resolution but before the value leaves the
    // executor boundary.
    pub(in crate::db) fn execute_fluent_project_terminal_value_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        terminal_kind: AggregateKind,
        projection: Expr,
    ) -> Result<AcceptedOptionalValueOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_projection_value(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedTerminalValue {
                terminal_kind,
                projection,
            },
            ScalarProjectionBoundaryOutput::into_terminal_value,
        )
    }
}
