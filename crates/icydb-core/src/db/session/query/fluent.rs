//! Module: db::session::query::fluent
//! Responsibility: fluent terminal adapters at the session/executor boundary.
//! Does not own: cursor handling, grouped execution, explain output, or attribution.
//! Boundary: maps fluent prepared strategies into executor requests and maps executor outputs back into fluent DTOs.

use crate::{
    db::{
        DbSession, EntityResponse, PersistedRow, Query, QueryError,
        executor::{
            ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest,
            ScalarTerminalBoundaryOutput, ScalarTerminalBoundaryRequest,
        },
        query::builder::{
            PreparedFluentExistingRowsTerminalRuntimeRequest,
            PreparedFluentExistingRowsTerminalStrategy, PreparedFluentNumericFieldStrategy,
            PreparedFluentOrderSensitiveTerminalStrategy, PreparedFluentProjectionRuntimeRequest,
            PreparedFluentProjectionStrategy, PreparedFluentScalarTerminalStrategy,
        },
        query::fluent::load::{FluentProjectionTerminalOutput, FluentScalarTerminalOutput},
        query::plan::FieldSlot,
    },
    traits::{CanisterKind, EntityValue},
    types::{Decimal, Id},
    value::Value,
};

impl<C: CanisterKind> DbSession<C> {
    // Execute one scalar terminal boundary and keep the executor-specific
    // request/output types contained inside the session adapter.
    fn execute_scalar_terminal_boundary<E>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<ScalarTerminalBoundaryOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.execute_scalar_terminal_request(plan, request)
        })
    }

    // Execute one projection terminal boundary and keep field projection
    // executor details out of fluent query modules.
    fn execute_scalar_projection_boundary<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<ScalarProjectionBoundaryOutput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.execute_scalar_projection_boundary(plan, target_field, request)
        })
    }

    // Execute one fluent count/exists terminal through a query-owned result
    // shape so fluent terminals do not import executor aggregate outputs.
    pub(in crate::db) fn execute_fluent_existing_rows_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: PreparedFluentExistingRowsTerminalStrategy,
    ) -> Result<FluentScalarTerminalOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (request, output_shape) = strategy.into_executor_request();
        let output = self.execute_scalar_terminal_boundary(query, request)?;

        match output_shape {
            PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows => output
                .into_count()
                .map(FluentScalarTerminalOutput::Count)
                .map_err(QueryError::execute),
            PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows => output
                .into_exists()
                .map(FluentScalarTerminalOutput::Exists)
                .map_err(QueryError::execute),
        }
    }

    // Execute one fluent id/extrema terminal through a query-owned result
    // shape after the session adapter has decoded storage keys into typed ids.
    pub(in crate::db) fn execute_fluent_scalar_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: PreparedFluentScalarTerminalStrategy,
    ) -> Result<FluentScalarTerminalOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let request = strategy.into_executor_request();

        self.execute_scalar_terminal_boundary(query, request)?
            .into_id::<E>()
            .map(FluentScalarTerminalOutput::Id)
            .map_err(QueryError::execute)
    }

    // Execute one fluent order-sensitive terminal through the session adapter.
    // The min/max pair request remains distinguished because it returns two ids.
    pub(in crate::db) fn execute_fluent_order_sensitive_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: PreparedFluentOrderSensitiveTerminalStrategy,
    ) -> Result<FluentScalarTerminalOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (request, returns_id_pair) = strategy.into_executor_request();
        let output = self.execute_scalar_terminal_boundary(query, request)?;

        if returns_id_pair {
            return output
                .into_id_pair::<E>()
                .map(FluentScalarTerminalOutput::IdPair)
                .map_err(QueryError::execute);
        }

        output
            .into_id::<E>()
            .map(FluentScalarTerminalOutput::Id)
            .map_err(QueryError::execute)
    }

    // Execute one fluent numeric-field terminal through the session-owned
    // request conversion layer.
    pub(in crate::db) fn execute_fluent_numeric_field_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: PreparedFluentNumericFieldStrategy,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();

        self.execute_with_plan(query, move |load, plan| {
            load.execute_numeric_field_boundary(plan, target_field, request)
        })
    }

    // Execute one fluent projection terminal through a query-owned output
    // shape after the session adapter has decoded any data keys into typed ids.
    pub(in crate::db) fn execute_fluent_projection_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: PreparedFluentProjectionStrategy,
    ) -> Result<FluentProjectionTerminalOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request, output_shape) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        match output_shape {
            PreparedFluentProjectionRuntimeRequest::Values
            | PreparedFluentProjectionRuntimeRequest::DistinctValues => output
                .into_values()
                .map(FluentProjectionTerminalOutput::Values)
                .map_err(QueryError::execute),
            PreparedFluentProjectionRuntimeRequest::CountDistinct => output
                .into_count()
                .map(FluentProjectionTerminalOutput::Count)
                .map_err(QueryError::execute),
            PreparedFluentProjectionRuntimeRequest::ValuesWithIds => output
                .into_values_with_ids::<E>()
                .map(FluentProjectionTerminalOutput::ValuesWithIds)
                .map_err(QueryError::execute),
            PreparedFluentProjectionRuntimeRequest::TerminalValue { .. } => output
                .into_terminal_value()
                .map(FluentProjectionTerminalOutput::TerminalValue)
                .map_err(QueryError::execute),
        }
    }

    // Execute the fluent `bytes()` terminal without leaking executor closure
    // assembly into query fluent code.
    pub(in crate::db) fn execute_fluent_bytes<E>(&self, query: &Query<E>) -> Result<u64, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, |load, plan| load.bytes(plan))
    }

    // Execute the fluent `bytes_by(field)` terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_bytes_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
    ) -> Result<u64, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bytes_by_slot(plan, target_slot)
        })
    }

    // Execute the fluent `take(k)` terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_take<E>(
        &self,
        query: &Query<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| load.take(plan, take_count))
    }

    // Execute one row-returning fluent top/bottom-k terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_ranked_rows_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
        descending: bool,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            if descending {
                load.top_k_by_slot(plan, target_slot, take_count)
            } else {
                load.bottom_k_by_slot(plan, target_slot, take_count)
            }
        })
    }

    // Execute one value-returning fluent top/bottom-k terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_ranked_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
        descending: bool,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            if descending {
                load.top_k_by_values_slot(plan, target_slot, take_count)
            } else {
                load.bottom_k_by_values_slot(plan, target_slot, take_count)
            }
        })
    }

    // Execute one id/value-returning fluent top/bottom-k terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_ranked_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
        descending: bool,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            if descending {
                load.top_k_by_with_ids_slot(plan, target_slot, take_count)
            } else {
                load.bottom_k_by_with_ids_slot(plan, target_slot, take_count)
            }
        })
    }
}
