//! Module: db::session::query::fluent
//! Responsibility: fluent terminal adapters at the session/executor boundary.
//! Does not own: cursor handling, grouped execution, explain output, or attribution.
//! Boundary: maps fluent prepared strategies into executor requests and maps executor outputs back into fluent DTOs.

#[cfg(feature = "diagnostics")]
use crate::db::{
    FluentTerminalExecutionAttribution, ScalarAggregateAttribution,
    diagnostics::{StoreCounterSnapshot, measure_local_instruction_delta as measure_query_stage},
    executor::with_scalar_aggregate_terminal_attribution,
};
use crate::{
    db::{
        DbSession, EntityResponse, PersistedRow, Query, QueryError,
        executor::{
            ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryOutput,
            ScalarProjectionBoundaryRequest, ScalarTerminalBoundaryOutput,
            ScalarTerminalBoundaryRequest,
        },
        query::builder::{
            AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal,
            CountRowsTerminal, DistinctValuesBySlotTerminal, ExistsRowsTerminal, FirstIdTerminal,
            FirstValueBySlotTerminal, LastIdTerminal, LastValueBySlotTerminal, MaxIdBySlotTerminal,
            MaxIdTerminal, MedianIdBySlotTerminal, MinIdBySlotTerminal, MinIdTerminal,
            MinMaxIdBySlotTerminal, NthIdBySlotTerminal, SumBySlotTerminal,
            SumDistinctBySlotTerminal, ValuesBySlotTerminal, ValuesBySlotWithIdsTerminal,
        },
        query::plan::{FieldSlot, expr::Expr},
    },
    traits::{CanisterKind, EntityValue},
    types::{Decimal, Id},
    value::Value,
};

// Fluent min/max terminal outputs use an optional typed id pair at the session
// boundary after executor storage keys have been decoded.
type FluentIdPair<E> = Option<(Id<E>, Id<E>)>;

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

    // Execute one scalar terminal boundary with diagnostics attribution. This
    // mirrors the normal scalar-terminal path while exposing the executor-owned
    // scalar aggregate collector used by SQL aggregate attribution.
    #[cfg(feature = "diagnostics")]
    fn execute_scalar_terminal_boundary_with_attribution<E>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<
        (
            ScalarTerminalBoundaryOutput,
            FluentTerminalExecutionAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (plan_lookup_local_instructions, plan_and_cache) = measure_query_stage(|| {
            self.cached_prepared_query_plan_for_entity_with_compile_phase_attribution::<E>(query)
        });
        let (plan, cache_attribution, compile_phase_attribution) = plan_and_cache?;
        let compile_local_instructions = plan_lookup_local_instructions;

        let store_counters_before = StoreCounterSnapshot::capture();
        let (scalar_aggregate_terminal, (executor_invocation_local_instructions, output)) =
            with_scalar_aggregate_terminal_attribution(|| {
                measure_query_stage(|| {
                    self.with_metrics(|| {
                        self.load_executor::<E>()
                            .execute_scalar_terminal_request(plan, request)
                    })
                    .map_err(QueryError::execute)
                })
            });
        let output = output?;
        let store_counters = store_counters_before.delta_since();
        let execute_local_instructions = executor_invocation_local_instructions;
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            output,
            FluentTerminalExecutionAttribution {
                compile_local_instructions,
                compile_schema_catalog_local_instructions: compile_phase_attribution.schema_catalog,
                compile_schema_info_local_instructions: compile_phase_attribution.schema_info,
                compile_prepare_local_instructions: compile_phase_attribution.prepare,
                compile_cache_key_local_instructions: compile_phase_attribution.cache_key,
                compile_cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                compile_plan_build_local_instructions: compile_phase_attribution.plan_build,
                compile_cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                plan_lookup_local_instructions,
                executor_invocation_local_instructions,
                execute_local_instructions,
                total_local_instructions,
                store_get_calls: store_counters.data_store_get_calls,
                index_store_get_calls: store_counters.index_store_get_calls,
                index_store_entry_reads: store_counters.index_store_entry_reads,
                scalar_aggregate: ScalarAggregateAttribution::from_executor(
                    scalar_aggregate_terminal,
                ),
                shared_query_plan_cache_hits: cache_attribution.hits,
                shared_query_plan_cache_misses: cache_attribution.misses,
            },
        ))
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

    // Execute one fluent count terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_count_rows_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: CountRowsTerminal,
    ) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let request = strategy.into_executor_request();
        let output = self.execute_scalar_terminal_boundary(query, request)?;

        output.into_count().map_err(QueryError::execute)
    }

    // Execute one fluent count terminal while reporting terminal-specific
    // diagnostics from the shared scalar aggregate execution path.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_fluent_count_rows_terminal_with_attribution<E>(
        &self,
        query: &Query<E>,
        strategy: CountRowsTerminal,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let request = strategy.into_executor_request();
        let (output, attribution) =
            self.execute_scalar_terminal_boundary_with_attribution(query, request)?;
        let count = output.into_count().map_err(QueryError::execute)?;

        Ok((count, attribution))
    }

    // Execute one fluent exists terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_exists_rows_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: ExistsRowsTerminal,
    ) -> Result<bool, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let request = strategy.into_executor_request();
        let output = self.execute_scalar_terminal_boundary(query, request)?;

        output.into_exists().map_err(QueryError::execute)
    }

    // Execute one fluent exists terminal while reporting terminal-specific
    // diagnostics from the shared scalar aggregate execution path.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_fluent_exists_rows_terminal_with_attribution<E>(
        &self,
        query: &Query<E>,
        strategy: ExistsRowsTerminal,
    ) -> Result<(bool, FluentTerminalExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let request = strategy.into_executor_request();
        let (output, attribution) =
            self.execute_scalar_terminal_boundary_with_attribution(query, request)?;
        let exists = output.into_exists().map_err(QueryError::execute)?;

        Ok((exists, attribution))
    }

    // Execute one scalar id terminal request and decode storage keys into typed ids.
    fn execute_scalar_id_terminal_boundary<E>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_terminal_boundary(query, request)?
            .into_id::<E>()
            .map_err(QueryError::execute)
    }

    // Execute one fluent `min()` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_min_id_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: MinIdTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `max()` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_max_id_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: MaxIdTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `min_by(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_min_id_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: MinIdBySlotTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `max_by(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_max_id_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: MaxIdBySlotTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one paired scalar id terminal request and decode storage keys into typed ids.
    fn execute_scalar_id_pair_terminal_boundary<E>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<FluentIdPair<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_terminal_boundary(query, request)?
            .into_id_pair::<E>()
            .map_err(QueryError::execute)
    }

    // Execute one fluent `first()` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_first_id_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: FirstIdTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `last()` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_last_id_terminal<E>(
        &self,
        query: &Query<E>,
        strategy: LastIdTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `nth_by(field, nth)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_nth_id_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: NthIdBySlotTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `median_by(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_median_id_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: MedianIdBySlotTerminal,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one fluent `min_max_by(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_min_max_id_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: MinMaxIdBySlotTerminal,
    ) -> Result<FluentIdPair<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_scalar_id_pair_terminal_boundary(query, strategy.into_executor_request())
    }

    // Execute one numeric-field terminal boundary and keep SUM/AVG executor
    // details out of the fluent query module.
    fn execute_numeric_field_boundary<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.execute_numeric_field_boundary(plan, target_field, request)
        })
    }

    // Execute one fluent `sum(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_sum_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: SumBySlotTerminal,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();

        self.execute_numeric_field_boundary(query, target_field, request)
    }

    // Execute one fluent `sum(distinct field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_sum_distinct_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: SumDistinctBySlotTerminal,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();

        self.execute_numeric_field_boundary(query, target_field, request)
    }

    // Execute one fluent `avg(field)` terminal through its concrete session boundary.
    pub(in crate::db) fn execute_fluent_avg_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: AvgBySlotTerminal,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();

        self.execute_numeric_field_boundary(query, target_field, request)
    }

    // Execute one fluent `avg(distinct field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_avg_distinct_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: AvgDistinctBySlotTerminal,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();

        self.execute_numeric_field_boundary(query, target_field, request)
    }

    // Execute one fluent `values_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_values_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: ValuesBySlotTerminal,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output.into_values().map_err(QueryError::execute)
    }

    // Execute one fluent `project_values(projection)` terminal with the bounded
    // projection expression carried into the executor projection boundary.
    pub(in crate::db) fn execute_fluent_project_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        projection: Expr,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let output = self.execute_scalar_projection_boundary(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedValues { projection },
        )?;

        output.into_values().map_err(QueryError::execute)
    }

    // Execute one fluent `distinct_values_by(field)` terminal through its
    // concrete session boundary.
    pub(in crate::db) fn execute_fluent_distinct_values_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: DistinctValuesBySlotTerminal,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output.into_values().map_err(QueryError::execute)
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
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output.into_count().map_err(QueryError::execute)
    }

    // Execute one fluent `values_by_with_ids(field)` terminal through its
    // concrete session boundary.
    pub(in crate::db) fn execute_fluent_values_by_with_ids_slot<E>(
        &self,
        query: &Query<E>,
        strategy: ValuesBySlotWithIdsTerminal,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output
            .into_values_with_ids::<E>()
            .map_err(QueryError::execute)
    }

    // Execute one fluent `project_values_with_ids(projection)` terminal with
    // projection fused into executor-side value extraction.
    pub(in crate::db) fn execute_fluent_project_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        projection: Expr,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let output = self.execute_scalar_projection_boundary(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedValuesWithIds { projection },
        )?;

        output
            .into_values_with_ids::<E>()
            .map_err(QueryError::execute)
    }

    // Execute one fluent `first_value_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_first_value_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: FirstValueBySlotTerminal,
    ) -> Result<Option<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output.into_terminal_value().map_err(QueryError::execute)
    }

    // Execute one fluent `last_value_by(field)` terminal through its concrete
    // session boundary.
    pub(in crate::db) fn execute_fluent_last_value_by_slot<E>(
        &self,
        query: &Query<E>,
        strategy: LastValueBySlotTerminal,
    ) -> Result<Option<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (target_field, request) = strategy.into_executor_request();
        let output = self.execute_scalar_projection_boundary(query, target_field, request)?;

        output.into_terminal_value().map_err(QueryError::execute)
    }

    // Execute one fluent first/last projected-value terminal with projection
    // applied after selected-row resolution but before the value leaves the
    // executor boundary.
    pub(in crate::db) fn execute_fluent_project_terminal_value_by_slot<E>(
        &self,
        query: &Query<E>,
        target_field: FieldSlot,
        terminal_kind: crate::db::query::plan::AggregateKind,
        projection: Expr,
    ) -> Result<Option<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let output = self.execute_scalar_projection_boundary(
            query,
            target_field,
            ScalarProjectionBoundaryRequest::ProjectedTerminalValue {
                terminal_kind,
                projection,
            },
        )?;

        output.into_terminal_value().map_err(QueryError::execute)
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

    // Execute one row-returning fluent `top_k_by(field, k)` terminal at the
    // session boundary.
    pub(in crate::db) fn execute_fluent_top_k_rows_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.top_k_by_slot(plan, target_slot, take_count)
        })
    }

    // Execute one row-returning fluent `bottom_k_by(field, k)` terminal at the
    // session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_rows_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bottom_k_by_slot(plan, target_slot, take_count)
        })
    }

    // Execute one value-returning fluent `top_k_by_values(field, k)` terminal
    // at the session boundary.
    pub(in crate::db) fn execute_fluent_top_k_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.top_k_by_values_slot(plan, target_slot, take_count)
        })
    }

    // Execute one value-returning fluent `bottom_k_by_values(field, k)` terminal
    // at the session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bottom_k_by_values_slot(plan, target_slot, take_count)
        })
    }

    // Execute one id/value-returning fluent `top_k_by_with_ids(field, k)`
    // terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_top_k_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.top_k_by_with_ids_slot(plan, target_slot, take_count)
        })
    }

    // Execute one id/value-returning fluent `bottom_k_by_with_ids(field, k)`
    // terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bottom_k_by_with_ids_slot(plan, target_slot, take_count)
        })
    }
}
