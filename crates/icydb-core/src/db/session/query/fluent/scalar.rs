//! Module: db::session::query::fluent::scalar
//! Responsibility: session adapters for scalar fluent terminals.
//! Does not own: fluent terminal construction, executor aggregate strategy, or read admission.
//! Boundary: maps scalar fluent strategies into scalar executor boundary requests.

#[cfg(feature = "diagnostics")]
use super::super::QueryAttributionCommon;
#[cfg(feature = "diagnostics")]
use crate::db::{
    FluentTerminalExecutionAttribution, ScalarAggregateAttribution,
    diagnostics::{StoreCounterSnapshot, measure_local_instruction_delta as measure_query_stage},
    executor::with_scalar_aggregate_terminal_attribution,
};
use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{
            ScalarNumericFieldBoundaryRequest, ScalarTerminalBoundaryOutput,
            ScalarTerminalBoundaryRequest,
        },
        query::{
            builder::{
                AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountRowsTerminal,
                ExistsRowsTerminal, FirstIdTerminal, LastIdTerminal, MaxIdBySlotTerminal,
                MaxIdTerminal, MedianIdBySlotTerminal, MinIdBySlotTerminal, MinIdTerminal,
                MinMaxIdBySlotTerminal, NthIdBySlotTerminal, SumBySlotTerminal,
                SumDistinctBySlotTerminal,
            },
            plan::FieldSlot,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
    types::{Decimal, Id},
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

    // Execute and decode one scalar terminal boundary so fluent terminal
    // methods do not each repeat executor-error adaptation.
    fn execute_scalar_terminal_value<E, T>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
        decode: impl FnOnce(ScalarTerminalBoundaryOutput) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        decode(self.execute_scalar_terminal_boundary(query, request)?).map_err(QueryError::execute)
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
        self.ensure_prepared_query_plan_is_current(&plan)?;

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
        let common_attribution = QueryAttributionCommon::new(
            plan_lookup_local_instructions,
            compile_phase_attribution,
            cache_attribution,
            store_counters,
        );

        Ok((
            output,
            FluentTerminalExecutionAttribution::from_common(
                common_attribution,
                executor_invocation_local_instructions,
                ScalarAggregateAttribution::from_executor(scalar_aggregate_terminal),
            ),
        ))
    }

    // Execute and decode one attributed scalar terminal boundary while keeping
    // fluent attribution wrapping aligned with the non-attributed terminal path.
    #[cfg(feature = "diagnostics")]
    fn execute_scalar_terminal_value_with_attribution<E, T>(
        &self,
        query: &Query<E>,
        request: ScalarTerminalBoundaryRequest,
        decode: impl FnOnce(ScalarTerminalBoundaryOutput) -> Result<T, InternalError>,
    ) -> Result<(T, FluentTerminalExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (output, attribution) =
            self.execute_scalar_terminal_boundary_with_attribution(query, request)?;
        let value = decode(output).map_err(QueryError::execute)?;

        Ok((value, attribution))
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
        self.execute_scalar_terminal_value(query, request, ScalarTerminalBoundaryOutput::into_count)
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
        self.execute_scalar_terminal_value_with_attribution(
            query,
            request,
            ScalarTerminalBoundaryOutput::into_count,
        )
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
        self.execute_scalar_terminal_value(
            query,
            request,
            ScalarTerminalBoundaryOutput::into_exists,
        )
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
        self.execute_scalar_terminal_value_with_attribution(
            query,
            request,
            ScalarTerminalBoundaryOutput::into_exists,
        )
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
        self.execute_scalar_terminal_value(
            query,
            request,
            ScalarTerminalBoundaryOutput::into_id::<E>,
        )
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
        self.execute_scalar_terminal_value(
            query,
            request,
            ScalarTerminalBoundaryOutput::into_id_pair::<E>,
        )
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
}
