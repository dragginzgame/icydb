//! Module: query::fluent::load::terminals::driver
//! Responsibility: terminal descriptor to session driver wiring.
//! Does not own: fluent terminal public API or output shaping.
//! Boundary: maps one descriptor type to one session execution/explain method.

use crate::{
    db::{
        DbSession, PersistedRow, Query,
        query::{
            builder::{
                AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal,
                CountRowsTerminal, DistinctValuesBySlotTerminal, ExistsRowsTerminal,
                FirstIdTerminal, FirstValueBySlotTerminal, LastIdTerminal, LastValueBySlotTerminal,
                MaxIdBySlotTerminal, MaxIdTerminal, MedianIdBySlotTerminal, MinIdBySlotTerminal,
                MinIdTerminal, MinMaxIdBySlotTerminal, NthIdBySlotTerminal, SumBySlotTerminal,
                SumDistinctBySlotTerminal, ValuesBySlotTerminal, ValuesBySlotWithIdsTerminal,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            intent::QueryError,
        },
        session::{AcceptedIdValuesOutput, AcceptedOptionalValueOutput, AcceptedValuesOutput},
    },
    traits::EntityValue,
    types::{Decimal, Id},
};

pub(super) type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

///
/// TerminalStrategyDriver
///
/// TerminalStrategyDriver is the fluent terminal wiring adapter between a
/// query-owned strategy object and the session-owned execution/explain
/// boundary. Implementations are deliberately thin: they only choose the
/// matching `DbSession` method for an existing strategy type.
///

pub(super) trait TerminalStrategyDriver<E: PersistedRow + EntityValue> {
    type Output;
    type ExplainOutput;

    fn execute(
        self,
        session: &DbSession<E::Canister>,
        query: &Query<E>,
    ) -> Result<Self::Output, QueryError>;

    fn explain(
        &self,
        session: &DbSession<E::Canister>,
        query: &Query<E>,
    ) -> Result<Self::ExplainOutput, QueryError>;
}

// Define one aggregate-style terminal driver implementation. The macro keeps
// terminal descriptors 1:1 with session methods while removing repeated explain
// wiring that is identical for every aggregate terminal.
macro_rules! impl_aggregate_terminal_driver {
    ($terminal:ty, $output:ty, $execute:ident) => {
        impl<E> TerminalStrategyDriver<E> for $terminal
        where
            E: PersistedRow + EntityValue,
        {
            type Output = $output;
            type ExplainOutput = ExplainAggregateTerminalPlan;

            fn execute(
                self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::Output, QueryError> {
                session.$execute(query, self)
            }

            fn explain(
                &self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::ExplainOutput, QueryError> {
                session.explain_query_prepared_aggregate_terminal_with_visible_indexes(query, self)
            }
        }
    };
}

// Define one projection-style terminal driver implementation. Projection
// terminals share the execution/explain shape but return execution descriptors
// rather than aggregate-terminal plans.
macro_rules! impl_projection_terminal_driver {
    ($terminal:ty, $output:ty, $execute:ident) => {
        impl<E> TerminalStrategyDriver<E> for $terminal
        where
            E: PersistedRow + EntityValue,
        {
            type Output = $output;
            type ExplainOutput = ExplainExecutionNodeDescriptor;

            fn execute(
                self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::Output, QueryError> {
                session.$execute(query, self)
            }

            fn explain(
                &self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::ExplainOutput, QueryError> {
                session.explain_query_prepared_projection_terminal_with_visible_indexes(query, self)
            }
        }
    };
}

impl_aggregate_terminal_driver!(CountRowsTerminal, u32, execute_fluent_count_rows_terminal);
impl_aggregate_terminal_driver!(
    ExistsRowsTerminal,
    bool,
    execute_fluent_exists_rows_terminal
);
impl_aggregate_terminal_driver!(MinIdTerminal, Option<Id<E>>, execute_fluent_min_id_terminal);
impl_aggregate_terminal_driver!(MaxIdTerminal, Option<Id<E>>, execute_fluent_max_id_terminal);
impl_aggregate_terminal_driver!(
    MinIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_min_id_by_slot
);
impl_aggregate_terminal_driver!(
    MaxIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_max_id_by_slot
);
impl_aggregate_terminal_driver!(
    SumBySlotTerminal,
    Option<Decimal>,
    execute_fluent_sum_by_slot
);
impl_aggregate_terminal_driver!(
    SumDistinctBySlotTerminal,
    Option<Decimal>,
    execute_fluent_sum_distinct_by_slot
);
impl_aggregate_terminal_driver!(
    AvgBySlotTerminal,
    Option<Decimal>,
    execute_fluent_avg_by_slot
);
impl_aggregate_terminal_driver!(
    AvgDistinctBySlotTerminal,
    Option<Decimal>,
    execute_fluent_avg_distinct_by_slot
);
impl_aggregate_terminal_driver!(
    FirstIdTerminal,
    Option<Id<E>>,
    execute_fluent_first_id_terminal
);
impl_aggregate_terminal_driver!(
    LastIdTerminal,
    Option<Id<E>>,
    execute_fluent_last_id_terminal
);
impl_aggregate_terminal_driver!(
    NthIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_nth_id_by_slot
);
impl_aggregate_terminal_driver!(
    MedianIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_median_id_by_slot
);
impl_aggregate_terminal_driver!(
    MinMaxIdBySlotTerminal,
    MinMaxByIds<E>,
    execute_fluent_min_max_id_by_slot
);

impl_projection_terminal_driver!(
    ValuesBySlotTerminal,
    AcceptedValuesOutput,
    execute_fluent_values_by_slot
);
impl_projection_terminal_driver!(
    DistinctValuesBySlotTerminal,
    AcceptedValuesOutput,
    execute_fluent_distinct_values_by_slot
);
impl_projection_terminal_driver!(
    CountDistinctBySlotTerminal,
    u32,
    execute_fluent_count_distinct_by_slot
);
impl_projection_terminal_driver!(
    ValuesBySlotWithIdsTerminal,
    AcceptedIdValuesOutput<E>,
    execute_fluent_values_by_with_ids_slot
);
impl_projection_terminal_driver!(
    FirstValueBySlotTerminal,
    AcceptedOptionalValueOutput,
    execute_fluent_first_value_by_slot
);
impl_projection_terminal_driver!(
    LastValueBySlotTerminal,
    AcceptedOptionalValueOutput,
    execute_fluent_last_value_by_slot
);
