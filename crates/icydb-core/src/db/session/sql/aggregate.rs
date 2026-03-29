use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        executor::{ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest},
        query::plan::{AggregateKind, FieldSlot},
        session::sql::explain::resolve_sql_aggregate_target_slot,
        sql::lowering::{SqlGlobalAggregateTerminal, compile_sql_global_aggregate_command},
    },
    traits::{CanisterKind, EntityValue},
    types::Id,
    value::Value,
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one reduced SQL global aggregate `SELECT` statement.
    ///
    /// This entrypoint is intentionally constrained to one aggregate terminal
    /// shape per statement and preserves existing terminal semantics.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // First lower the SQL surface onto the existing single-terminal
        // aggregate command authority so execution never has to rediscover the
        // accepted aggregate shape family.
        let command = compile_sql_global_aggregate_command::<E>(sql, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        // Then dispatch each accepted terminal onto the existing load/query
        // boundaries instead of reopening aggregate execution ownership here.
        match command.terminal() {
            SqlGlobalAggregateTerminal::CountRows => self
                .execute_load_query_with(command.query(), |load, plan| {
                    load.execute_scalar_terminal_request(
                        plan,
                        crate::db::executor::ScalarTerminalBoundaryRequest::Count,
                    )?
                    .into_count()
                })
                .map(|count| Value::Uint(u64::from(count))),
            SqlGlobalAggregateTerminal::CountField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;

                self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_scalar_projection_boundary(
                        plan,
                        target_slot,
                        ScalarProjectionBoundaryRequest::Values,
                    )?
                    .into_values()
                })
                .map(|values| {
                    let count = values
                        .into_iter()
                        .filter(|value| !matches!(value, Value::Null))
                        .count();

                    Value::Uint(u64::try_from(count).unwrap_or(u64::MAX))
                })
            }
            SqlGlobalAggregateTerminal::SumField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;

                self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_numeric_field_boundary(
                        plan,
                        target_slot,
                        ScalarNumericFieldBoundaryRequest::Sum,
                    )
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::AvgField(field) => {
                let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;

                self.execute_load_query_with(command.query(), |load, plan| {
                    load.execute_numeric_field_boundary(
                        plan,
                        target_slot,
                        ScalarNumericFieldBoundaryRequest::Avg,
                    )
                })
                .map(|value| value.map_or(Value::Null, Value::Decimal))
            }
            SqlGlobalAggregateTerminal::MinField(field) => self
                .execute_ranked_sql_aggregate_field::<E>(
                    command.query(),
                    field,
                    AggregateKind::Min,
                ),
            SqlGlobalAggregateTerminal::MaxField(field) => self
                .execute_ranked_sql_aggregate_field::<E>(
                    command.query(),
                    field,
                    AggregateKind::Max,
                ),
        }
    }

    // Execute one ranked field aggregate by resolving the winning id first and
    // then reading the projected field through the typed load surface.
    fn execute_ranked_sql_aggregate_field<E>(
        &self,
        query: &Query<E>,
        field: &str,
        kind: AggregateKind,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let target_slot = resolve_sql_aggregate_target_slot::<E>(field)?;
        let matched_id = self.execute_ranked_sql_aggregate_id(query, target_slot, kind)?;

        match matched_id {
            Some(id) => self
                .load::<E>()
                .by_id(id)
                .first_value_by(field)
                .map(|value| value.unwrap_or(Value::Null)),
            None => Ok(Value::Null),
        }
    }

    // Resolve the id selected by one ranked aggregate terminal through the
    // shared scalar terminal boundary before any field-value load occurs.
    fn execute_ranked_sql_aggregate_id<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        kind: AggregateKind,
    ) -> Result<Option<Id<E>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if !matches!(kind, AggregateKind::Min | AggregateKind::Max) {
            return Err(QueryError::invariant(
                "ranked SQL aggregate id helper only supports MIN/MAX",
            ));
        }

        self.execute_load_query_with(query, |load, plan| {
            load.execute_scalar_terminal_request(
                plan,
                crate::db::executor::ScalarTerminalBoundaryRequest::IdBySlot {
                    kind,
                    target_field: target_slot,
                },
            )?
            .into_id()
        })
    }
}
