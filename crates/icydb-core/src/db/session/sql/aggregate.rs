//! Module: db::session::sql::aggregate
//! Responsibility: module-local ownership and contracts for db::session::sql::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        executor::{ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest},
        query::plan::{AggregateKind, FieldSlot},
        session::sql::explain::resolve_sql_aggregate_target_slot,
        session::sql::{SqlParsedStatement, SqlStatementRoute},
        sql::lowering::{
            SqlGlobalAggregateTerminal, compile_sql_global_aggregate_command,
            is_sql_global_aggregate_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::{CanisterKind, EntityValue},
    types::Id,
    value::Value,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlAggregateSurface {
    QueryFrom,
    ExecuteSql,
    ExecuteSqlGrouped,
    ExecuteSqlDispatch,
    GeneratedQuerySurface,
}

pub(in crate::db::session::sql) fn parsed_requires_dedicated_sql_aggregate_lane(
    parsed: &SqlParsedStatement,
) -> bool {
    is_sql_global_aggregate_statement(&parsed.statement)
}

pub(in crate::db::session::sql) const fn unsupported_sql_aggregate_lane_message(
    surface: SqlAggregateSurface,
) -> &'static str {
    match surface {
        SqlAggregateSurface::QueryFrom => {
            "query_from_sql rejects global aggregate SELECT; use execute_sql_aggregate(...)"
        }
        SqlAggregateSurface::ExecuteSql => {
            "execute_sql rejects global aggregate SELECT; use execute_sql_aggregate(...)"
        }
        SqlAggregateSurface::ExecuteSqlGrouped => {
            "execute_sql_grouped rejects global aggregate SELECT; use execute_sql_aggregate(...)"
        }
        SqlAggregateSurface::ExecuteSqlDispatch => {
            "execute_sql_dispatch rejects global aggregate SELECT; use execute_sql_aggregate(...)"
        }
        SqlAggregateSurface::GeneratedQuerySurface => {
            "generated SQL query surface rejects global aggregate SELECT; use execute_sql_aggregate(...)"
        }
    }
}

const fn unsupported_sql_aggregate_surface_lane_message(route: &SqlStatementRoute) -> &'static str {
    match route {
        SqlStatementRoute::Query { .. } => {
            "execute_sql_aggregate requires constrained global aggregate SELECT"
        }
        SqlStatementRoute::Explain { .. } => {
            "execute_sql_aggregate rejects EXPLAIN; use execute_sql_dispatch"
        }
        SqlStatementRoute::Describe { .. } => {
            "execute_sql_aggregate rejects DESCRIBE; use execute_sql_dispatch"
        }
        SqlStatementRoute::ShowIndexes { .. } => {
            "execute_sql_aggregate rejects SHOW INDEXES; use execute_sql_dispatch"
        }
        SqlStatementRoute::ShowColumns { .. } => {
            "execute_sql_aggregate rejects SHOW COLUMNS; use execute_sql_dispatch"
        }
        SqlStatementRoute::ShowEntities => {
            "execute_sql_aggregate rejects SHOW ENTITIES; use execute_sql_dispatch"
        }
    }
}

const fn unsupported_sql_aggregate_grouped_message() -> &'static str {
    "execute_sql_aggregate rejects grouped SELECT; use execute_sql_grouped(...)"
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one reduced SQL global aggregate `SELECT` statement.
    ///
    /// This entrypoint is intentionally constrained to one aggregate terminal
    /// shape per statement and preserves existing terminal semantics.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // First keep wrong-lane traffic on an explicit aggregate-surface
        // contract instead of relying on generic lowering failures.
        let parsed = self.parse_sql_statement(sql)?;
        match &parsed.statement {
            SqlStatement::Select(_) if is_sql_global_aggregate_statement(&parsed.statement) => {}
            SqlStatement::Select(statement) if !statement.group_by.is_empty() => {
                return Err(QueryError::unsupported_query(
                    unsupported_sql_aggregate_grouped_message(),
                ));
            }
            SqlStatement::Delete(_) => {
                return Err(QueryError::unsupported_query(
                    "execute_sql_aggregate rejects DELETE; use execute_sql_dispatch",
                ));
            }
            _ => {
                return Err(QueryError::unsupported_query(
                    unsupported_sql_aggregate_surface_lane_message(parsed.route()),
                ));
            }
        }

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
        if !kind.is_extrema() {
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
