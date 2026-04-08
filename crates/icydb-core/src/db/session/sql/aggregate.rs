//! Module: db::session::sql::aggregate
//! Responsibility: module-local ownership and contracts for db::session::sql::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        executor::{ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest},
        session::sql::surface::sql_statement_route_from_statement,
        session::sql::{SqlParsedStatement, SqlStatementRoute},
        sql::lowering::{
            PreparedSqlScalarAggregateRuntimeDescriptor, SqlGlobalAggregateCommand,
            compile_sql_global_aggregate_command_from_prepared, is_sql_global_aggregate_statement,
            prepare_sql_statement,
        },
        sql::parser::{SqlStatement, parse_sql},
    },
    traits::{CanisterKind, EntityValue},
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
    // Require one resolved target slot from a prepared field-target SQL
    // aggregate strategy before dispatching into execution families.
    fn prepared_sql_scalar_target_slot_required(
        strategy: &crate::db::sql::lowering::PreparedSqlScalarAggregateStrategy,
        message: &'static str,
    ) -> Result<crate::db::query::plan::FieldSlot, QueryError> {
        strategy
            .target_slot()
            .cloned()
            .ok_or_else(|| QueryError::invariant(message))
    }

    // Execute prepared COUNT(*) through the shared existing-rows scalar
    // terminal boundary.
    fn execute_prepared_sql_scalar_count_rows<E>(
        &self,
        command: &SqlGlobalAggregateCommand<E>,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_load_query_with(command.query(), |load, plan| {
            load.execute_scalar_terminal_request(
                plan,
                crate::db::executor::ScalarTerminalBoundaryRequest::Count,
            )?
            .into_count()
        })
        .map(|count| Value::Uint(u64::from(count)))
    }

    // Execute prepared COUNT(field) through the shared scalar projection
    // boundary.
    fn execute_prepared_sql_scalar_count_field<E>(
        &self,
        command: &SqlGlobalAggregateCommand<E>,
        strategy: &crate::db::sql::lowering::PreparedSqlScalarAggregateStrategy,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let target_slot = Self::prepared_sql_scalar_target_slot_required(
            strategy,
            "prepared COUNT(field) SQL aggregate strategy requires target slot",
        )?;

        self.execute_load_query_with(command.query(), |load, plan| {
            load.execute_scalar_projection_boundary(
                plan,
                target_slot.clone(),
                ScalarProjectionBoundaryRequest::CountNonNull,
            )?
            .into_count()
        })
        .map(|count| Value::Uint(u64::from(count)))
    }

    // Execute prepared SUM/AVG(field) through the shared numeric field
    // boundary.
    fn execute_prepared_sql_scalar_numeric_field<E>(
        &self,
        command: &SqlGlobalAggregateCommand<E>,
        strategy: &crate::db::sql::lowering::PreparedSqlScalarAggregateStrategy,
        request: ScalarNumericFieldBoundaryRequest,
        message: &'static str,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let target_slot = Self::prepared_sql_scalar_target_slot_required(strategy, message)?;

        self.execute_load_query_with(command.query(), |load, plan| {
            load.execute_numeric_field_boundary(plan, target_slot.clone(), request)
        })
        .map(|value| value.map_or(Value::Null, Value::Decimal))
    }

    // Execute prepared MIN/MAX(field) through the shared extrema-value
    // boundary.
    fn execute_prepared_sql_scalar_extrema_field<E>(
        &self,
        command: &SqlGlobalAggregateCommand<E>,
        strategy: &crate::db::sql::lowering::PreparedSqlScalarAggregateStrategy,
        kind: crate::db::query::plan::AggregateKind,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let target_slot = Self::prepared_sql_scalar_target_slot_required(
            strategy,
            "prepared extrema SQL aggregate strategy requires target slot",
        )?;

        self.execute_load_query_with(command.query(), |load, plan| {
            load.execute_scalar_extrema_value_boundary(plan, target_slot.clone(), kind)
        })
        .map(|value| value.unwrap_or(Value::Null))
    }

    // Execute one prepared typed SQL scalar aggregate strategy through the
    // existing aggregate boundary families without rediscovering behavior from
    // raw SQL terminal variants at the session layer.
    fn execute_prepared_sql_scalar_aggregate<E>(
        &self,
        command: &SqlGlobalAggregateCommand<E>,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let strategy = command.prepared_scalar_strategy();

        match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                self.execute_prepared_sql_scalar_count_rows(command)
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField => {
                self.execute_prepared_sql_scalar_count_field(command, &strategy)
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Sum,
            } => self.execute_prepared_sql_scalar_numeric_field(
                command,
                &strategy,
                ScalarNumericFieldBoundaryRequest::Sum,
                "prepared SUM(field) SQL aggregate strategy requires target slot",
            ),
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Avg,
            } => self.execute_prepared_sql_scalar_numeric_field(
                command,
                &strategy,
                ScalarNumericFieldBoundaryRequest::Avg,
                "prepared AVG(field) SQL aggregate strategy requires target slot",
            ),
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { kind } => {
                self.execute_prepared_sql_scalar_extrema_field(command, &strategy, kind)
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. } => {
                Err(QueryError::invariant(
                    "prepared SQL scalar aggregate numeric runtime descriptor drift",
                ))
            }
        }
    }

    /// Execute one reduced SQL global aggregate `SELECT` statement.
    ///
    /// This entrypoint is intentionally constrained to one aggregate terminal
    /// shape per statement and preserves existing terminal semantics.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Parse once into one owned statement so the aggregate lane can keep
        // its surface checks and lowering on the same statement instance.
        let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

        // First keep wrong-lane traffic on an explicit aggregate-surface
        // contract instead of relying on generic lowering failures.
        match &statement {
            SqlStatement::Select(_) if is_sql_global_aggregate_statement(&statement) => {}
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
                let route = sql_statement_route_from_statement(&statement);

                return Err(QueryError::unsupported_query(
                    unsupported_sql_aggregate_surface_lane_message(&route),
                ));
            }
        }

        // First lower the SQL surface onto the existing single-terminal
        // aggregate command authority so execution never has to rediscover the
        // accepted aggregate shape family.
        let command = compile_sql_global_aggregate_command_from_prepared::<E>(
            prepare_sql_statement(statement, E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        // Then dispatch through one prepared typed-scalar aggregate strategy so
        // SQL aggregate execution and SQL aggregate explain consume the same
        // behavioral source instead of matching raw terminal variants twice.
        self.execute_prepared_sql_scalar_aggregate(&command)
    }
}
