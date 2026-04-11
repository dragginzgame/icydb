//! Module: db::session::sql::aggregate
//! Responsibility: session-owned execution and shaping helpers for lowered SQL
//! scalar aggregate commands.
//! Does not own: aggregate lowering or aggregate executor route selection.
//! Boundary: binds lowered SQL aggregate commands onto authority-aware planning and result shaping.

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        executor::{ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest},
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        session::sql::surface::sql_statement_route_from_statement,
        session::sql::{SqlDispatchResult, SqlParsedStatement, SqlStatementRoute},
        sql::lowering::{
            PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommand, SqlGlobalAggregateCommandCore,
            compile_sql_global_aggregate_command_core_from_prepared,
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
    }
}

const fn unsupported_sql_aggregate_surface_lane_message(route: &SqlStatementRoute) -> &'static str {
    match route {
        SqlStatementRoute::Query { .. } => {
            "execute_sql_aggregate requires constrained global aggregate SELECT"
        }
        SqlStatementRoute::Insert { .. } => {
            "execute_sql_aggregate rejects INSERT; use execute_sql_dispatch"
        }
        SqlStatementRoute::Update { .. } => {
            "execute_sql_aggregate rejects UPDATE; use execute_sql_dispatch"
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
    // Build the canonical SQL aggregate label projected by the prepared
    // aggregate strategy so unified dispatch rows stay parser-stable.
    pub(in crate::db::session::sql) fn sql_scalar_aggregate_label(
        strategy: &PreparedSqlScalarAggregateStrategy,
    ) -> String {
        let kind = match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows
            | PreparedSqlScalarAggregateRuntimeDescriptor::CountField => "COUNT",
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Sum,
            } => "SUM",
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Avg,
            } => "AVG",
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: crate::db::query::plan::AggregateKind::Min,
            } => "MIN",
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: crate::db::query::plan::AggregateKind::Max,
            } => "MAX",
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                unreachable!("prepared SQL scalar aggregate strategy drifted outside SQL support")
            }
        };

        match strategy.projected_field() {
            Some(field) if strategy.is_distinct() => format!("{kind}(DISTINCT {field})"),
            Some(field) => format!("{kind}({field})"),
            None => format!("{kind}(*)"),
        }
    }

    // Deduplicate one projected aggregate input stream while preserving the
    // first-observed value order used by SQL aggregate reduction.
    fn dedup_structural_sql_aggregate_input_values(values: Vec<Value>) -> Vec<Value> {
        let mut deduped = Vec::with_capacity(values.len());

        for value in values {
            if deduped.iter().any(|current| current == &value) {
                continue;
            }
            deduped.push(value);
        }

        deduped
    }

    // Reduce one structural aggregate field projection into canonical aggregate
    // value semantics for the unified SQL dispatch/query surface.
    fn reduce_structural_sql_aggregate_field_values(
        values: Vec<Value>,
        strategy: &PreparedSqlScalarAggregateStrategy,
    ) -> Result<Value, QueryError> {
        let values = if strategy.is_distinct() {
            Self::dedup_structural_sql_aggregate_input_values(values)
        } else {
            values
        };

        match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => Err(QueryError::invariant(
                "COUNT(*) structural reduction does not consume projected field values",
            )),
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField => {
                let count = values
                    .into_iter()
                    .filter(|value| !matches!(value, Value::Null))
                    .count();

                Ok(Value::Uint(u64::try_from(count).unwrap_or(u64::MAX)))
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind:
                    crate::db::query::plan::AggregateKind::Sum
                    | crate::db::query::plan::AggregateKind::Avg,
            } => {
                let mut sum = None;
                let mut row_count = 0_u64;

                for value in values {
                    if matches!(value, Value::Null) {
                        continue;
                    }

                    let decimal = coerce_numeric_decimal(&value).ok_or_else(|| {
                        QueryError::invariant(
                            "numeric SQL aggregate dispatch encountered non-numeric projected value",
                        )
                    })?;
                    sum = Some(sum.map_or(decimal, |current| add_decimal_terms(current, decimal)));
                    row_count = row_count.saturating_add(1);
                }

                match strategy.runtime_descriptor() {
                    PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                        kind: crate::db::query::plan::AggregateKind::Sum,
                    } => Ok(sum.map_or(Value::Null, Value::Decimal)),
                    PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                        kind: crate::db::query::plan::AggregateKind::Avg,
                    } => Ok(sum
                        .and_then(|sum| average_decimal_terms(sum, row_count))
                        .map_or(Value::Null, Value::Decimal)),
                    _ => unreachable!("numeric SQL aggregate strategy drifted during reduction"),
                }
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind:
                    crate::db::query::plan::AggregateKind::Min
                    | crate::db::query::plan::AggregateKind::Max,
            } => {
                let mut selected = None::<Value>;

                for value in values {
                    if matches!(value, Value::Null) {
                        continue;
                    }

                    let replace = match selected.as_ref() {
                        None => true,
                        Some(current) => {
                            let ordering =
                                compare_numeric_or_strict_order(&value, current).ok_or_else(
                                    || {
                                        QueryError::invariant(
                                            "extrema SQL aggregate dispatch encountered incomparable projected values",
                                        )
                                    },
                                )?;

                            match strategy.runtime_descriptor() {
                                PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                                    kind: crate::db::query::plan::AggregateKind::Min,
                                } => ordering.is_lt(),
                                PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                                    kind: crate::db::query::plan::AggregateKind::Max,
                                } => ordering.is_gt(),
                                _ => unreachable!(
                                    "extrema SQL aggregate strategy drifted during reduction"
                                ),
                            }
                        }
                    };

                    if replace {
                        selected = Some(value);
                    }
                }

                Ok(selected.unwrap_or(Value::Null))
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                Err(QueryError::invariant(
                    "prepared SQL scalar aggregate strategy drifted outside SQL support",
                ))
            }
        }
    }

    // Project one single-field structural query and return its canonical field
    // values for aggregate reduction.
    fn execute_structural_sql_aggregate_field_projection(
        &self,
        query: crate::db::query::intent::StructuralQuery,
        authority: crate::db::executor::EntityAuthority,
    ) -> Result<Vec<Value>, QueryError> {
        let (_, rows, _) = self
            .execute_structural_sql_projection(query, authority)?
            .into_parts();
        let mut projected = Vec::with_capacity(rows.len());

        for row in rows {
            let [value] = row.as_slice() else {
                return Err(QueryError::invariant(
                    "structural SQL aggregate projection must emit exactly one field",
                ));
            };

            projected.push(value.clone());
        }

        Ok(projected)
    }

    // Execute one generic-free prepared SQL aggregate command through the
    // structural SQL projection path and package the result as one row-shaped
    // dispatch payload for unified SQL loops.
    pub(in crate::db::session::sql) fn execute_sql_aggregate_dispatch_for_authority(
        &self,
        command: SqlGlobalAggregateCommandCore,
        authority: crate::db::executor::EntityAuthority,
        label_override: Option<String>,
    ) -> Result<SqlDispatchResult, QueryError> {
        let model = authority.model();
        let strategy = command
            .prepared_scalar_strategy_with_model(model)
            .map_err(QueryError::from_sql_lowering_error)?;
        let label = label_override.unwrap_or_else(|| Self::sql_scalar_aggregate_label(&strategy));
        let value = match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                let (_, _, row_count) = self
                    .execute_structural_sql_projection(
                        command
                            .query()
                            .clone()
                            .select_fields([authority.primary_key_name()]),
                        authority,
                    )?
                    .into_parts();

                Value::Uint(u64::from(row_count))
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField
            | PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                let Some(field) = strategy.projected_field() else {
                    return Err(QueryError::invariant(
                        "field-target SQL aggregate strategy requires projected field label",
                    ));
                };
                let values = self.execute_structural_sql_aggregate_field_projection(
                    command.query().clone().select_fields([field]),
                    authority,
                )?;

                Self::reduce_structural_sql_aggregate_field_values(values, &strategy)?
            }
        };

        Ok(SqlDispatchResult::Projection {
            columns: vec![label],
            rows: vec![vec![value]],
            row_count: 1,
        })
    }

    // Compile one already-parsed SQL aggregate statement into the shared
    // generic-free aggregate command used by unified dispatch/query surfaces.
    pub(in crate::db::session::sql) fn compile_sql_aggregate_command_core_for_authority(
        parsed: &SqlParsedStatement,
        authority: crate::db::executor::EntityAuthority,
    ) -> Result<SqlGlobalAggregateCommandCore, QueryError> {
        compile_sql_global_aggregate_command_core_from_prepared(
            parsed.prepare(authority.model().name())?,
            authority.model(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)
    }

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
        let prepared = prepare_sql_statement(statement, E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let command = compile_sql_global_aggregate_command_from_prepared::<E>(
            prepared.clone(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let strategy = command.prepared_scalar_strategy();

        // DISTINCT field aggregates reuse the existing structural projection +
        // reduction lane so SQL deduplicates aggregate inputs before folding.
        if strategy.is_distinct() {
            let dispatch = compile_sql_global_aggregate_command_core_from_prepared(
                prepared,
                E::MODEL,
                MissingRowPolicy::Ignore,
            )
            .map_err(QueryError::from_sql_lowering_error)?;
            let authority = crate::db::executor::EntityAuthority::for_type::<E>();
            let SqlDispatchResult::Projection { rows, .. } =
                self.execute_sql_aggregate_dispatch_for_authority(dispatch, authority, None)?
            else {
                return Err(QueryError::invariant(
                    "DISTINCT SQL aggregate dispatch must finalize as one projection row",
                ));
            };
            let Some(mut row) = rows.into_iter().next() else {
                return Err(QueryError::invariant(
                    "DISTINCT SQL aggregate dispatch must emit one projection row",
                ));
            };
            if row.len() != 1 {
                return Err(QueryError::invariant(
                    "DISTINCT SQL aggregate dispatch must emit exactly one projected value",
                ));
            }
            let value = row.pop().ok_or_else(|| {
                QueryError::invariant(
                    "DISTINCT SQL aggregate dispatch must emit exactly one projected value",
                )
            })?;

            return Ok(value);
        }

        // Then dispatch through one prepared typed-scalar aggregate strategy so
        // SQL aggregate execution and SQL aggregate explain consume the same
        // behavioral source instead of matching raw terminal variants twice.
        self.execute_prepared_sql_scalar_aggregate(&command)
    }
}
