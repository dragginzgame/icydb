//! Module: db::session::sql::aggregate
//! Responsibility: session-owned execution and shaping helpers for lowered SQL
//! scalar aggregate commands.
//! Does not own: aggregate lowering or aggregate executor route selection.
//! Boundary: binds lowered SQL aggregate commands onto authority-aware planning and result shaping.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        session::sql::{SqlParsedStatement, SqlStatementResult},
        sql::lowering::{
            PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommandCore, compile_sql_global_aggregate_command_core_from_prepared,
            is_sql_global_aggregate_statement,
        },
    },
    traits::CanisterKind,
    value::Value,
};

#[cfg(test)]
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

#[cfg(test)]
pub(in crate::db::session::sql) const fn unsupported_sql_aggregate_lane_message(
    surface: SqlAggregateSurface,
) -> &'static str {
    match surface {
        SqlAggregateSurface::QueryFrom => "structural SQL lowering rejects global aggregate SELECT",
        SqlAggregateSurface::ExecuteSql => "scalar SQL execution rejects global aggregate SELECT",
        SqlAggregateSurface::ExecuteSqlGrouped => {
            "grouped SQL execution rejects global aggregate SELECT"
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Build the canonical SQL aggregate label projected by the prepared
    // aggregate strategy so unified statement rows stay parser-stable.
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
    // value semantics for the unified SQL statement/query surface.
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
                            "numeric SQL aggregate statement encountered non-numeric projected value",
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
                                            "extrema SQL aggregate statement encountered incomparable projected values",
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
    // statement payload for unified SQL loops.
    pub(in crate::db::session::sql) fn execute_sql_aggregate_statement_for_authority(
        &self,
        command: SqlGlobalAggregateCommandCore,
        authority: crate::db::executor::EntityAuthority,
        label_override: Option<String>,
    ) -> Result<SqlStatementResult, QueryError> {
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

        Ok(SqlStatementResult::Projection {
            columns: vec![label],
            rows: vec![vec![value]],
            row_count: 1,
        })
    }

    // Compile one already-parsed SQL aggregate statement into the shared
    // generic-free aggregate command used by unified statement/query surfaces.
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
}
