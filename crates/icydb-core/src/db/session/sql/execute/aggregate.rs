use crate::{
    db::{
        DbSession, QueryError,
        executor::EntityAuthority,
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::intent::StructuralQuery,
        session::sql::SqlStatementResult,
        sql::lowering::{
            PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommandCore, is_sql_global_aggregate_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::CanisterKind,
    value::Value,
};

fn parsed_requires_dedicated_sql_aggregate_lane(statement: &SqlStatement) -> bool {
    is_sql_global_aggregate_statement(statement)
}

// Keep the dedicated SQL aggregate lane on parser-owned outward labels
// without reopening alias semantics in lowering or runtime strategy state.
fn sql_aggregate_statement_label_overrides(statement: &SqlStatement) -> Vec<Option<String>> {
    let SqlStatement::Select(select) = statement else {
        return Vec::new();
    };

    (0..select.projection_aliases.len())
        .map(|index| select.projection_alias(index).map(str::to_string))
        .collect()
}

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

fn reduce_structural_sql_aggregate_field_values(
    values: Vec<Value>,
    strategy: &PreparedSqlScalarAggregateStrategy,
) -> Result<Value, QueryError> {
    let values = if strategy.is_distinct() {
        dedup_structural_sql_aggregate_input_values(values)
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
                crate::db::query::plan::AggregateKind::Sum | crate::db::query::plan::AggregateKind::Avg,
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
                crate::db::query::plan::AggregateKind::Min | crate::db::query::plan::AggregateKind::Max,
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
                            compare_numeric_or_strict_order(&value, current).ok_or_else(|| {
                                QueryError::invariant(
                                    "extrema SQL aggregate statement encountered incomparable projected values",
                                )
                            })?;

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

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql::execute) fn sql_query_requires_aggregate_lane(
        statement: &SqlStatement,
    ) -> bool {
        parsed_requires_dedicated_sql_aggregate_lane(statement)
    }

    pub(in crate::db::session::sql::execute) fn sql_query_aggregate_label_overrides(
        statement: &SqlStatement,
    ) -> Vec<Option<String>> {
        sql_aggregate_statement_label_overrides(statement)
    }

    // Build the canonical SQL aggregate label projected by the prepared
    // aggregate strategy so unified statement rows stay parser-stable.
    fn sql_scalar_aggregate_label(strategy: &PreparedSqlScalarAggregateStrategy) -> String {
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

    // Project one single-field structural query and return its canonical field
    // values for aggregate reduction.
    fn execute_structural_sql_aggregate_field_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<Vec<Value>, QueryError> {
        let (payload, _) = self.execute_structural_sql_projection(query, authority, None)?;
        let (_, _, rows, _) = payload.into_parts();
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
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_for_authority(
        &self,
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
        label_overrides: Vec<Option<String>>,
    ) -> Result<SqlStatementResult, QueryError> {
        let model = authority.model();
        let strategies = command
            .prepared_scalar_strategies_with_model(model)
            .map_err(QueryError::from_sql_lowering_error)?;
        let mut columns = Vec::with_capacity(strategies.len());
        let mut fixed_scales = Vec::with_capacity(strategies.len());
        let mut row = Vec::with_capacity(strategies.len());

        for (index, strategy) in strategies.iter().enumerate() {
            columns.push(
                label_overrides
                    .get(index)
                    .and_then(|label| label.clone())
                    .unwrap_or_else(|| Self::sql_scalar_aggregate_label(strategy)),
            );
            fixed_scales.push(None);

            let value = match strategy.runtime_descriptor() {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                    let (payload, _) = self.execute_structural_sql_projection(
                        command
                            .query()
                            .clone()
                            .select_fields([authority.primary_key_name()]),
                        authority,
                        None,
                    )?;
                    let (_, _, _, row_count) = payload.into_parts();

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

                    reduce_structural_sql_aggregate_field_values(values, strategy)?
                }
            };

            row.push(value);
        }

        Ok(SqlStatementResult::Projection {
            columns,
            fixed_scales,
            rows: vec![row],
            row_count: 1,
        })
    }
}
