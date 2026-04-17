use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{EntityAuthority, ScalarTerminalBoundaryRequest},
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::builder::scalar_projection::render_scalar_projection_expr_sql_label,
        query::{
            intent::StructuralQuery,
            plan::expr::{ProjectionField, ProjectionSelection},
        },
        session::sql::{SqlCacheAttribution, SqlStatementResult},
        sql::lowering::{
            PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommandCore, is_sql_global_aggregate_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::{CanisterKind, EntityValue},
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

        if let Some(input_expr) = strategy.input_expr() {
            let input = render_scalar_projection_expr_sql_label(input_expr);
            let distinct = if strategy.is_distinct() {
                "DISTINCT "
            } else {
                ""
            };

            return format!("{kind}({distinct}{input})");
        }

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

    // Project one single-expression structural query and return its canonical
    // values for aggregate reduction.
    fn execute_structural_sql_aggregate_input_projection(
        &self,
        query: StructuralQuery,
        input_expr: crate::db::query::plan::expr::Expr,
        authority: EntityAuthority,
    ) -> Result<Vec<Value>, QueryError> {
        let projection_query =
            query.projection_selection(ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                expr: input_expr,
                alias: None,
            }]));

        self.execute_structural_sql_aggregate_field_projection(projection_query, authority)
    }

    // Decide whether one field-target COUNT aggregate is semantically
    // equivalent to COUNT(*) because the field is guaranteed non-null and the
    // strategy does not deduplicate inputs.
    fn sql_count_field_uses_shared_count_terminal(
        model: &'static crate::model::entity::EntityModel,
        strategy: &PreparedSqlScalarAggregateStrategy,
    ) -> bool {
        if strategy.is_distinct() {
            return false;
        }

        let Some(target_slot) = strategy.target_slot() else {
            return false;
        };
        let Some(field) = model.fields().get(target_slot.index()) else {
            return false;
        };

        !field.nullable()
    }

    // Execute one SQL COUNT(*) aggregate through the shared typed scalar
    // terminal boundary so SQL reuses the existing count-route ownership.
    fn execute_count_rows_sql_aggregate_with_shared_terminal<E>(
        &self,
        query: &StructuralQuery,
    ) -> Result<(Value, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = Query::<E>::from_inner(query.clone());
        let (plan, attribution) =
            self.cached_prepared_query_plan_for_entity::<E>(query.structural())?;
        let output = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Count)
            })
            .map_err(QueryError::execute)?;
        let count = output.into_count().map_err(QueryError::execute)?;

        Ok((
            Value::Uint(u64::from(count)),
            SqlCacheAttribution::from_shared_query_plan_cache(attribution),
        ))
    }

    // Execute one prepared SQL aggregate command and package the result as one
    // row-shaped statement payload for unified SQL loops.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_for_authority<
        E,
    >(
        &self,
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
        label_overrides: Vec<Option<String>>,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let model = authority.model();
        let strategies = command
            .prepared_scalar_strategies_with_model(model)
            .map_err(QueryError::from_sql_lowering_error)?;
        let mut unique_values = Vec::with_capacity(strategies.len());
        let mut columns = Vec::with_capacity(command.output_remap().len());
        let mut fixed_scales = Vec::with_capacity(command.output_remap().len());
        let mut row = Vec::with_capacity(command.output_remap().len());
        let mut cache_attribution = SqlCacheAttribution::default();

        // Phase 1: execute each unique prepared aggregate terminal once.
        for strategy in &strategies {
            let value = match strategy.runtime_descriptor() {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                    let (value, count_cache_attribution) = self
                        .execute_count_rows_sql_aggregate_with_shared_terminal::<E>(
                            command.query(),
                        )?;
                    cache_attribution = cache_attribution.merge(count_cache_attribution);

                    value
                }
                PreparedSqlScalarAggregateRuntimeDescriptor::CountField
                    if Self::sql_count_field_uses_shared_count_terminal(model, strategy) =>
                {
                    let (value, count_cache_attribution) = self
                        .execute_count_rows_sql_aggregate_with_shared_terminal::<E>(
                            command.query(),
                        )?;
                    cache_attribution = cache_attribution.merge(count_cache_attribution);

                    value
                }
                PreparedSqlScalarAggregateRuntimeDescriptor::CountField
                | PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
                | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                    let values = if let Some(input_expr) = strategy.input_expr() {
                        self.execute_structural_sql_aggregate_input_projection(
                            command.query().clone(),
                            input_expr.clone(),
                            authority,
                        )?
                    } else {
                        let Some(field) = strategy.projected_field() else {
                            return Err(QueryError::invariant(
                                "field-target SQL aggregate strategy requires projected field label",
                            ));
                        };

                        self.execute_structural_sql_aggregate_field_projection(
                            command.query().clone().select_fields([field]),
                            authority,
                        )?
                    };

                    reduce_structural_sql_aggregate_field_values(values, strategy)?
                }
            };

            unique_values.push(value);
        }

        // Phase 2: fan unique terminal values back out into original SQL output
        // order so duplicate aggregate projections preserve both labels and
        // column multiplicity without rerunning identical work.
        for (output_index, unique_index) in command.output_remap().iter().copied().enumerate() {
            let strategy = strategies.get(unique_index).ok_or_else(|| {
                QueryError::invariant(
                    "global aggregate output remap referenced missing unique terminal strategy",
                )
            })?;
            let value = unique_values.get(unique_index).cloned().ok_or_else(|| {
                QueryError::invariant(
                    "global aggregate output remap referenced missing reduced terminal value",
                )
            })?;

            columns.push(
                label_overrides
                    .get(output_index)
                    .cloned()
                    .flatten()
                    .unwrap_or_else(|| Self::sql_scalar_aggregate_label(strategy)),
            );
            fixed_scales.push(None);
            row.push(value);
        }

        Ok((
            SqlStatementResult::Projection {
                columns,
                fixed_scales,
                rows: vec![row],
                row_count: 1,
            },
            cache_attribution,
        ))
    }
}
