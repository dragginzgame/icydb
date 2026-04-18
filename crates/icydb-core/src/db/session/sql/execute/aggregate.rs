use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{
            EntityAuthority, ScalarTerminalBoundaryRequest,
            projection::{
                eval_binary_expr, eval_projection_function_call, eval_unary_expr,
                projection_function_name,
            },
        },
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::{
            builder::AggregateExpr,
            intent::StructuralQuery,
            plan::expr::{Expr, ProjectionField, ProjectionSelection},
        },
        session::sql::{
            SqlCacheAttribution, SqlStatementResult,
            projection::{
                SqlProjectionPayload, projection_fixed_scales_from_projection_spec,
                projection_labels_from_projection_spec,
            },
        },
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

    // Project one single-field structural query and return its canonical field
    // values for aggregate reduction.
    fn execute_structural_sql_aggregate_field_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<Vec<Value>, QueryError> {
        let (payload, _) =
            self.execute_structural_sql_projection_without_sql_cache(query, authority)?;
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

    // Project one aggregate input plus one optional filter expression and
    // admit values only when the filter reuses the shared TRUE-only row
    // admission boundary.
    fn execute_structural_sql_aggregate_projection_with_optional_filter(
        &self,
        query: StructuralQuery,
        projected_expr: Expr,
        filter_expr: Option<Expr>,
        authority: EntityAuthority,
    ) -> Result<Vec<Value>, QueryError> {
        let mut projection_fields = vec![ProjectionField::Scalar {
            expr: projected_expr,
            alias: None,
        }];
        if let Some(filter_expr) = filter_expr {
            projection_fields.push(ProjectionField::Scalar {
                expr: filter_expr,
                alias: None,
            });
        }

        let projection_query =
            query.projection_selection(ProjectionSelection::from_scalar_fields(projection_fields));
        let (payload, _) =
            self.execute_structural_sql_projection_without_sql_cache(projection_query, authority)?;
        let (_, _, rows, _) = payload.into_parts();
        let mut projected = Vec::with_capacity(rows.len());

        for row in rows {
            match row.as_slice() {
                [value] | [value, Value::Bool(true)] => {
                    projected.push(value.clone());
                }
                [_, Value::Bool(false) | Value::Null] => {}
                [_, other] => {
                    return Err(QueryError::invariant(format!(
                        "structural SQL aggregate filter expression produced non-boolean value: {other:?}",
                    )));
                }
                _ => {
                    return Err(QueryError::invariant(
                        "structural SQL aggregate filter projection must emit one value plus one optional boolean filter",
                    ));
                }
            }
        }

        Ok(projected)
    }

    // Decide whether one field-target COUNT aggregate is semantically
    // equivalent to COUNT(*) because the field is guaranteed non-null and the
    // strategy does not deduplicate inputs.
    fn sql_count_field_uses_shared_count_terminal(
        model: &'static crate::model::entity::EntityModel,
        strategy: &PreparedSqlScalarAggregateStrategy,
    ) -> bool {
        if strategy.filter_expr().is_some() {
            return false;
        }

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

    // Resolve one aggregate leaf in the output projection against the already
    // prepared unique aggregate strategy list.
    fn resolve_global_aggregate_expr_index(
        strategies: &[PreparedSqlScalarAggregateStrategy],
        aggregate_expr: &AggregateExpr,
    ) -> Option<usize> {
        strategies.iter().position(|strategy| {
            let same_kind = strategy.aggregate_kind() == aggregate_expr.kind();
            let same_distinct = strategy.is_distinct() == aggregate_expr.is_distinct();
            let same_filter = strategy.filter_expr() == aggregate_expr.filter_expr();
            let same_input = match strategy.input_expr() {
                Some(input_expr) => aggregate_expr.input_expr() == Some(input_expr),
                None => strategy.projected_field() == aggregate_expr.target_field(),
            };

            same_kind && same_distinct && same_filter && same_input
        })
    }

    // Evaluate one global post-aggregate output expression against the reduced
    // unique aggregate values.
    #[expect(
        clippy::too_many_lines,
        reason = "global aggregate output evaluation intentionally owns the full post-aggregate scalar expression recursion, including CASE, on one invariant-checked seam"
    )]
    fn evaluate_global_aggregate_output_expr(
        expr: &Expr,
        strategies: &[PreparedSqlScalarAggregateStrategy],
        unique_values: &[Value],
    ) -> Result<Value, QueryError> {
        match expr {
            Expr::Aggregate(aggregate_expr) => {
                let Some(index) =
                    Self::resolve_global_aggregate_expr_index(strategies, aggregate_expr)
                else {
                    return Err(QueryError::invariant(format!(
                        "global aggregate projection evaluation referenced unknown aggregate expression kind={:?} target_field={:?} distinct={}",
                        aggregate_expr.kind(),
                        aggregate_expr.target_field(),
                        aggregate_expr.is_distinct(),
                    )));
                };

                unique_values.get(index).cloned().ok_or_else(|| {
                    QueryError::invariant(format!(
                        "global aggregate projection evaluation referenced aggregate output index={index} but only {} outputs are available",
                        unique_values.len(),
                    ))
                })
            }
            Expr::Literal(value) => Ok(value.clone()),
            Expr::FunctionCall { function, args } => {
                let mut evaluated_args = Vec::with_capacity(args.len());

                for arg in args {
                    evaluated_args.push(Self::evaluate_global_aggregate_output_expr(
                        arg,
                        strategies,
                        unique_values,
                    )?);
                }

                eval_projection_function_call(*function, evaluated_args.as_slice()).map_err(|err| {
                    QueryError::invariant(format!(
                        "global aggregate projection evaluation failed in function {}: {err}",
                        projection_function_name(*function),
                    ))
                })
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    let condition = Self::evaluate_global_aggregate_output_expr(
                        arm.condition(),
                        strategies,
                        unique_values,
                    )?;
                    let Value::Bool(condition) = condition else {
                        return Err(QueryError::invariant(format!(
                            "global aggregate projection evaluation produced non-boolean CASE condition value: {condition:?}",
                        )));
                    };

                    if condition {
                        return Self::evaluate_global_aggregate_output_expr(
                            arm.result(),
                            strategies,
                            unique_values,
                        );
                    }
                }

                Self::evaluate_global_aggregate_output_expr(
                    else_expr.as_ref(),
                    strategies,
                    unique_values,
                )
            }
            Expr::Binary { op, left, right } => {
                let left = Self::evaluate_global_aggregate_output_expr(
                    left.as_ref(),
                    strategies,
                    unique_values,
                )?;
                let right = Self::evaluate_global_aggregate_output_expr(
                    right.as_ref(),
                    strategies,
                    unique_values,
                )?;

                eval_binary_expr(*op, &left, &right).map_err(|err| {
                    QueryError::invariant(format!(
                        "global aggregate projection evaluation failed for binary op {op:?}: {err}",
                    ))
                })
            }
            Expr::Unary { op, expr } => {
                let value = Self::evaluate_global_aggregate_output_expr(
                    expr.as_ref(),
                    strategies,
                    unique_values,
                )?;

                eval_unary_expr(*op, &value).map_err(|err| {
                    QueryError::invariant(format!(
                        "global aggregate projection evaluation failed for unary op {op:?}: {err}",
                    ))
                })
            }
            Expr::Field(field) => Err(QueryError::invariant(format!(
                "global aggregate projection evaluation referenced direct field '{}'",
                field.as_str(),
            ))),
            #[cfg(test)]
            Expr::Alias { .. } => Err(QueryError::invariant(
                "global aggregate projection evaluation encountered unsupported test-only expression wrapper",
            )),
        }
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
        let (plan, attribution) = self.cached_prepared_query_plan_for_entity::<E>(&query)?;
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
    #[expect(
        clippy::too_many_lines,
        reason = "global aggregate statement execution intentionally owns scalar, filtered, and shared count-lane dispatch on one explicit SQL boundary"
    )]
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_for_authority<
        E,
    >(
        &self,
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let model = authority.model();
        let strategies = command.strategies();
        let mut unique_values = Vec::with_capacity(strategies.len());
        let mut cache_attribution = SqlCacheAttribution::default();

        // Phase 1: execute each unique prepared aggregate terminal once.
        for strategy in strategies {
            let value = match strategy.runtime_descriptor() {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows
                    if strategy.filter_expr().is_none() =>
                {
                    let (value, count_cache_attribution) = self
                        .execute_count_rows_sql_aggregate_with_shared_terminal::<E>(
                            command.query(),
                        )?;
                    cache_attribution = cache_attribution.merge(count_cache_attribution);

                    value
                }
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                    let values = self
                        .execute_structural_sql_aggregate_projection_with_optional_filter(
                            command.query().clone(),
                            Expr::Literal(Value::Uint(1)),
                            strategy.filter_expr().cloned(),
                            authority,
                        )?;

                    Value::Uint(u64::try_from(values.len()).unwrap_or(u64::MAX))
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
                        self.execute_structural_sql_aggregate_projection_with_optional_filter(
                            command.query().clone(),
                            input_expr.clone(),
                            strategy.filter_expr().cloned(),
                            authority,
                        )?
                    } else {
                        let Some(field) = strategy.projected_field() else {
                            return Err(QueryError::invariant(
                                "field-target SQL aggregate strategy requires projected field label",
                            ));
                        };
                        match strategy.filter_expr() {
                            None => self.execute_structural_sql_aggregate_field_projection(
                                command.query().clone().select_fields([field]),
                                authority,
                            )?,
                            Some(filter_expr) => self
                                .execute_structural_sql_aggregate_projection_with_optional_filter(
                                    command.query().clone(),
                                    Expr::Field(crate::db::query::plan::expr::FieldId::new(field)),
                                    Some(filter_expr.clone()),
                                    authority,
                                )?,
                        }
                    };

                    reduce_structural_sql_aggregate_field_values(values, strategy)?
                }
            };

            unique_values.push(value);
        }

        // Phase 2: apply optional global aggregate HAVING on the single
        // reduced aggregate row before post-aggregate output projection.
        let projection = command.projection();
        let columns = projection_labels_from_projection_spec(projection);
        let fixed_scales = projection_fixed_scales_from_projection_spec(projection);

        if let Some(expr) = command.having() {
            let matched = matches!(
                Self::evaluate_global_aggregate_output_expr(
                    expr,
                    strategies,
                    unique_values.as_slice(),
                )?,
                Value::Bool(true)
            );

            if !matched {
                return Ok((
                    SqlProjectionPayload::new(columns, fixed_scales, Vec::new(), 0)
                        .into_statement_result(),
                    cache_attribution,
                ));
            }
        }

        // Phase 3: evaluate the planner-owned global output projection over
        // the reduced unique aggregate values so aggregate results can feed
        // normal scalar wrappers like ROUND(...) and binary arithmetic.
        let mut row = Vec::with_capacity(projection.len());

        for field in projection.fields() {
            let crate::db::query::plan::expr::ProjectionField::Scalar { expr, .. } = field;
            row.push(Self::evaluate_global_aggregate_output_expr(
                expr,
                strategies,
                unique_values.as_slice(),
            )?);
        }

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, vec![row], 1).into_statement_result(),
            cache_attribution,
        ))
    }
}
