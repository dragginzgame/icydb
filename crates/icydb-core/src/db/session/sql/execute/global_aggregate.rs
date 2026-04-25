//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate execution above shared executor terminals.
//! Does not own: aggregate lowering, grouped runtime internals, or projection expression semantics.
//! Boundary: adapts lowered SQL global aggregate commands onto existing session/executor seams.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{
            EntityAuthority, ProjectedValueAggregateKind, ProjectedValueAggregateRequest,
            ScalarTerminalBoundaryRequest, execute_projected_value_aggregate,
            projection::{
                GroupedProjectionExpr, GroupedRowView, compile_grouped_projection_expr,
                eval_grouped_projection_expr, evaluate_grouped_having_expr,
            },
        },
        query::plan::{
            GroupedAggregateExecutionSpec,
            expr::{
                Expr, ProjectionField, ProjectionSelection, collapse_true_only_boolean_admission,
            },
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
            SqlGlobalAggregateCommandCore,
        },
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};

type CompiledGlobalAggregatePostAggregateContract = (
    Vec<GroupedAggregateExecutionSpec>,
    Vec<GroupedProjectionExpr>,
    Option<GroupedProjectionExpr>,
);

// Map one prepared SQL scalar aggregate strategy onto the executor-owned
// projected-value aggregate reducer contract.
fn projected_value_aggregate_request_from_sql_strategy(
    strategy: &PreparedSqlScalarAggregateStrategy,
) -> Result<ProjectedValueAggregateRequest, QueryError> {
    let kind = match strategy.runtime_descriptor() {
        PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => Err(QueryError::invariant(
            "COUNT(*) structural reduction does not consume projected field values",
        )),
        PreparedSqlScalarAggregateRuntimeDescriptor::CountField => {
            Ok(ProjectedValueAggregateKind::CountField)
        }
        PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
            kind: crate::db::query::plan::AggregateKind::Sum,
        } => Ok(ProjectedValueAggregateKind::Sum),
        PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
            kind: crate::db::query::plan::AggregateKind::Avg,
        } => Ok(ProjectedValueAggregateKind::Avg),
        PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
            kind: crate::db::query::plan::AggregateKind::Min,
        } => Ok(ProjectedValueAggregateKind::Min),
        PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
            kind: crate::db::query::plan::AggregateKind::Max,
        } => Ok(ProjectedValueAggregateKind::Max),
        PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
        | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
            Err(QueryError::invariant(
                "prepared SQL scalar aggregate strategy drifted outside SQL support",
            ))
        }
    }?;

    Ok(ProjectedValueAggregateRequest::new(
        kind,
        strategy.is_distinct(),
    ))
}

impl<C: CanisterKind> DbSession<C> {
    // Project one single-field structural query and return its canonical field
    // values for aggregate reduction.
    fn execute_structural_sql_aggregate_field_projection(
        &self,
        query: crate::db::query::intent::StructuralQuery,
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
        query: crate::db::query::intent::StructuralQuery,
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
                [value] => {
                    projected.push(value.clone());
                }
                [value, filter_value] => {
                    if collapse_true_only_boolean_admission(filter_value.clone(), |found| {
                        QueryError::invariant(format!(
                            "structural SQL aggregate filter expression produced non-boolean value: {:?}",
                            found.as_ref(),
                        ))
                    })? {
                        projected.push(value.clone());
                    }
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

    // Compile the implicit single aggregate row contract once so global
    // post-aggregate projection and HAVING stop recursively matching on raw
    // planner expressions at execution time.
    fn compile_global_aggregate_post_aggregate_contract(
        strategies: &[PreparedSqlScalarAggregateStrategy],
        projection: &crate::db::query::plan::expr::ProjectionSpec,
        having: Option<&Expr>,
    ) -> Result<CompiledGlobalAggregatePostAggregateContract, QueryError> {
        // Phase 1: adapt prepared SQL aggregate strategies onto the grouped
        // aggregate identity contract used by the shared post-aggregate
        // expression compiler.
        let aggregate_execution_specs = strategies
            .iter()
            .map(|strategy| {
                GroupedAggregateExecutionSpec::from_uncompiled_parts(
                    strategy.aggregate_kind(),
                    strategy.target_slot().cloned(),
                    strategy.input_expr().cloned().or_else(|| {
                        strategy.projected_field().map(|field| {
                            Expr::Field(crate::db::query::plan::expr::FieldId::new(field))
                        })
                    }),
                    strategy.filter_expr().cloned(),
                    strategy.is_distinct(),
                )
            })
            .collect::<Vec<_>>();

        // Phase 2: compile the outward singleton projection once against the
        // implicit aggregate-only row shape.
        let mut compiled_projection = Vec::with_capacity(projection.len());
        for field in projection.fields() {
            let ProjectionField::Scalar { expr, .. } = field;
            compiled_projection.push(
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        QueryError::invariant(format!(
                            "global aggregate output projection must stay on the shared grouped post-aggregate compilation seam: {err}",
                        ))
                    })?,
            );
        }

        // Phase 3: compile optional global aggregate HAVING on the same
        // shared post-aggregate expression seam.
        let compiled_post_aggregate_filter = having
            .map(|expr| {
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        QueryError::invariant(format!(
                            "global aggregate HAVING must stay on the shared grouped post-aggregate seam: {err}",
                        ))
                    })
            })
            .transpose()?;

        Ok((
            aggregate_execution_specs,
            compiled_projection,
            compiled_post_aggregate_filter,
        ))
    }

    // Execute one SQL COUNT(*) aggregate through the shared typed scalar
    // terminal boundary so SQL reuses the existing count-route ownership.
    fn execute_count_rows_sql_aggregate_with_shared_terminal<E>(
        &self,
        query: &crate::db::query::intent::StructuralQuery,
    ) -> Result<(Value, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = crate::db::Query::<E>::from_inner(query.clone());
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
        let (aggregate_execution_specs, compiled_projection, compiled_post_aggregate_filter) =
            Self::compile_global_aggregate_post_aggregate_contract(
                strategies,
                command.projection(),
                command.having(),
            )?;
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

                    let request = projected_value_aggregate_request_from_sql_strategy(strategy)?;
                    execute_projected_value_aggregate(values, request)
                        .map_err(QueryError::execute)?
                }
            };

            unique_values.push(value);
        }

        // Phase 2: apply optional global aggregate HAVING on the single
        // reduced aggregate row before post-aggregate output projection.
        let projection = command.projection();
        let columns = projection_labels_from_projection_spec(projection);
        let fixed_scales = projection_fixed_scales_from_projection_spec(projection);
        let grouped_row = GroupedRowView::new(
            &[],
            unique_values.as_slice(),
            &[],
            aggregate_execution_specs.as_slice(),
        );

        if let Some(expr) = compiled_post_aggregate_filter.as_ref() {
            let matched = evaluate_grouped_having_expr(expr, &grouped_row).map_err(|err| {
                QueryError::invariant(format!(
                    "global aggregate HAVING evaluation must stay on the shared grouped post-aggregate seam: {err}",
                ))
            })?;
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
        let mut row = Vec::with_capacity(compiled_projection.len());

        for expr in compiled_projection {
            row.push(eval_grouped_projection_expr(&expr, &grouped_row).map_err(|err| {
                QueryError::invariant(format!(
                    "global aggregate output projection evaluation must stay on the shared grouped post-aggregate seam: {err}",
                ))
            })?);
        }

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, vec![row], 1).into_statement_result(),
            cache_attribution,
        ))
    }
}
