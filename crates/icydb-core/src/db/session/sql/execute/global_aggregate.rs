//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate execution above shared executor terminals.
//! Does not own: aggregate lowering, grouped runtime internals, or projection expression semantics.
//! Boundary: adapts lowered SQL global aggregate commands onto existing session/executor seams.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{
            AggregateEmptyBehavior, EntityAuthority, PreparedScalarAggregateTerminal,
            PreparedScalarAggregateTerminalSet, ScalarAggregateInput, ScalarAggregateTerminalKind,
            ScalarTerminalBoundaryRequest,
            projection::{
                GroupedProjectionExpr, GroupedRowView, compile_grouped_projection_expr,
                eval_grouped_projection_expr, evaluate_grouped_having_expr,
            },
        },
        query::plan::{
            ExprPlanError, GroupedAggregateExecutionSpec, PlanError,
            expr::{Expr, ProjectionField, compile_scalar_projection_expr},
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

impl<C: CanisterKind> DbSession<C> {
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

    // Compile one SQL aggregate-owned expression onto the scalar retained-slot
    // expression seam. Aggregate lowering already validates these shapes, so a
    // miss here indicates drift between lowering and executor preparation.
    fn compile_sql_scalar_aggregate_terminal_expr(
        model: &'static crate::model::entity::EntityModel,
        expr: &Expr,
    ) -> Result<crate::db::query::plan::expr::ScalarProjectionExpr, QueryError> {
        if let Some(field) = Self::first_unknown_sql_scalar_aggregate_expr_field(model, expr) {
            return Err(QueryError::Plan(Box::new(PlanError::from(
                ExprPlanError::unknown_expr_field(field),
            ))));
        }

        compile_scalar_projection_expr(model, expr).ok_or_else(|| {
            QueryError::invariant(
                "prepared SQL scalar aggregate expression must compile on the scalar seam",
            )
        })
    }

    // Preserve the old projection-planning admission error for aggregate input
    // and FILTER expressions before handing the compiled terminal to executor
    // reduction.
    fn first_unknown_sql_scalar_aggregate_expr_field(
        model: &'static crate::model::entity::EntityModel,
        expr: &Expr,
    ) -> Option<String> {
        let mut first_unknown = None;
        let _ = expr.try_for_each_tree_expr(&mut |node| {
            if first_unknown.is_some() {
                return Ok(());
            }
            if let Expr::Field(field) = node
                && model.resolve_field_slot(field.as_str()).is_none()
            {
                first_unknown = Some(field.as_str().to_string());
            }

            Ok::<(), ()>(())
        });

        first_unknown
    }

    // Map one prepared SQL scalar aggregate strategy onto the executor-owned
    // scalar aggregate terminal contract used by non-fast-path global
    // aggregate reducers.
    fn prepared_scalar_aggregate_terminal_from_sql_strategy(
        model: &'static crate::model::entity::EntityModel,
        strategy: &PreparedSqlScalarAggregateStrategy,
    ) -> Result<PreparedScalarAggregateTerminal, QueryError> {
        let kind = match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => {
                ScalarAggregateTerminalKind::CountRows
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField => {
                ScalarAggregateTerminalKind::CountValues
            }
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Sum,
            } => ScalarAggregateTerminalKind::Sum,
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: crate::db::query::plan::AggregateKind::Avg,
            } => ScalarAggregateTerminalKind::Avg,
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: crate::db::query::plan::AggregateKind::Min,
            } => ScalarAggregateTerminalKind::Min,
            PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: crate::db::query::plan::AggregateKind::Max,
            } => ScalarAggregateTerminalKind::Max,
            PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                return Err(QueryError::invariant(
                    "prepared SQL scalar aggregate strategy drifted outside SQL support",
                ));
            }
        };
        let input = match strategy.runtime_descriptor() {
            PreparedSqlScalarAggregateRuntimeDescriptor::CountRows => ScalarAggregateInput::Rows,
            PreparedSqlScalarAggregateRuntimeDescriptor::CountField
            | PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
            | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                if let Some(input_expr) = strategy.input_expr() {
                    ScalarAggregateInput::Expr(Self::compile_sql_scalar_aggregate_terminal_expr(
                        model, input_expr,
                    )?)
                } else {
                    let Some(target_slot) = strategy.target_slot() else {
                        return Err(QueryError::invariant(
                            "field-target SQL aggregate strategy requires a resolved field slot",
                        ));
                    };

                    ScalarAggregateInput::Field {
                        slot: target_slot.index(),
                        field: target_slot.field().to_string(),
                    }
                }
            }
        };
        let filter = strategy
            .filter_expr()
            .map(|expr| Self::compile_sql_scalar_aggregate_terminal_expr(model, expr))
            .transpose()?;
        let empty_behavior = match kind {
            ScalarAggregateTerminalKind::CountRows | ScalarAggregateTerminalKind::CountValues => {
                AggregateEmptyBehavior::Zero
            }
            ScalarAggregateTerminalKind::Sum
            | ScalarAggregateTerminalKind::Avg
            | ScalarAggregateTerminalKind::Min
            | ScalarAggregateTerminalKind::Max => AggregateEmptyBehavior::Null,
        };

        Ok(PreparedScalarAggregateTerminal::from_validated_parts(
            kind,
            input,
            filter,
            strategy.is_distinct(),
            empty_behavior,
        ))
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
        let mut unique_values = vec![None; strategies.len()];
        let mut scalar_aggregate_terminals = Vec::new();
        let mut scalar_aggregate_terminal_positions = Vec::new();
        let mut cache_attribution = SqlCacheAttribution::default();

        // Phase 1: keep unfiltered COUNT(*) and non-null COUNT(field) on the
        // existing shared scalar count terminal, and stage every remaining SQL
        // global aggregate for executor-owned scalar terminal reduction.
        for (strategy_index, strategy) in strategies.iter().enumerate() {
            match strategy.runtime_descriptor() {
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows
                    if strategy.filter_expr().is_none() =>
                {
                    let (value, count_cache_attribution) = self
                        .execute_count_rows_sql_aggregate_with_shared_terminal::<E>(
                            command.query(),
                        )?;
                    cache_attribution = cache_attribution.merge(count_cache_attribution);

                    unique_values[strategy_index] = Some(value);
                }
                PreparedSqlScalarAggregateRuntimeDescriptor::CountField
                    if Self::sql_count_field_uses_shared_count_terminal(model, strategy) =>
                {
                    let (value, count_cache_attribution) = self
                        .execute_count_rows_sql_aggregate_with_shared_terminal::<E>(
                            command.query(),
                        )?;
                    cache_attribution = cache_attribution.merge(count_cache_attribution);

                    unique_values[strategy_index] = Some(value);
                }
                PreparedSqlScalarAggregateRuntimeDescriptor::CountRows
                | PreparedSqlScalarAggregateRuntimeDescriptor::CountField
                | PreparedSqlScalarAggregateRuntimeDescriptor::NumericField { .. }
                | PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField { .. } => {
                    scalar_aggregate_terminals.push(
                        Self::prepared_scalar_aggregate_terminal_from_sql_strategy(
                            model, strategy,
                        )?,
                    );
                    scalar_aggregate_terminal_positions.push(strategy_index);
                }
            }
        }

        if !scalar_aggregate_terminals.is_empty() {
            let query = crate::db::Query::<E>::from_inner(command.query().clone());
            let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(&query)?;
            let terminal_values = self
                .with_metrics(|| {
                    self.load_executor::<E>()
                        .execute_scalar_aggregate_terminals(
                            plan,
                            PreparedScalarAggregateTerminalSet::new(scalar_aggregate_terminals),
                        )
                })
                .map_err(QueryError::execute)?;
            if terminal_values.len() != scalar_aggregate_terminal_positions.len() {
                return Err(QueryError::invariant(
                    "scalar aggregate terminal output count must match staged SQL terminals",
                ));
            }

            for (strategy_index, value) in scalar_aggregate_terminal_positions
                .into_iter()
                .zip(terminal_values)
            {
                unique_values[strategy_index] = Some(value);
            }
        }
        let unique_values = unique_values
            .into_iter()
            .map(|value| {
                value.ok_or_else(|| {
                    QueryError::invariant(
                        "SQL global aggregate terminal did not produce a reduced value",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

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
