//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while keeping
//! only route and write wiring in child modules.

mod write;

#[cfg(feature = "diagnostics")]
use crate::db::executor::pipeline::execute_initial_grouped_rows_for_canister_with_phase_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::physical_access::with_physical_access_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        executor::{
            EntityAuthority, ProjectedValueAggregateKind, ProjectedValueAggregateRequest,
            ScalarTerminalBoundaryRequest, SharedPreparedExecutionPlan,
            assemble_load_execution_node_descriptor_from_route_facts,
            execute_projected_value_aggregate,
            explain::assemble_scalar_aggregate_execution_descriptor_with_projection,
            freeze_load_execution_route_facts,
            planning::route::AggregateRouteShape,
            projection::{
                GroupedProjectionExpr, GroupedRowView, compile_grouped_projection_expr,
                eval_grouped_projection_expr, evaluate_grouped_having_expr,
            },
        },
        query::{
            builder::scalar_projection::render_scalar_projection_expr_sql_label,
            explain::{ExplainAggregateTerminalPlan, FinalizedQueryDiagnostics},
            intent::StructuralQuery,
            plan::{
                GroupedAggregateExecutionSpec,
                expr::{
                    Expr, ProjectionField, ProjectionSelection,
                    collapse_true_only_boolean_admission,
                },
            },
        },
        schema::commit_schema_fingerprint_for_model,
        session::sql::{
            CompiledSqlCommand, SqlCacheAttribution, SqlStatementResult,
            projection::{
                SqlProjectionPayload, annotate_sql_projection_debug_on_execution_descriptor,
                execute_sql_projection_rows_for_canister,
                projection_fixed_scales_from_projection_spec,
                projection_labels_from_projection_spec,
            },
        },
        sql::{
            lowering::{
                LoweredSqlCommand, LoweredSqlLaneKind, PreparedSqlScalarAggregateRuntimeDescriptor,
                PreparedSqlScalarAggregateStrategy, SqlGlobalAggregateCommandCore,
                bind_lowered_sql_explain_global_aggregate_structural,
                bind_lowered_sql_query_structural, lowered_sql_command_lane,
            },
            parser::SqlExplainMode,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExplainSqlLane {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

// Render one shell-facing SQL execution explain report with a phase legend and
// one indented immutable diagnostics artifact.
fn render_sql_execution_explain(diagnostics: &FinalizedQueryDiagnostics) -> String {
    let mut lines = vec![
        "phases:".to_string(),
        "  c=compile: parse, lower, and compile the SQL surface".to_string(),
        "  p=planner: resolve visible indexes and build the structural access plan".to_string(),
        "  s=store: traverse physical index/data storage and decode physical access payloads"
            .to_string(),
        "  e=executor: run residual filter, order, group, aggregate, and projection logic"
            .to_string(),
        "  d=decode: package the public SQL result payload for the shell".to_string(),
    ];
    lines.push("execution:".to_string());
    lines.push(diagnostics.render_text_verbose_with_tree_indent("  "));

    lines.join("\n")
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_execute_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "diagnostics")]
fn measure_execute_phase_with_physical_access<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> ((u64, u64), Result<T, E>) {
    let (store_local_instructions, (execute_local_instructions, result)) =
        with_physical_access_attribution(|| measure_execute_phase(run));

    (
        (execute_local_instructions, store_local_instructions),
        result,
    )
}

impl<C: CanisterKind> DbSession<C> {
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
                            "global aggregate HAVING must stay on the shared grouped post-aggregate compilation seam: {err}",
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
        query: &StructuralQuery,
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
    fn execute_global_aggregate_statement_for_authority<E>(
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

    // Execute one SQL projection from one shared lower prepared plan plus
    // one thin SQL projection contract so cached and explicit-bypass paths
    // share the same final row-materialization shell.
    fn execute_structural_sql_projection_from_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
    }

    // Execute one grouped SQL statement from one shared lowered prepared plan
    // plus one thin SQL projection contract so normal and diagnostics
    // surfaces share the same grouped plan-to-statement shell without keeping
    // another submodule-only helper boundary alive.
    fn execute_grouped_sql_statement_from_prepared_plan_with<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        authority: EntityAuthority,
        execute_grouped: impl FnOnce(
            &Self,
            EntityAuthority,
            crate::db::query::plan::AccessPlannedQuery,
        )
            -> Result<(crate::db::executor::GroupedCursorPage, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let plan = prepared_plan.logical_plan().clone();
        let (page, extra) = execute_grouped(self, authority, plan)?;
        let next_cursor = page
            .next_cursor
            .map(|cursor| {
                let Some(token) = cursor.as_grouped() else {
                    return Err(QueryError::grouped_paged_emitted_scalar_continuation());
                };

                crate::db::cursor::encode_grouped_cursor_token(token).map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize grouped continuation cursor: {err}"
                    ))
                })
            })
            .transpose()?;
        let row_count = u32::try_from(page.rows.len()).unwrap_or(u32::MAX);

        Ok((
            SqlStatementResult::Grouped {
                columns,
                fixed_scales,
                rows: page
                    .rows
                    .into_iter()
                    .map(crate::db::GroupedRow::from_runtime_row)
                    .collect(),
                row_count,
                next_cursor,
            },
            extra,
        ))
    }

    // Render one lowered SQL EXPLAIN payload through the session-owned
    // planner visibility boundary for one resolved authority.
    fn explain_lowered_sql_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        let lane = match lowered_sql_command_lane(lowered) {
            LoweredSqlLaneKind::Query => ExplainSqlLane::Query,
            LoweredSqlLaneKind::Explain => ExplainSqlLane::Explain,
            LoweredSqlLaneKind::Describe => ExplainSqlLane::Describe,
            LoweredSqlLaneKind::ShowIndexes => ExplainSqlLane::ShowIndexes,
            LoweredSqlLaneKind::ShowColumns => ExplainSqlLane::ShowColumns,
            LoweredSqlLaneKind::ShowEntities => ExplainSqlLane::ShowEntities,
        };
        if lane != ExplainSqlLane::Explain {
            let message = match lane {
                ExplainSqlLane::Describe => "explain_sql rejects DESCRIBE",
                ExplainSqlLane::ShowIndexes => "explain_sql rejects SHOW INDEXES",
                ExplainSqlLane::ShowColumns => "explain_sql rejects SHOW COLUMNS",
                ExplainSqlLane::ShowEntities => "explain_sql rejects SHOW ENTITIES",
                ExplainSqlLane::Query | ExplainSqlLane::Explain => "explain_sql requires EXPLAIN",
            };

            return Err(QueryError::unsupported_query(message));
        }

        if let Some(rendered) =
            self.render_lowered_sql_explain_plan_or_json_for_authority(lowered, authority)?
        {
            return Ok(rendered);
        }

        if let Some((mode, verbose, command)) =
            bind_lowered_sql_explain_global_aggregate_structural(
                lowered,
                authority.model(),
                MissingRowPolicy::Ignore,
            )
            .map_err(QueryError::from_sql_lowering_error)?
        {
            return self.explain_sql_global_aggregate_structural_for_authority(
                mode, verbose, command, authority,
            );
        }

        Err(QueryError::unsupported_query(
            "shared EXPLAIN execution could not classify lowered SQL shape",
        ))
    }

    // Render one lowered SQL EXPLAIN PLAN / JSON payload through the session-
    // owned planner visibility boundary for one resolved authority.
    fn render_lowered_sql_explain_plan_or_json_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<Option<String>, QueryError> {
        let Some((mode, _, query)) = lowered.explain_query() else {
            return Ok(None);
        };
        if matches!(mode, SqlExplainMode::Execution) {
            return Ok(None);
        }

        let structural = bind_lowered_sql_query_structural(
            authority.model(),
            query.clone(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let mut plan = structural.build_plan_with_visible_indexes(&visible_indexes)?;
        authority.finalize_static_planning_shape(&mut plan);
        let explain = plan.explain();
        let rendered = match mode {
            SqlExplainMode::Plan => explain.render_text_canonical(),
            SqlExplainMode::Json => explain.render_json_canonical(),
            SqlExplainMode::Execution => unreachable!("execution explain is handled separately"),
        };

        Ok(Some(rendered))
    }

    // Render one SQL EXPLAIN EXECUTION payload through the shared planner-owned
    // covering contract. Covering visibility is now part of route planning, so
    // SQL EXPLAIN does not need a separate store-backed authority pass.
    fn explain_lowered_sql_execution_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<Option<String>, QueryError> {
        let Some((SqlExplainMode::Execution, verbose, query)) = lowered.explain_query() else {
            return Ok(None);
        };

        let structural = bind_lowered_sql_query_structural(
            authority.model(),
            query.clone(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let mut reuse = None;
        let mut plan = if verbose {
            let cache_schema_fingerprint =
                commit_schema_fingerprint_for_model(authority.model().path, authority.model());
            let (prepared_plan, cache_attribution) = self.cached_shared_query_plan_for_authority(
                authority,
                cache_schema_fingerprint,
                &structural,
            )?;
            reuse = Some(crate::db::session::query::query_plan_cache_reuse_event(
                cache_attribution,
            ));

            prepared_plan.logical_plan().clone()
        } else {
            let mut plan = structural.build_plan_with_visible_indexes(&visible_indexes)?;
            authority.finalize_static_planning_shape(&mut plan);
            plan
        };

        if verbose {
            plan.finalize_access_choice_for_model_with_indexes(
                authority.model(),
                visible_indexes.as_slice(),
            );
        }

        let diagnostics = if verbose {
            structural.finalized_execution_diagnostics_from_plan_with_descriptor_mutator(
                &plan,
                reuse,
                |descriptor| {
                    annotate_sql_projection_debug_on_execution_descriptor(
                        descriptor,
                        &plan,
                        plan.frozen_projection_spec(),
                    );
                },
            )?
        } else {
            let route_facts = freeze_load_execution_route_facts(
                authority.fields(),
                authority.primary_key_name(),
                &plan,
            )
            .map_err(QueryError::execute)?;
            let mut descriptor =
                assemble_load_execution_node_descriptor_from_route_facts(&plan, &route_facts);
            annotate_sql_projection_debug_on_execution_descriptor(
                &mut descriptor,
                &plan,
                plan.frozen_projection_spec(),
            );
            return Ok(Some(render_sql_execution_explain(
                &FinalizedQueryDiagnostics::new(descriptor, Vec::new(), Vec::new(), None),
            )));
        };

        Ok(Some(render_sql_execution_explain(&diagnostics)))
    }

    // Render one EXPLAIN payload for constrained global aggregate SQL command
    // entirely through the session-owned planner visibility boundary.
    #[inline(never)]
    fn explain_sql_global_aggregate_structural_for_authority(
        &self,
        mode: SqlExplainMode,
        verbose: bool,
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        let model = command.query().model();
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let strategies = command.strategies();

        match mode {
            SqlExplainMode::Plan => Ok(command
                .query()
                .build_plan_with_visible_indexes(&visible_indexes)
                .map(|mut plan| {
                    authority.finalize_static_planning_shape(&mut plan);
                    plan
                })?
                .explain()
                .render_text_canonical()),
            SqlExplainMode::Execution => {
                let mut plan = command
                    .query()
                    .build_plan_with_visible_indexes(&visible_indexes)?;
                authority.finalize_static_planning_shape(&mut plan);
                let _ = verbose;
                let mut rendered = Vec::with_capacity(strategies.len());

                for strategy in strategies {
                    let query_explain = plan.explain();
                    let mut execution =
                        assemble_scalar_aggregate_execution_descriptor_with_projection(
                            &plan,
                            AggregateRouteShape::new_from_fields(
                                strategy.aggregate_kind(),
                                strategy.projected_field(),
                                model.fields(),
                                model.primary_key().name(),
                            ),
                            strategy.aggregate_kind(),
                            strategy.projected_field(),
                        );
                    if let Some(filter_expr) = strategy.filter_expr() {
                        execution.node_properties.insert(
                            "filter_expr",
                            render_scalar_projection_expr_sql_label(filter_expr).into(),
                        );
                    }
                    let terminal_plan = ExplainAggregateTerminalPlan::new(
                        query_explain,
                        strategy.aggregate_kind(),
                        execution,
                    );

                    rendered.push(render_sql_execution_explain(
                        &FinalizedQueryDiagnostics::new(
                            terminal_plan.execution_node_descriptor(),
                            Vec::new(),
                            Vec::new(),
                            None,
                        ),
                    ));
                }

                Ok(rendered.join("\n\n"))
            }
            SqlExplainMode::Json => Ok(command
                .query()
                .build_plan_with_visible_indexes(&visible_indexes)
                .map(|mut plan| {
                    authority.finalize_static_planning_shape(&mut plan);
                    plan
                })?
                .explain()
                .render_json_canonical()),
        }
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    // Execute one structural SQL load query through only the shared lower
    // query-plan cache for lowered or aggregate-only bypass paths.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection_without_sql_cache(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let cache_schema_fingerprint = crate::db::schema::commit_schema_fingerprint_for_model(
            authority.model().path,
            authority.model(),
        );
        let (prepared_plan, projection, cache_attribution) =
            self.sql_select_prepared_plan(&query, authority, cache_schema_fingerprint)?;

        self.execute_structural_sql_projection_from_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_compiled_sql<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) = self.execute_compiled_sql_with_cache_attribution::<E>(compiled)?;

        Ok(result)
    }

    // Keep one perf-only execution entrypoint that returns cache attribution
    // together with planner/runtime instruction splits for shell-facing tools.
    #[cfg(feature = "diagnostics")]
    fn execute_non_select_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if matches!(compiled, CompiledSqlCommand::Select { .. }) {
            return Err(QueryError::execute(
                InternalError::query_executor_invariant(
                    "non-select SQL phase attribution helper received SELECT",
                ),
            ));
        }

        let ((execute_local_instructions, store_local_instructions), result) =
            measure_execute_phase_with_physical_access(|| {
                self.execute_compiled_sql_with_cache_attribution::<E>(compiled)
            });
        let (result, cache_attribution) = result?;

        Ok((
            result,
            cache_attribution,
            SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                execute_local_instructions,
                store_local_instructions,
            ),
        ))
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority = EntityAuthority::for_type::<E>();

        match compiled {
            CompiledSqlCommand::Select {
                query,
                compiled_cache_key,
            } => {
                if query.has_grouping() {
                    let (planner_local_instructions, resolved_query_plan) =
                        measure_execute_phase(|| {
                            self.sql_select_prepared_plan(
                                query,
                                authority,
                                compiled_cache_key.schema_fingerprint(),
                            )
                        });
                    let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                    let ((execute_local_instructions, store_local_instructions), statement_result) =
                        measure_execute_phase_with_physical_access(move || {
                            self.execute_grouped_sql_statement_from_prepared_plan_with(
                                prepared_plan,
                                projection,
                                authority,
                                |session, authority, plan| {
                                    execute_initial_grouped_rows_for_canister_with_phase_attribution(
                                        &session.db,
                                        session.debug,
                                        authority,
                                        plan,
                                    )
                                    .map_err(QueryError::execute)
                                },
                            )
                        });
                    let (statement_result, grouped_phase_attribution) = statement_result?;

                    return Ok((
                        statement_result,
                        cache_attribution,
                        SqlExecutePhaseAttribution {
                            planner_local_instructions,
                            store_local_instructions,
                            executor_local_instructions: execute_local_instructions
                                .saturating_sub(store_local_instructions),
                            grouped_stream_local_instructions: grouped_phase_attribution
                                .stream_local_instructions,
                            grouped_fold_local_instructions: grouped_phase_attribution
                                .fold_local_instructions,
                            grouped_finalize_local_instructions: grouped_phase_attribution
                                .finalize_local_instructions,
                            grouped_count: grouped_phase_attribution.grouped_count,
                        },
                    ));
                }

                let (planner_local_instructions, resolved_query_plan) =
                    measure_execute_phase(|| {
                        self.sql_select_prepared_plan(
                            query,
                            authority,
                            compiled_cache_key.schema_fingerprint(),
                        )
                    });
                let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                let ((execute_local_instructions, store_local_instructions), payload) =
                    measure_execute_phase_with_physical_access(move || {
                        self.execute_structural_sql_projection_from_prepared_plan(
                            prepared_plan,
                            projection,
                            SqlCacheAttribution::default(),
                        )
                        .map(|(payload, _)| payload)
                    });
                let payload = payload?;

                Ok((
                    payload.into_statement_result(),
                    cache_attribution,
                    SqlExecutePhaseAttribution {
                        planner_local_instructions,
                        store_local_instructions,
                        executor_local_instructions: execute_local_instructions
                            .saturating_sub(store_local_instructions),
                        grouped_stream_local_instructions: 0,
                        grouped_fold_local_instructions: 0,
                        grouped_finalize_local_instructions: 0,
                        grouped_count: crate::db::executor::GroupedCountAttribution::none(),
                    },
                ))
            }
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::GlobalAggregate { .. }
            | CompiledSqlCommand::Explain(..)
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..)
            | CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities => {
                self.execute_non_select_compiled_sql_with_phase_attribution::<E>(compiled)
            }
        }
    }

    pub(in crate::db) fn execute_compiled_sql_with_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority = EntityAuthority::for_type::<E>();

        match compiled {
            CompiledSqlCommand::Select {
                query,
                compiled_cache_key,
            } => {
                if query.has_grouping() {
                    let (prepared_plan, projection, cache_attribution) = self
                        .sql_select_prepared_plan(
                            query,
                            authority,
                            compiled_cache_key.schema_fingerprint(),
                        )?;
                    let (statement_result, ()) =
                        self.execute_grouped_sql_statement_from_prepared_plan_with(
                            prepared_plan,
                            projection,
                            authority,
                            |session, authority, plan| {
                                crate::db::executor::pipeline::execute_initial_grouped_rows_for_canister(
                                    &session.db,
                                    session.debug,
                                    authority,
                                    plan,
                                )
                                .map_err(QueryError::execute)
                                .map(|page| (page, ()))
                            },
                        )?;

                    return Ok((statement_result, cache_attribution));
                }

                let (prepared_plan, projection, cache_attribution) = self
                    .sql_select_prepared_plan(
                        query,
                        authority,
                        compiled_cache_key.schema_fingerprint(),
                    )?;
                let (payload, cache_attribution) = self
                    .execute_structural_sql_projection_from_prepared_plan(
                        prepared_plan,
                        projection,
                        cache_attribution,
                    )?;

                Ok((payload.into_statement_result(), cache_attribution))
            }
            CompiledSqlCommand::Delete { query, returning } => self
                .execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref())
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::GlobalAggregate { command } => self
                .execute_global_aggregate_statement_for_authority::<E>(*command.clone(), authority),
            CompiledSqlCommand::Explain(lowered) => {
                if let Some(explain) =
                    self.explain_lowered_sql_execution_for_authority(lowered, authority)?
                {
                    return Ok((
                        SqlStatementResult::Explain(explain),
                        SqlCacheAttribution::default(),
                    ));
                }

                self.explain_lowered_sql_for_authority(lowered, authority)
                    .map(SqlStatementResult::Explain)
                    .map(|result| (result, SqlCacheAttribution::default()))
            }
            CompiledSqlCommand::Insert(statement) => self
                .execute_sql_insert_statement::<E>(statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::Update(statement) => self
                .execute_sql_update_statement::<E>(statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::DescribeEntity => Ok((
                SqlStatementResult::Describe(self.describe_entity::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowIndexesEntity => Ok((
                SqlStatementResult::ShowIndexes(self.show_indexes::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowColumnsEntity => Ok((
                SqlStatementResult::ShowColumns(self.show_columns::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowEntities => Ok((
                SqlStatementResult::ShowEntities(self.show_entities()),
                SqlCacheAttribution::default(),
            )),
        }
    }

    /// Compile and then execute one parsed reduced SQL statement into one
    /// unified SQL payload for session-owned tests.
    #[cfg(test)]
    pub(in crate::db) fn execute_sql_statement_inner<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = crate::db::session::sql::parse_sql_statement(sql)?;
        let (compiled, _, _) = match statement {
            crate::db::sql::parser::SqlStatement::Insert(_)
            | crate::db::sql::parser::SqlStatement::Update(_)
            | crate::db::sql::parser::SqlStatement::Delete(_) => {
                self.compile_sql_update_with_cache_attribution::<E>(sql)?
            }
            crate::db::sql::parser::SqlStatement::Select(_)
            | crate::db::sql::parser::SqlStatement::Explain(_)
            | crate::db::sql::parser::SqlStatement::Describe(_)
            | crate::db::sql::parser::SqlStatement::ShowIndexes(_)
            | crate::db::sql::parser::SqlStatement::ShowColumns(_)
            | crate::db::sql::parser::SqlStatement::ShowEntities(_) => {
                self.compile_sql_query_with_cache_attribution::<E>(sql)?
            }
        };

        self.execute_compiled_sql::<E>(&compiled)
    }
}
