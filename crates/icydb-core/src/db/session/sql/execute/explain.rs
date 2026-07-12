//! Module: db::session::sql::execute::explain
//! Responsibility: SQL EXPLAIN execution and rendering adapters.
//! Does not own: planner/executor route policy or diagnostics DTO definitions.
//! Boundary: renders lowered SQL explain statements through session visibility and route facts.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{
            EntityAuthority, assemble_load_execution_node_descriptor_from_route_facts,
            explain::assemble_scalar_aggregate_execution_descriptor_with_projection,
            freeze_load_execution_route_facts_for_authority,
        },
        query::{
            admission::{QueryAdmissionLane, QueryAdmissionPolicy, QueryAdmissionSummary},
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            explain::{
                ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainPlan,
                FinalizedQueryDiagnostics, property_keys,
            },
            intent::StructuralQuery,
            plan::AccessPlannedQuery,
        },
        schema::SchemaInfo,
        session::{
            AcceptedSchemaCatalogContext,
            query::{QueryPlanCacheAttribution, query_plan_cache_reuse_event},
            sql::projection::annotate_sql_projection_debug_on_execution_descriptor,
        },
        sql::{
            lowering::{
                LoweredSqlCommand, PreparedSqlScalarAggregateStrategy, SqlGlobalAggregateCommand,
                bind_lowered_sql_explain_global_aggregate_with_schema,
                bind_lowered_sql_query_structural_with_schema,
            },
            parser::SqlExplainMode,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

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

fn render_sql_execution_explain_json(diagnostics: &FinalizedQueryDiagnostics) -> String {
    diagnostics.render_json_canonical()
}

fn render_sql_execution_explain_json_array(diagnostics: &[String]) -> String {
    let mut out = String::from("{\"terminals\":[");
    for (index, diagnostic) in diagnostics.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(diagnostic);
    }
    out.push_str("]}");

    out
}

fn diagnostic_explain_admission_for_plan(plan: &AccessPlannedQuery) -> QueryAdmissionSummary {
    QueryAdmissionPolicy::diagnostic_explain().evaluate(QueryAdmissionSummary::from_plan(
        QueryAdmissionLane::DiagnosticExplain,
        plan,
    ))
}

impl<C: CanisterKind> DbSession<C> {
    // Borrow one lowered SQL query plan from the shared prepared-plan cache when
    // the explain renderer only needs immutable logical/route facts.
    fn try_map_cached_sql_query_explain_plan_for_accepted_authority<T>(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        structural: &StructuralQuery,
        map: impl FnOnce(&AccessPlannedQuery) -> Result<T, QueryError>,
    ) -> Result<(T, QueryPlanCacheAttribution), QueryError> {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority, catalog, structural,
            )?;
        let mapped = map(prepared_plan.logical_plan())?;

        Ok((mapped, cache_attribution))
    }

    // Resolve one lowered SQL query through the shared prepared-plan cache and
    // return an owned logical plan for explain-only descriptor mutation.
    fn cached_sql_query_explain_plan_for_accepted_authority(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        structural: &StructuralQuery,
    ) -> Result<(AccessPlannedQuery, QueryPlanCacheAttribution), QueryError> {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority, catalog, structural,
            )?;

        Ok((prepared_plan.logical_plan().clone(), cache_attribution))
    }

    pub(in crate::db::session::sql::execute) fn explain_lowered_sql_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        if !lowered.is_explain_lane() {
            return Err(QueryError::unsupported_query());
        }

        if let Some(rendered) = self.render_lowered_sql_explain_plan_or_json_for_authority(
            lowered,
            authority.clone(),
            catalog,
            schema_info,
        )? {
            return Ok(rendered);
        }

        if let Some((mode, verbose, command)) =
            bind_lowered_sql_explain_global_aggregate_with_schema(
                lowered,
                authority.model(),
                MissingRowPolicy::Ignore,
                schema_info,
            )
            .map_err(QueryError::from_sql_lowering_error)?
        {
            return self.explain_sql_global_aggregate_structural_for_authority(
                mode,
                verbose,
                command,
                authority,
                catalog,
                schema_info,
            );
        }

        Err(QueryError::unsupported_query())
    }

    // Render one lowered SQL EXPLAIN PLAN / JSON payload through the session-
    // owned planner visibility boundary for one resolved authority.
    fn render_lowered_sql_explain_plan_or_json_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        schema_info: &SchemaInfo,
    ) -> Result<Option<String>, QueryError> {
        let Some((mode, _, query)) = lowered.explain_query() else {
            return Ok(None);
        };
        if matches!(
            mode,
            SqlExplainMode::Execution | SqlExplainMode::ExecutionJson
        ) {
            return Ok(None);
        }

        let structural = bind_lowered_sql_query_structural_with_schema(
            authority.model(),
            query.clone(),
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (rendered, _) = self.try_map_cached_sql_query_explain_plan_for_accepted_authority(
            authority,
            catalog,
            &structural,
            |plan| {
                let explain = plan.explain();
                let rendered = match mode {
                    SqlExplainMode::Plan => explain.render_text_canonical(),
                    SqlExplainMode::Json => explain.render_json_canonical(),
                    SqlExplainMode::Execution | SqlExplainMode::ExecutionJson => {
                        return Err(QueryError::execute(
                            InternalError::query_executor_invariant(),
                        ));
                    }
                };

                Ok(rendered)
            },
        )?;

        Ok(Some(rendered))
    }

    // Render one SQL EXPLAIN EXECUTION payload through the shared planner-owned
    // covering contract. Covering visibility is now part of route planning, so
    // SQL EXPLAIN does not need a separate store-backed authority pass.
    pub(in crate::db::session::sql::execute) fn explain_lowered_sql_execution_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        schema_info: &SchemaInfo,
    ) -> Result<Option<String>, QueryError> {
        let Some((
            mode @ (SqlExplainMode::Execution | SqlExplainMode::ExecutionJson),
            verbose,
            query,
        )) = lowered.explain_query()
        else {
            return Ok(None);
        };

        let structural = bind_lowered_sql_query_structural_with_schema(
            authority.model(),
            query.clone(),
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        if verbose {
            let (mut plan, cache_attribution) = self
                .cached_sql_query_explain_plan_for_accepted_authority(
                    authority.clone(),
                    catalog,
                    &structural,
                )?;
            let visible_indexes = self
                .visible_indexes_for_store_accepted_schema(authority.store_path(), schema_info)?;
            plan.finalize_access_choice_for_model_with_semantic_indexes_and_schema(
                authority.model(),
                visible_indexes.accepted_semantic_index_contracts(),
                schema_info,
            );
            let projection = plan.frozen_projection_spec().map_err(QueryError::execute)?;
            let diagnostics = structural
                .finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator(
                    &plan,
                    &authority,
                    Some(query_plan_cache_reuse_event(cache_attribution)),
                    |descriptor| {
                        annotate_sql_projection_debug_on_execution_descriptor(
                            descriptor, &plan, projection,
                        );
                    },
                )?;

            return Ok(Some(render_sql_execution_explain(&diagnostics)));
        }

        let (rendered, _) = self.try_map_cached_sql_query_explain_plan_for_accepted_authority(
            authority.clone(),
            catalog,
            &structural,
            |plan| {
                let route_facts = freeze_load_execution_route_facts_for_authority(&authority, plan)
                    .map_err(QueryError::execute)?;
                let projection = plan.frozen_projection_spec().map_err(QueryError::execute)?;
                let mut descriptor =
                    assemble_load_execution_node_descriptor_from_route_facts(plan, &route_facts)
                        .map_err(QueryError::execute)?;
                annotate_sql_projection_debug_on_execution_descriptor(
                    &mut descriptor,
                    plan,
                    projection,
                );

                let diagnostics =
                    FinalizedQueryDiagnostics::new(descriptor, Vec::new(), Vec::new(), None)
                        .with_admission(diagnostic_explain_admission_for_plan(plan));
                Ok(match mode {
                    SqlExplainMode::Execution => render_sql_execution_explain(&diagnostics),
                    SqlExplainMode::ExecutionJson => {
                        render_sql_execution_explain_json(&diagnostics)
                    }
                    SqlExplainMode::Plan | SqlExplainMode::Json => {
                        return Err(QueryError::execute(
                            InternalError::query_executor_invariant(),
                        ));
                    }
                })
            },
        )?;

        Ok(Some(rendered))
    }

    // Resolve one global aggregate base query through the same shared
    // prepared-plan cache as runtime aggregate execution, then borrow immutable
    // logical facts for aggregate descriptor assembly.
    fn try_map_cached_sql_global_aggregate_explain_plan_for_accepted_authority<T>(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        command: &SqlGlobalAggregateCommand,
        map: impl FnOnce(&AccessPlannedQuery) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let (mapped, _) = self.try_map_cached_sql_query_explain_plan_for_accepted_authority(
            authority,
            catalog,
            command.query(),
            map,
        )?;

        Ok(mapped)
    }

    // Render one EXPLAIN payload for constrained global aggregate SQL command
    // through the same shared prepared-plan cache used by runtime execution.
    #[inline(never)]
    fn explain_sql_global_aggregate_structural_for_authority(
        &self,
        mode: SqlExplainMode,
        verbose: bool,
        command: SqlGlobalAggregateCommand,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        let strategies = command.strategies();

        match mode {
            SqlExplainMode::Plan => self
                .try_map_cached_sql_global_aggregate_explain_plan_for_accepted_authority(
                    authority,
                    catalog,
                    &command,
                    |plan| Ok(plan.explain().render_text_canonical()),
                ),
            SqlExplainMode::Execution => {
                let _ = verbose;

                self.try_map_cached_sql_global_aggregate_explain_plan_for_accepted_authority(
                    authority.clone(),
                    catalog,
                    &command,
                    |plan| {
                        self.render_global_aggregate_execution_explain(
                            &command,
                            strategies,
                            plan,
                            &authority,
                            schema_info,
                        )
                    },
                )
            }
            SqlExplainMode::ExecutionJson => {
                let _ = verbose;

                self.try_map_cached_sql_global_aggregate_explain_plan_for_accepted_authority(
                    authority.clone(),
                    catalog,
                    &command,
                    |plan| {
                        self.render_global_aggregate_execution_explain_json(
                            &command,
                            strategies,
                            plan,
                            &authority,
                            schema_info,
                        )
                    },
                )
            }
            SqlExplainMode::Json => self
                .try_map_cached_sql_global_aggregate_explain_plan_for_accepted_authority(
                    authority,
                    catalog,
                    &command,
                    |plan| Ok(plan.explain().render_json_canonical()),
                ),
        }
    }

    fn render_global_aggregate_execution_explain(
        &self,
        command: &SqlGlobalAggregateCommand,
        strategies: &[PreparedSqlScalarAggregateStrategy],
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        let query_explain = plan.explain();
        let mut rendered = Vec::with_capacity(strategies.len());

        for strategy in strategies {
            rendered.push(self.render_global_aggregate_terminal_explain(
                command,
                strategy,
                &query_explain,
                plan,
                authority,
                schema_info,
            )?);
        }

        Ok(rendered.join("\n\n"))
    }

    fn render_global_aggregate_execution_explain_json(
        &self,
        command: &SqlGlobalAggregateCommand,
        strategies: &[PreparedSqlScalarAggregateStrategy],
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        let query_explain = plan.explain();
        let mut rendered = Vec::with_capacity(strategies.len());

        for strategy in strategies {
            rendered.push(self.render_global_aggregate_terminal_explain_json(
                command,
                strategy,
                &query_explain,
                plan,
                authority,
                schema_info,
            )?);
        }

        Ok(render_sql_execution_explain_json_array(&rendered))
    }

    fn render_global_aggregate_terminal_explain(
        &self,
        command: &SqlGlobalAggregateCommand,
        strategy: &PreparedSqlScalarAggregateStrategy,
        query_explain: &ExplainPlan,
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        let execution = self.global_aggregate_terminal_execution_descriptor(
            command,
            strategy,
            plan,
            authority,
            schema_info,
        )?;
        let terminal_plan = ExplainAggregateTerminalPlan::new(
            query_explain.clone(),
            strategy.aggregate_kind(),
            execution,
        );

        Ok(render_sql_execution_explain(
            &FinalizedQueryDiagnostics::new(
                terminal_plan.execution_node_descriptor(),
                Vec::new(),
                Vec::new(),
                None,
            )
            .with_admission(diagnostic_explain_admission_for_plan(plan)),
        ))
    }

    fn render_global_aggregate_terminal_explain_json(
        &self,
        command: &SqlGlobalAggregateCommand,
        strategy: &PreparedSqlScalarAggregateStrategy,
        query_explain: &ExplainPlan,
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<String, QueryError> {
        let execution = self.global_aggregate_terminal_execution_descriptor(
            command,
            strategy,
            plan,
            authority,
            schema_info,
        )?;
        let terminal_plan = ExplainAggregateTerminalPlan::new(
            query_explain.clone(),
            strategy.aggregate_kind(),
            execution,
        );
        let diagnostics = FinalizedQueryDiagnostics::new(
            terminal_plan.execution_node_descriptor(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .with_admission(diagnostic_explain_admission_for_plan(plan));

        Ok(render_sql_execution_explain_json(&diagnostics))
    }

    fn global_aggregate_terminal_execution_descriptor(
        &self,
        command: &SqlGlobalAggregateCommand,
        strategy: &PreparedSqlScalarAggregateStrategy,
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<ExplainExecutionDescriptor, QueryError> {
        let mut execution = assemble_scalar_aggregate_execution_descriptor_with_projection(
            plan,
            authority
                .aggregate_route_shape(strategy.aggregate_kind(), strategy.projected_field())
                .map_err(QueryError::execute)?,
            strategy.aggregate_kind(),
            strategy.projected_field(),
        )
        .map_err(QueryError::execute)?;
        if let Some(filter_expr) = strategy.filter_expr() {
            execution.node_properties.insert(
                property_keys::FILTER_EXPR,
                render_scalar_projection_expr_plan_label(filter_expr).into(),
            );
        }
        self.annotate_global_aggregate_direct_count_metadata_eligibility(
            &mut execution,
            command,
            authority,
            schema_info,
        )?;

        Ok(execution)
    }

    fn annotate_global_aggregate_direct_count_metadata_eligibility(
        &self,
        execution: &mut ExplainExecutionDescriptor,
        command: &SqlGlobalAggregateCommand,
        authority: &EntityAuthority,
        schema_info: &SchemaInfo,
    ) -> Result<(), QueryError> {
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok(());
        }

        let visible_indexes =
            self.visible_indexes_for_store_accepted_schema(authority.store_path(), schema_info)?;
        let prefix_specs =
            super::direct_count::direct_count_cardinality_prefix_specs_for_accepted_authority(
                authority,
                command.query(),
                &visible_indexes,
                schema_info,
            )?;
        let prefix_count = prefix_specs.as_ref().map_or(0, Vec::len);
        execution.node_properties.insert(
            property_keys::AGGREGATE_DIRECT_COUNT_METADATA_ELIGIBLE,
            Value::from(prefix_count != 0),
        );
        if prefix_count != 0 {
            execution.node_properties.insert(
                property_keys::AGGREGATE_DIRECT_COUNT_PREFIXES,
                Value::from(u64::try_from(prefix_count).unwrap_or(u64::MAX)),
            );
        }

        Ok(())
    }
}
