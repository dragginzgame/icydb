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
            freeze_load_execution_route_facts, planning::route::AggregateRouteShape,
        },
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            explain::{ExplainAggregateTerminalPlan, FinalizedQueryDiagnostics},
            intent::StructuralQuery,
            plan::AccessPlannedQuery,
        },
        session::{
            query::{QueryPlanCacheAttribution, query_plan_cache_reuse_event},
            sql::projection::annotate_sql_projection_debug_on_execution_descriptor,
        },
        sql::{
            lowering::{
                LoweredSqlCommand, LoweredSqlLaneKind, SqlGlobalAggregateCommandCore,
                bind_lowered_sql_explain_global_aggregate_structural,
                bind_lowered_sql_query_structural, lowered_sql_command_lane,
            },
            parser::SqlExplainMode,
        },
    },
    traits::CanisterKind,
};

///
/// ExplainSqlLane
///
/// ExplainSqlLane classifies lowered SQL statement families only for the
/// `EXPLAIN` surface gate. It prevents non-explain statements from slipping
/// through the explain renderer with ambiguous errors.
///

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

impl<C: CanisterKind> DbSession<C> {
    // Borrow one lowered SQL query plan from the shared prepared-plan cache when
    // the explain renderer only needs immutable logical/route facts.
    fn try_map_cached_sql_query_explain_plan_for_authority<T>(
        &self,
        authority: EntityAuthority,
        structural: &StructuralQuery,
        map: impl FnOnce(&AccessPlannedQuery) -> Result<T, QueryError>,
    ) -> Result<(T, QueryPlanCacheAttribution), QueryError> {
        self.try_map_cached_shared_query_plan_ref_for_authority(
            authority,
            structural,
            |prepared_plan| map(prepared_plan.logical_plan()),
        )
    }

    // Resolve one lowered SQL query through the shared prepared-plan cache and
    // return an owned logical plan for explain-only descriptor mutation.
    fn cached_sql_query_explain_plan_for_authority(
        &self,
        authority: EntityAuthority,
        structural: &StructuralQuery,
    ) -> Result<(AccessPlannedQuery, QueryPlanCacheAttribution), QueryError> {
        let (prepared_plan, cache_attribution) =
            self.cached_shared_query_plan_for_authority(authority, structural)?;

        Ok((prepared_plan.logical_plan().clone(), cache_attribution))
    }

    pub(in crate::db::session::sql::execute) fn explain_lowered_sql_for_authority(
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
        let (rendered, _) = self.try_map_cached_sql_query_explain_plan_for_authority(
            authority,
            &structural,
            |plan| {
                let explain = plan.explain();
                let rendered = match mode {
                    SqlExplainMode::Plan => explain.render_text_canonical(),
                    SqlExplainMode::Json => explain.render_json_canonical(),
                    SqlExplainMode::Execution => {
                        unreachable!("execution explain is handled separately")
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
        if verbose {
            let (mut plan, cache_attribution) =
                self.cached_sql_query_explain_plan_for_authority(authority, &structural)?;
            let visible_indexes =
                self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
            plan.finalize_access_choice_for_model_with_indexes(
                authority.model(),
                visible_indexes.as_slice(),
            );
            let diagnostics = structural
                .finalized_execution_diagnostics_from_plan_with_descriptor_mutator(
                    &plan,
                    Some(query_plan_cache_reuse_event(cache_attribution)),
                    |descriptor| {
                        annotate_sql_projection_debug_on_execution_descriptor(
                            descriptor,
                            &plan,
                            plan.frozen_projection_spec(),
                        );
                    },
                )?;

            return Ok(Some(render_sql_execution_explain(&diagnostics)));
        }

        let (rendered, _) = self.try_map_cached_sql_query_explain_plan_for_authority(
            authority,
            &structural,
            |plan| {
                let route_facts = freeze_load_execution_route_facts(
                    authority.fields(),
                    authority.primary_key_name(),
                    plan,
                )
                .map_err(QueryError::execute)?;
                let mut descriptor =
                    assemble_load_execution_node_descriptor_from_route_facts(plan, &route_facts);
                annotate_sql_projection_debug_on_execution_descriptor(
                    &mut descriptor,
                    plan,
                    plan.frozen_projection_spec(),
                );

                Ok(render_sql_execution_explain(
                    &FinalizedQueryDiagnostics::new(descriptor, Vec::new(), Vec::new(), None),
                ))
            },
        )?;

        Ok(Some(rendered))
    }

    // Resolve one global aggregate base query through the same shared
    // prepared-plan cache as runtime aggregate execution, then borrow immutable
    // logical facts for aggregate descriptor assembly.
    fn try_map_cached_sql_global_aggregate_explain_plan_for_authority<T>(
        &self,
        authority: EntityAuthority,
        command: &SqlGlobalAggregateCommandCore,
        map: impl FnOnce(&AccessPlannedQuery) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let (mapped, _) = self.try_map_cached_sql_query_explain_plan_for_authority(
            authority,
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
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        let model = command.query().model();
        let strategies = command.strategies();

        match mode {
            SqlExplainMode::Plan => self
                .try_map_cached_sql_global_aggregate_explain_plan_for_authority(
                    authority,
                    &command,
                    |plan| Ok(plan.explain().render_text_canonical()),
                ),
            SqlExplainMode::Execution => {
                let _ = verbose;

                self.try_map_cached_sql_global_aggregate_explain_plan_for_authority(
                    authority,
                    &command,
                    |plan| {
                        let mut rendered = Vec::with_capacity(strategies.len());

                        for strategy in strategies {
                            let query_explain = plan.explain();
                            let mut execution =
                                assemble_scalar_aggregate_execution_descriptor_with_projection(
                                    plan,
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
                                    render_scalar_projection_expr_plan_label(filter_expr).into(),
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
                    },
                )
            }
            SqlExplainMode::Json => self
                .try_map_cached_sql_global_aggregate_explain_plan_for_authority(
                    authority,
                    &command,
                    |plan| Ok(plan.explain().render_json_canonical()),
                ),
        }
    }
}
