//! Module: db::session::sql::explain
//! Responsibility: session-owned SQL EXPLAIN lowering, plan rendering, and
//! execution-descriptor rendering across the supported SQL explain surfaces.
//! Does not own: SQL parsing, structural plan construction, or descriptor text/JSON formatting.
//! Boundary: keeps SQL explain classification and authority-aware planning at the session edge.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{
            EntityAuthority, assemble_load_execution_node_descriptor,
            explain::assemble_scalar_aggregate_execution_descriptor_with_projection,
            planning::route::AggregateRouteShape,
        },
        query::explain::ExplainAggregateTerminalPlan,
        session::sql::projection::annotate_sql_projection_debug_on_execution_descriptor,
        sql::lowering::{
            LoweredSqlCommand, LoweredSqlLaneKind, SqlGlobalAggregateCommandCore,
            bind_lowered_sql_explain_global_aggregate_structural,
            bind_lowered_sql_query_structural, lowered_sql_command_lane,
        },
        sql::parser::SqlExplainMode,
    },
    traits::CanisterKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExplainSqlLane {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

const fn explain_sql_lane(command: &LoweredSqlCommand) -> ExplainSqlLane {
    match lowered_sql_command_lane(command) {
        LoweredSqlLaneKind::Query => ExplainSqlLane::Query,
        LoweredSqlLaneKind::Explain => ExplainSqlLane::Explain,
        LoweredSqlLaneKind::Describe => ExplainSqlLane::Describe,
        LoweredSqlLaneKind::ShowIndexes => ExplainSqlLane::ShowIndexes,
        LoweredSqlLaneKind::ShowColumns => ExplainSqlLane::ShowColumns,
        LoweredSqlLaneKind::ShowEntities => ExplainSqlLane::ShowEntities,
    }
}

const fn unsupported_explain_sql_lane_message(lane: ExplainSqlLane) -> &'static str {
    match lane {
        ExplainSqlLane::Describe => "explain_sql rejects DESCRIBE",
        ExplainSqlLane::ShowIndexes => "explain_sql rejects SHOW INDEXES",
        ExplainSqlLane::ShowColumns => "explain_sql rejects SHOW COLUMNS",
        ExplainSqlLane::ShowEntities => "explain_sql rejects SHOW ENTITIES",
        ExplainSqlLane::Query | ExplainSqlLane::Explain => "explain_sql requires EXPLAIN",
    }
}

// Render the shell-facing SQL EXPLAIN EXECUTION header that explains how the
// compact perf footer maps onto the current query lifecycle.
fn sql_execution_phase_breakdown_lines() -> Vec<String> {
    vec![
        "phases:".to_string(),
        "  c=compile: parse, lower, and compile the SQL surface".to_string(),
        "  p=planner: resolve visible indexes and build the structural access plan".to_string(),
        "  s=store: traverse physical index/data storage and decode physical access payloads"
            .to_string(),
        "  e=executor: run residual filter, order, group, aggregate, and projection logic"
            .to_string(),
        "  d=decode: package the public SQL result payload for the shell".to_string(),
    ]
}

// Indent one already-rendered multiline execution block so it nests cleanly
// under the SQL EXPLAIN EXECUTION `execution:` section header.
fn indent_multiline_block(block: &str, indent: &str) -> String {
    let mut rendered = String::new();

    for (index, line) in block.lines().enumerate() {
        if index > 0 {
            rendered.push('\n');
        }
        rendered.push_str(indent);
        rendered.push_str(line);
    }

    rendered
}

// Render one shell-facing SQL execution explain report with a phase legend and
// one indented verbose execution tree.
fn render_sql_execution_explain(descriptor: &crate::db::ExplainExecutionNodeDescriptor) -> String {
    let mut lines = sql_execution_phase_breakdown_lines();
    lines.push("execution:".to_string());
    lines.push(indent_multiline_block(
        descriptor.render_text_tree_verbose().as_str(),
        "  ",
    ));

    lines.join("\n")
}

impl<C: CanisterKind> DbSession<C> {
    // Render one lowered SQL EXPLAIN payload through the session-owned planner
    // visibility boundary for one resolved authority.
    pub(in crate::db) fn explain_lowered_sql_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        // First validate lane selection once on the shared lowered-command path
        // so explain callers do not rebuild lane guards around the same shape.
        let lane = explain_sql_lane(lowered);
        if lane != ExplainSqlLane::Explain {
            return Err(QueryError::unsupported_query(
                unsupported_explain_sql_lane_message(lane),
            ));
        }

        // Then prefer the structural planner-owned renderer because plan/json
        // explain output can stay generic-free all the way to the final render step.
        if let Some(rendered) =
            self.render_lowered_sql_explain_plan_or_json_for_authority(lowered, authority)?
        {
            return Ok(rendered);
        }

        // Structural global aggregate explain is the remaining explain-only
        // shape that still needs dedicated aggregate descriptor rendering.
        if let Some((mode, command)) = bind_lowered_sql_explain_global_aggregate_structural(
            lowered,
            authority.model(),
            MissingRowPolicy::Ignore,
        ) {
            return self
                .explain_sql_global_aggregate_structural_for_authority(mode, command, authority);
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
        let Some((mode, query)) = lowered.explain_query() else {
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
        let (_visible_indexes, mut plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
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
    pub(in crate::db::session::sql) fn explain_lowered_sql_execution_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<Option<String>, QueryError> {
        let Some((SqlExplainMode::Execution, query)) = lowered.explain_query() else {
            return Ok(None);
        };

        let structural = bind_lowered_sql_query_structural(
            authority.model(),
            query.clone(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (_, mut plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
        authority.finalize_static_planning_shape(&mut plan);
        let mut descriptor = assemble_load_execution_node_descriptor(
            authority.fields(),
            authority.primary_key_name(),
            &plan,
        )
        .map_err(QueryError::execute)?;
        annotate_sql_projection_debug_on_execution_descriptor(
            &mut descriptor,
            authority.model(),
            &plan,
            &plan.projection_spec(authority.model()),
        );

        Ok(Some(render_sql_execution_explain(&descriptor)))
    }

    // Render one EXPLAIN payload for constrained global aggregate SQL command
    // entirely through the session-owned planner visibility boundary.
    #[inline(never)]
    fn explain_sql_global_aggregate_structural_for_authority(
        &self,
        mode: SqlExplainMode,
        command: SqlGlobalAggregateCommandCore,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        let model = command.query().model();
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let strategies = command
            .prepared_scalar_strategies(model)
            .map_err(QueryError::from_sql_lowering_error)?;

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
                let mut rendered = Vec::with_capacity(strategies.len());

                for strategy in strategies {
                    let query_explain = plan.explain();
                    let execution = assemble_scalar_aggregate_execution_descriptor_with_projection(
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
                    let terminal_plan = ExplainAggregateTerminalPlan::new(
                        query_explain,
                        strategy.aggregate_kind(),
                        execution,
                    );

                    rendered.push(render_sql_execution_explain(
                        &terminal_plan.execution_node_descriptor(),
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
}
