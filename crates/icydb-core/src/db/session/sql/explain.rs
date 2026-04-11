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
            assemble_prepared_sql_scalar_aggregate_execution_descriptor,
            route::AggregateRouteShape,
        },
        query::explain::ExplainAggregateTerminalPlan,
        session::sql::surface::{SqlSurface, session_sql_lane, unsupported_sql_lane_message},
        sql::lowering::{
            LoweredSqlCommand, LoweredSqlQuery, SqlGlobalAggregateCommandCore,
            bind_lowered_sql_explain_global_aggregate_structural,
            bind_lowered_sql_query_structural, bind_lowered_sql_select_query_structural,
        },
        sql::parser::SqlExplainMode,
    },
    traits::CanisterKind,
};

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
        let lane = session_sql_lane(lowered);
        if lane != crate::db::session::sql::surface::SqlLaneKind::Explain {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::Explain,
                lane,
            )));
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
            "shared EXPLAIN dispatch could not classify lowered SQL shape",
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
        let explain = plan.explain_with_model(authority.model());
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
        let LoweredSqlQuery::Select(select) = query else {
            return Ok(None);
        };

        let structural = bind_lowered_sql_select_query_structural(
            authority.model(),
            select.clone(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (_, mut plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
        authority.finalize_static_planning_shape(&mut plan);
        let descriptor = assemble_load_execution_node_descriptor(
            authority.fields(),
            authority.primary_key_name(),
            &plan,
        )
        .map_err(QueryError::execute)?;

        Ok(Some(descriptor.render_text_tree()))
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
        let strategy = command
            .prepared_scalar_strategy_with_model(model)
            .map_err(QueryError::from_sql_lowering_error)?;

        match mode {
            SqlExplainMode::Plan => Ok(command
                .query()
                .build_plan_with_visible_indexes(&visible_indexes)
                .map(|mut plan| {
                    authority.finalize_static_planning_shape(&mut plan);
                    plan
                })?
                .explain_with_model(model)
                .render_text_canonical()),
            SqlExplainMode::Execution => {
                let mut plan = command
                    .query()
                    .build_plan_with_visible_indexes(&visible_indexes)?;
                authority.finalize_static_planning_shape(&mut plan);
                let query_explain = plan.explain_with_model(model);
                let execution = assemble_prepared_sql_scalar_aggregate_execution_descriptor(
                    &plan,
                    &strategy,
                    AggregateRouteShape::new_from_fields(
                        strategy.aggregate_kind(),
                        strategy.projected_field(),
                        model.fields(),
                        model.primary_key().name(),
                    ),
                );
                let terminal_plan = ExplainAggregateTerminalPlan::new(
                    query_explain,
                    strategy.aggregate_kind(),
                    execution,
                );

                Ok(terminal_plan.execution_node_descriptor().render_text_tree())
            }
            SqlExplainMode::Json => Ok(command
                .query()
                .build_plan_with_visible_indexes(&visible_indexes)
                .map(|mut plan| {
                    authority.finalize_static_planning_shape(&mut plan);
                    plan
                })?
                .explain_with_model(model)
                .render_json_canonical()),
        }
    }
}
