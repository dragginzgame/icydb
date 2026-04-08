//! Module: db::session::sql::explain
//! Responsibility: module-local ownership and contracts for db::session::sql::explain.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{
            EntityAuthority, assemble_load_execution_node_descriptor_with_model_and_visible_indexes,
        },
        query::builder::aggregate::AggregateExpr,
        query::intent::StructuralQuery,
        query::plan::{FieldSlot, resolve_aggregate_target_field_slot},
        session::sql::surface::{SqlSurface, session_sql_lane, unsupported_sql_lane_message},
        sql::lowering::{
            LoweredSqlCommand, LoweredSqlQuery, SqlGlobalAggregateCommandCore,
            SqlGlobalAggregateTerminal, apply_lowered_select_shape,
            bind_lowered_sql_explain_global_aggregate_structural,
            bind_lowered_sql_query_structural,
        },
        sql::parser::SqlExplainMode,
    },
    model::EntityModel,
    traits::CanisterKind,
};

// Resolve one aggregate target field through planner slot contracts before
// aggregate explain descriptor assembly.
fn resolve_sql_aggregate_target_slot_with_model(
    model: &'static EntityModel,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    resolve_aggregate_target_field_slot(model, field)
}

// Convert one lowered global SQL aggregate terminal into aggregate expression
// contracts used by aggregate explain execution descriptors.
fn sql_global_aggregate_terminal_to_expr_with_model(
    model: &'static EntityModel,
    terminal: &SqlGlobalAggregateTerminal,
) -> Result<AggregateExpr, QueryError> {
    match terminal {
        SqlGlobalAggregateTerminal::CountRows => Ok(crate::db::query::builder::aggregate::count()),
        SqlGlobalAggregateTerminal::CountField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(crate::db::query::builder::aggregate::count_by(
                field.as_str(),
            ))
        }
        SqlGlobalAggregateTerminal::SumField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(crate::db::query::builder::aggregate::sum(field.as_str()))
        }
        SqlGlobalAggregateTerminal::AvgField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(crate::db::query::builder::aggregate::avg(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MinField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(crate::db::query::builder::aggregate::min_by(field.as_str()))
        }
        SqlGlobalAggregateTerminal::MaxField(field) => {
            let _ = resolve_sql_aggregate_target_slot_with_model(model, field)?;

            Ok(crate::db::query::builder::aggregate::max_by(field.as_str()))
        }
    }
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
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = structural.build_plan_with_visible_indexes(&visible_indexes)?;
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

        let structural = apply_lowered_select_shape(
            StructuralQuery::new(authority.model(), MissingRowPolicy::Ignore),
            select.clone(),
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = structural.build_plan_with_visible_indexes(&visible_indexes)?;
        let descriptor = assemble_load_execution_node_descriptor_with_model_and_visible_indexes(
            authority.model(),
            &visible_indexes,
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

        match mode {
            SqlExplainMode::Plan => {
                let _ =
                    sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

                Ok(command
                    .query()
                    .build_plan_with_visible_indexes(&visible_indexes)?
                    .explain_with_model(model)
                    .render_text_canonical())
            }
            SqlExplainMode::Execution => {
                let aggregate =
                    sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;
                let plan = command
                    .query()
                    .explain_aggregate_terminal_with_visible_indexes(&visible_indexes, aggregate)?;

                Ok(plan.execution_node_descriptor().render_text_tree())
            }
            SqlExplainMode::Json => {
                let _ =
                    sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

                Ok(command
                    .query()
                    .build_plan_with_visible_indexes(&visible_indexes)?
                    .explain_with_model(model)
                    .render_json_canonical())
            }
        }
    }
}
