//! Module: db::session::sql::explain
//! Responsibility: module-local ownership and contracts for db::session::sql::explain.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        MissingRowPolicy, QueryError,
        query::builder::aggregate::AggregateExpr,
        query::plan::{FieldSlot, resolve_aggregate_target_field_slot},
        session::sql::surface::{SqlSurface, session_sql_lane, unsupported_sql_lane_message},
        sql::lowering::{
            LoweredSqlCommand, SqlGlobalAggregateCommandCore, SqlGlobalAggregateTerminal,
            bind_lowered_sql_explain_global_aggregate_structural,
            render_lowered_sql_explain_plan_or_json,
        },
        sql::parser::SqlExplainMode,
    },
    model::EntityModel,
};

// Resolve one aggregate target field through planner slot contracts before
// aggregate terminal execution.
fn resolve_sql_aggregate_target_slot_with_model(
    model: &'static EntityModel,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    resolve_aggregate_target_field_slot(model, field)
}

pub(in crate::db::session::sql) fn resolve_sql_aggregate_target_slot<E>(
    field: &str,
) -> Result<FieldSlot, QueryError>
where
    E: crate::traits::EntityKind,
{
    resolve_sql_aggregate_target_slot_with_model(E::MODEL, field)
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

impl LoweredSqlCommand {
    /// Render this lowered SQL command through the shared EXPLAIN surface for
    /// one concrete model authority.
    #[inline(never)]
    pub fn explain_for_model(&self, model: &'static EntityModel) -> Result<String, QueryError> {
        // First validate lane selection once on the shared lowered-command path
        // so explain callers do not rebuild lane guards around the same shape.
        let lane = session_sql_lane(self);
        if lane != crate::db::session::sql::surface::SqlLaneKind::Explain {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::Explain,
                lane,
            )));
        }

        // Then prefer the structural renderer because plan/json explain output
        // can stay generic-free all the way to the final render step.
        if let Some(rendered) =
            render_lowered_sql_explain_plan_or_json(self, model, MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)?
        {
            return Ok(rendered);
        }

        // Structural global aggregate explain is the remaining explain-only
        // shape that still needs dedicated aggregate descriptor rendering.
        if let Some((mode, command)) = bind_lowered_sql_explain_global_aggregate_structural(
            self,
            model,
            MissingRowPolicy::Ignore,
        ) {
            return explain_sql_global_aggregate_structural(mode, command);
        }

        Err(QueryError::unsupported_query(
            "shared EXPLAIN dispatch could not classify lowered SQL shape",
        ))
    }
}

// Render one EXPLAIN payload for constrained global aggregate SQL command
// entirely through structural query and descriptor authority.
#[inline(never)]
fn explain_sql_global_aggregate_structural(
    mode: SqlExplainMode,
    command: SqlGlobalAggregateCommandCore,
) -> Result<String, QueryError> {
    let model = command.query().model();

    match mode {
        SqlExplainMode::Plan => {
            let _ = sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

            Ok(command
                .query()
                .build_plan()?
                .explain_with_model(model)
                .render_text_canonical())
        }
        SqlExplainMode::Execution => {
            let aggregate =
                sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;
            let plan = command.query().explain_aggregate_terminal(aggregate)?;

            Ok(plan.execution_node_descriptor().render_text_tree())
        }
        SqlExplainMode::Json => {
            let _ = sql_global_aggregate_terminal_to_expr_with_model(model, command.terminal())?;

            Ok(command
                .query()
                .build_plan()?
                .explain_with_model(model)
                .render_json_canonical())
        }
    }
}
