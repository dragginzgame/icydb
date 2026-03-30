//! Module: db::session::sql::computed_projection
//! Responsibility: module-local ownership and contracts for db::session::sql::computed_projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod eval;
mod model;
mod plan;

use crate::db::{
    QueryError,
    session::sql::projection::SqlProjectionPayload,
    sql::parser::{SqlExplainMode, SqlStatement},
};

pub(in crate::db::session::sql) use crate::db::session::sql::computed_projection::model::SqlComputedProjectionPlan;

pub(in crate::db::session::sql) fn computed_sql_projection_plan(
    statement: &SqlStatement,
) -> Result<Option<SqlComputedProjectionPlan>, QueryError> {
    crate::db::session::sql::computed_projection::plan::computed_sql_projection_plan(statement)
}

pub(in crate::db::session::sql) fn computed_sql_projection_explain_plan(
    statement: &SqlStatement,
) -> Result<Option<(SqlExplainMode, SqlComputedProjectionPlan)>, QueryError> {
    crate::db::session::sql::computed_projection::plan::computed_sql_projection_explain_plan(
        statement,
    )
}

pub(in crate::db::session::sql) fn apply_computed_sql_projection_payload(
    payload: SqlProjectionPayload,
    plan: &SqlComputedProjectionPlan,
) -> Result<SqlProjectionPayload, QueryError> {
    crate::db::session::sql::computed_projection::eval::apply_computed_sql_projection_payload(
        payload, plan,
    )
}
