//! Module: db::cursor::resume
//! Responsibility: module-local ownership and contracts for db::cursor::resume.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::{CursorBoundary, order::apply_cursor_boundary},
        query::plan::{AccessPlannedQuery, OrderSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

/// Apply one strict resume boundary under canonical cursor order semantics.
pub(in crate::db) fn apply_resume_bound<E, R, F>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    boundary: &CursorBoundary,
    entity_of: F,
) where
    E: EntityKind + EntityValue,
    F: Fn(&R) -> &E + Copy,
{
    apply_cursor_boundary::<E, R, F>(rows, order, boundary, entity_of);
}

/// Apply the post-order resume-bound phase for one load execution window.
pub(in crate::db) fn apply_resume_bound_phase<K, E, R, F>(
    plan: &AccessPlannedQuery<K>,
    rows: &mut Vec<R>,
    resume_boundary: Option<&CursorBoundary>,
    ordered: bool,
    rows_after_order: usize,
    entity_of: F,
) -> Result<(bool, usize), InternalError>
where
    E: EntityKind + EntityValue,
    F: Fn(&R) -> &E + Copy,
{
    let logical = plan.scalar_plan();
    if logical.mode.is_load()
        && let Some(resume_boundary) = resume_boundary
    {
        let Some(order) = logical.order.as_ref() else {
            return Err(InternalError::query_executor_invariant(
                "cursor boundary requires ordering",
            ));
        };
        if !ordered {
            return Err(InternalError::query_executor_invariant(
                "cursor boundary must run after ordering",
            ));
        }

        apply_resume_bound::<E, R, F>(rows, order, resume_boundary, entity_of);
        return Ok((true, rows.len()));
    }

    // No resume boundary; preserve post-order cardinality for continuation
    // decisions and diagnostics.
    Ok((false, rows_after_order))
}
