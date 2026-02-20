//! Cursor protocol components split from query plan internals.

pub(crate) mod continuation;
pub(crate) mod cursor_anchor;
pub(crate) mod cursor_spine;

pub(crate) use crate::db::query::{
    explain::ExplainPlan,
    plan::{
        CursorBoundary, CursorBoundarySlot, CursorPlanError, Direction, LogicalPlan,
        OrderPlanError, OrderSpec, PlanError,
    },
};

pub(super) fn encode_plan_hex(bytes: &[u8]) -> String {
    crate::db::cursor::encode_cursor(bytes)
}
