//! Cursor protocol components split from query plan internals.

pub(crate) mod anchor;
pub(crate) mod continuation;
pub(crate) mod spine;

pub(crate) use crate::db::query::{
    explain::ExplainPlan,
    plan::{
        CursorBoundary, CursorBoundarySlot, CursorPlanError, Direction, LogicalPlan,
        OrderPlanError, OrderSpec, PlanError,
    },
};

pub(super) fn encode_plan_hex(bytes: &[u8]) -> String {
    crate::db::codec::cursor::encode_cursor(bytes)
}
