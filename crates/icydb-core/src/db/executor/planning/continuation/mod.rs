//! Module: executor::planning::continuation
//! Responsibility: executor-owned continuation runtime contracts and load-cursor resolution.
//! Does not own: planner cursor semantics, query policy validation, or cursor wire codec rules.
//! Boundary: load entrypoints consume this module for prepared cursor resolution and continuation runtime state.

mod capabilities;
mod engine;
mod grouped;
mod route;
mod scalar;

pub(in crate::db::executor) use capabilities::ContinuationCapabilities;
pub(in crate::db::executor) use engine::{LoadCursorInput, LoadCursorResolver, PreparedLoadCursor};
pub(in crate::db::executor) use grouped::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
};
pub(in crate::db::executor) use route::{AccessWindow, ContinuationMode, RouteContinuationPlan};
pub(in crate::db::executor) use scalar::{
    ResolvedScalarContinuationContext, ScalarContinuationBindings, ScalarContinuationContext,
};
