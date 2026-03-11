//! Module: executor::continuation
//! Responsibility: executor-owned continuation cursor resolution and token-emission contracts.
//! Does not own: planner cursor semantics, query policy validation, or cursor wire codec rules.
//! Boundary: load entrypoints consume this module for shape-aware continuation runtime state.

mod capabilities;
mod engine;
mod grouped;
mod route;
mod scalar;

pub(in crate::db::executor) use capabilities::ContinuationCapabilities;
pub(in crate::db::executor) use engine::{
    ContinuationEngine, LoadCursorInput, PreparedLoadCursor, RequestedLoadExecutionShape,
    ResolvedLoadCursorContext,
};
pub(in crate::db::executor) use grouped::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
};
pub(in crate::db::executor) use route::{AccessWindow, ContinuationMode, RouteContinuationPlan};
pub(in crate::db::executor) use scalar::{
    ResolvedScalarContinuationContext, ScalarContinuationBindings, ScalarContinuationContext,
    ScalarRouteContinuationInvariantProjection,
};
