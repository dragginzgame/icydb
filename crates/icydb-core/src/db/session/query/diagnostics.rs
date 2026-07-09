//! Module: db::session::query::diagnostics
//! Responsibility: diagnostics-only query execution attribution.
//! Does not own: normal execution dispatch, cursor handling, fluent adaptation, or explain surfaces.
//! Boundary: measures the existing execution path and shapes public attribution counters.

mod execution;
mod model;

pub(in crate::db::session::query) use model::QueryAttributionCommon;
pub use model::{
    DirectDataRowAttribution, FluentTerminalExecutionAttribution, GroupedCountAttribution,
    GroupedExecutionAttribution, KernelRowAttribution, QueryExecutionAttribution,
    ScalarAggregateAttribution,
};
