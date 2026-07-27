//! Module: db::session::query::diagnostics
//! Responsibility: diagnostics-only query execution attribution.
//! Does not own: normal execution dispatch, cursor handling, fluent adaptation, or explain surfaces.
//! Boundary: measures the existing execution path and shapes public attribution counters.

mod model;

pub use model::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};
