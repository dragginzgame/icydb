//! Module: metrics::sink::counters
//! Responsibility: domain-specific metrics counter mutation helper wiring.
//! Does not own: metrics sink dispatch, span lifetimes, or report/reset APIs.
//! Boundary: re-exports counter helpers used only by the concrete metrics sink.
mod cache;
mod execution;
mod planning;
mod schema;
mod sql;

pub(super) use cache::*;
pub(super) use execution::*;
pub(super) use planning::*;
pub(super) use schema::*;
pub(super) use sql::*;

// Replace one entity-scoped gauge contribution inside an aggregate total. This
// keeps global footprint gauges current even when the same entity reports a
// newer observed size later in the window.
pub(super) const fn replace_gauge_total(total: &mut u64, previous: u64, current: u64) {
    *total = total.saturating_sub(previous).saturating_add(current);
}
