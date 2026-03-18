//! Module: db::executor::route::hints
//! Responsibility: route-owned bounded-fetch and scan-budget hint derivation.
//! Does not own: route capability derivation or dispatch execution.
//! Boundary: emits optional hints consumed by stream/runtime surfaces.

mod aggregate;
mod load;

pub(in crate::db::executor::route) use aggregate::{
    aggregate_probe_fetch_hint_for_model, aggregate_seek_spec_for_model, count_pushdown_fetch_hint,
};
#[cfg(test)]
pub(in crate::db::executor) use load::residual_predicate_pushdown_fetch_cap;
pub(in crate::db::executor::route) use load::{
    assess_index_range_limit_pushdown_for_model, bounded_probe_hint_is_safe, load_scan_budget_hint,
    top_n_seek_spec_for_model,
};
