//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

mod access_projection;
mod execution;
mod json;
mod nodes;
mod plan;
mod render;
mod writer;

pub(in crate::db) use access_projection::{
    explain_access_execution_node_type, explain_access_plan, explain_access_strategy_label,
};
pub use execution::*;
pub use plan::*;

///
/// TESTS
///

#[cfg(test)]
mod tests;
