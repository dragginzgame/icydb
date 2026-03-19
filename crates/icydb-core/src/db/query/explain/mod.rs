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
#[cfg(test)]
mod test_support;
mod writer;

pub use execution::*;
pub use plan::*;

///
/// TESTS
///

#[cfg(test)]
mod tests;
