//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders plus
//! fluent terminal descriptors that project aggregate-like query results.
//! Does not own: aggregate validation policy, executor fold semantics, or
//! session execution wiring.
//! Boundary: fluent aggregate intent construction and terminal descriptors
//! lowered into query/session-owned execution contracts.

pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod strategy;

///
/// TESTS
///

#[cfg(test)]
mod tests;

pub(crate) use explain::*;
pub use expr::{
    AggregateExpr, avg, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
};
pub(crate) use strategy::*;
