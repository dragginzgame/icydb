//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders.
//! Does not own: aggregate validation policy or executor fold semantics.
//! Boundary: fluent aggregate intent construction lowered into grouped specs.

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
