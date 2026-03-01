//! Module: query::builder
//! Responsibility: fluent field-level predicate construction helpers.
//! Does not own: query intent compilation or planner validation.
//! Boundary: user-facing ergonomic builder layer.

pub(crate) mod aggregate;
pub(crate) mod field;

pub use aggregate::{
    AggregateExpr, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
};
pub use field::FieldRef;
