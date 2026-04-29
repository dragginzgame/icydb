//! Module: query::builder
//! Responsibility: fluent field-level predicate construction helpers.
//! Does not own: query intent compilation or planner validation.
//! Boundary: user-facing ergonomic builder layer.

pub(crate) mod aggregate;
pub(crate) mod field;
pub(crate) mod numeric_projection;
pub(crate) mod scalar_projection;
pub(crate) mod text_projection;

pub(crate) use aggregate::{
    AggregateExplain, ExistingRowsRequest, ExistingRowsTerminalStrategy, NumericFieldStrategy,
    OrderSensitiveTerminalStrategy, ProjectionRequest, ProjectionStrategy, ScalarTerminalStrategy,
};
pub use aggregate::{
    AggregateExpr, avg, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
};
#[cfg(test)]
pub(crate) use aggregate::{NumericFieldRequest, OrderRequest};
pub use field::FieldRef;
pub use numeric_projection::{
    NumericProjectionExpr, RoundProjectionExpr, add, div, mul, round, round_expr, sub,
};
pub use scalar_projection::ValueProjectionExpr;
pub use text_projection::{
    TextProjectionExpr, contains, ends_with, left, length, lower, ltrim, position, replace, right,
    rtrim, starts_with, substring, substring_with_length, trim, upper,
};
