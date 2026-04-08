//! Module: query::builder
//! Responsibility: fluent field-level predicate construction helpers.
//! Does not own: query intent compilation or planner validation.
//! Boundary: user-facing ergonomic builder layer.

pub(crate) mod aggregate;
pub(crate) mod field;

pub use aggregate::{
    AggregateExpr, avg, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
};
pub(crate) use aggregate::{
    PreparedFluentAggregateExplainStrategy, PreparedFluentExistingRowsTerminalRuntimeRequest,
    PreparedFluentExistingRowsTerminalStrategy, PreparedFluentNumericFieldRuntimeRequest,
    PreparedFluentNumericFieldStrategy, PreparedFluentOrderSensitiveTerminalRuntimeRequest,
    PreparedFluentOrderSensitiveTerminalStrategy, PreparedFluentProjectionRuntimeRequest,
    PreparedFluentProjectionStrategy, PreparedFluentScalarTerminalRuntimeRequest,
    PreparedFluentScalarTerminalStrategy,
};
pub use field::FieldRef;
