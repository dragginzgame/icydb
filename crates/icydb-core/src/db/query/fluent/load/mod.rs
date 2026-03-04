//! Module: query::fluent::load
//! Responsibility: fluent load-query builder, pagination, and execution routing.
//! Does not own: planner semantics or row-level predicate evaluation.
//! Boundary: session API facade over query intent/planning/execution.

mod builder;
mod pagination;
mod terminals;
mod validation;

pub use builder::FluentLoadQuery;
pub use pagination::PagedLoadQuery;
