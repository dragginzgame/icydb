//! Module: query::expr
//! Responsibility: schema-agnostic filter/order expression wrappers and lowering.
//! Does not own: planner route selection or executor evaluation.
//! Boundary: intent boundary lowers these to validated predicate/order forms.

mod filter;
mod order;

pub use filter::{FilterExpr, FilterValue};
pub use order::{OrderExpr, OrderTerm, asc, desc, field};
