//! Module: query::plan::expr::predicate
//! Responsibility: predicate bridge, predicate compilation, and predicate program contracts.
//! Does not own: boolean canonicalization, type inference, or projection evaluation.
//! Boundary: all predicate-shaped expression crossings are exported from this module root.

mod compile;

#[cfg(test)]
pub(in crate::db) use compile::compile_normalized_bool_expr_to_predicate;
pub(in crate::db) use compile::derive_normalized_bool_expr_predicate_subset;
