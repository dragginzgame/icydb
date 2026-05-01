//! Module: query::plan::expr::predicate
//! Responsibility: predicate bridge, predicate compilation, and predicate program contracts.
//! Does not own: boolean canonicalization, type inference, or projection evaluation.
//! Boundary: all predicate-shaped expression crossings are exported from this module root.

mod bridge;
mod compile;
mod compiled;

pub(in crate::db) use bridge::normalized_bool_expr_from_predicate;
#[cfg(test)]
pub(in crate::db) use bridge::{
    canonicalize_runtime_predicate_via_bool_expr, predicate_to_runtime_bool_expr_for_test,
};
pub(in crate::db) use compile::derive_normalized_bool_expr_predicate_subset;
#[cfg(test)]
pub(in crate::db) use compile::{
    compile_canonical_bool_expr_to_compiled_predicate, compile_normalized_bool_expr_to_predicate,
};
pub(in crate::db) use compiled::CompiledPredicate;
