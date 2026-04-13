//! Module: executor::projection::eval::operators
//! Responsibility: unary/binary expression operator evaluation for projection eval.
//! Does not own: row field resolution or grouped aggregate index resolution.
//! Boundary: pure operator semantics for scalar and grouped projection evaluation.

mod binary;
#[cfg(test)]
mod unary;

pub(in crate::db) use binary::eval_binary_expr;
#[cfg(test)]
pub(in crate::db::executor) use unary::eval_unary_expr;
