//! Module: executor::load::projection::eval::operators
//! Responsibility: unary/binary expression operator evaluation for projection eval.
//! Does not own: row field resolution or grouped aggregate index resolution.
//! Boundary: pure operator semantics for scalar and grouped projection evaluation.

mod binary;
mod unary;

pub(in crate::db::executor) use binary::eval_binary_expr;
pub(in crate::db::executor) use unary::eval_unary_expr;
