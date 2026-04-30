//! Module: executor::projection::eval::operators
//! Responsibility: compatibility adapter for compiled expression operator evaluation.
//! Does not own: row field resolution, grouped aggregate index resolution, or scalar semantics.
//! Boundary: scalar projection evaluation delegates operator semantics to the unified compiled expression engine.

pub(in crate::db) use crate::db::query::plan::expr::{
    evaluate_binary_expr as eval_binary_expr, evaluate_unary_expr as eval_unary_expr,
};
