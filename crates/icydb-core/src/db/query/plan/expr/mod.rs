//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.

mod ast;
mod projection;
mod type_inference;

pub(crate) use ast::*;
pub(crate) use projection::*;
pub(crate) use type_inference::*;
