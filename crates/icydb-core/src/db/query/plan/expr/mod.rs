//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.

mod ast;
mod canonicalize;
mod preview;
mod projection;
mod scalar;
mod type_inference;

pub(crate) use ast::*;
pub(in crate::db) use canonicalize::*;
pub(in crate::db) use preview::*;
pub(crate) use projection::*;
pub(in crate::db) use scalar::*;
pub(crate) use type_inference::*;
