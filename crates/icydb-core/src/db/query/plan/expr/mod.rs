//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.

mod aggregate_input;
mod ast;
mod canonicalize;
mod field_kind_semantics;
mod function_semantics;
mod predicate_compile;
mod preview;
mod projection;
mod scalar;
mod type_inference;

pub(in crate::db) use aggregate_input::*;
pub(crate) use ast::*;
pub(in crate::db) use canonicalize::*;
pub(in crate::db) use field_kind_semantics::*;
pub(crate) use function_semantics::*;
pub(in crate::db) use predicate_compile::*;
pub(in crate::db) use preview::*;
pub(crate) use projection::*;
pub(in crate::db) use scalar::*;
pub(crate) use type_inference::*;
