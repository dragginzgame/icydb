//! Module: gen
//! Responsibility: schema registration tokens.
//! Does not own: node parsing or runtime validation.
//! Boundary: parsed nodes to schema writes.

pub mod implementor;

pub use implementor::*;
