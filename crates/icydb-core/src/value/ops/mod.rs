//! Module: value::ops
//!
//! Responsibility: behavior-oriented operations over the `Value` representation.
//! Does not own: the `Value` enum shape or persistence encoding.
//! Boundary: text, collection, numeric, and ordering helpers.

mod collection;
pub(crate) mod numeric;
pub(crate) mod ordering;
mod partial_ord;
mod text;
