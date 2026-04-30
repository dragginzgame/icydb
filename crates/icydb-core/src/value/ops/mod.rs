//! Module: value::ops
//!
//! Responsibility: behavior-oriented operations over the `Value` representation.
//! Does not own: the `Value` enum shape or persistence encoding.
//! Boundary: text, collection, numeric, and ordering helpers.

pub mod collection;
pub mod numeric;
pub mod ordering;
pub mod partial_ord;
pub mod text;
