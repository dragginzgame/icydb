//! Module: base::normalizer
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

pub mod intl;
pub mod num;
pub mod text;
pub mod web;
