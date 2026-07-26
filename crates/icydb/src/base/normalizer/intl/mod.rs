//! Module: base::normalizer::intl
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

pub mod iso;
pub mod phone;
