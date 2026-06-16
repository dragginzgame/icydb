//! Module: base::sanitizer::intl
//!
//! Responsibility: base sanitizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade sanitizer traits.

pub mod iso;
pub mod phone;
