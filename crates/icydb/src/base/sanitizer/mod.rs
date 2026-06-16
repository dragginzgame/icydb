//! Module: base::sanitizer
//!
//! Responsibility: base sanitizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade sanitizer traits.

pub mod intl;
pub mod num;
pub mod text;
pub mod time;
pub mod web;
