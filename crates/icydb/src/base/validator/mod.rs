//! Module: base::validator
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

pub mod bytes;
pub mod collection;
pub mod decimal;
pub mod hash;
pub mod intl;
pub mod len;
pub mod num;
pub mod text;
pub mod web;
