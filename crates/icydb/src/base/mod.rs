//! Module: base
//!
//! Responsibility: facade module surface.
//! Does not own: core runtime ownership.
//! Boundary: keeps public facade shape stable for downstream code.

pub(crate) mod helper;
pub mod sanitizer;
pub mod types;
pub mod validator;
