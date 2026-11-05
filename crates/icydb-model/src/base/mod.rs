//! Module: base
//!
//! Responsibility: facade module surface.
//! Does not own: database runtime authority.
//! Boundary: keeps public facade shape stable for downstream code.

pub(crate) mod helper;
pub mod normalizer;
pub mod types;
pub mod validator;

#[cfg(all(test, feature = "build", not(target_arch = "wasm32")))]
mod tests;
