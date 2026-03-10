//! Module: types::float
//! Responsibility: module-local ownership and contracts for types::float.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod float32;
mod float64;

pub use float32::*;
pub use float64::*;
