//! Module: types::identity
//! Responsibility: module-local ownership and contracts for types::identity.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod entity_tag;
mod generate_key;
mod id;
mod projection;

pub use entity_tag::*;
pub use generate_key::*;
pub use id::*;
pub use projection::*;
