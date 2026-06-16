//! Module: lib
//! Responsibility: small shared utility surface for workspace crates.
//! Does not own: runtime database contracts, schema authority, or codegen policy.
//! Boundary: re-exports focused helpers without widening downstream dependencies.

mod case;

pub use case::{Case, Casing, to_snake_case};
