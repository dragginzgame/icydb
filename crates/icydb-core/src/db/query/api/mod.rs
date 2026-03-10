//! Module: db::query::api
//! Responsibility: module-local ownership and contracts for db::query::api.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Query API helpers that live at the query/session boundary.
//! Boundary rule: cardinality semantics belong here, not on transport DTOs
//! from `db::response`.

mod private;
mod result_ext;

pub use result_ext::ResponseCardinalityExt;
