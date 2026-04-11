//! Module: db::query::api
//! Responsibility: query/session-facing response helpers that define cardinality
//! semantics for canonical response DTOs.
//! Does not own: transport DTO definitions or executor/runtime behavior.
//! Boundary: keeps query-layer response semantics out of lower transport modules.

mod result_ext;

pub use result_ext::ResponseCardinalityExt;
