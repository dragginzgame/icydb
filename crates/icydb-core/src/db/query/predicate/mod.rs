//! Module: db::query::predicate
//! Responsibility: query-owned predicate validation against schema metadata.
//! Does not own: predicate AST definitions, runtime predicate evaluation, or index key encoding.
//! Boundary: query planning consumes this to validate predicate semantics with `SchemaInfo`.

mod validate;

pub(in crate::db) use validate::{reject_unsupported_query_features, validate_predicate};
