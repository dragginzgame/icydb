//! Shared execution/query contracts for `db` subsystems.
//!
//! This module defines boundary-safe DTOs and policy enums consumed by both
//! query planning and executor runtime without requiring executor imports from
//! `db::query::*`.
//!
//! Contract extraction goals:
//! - keep layering direction explicit
//! - centralize shared semantic contracts
//! - prevent cross-layer namespace leakage

mod consistency;
mod predicate_model;
mod predicate_schema;

pub use consistency::ReadConsistency;
#[cfg(test)]
pub(crate) use predicate_model::ScalarType;
pub use predicate_model::{CoercionId, UnsupportedQueryFeature};
pub(crate) use predicate_model::{FieldType, literal_matches_type};
pub(crate) use predicate_schema::SchemaInfo;
pub use predicate_schema::ValidateError;
