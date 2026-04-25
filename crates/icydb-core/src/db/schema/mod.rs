//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod describe;
mod errors;
mod fingerprint;
mod format;
mod info;
mod types;

pub use describe::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
};
pub use errors::ValidateError;

pub(in crate::db) use describe::{describe_entity_fields, describe_entity_model};
pub(crate) use fingerprint::{
    commit_schema_fingerprint_for_entity, commit_schema_fingerprint_for_model,
};
pub(in crate::db) use format::{show_indexes_for_model, show_indexes_for_model_with_runtime_state};
pub(crate) use info::SchemaInfo;
pub(crate) use types::{FieldType, field_type_from_model_kind, literal_matches_type};
