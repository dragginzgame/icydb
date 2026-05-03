//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod describe;
mod errors;
mod fingerprint;
mod format;
mod identity;
mod info;
mod layout;
mod proposal;
mod snapshot;
mod store;
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
pub(in crate::db) use identity::FieldId;
pub(crate) use info::SchemaInfo;
pub(in crate::db) use layout::{SchemaFieldSlot, SchemaRowLayout, SchemaVersion};
pub(in crate::db) use proposal::compiled_schema_proposal_for_model;
pub(in crate::db) use snapshot::{
    PersistedFieldKind, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
};
pub use store::SchemaStore;
pub(crate) use types::{FieldType, field_type_from_model_kind, literal_matches_type};
