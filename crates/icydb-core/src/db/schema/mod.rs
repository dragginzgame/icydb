//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod codec;
mod describe;
mod errors;
mod fingerprint;
mod format;
mod identity;
mod info;
mod layout;
mod proposal;
mod reconcile;
mod snapshot;
mod store;
mod types;

pub use describe::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
};
pub use errors::ValidateError;

pub(in crate::db) use codec::{decode_persisted_schema_snapshot, encode_persisted_schema_snapshot};
pub(in crate::db) use describe::{
    describe_entity_fields, describe_entity_fields_with_persisted_schema, describe_entity_model,
    describe_entity_model_with_persisted_schema,
};
pub(crate) use fingerprint::{
    commit_schema_fingerprint_for_entity, commit_schema_fingerprint_for_model,
};
pub(in crate::db) use format::{show_indexes_for_model, show_indexes_for_model_with_runtime_state};
pub(in crate::db) use identity::FieldId;
pub(crate) use info::SchemaInfo;
pub(in crate::db) use layout::{SchemaFieldSlot, SchemaRowLayout, SchemaVersion};
pub(in crate::db) use proposal::compiled_schema_proposal_for_model;
pub(in crate::db) use reconcile::{ensure_initial_schema_snapshot, reconcile_runtime_schemas};
pub(in crate::db) use snapshot::{
    PersistedEnumVariant, PersistedFieldKind, PersistedFieldSnapshot, PersistedRelationStrength,
    PersistedSchemaSnapshot, SchemaFieldDefault,
};
pub use store::SchemaStore;
pub(crate) use types::{FieldType, field_type_from_model_kind, literal_matches_type};
