//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod capabilities;
mod codec;
mod describe;
mod errors;
mod fingerprint;
mod format;
mod identity;
mod info;
mod integrity;
mod layout;
mod mutation;
mod proposal;
mod reconcile;
mod runtime;
mod snapshot;
mod store;
mod transition;
mod types;

pub use describe::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
};
pub use errors::ValidateError;

pub(in crate::db) use capabilities::{SqlCapabilities, sql_capabilities};
pub(in crate::db::schema) use codec::{
    decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
};
pub(in crate::db) use describe::{
    describe_entity_fields, describe_entity_fields_with_persisted_schema, describe_entity_model,
    describe_entity_model_with_persisted_schema,
};
pub(in crate::db) use fingerprint::{
    accepted_commit_schema_fingerprint, accepted_schema_cache_fingerprint,
};
pub(in crate::db) use format::{show_indexes_for_model, show_indexes_for_model_with_runtime_state};
pub(in crate::db) use identity::FieldId;
pub(in crate::db) use info::{
    SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo, SchemaIndexFieldPathInfo,
    SchemaIndexInfo, SchemaInfo,
};
pub(in crate::db::schema) use integrity::{
    schema_snapshot_index_integrity_detail, schema_snapshot_integrity_detail,
};
pub(in crate::db) use layout::{SchemaFieldSlot, SchemaRowLayout, SchemaVersion};
#[cfg(test)]
pub(in crate::db::schema) use mutation::AcceptedSchemaMutationError;
#[cfg(test)]
pub(in crate::db::schema) use mutation::SchemaMutation;
#[cfg(test)]
pub(in crate::db::schema) use mutation::SchemaRebuildAction;
#[cfg(test)]
pub(in crate::db::schema) use mutation::{MutationCompatibility, RebuildRequirement};
pub(in crate::db::schema) use mutation::{
    MutationPlan, MutationPublicationBlocker, MutationPublicationPreflight,
    MutationPublicationStatus, SchemaFieldPathIndexRebuildRow, SchemaFieldPathIndexRunner,
    SchemaFieldPathIndexRunnerFailure, SchemaMutationAcceptedSnapshotPublicationSink,
    SchemaMutationDeveloperReport, SchemaMutationExecutionPlan, SchemaMutationPublishStatus,
    SchemaMutationRequest, SchemaMutationRunnerCapability, SchemaMutationRunnerContract,
    SchemaMutationRunnerInput, SchemaMutationRunnerPhase, SchemaMutationRuntimeEpoch,
    SchemaMutationRuntimeInvalidationSink, SchemaMutationSupportedExecutionPath,
    SchemaMutationSupportedPathRejection, SchemaMutationValidationStatus,
    schema_mutation_request_for_snapshots,
};
pub(in crate::db) use mutation::{
    SchemaFieldPathIndexRebuildKey, SchemaFieldPathIndexRebuildTarget,
};
#[cfg(test)]
pub(in crate::db::schema) use mutation::{SchemaMutationDelta, classify_schema_mutation_delta};
pub(in crate::db) use proposal::compiled_schema_proposal_for_model;
pub(in crate::db) use reconcile::{ensure_accepted_schema_snapshot, reconcile_runtime_schemas};
pub(in crate::db) use runtime::{
    AcceptedFieldAbsencePolicy, AcceptedFieldDecodeContract, AcceptedGeneratedCompatibleRowShape,
    AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeDescriptor, AcceptedRowLayoutRuntimeField,
};
pub(in crate::db) use snapshot::{
    AcceptedSchemaSnapshot, PersistedEnumVariant, PersistedFieldKind, PersistedFieldSnapshot,
    PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    PersistedNestedLeafSnapshot, PersistedRelationStrength, PersistedSchemaSnapshot,
    SchemaFieldDefault, SchemaFieldWritePolicy,
};
pub use store::SchemaStore;
pub(in crate::db::schema) use transition::{
    SchemaTransitionDecision, SchemaTransitionPlanKind, decide_schema_transition,
};
pub(crate) use types::{FieldType, ScalarType, field_type_from_model_kind, literal_matches_type};
pub(in crate::db) use types::{
    canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
};
