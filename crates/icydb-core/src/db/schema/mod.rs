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
    EntityRelationDescription, EntityRelationStrength, EntitySchemaCheckDescription,
    EntitySchemaDescription,
};
pub use errors::ValidateError;

pub(in crate::db) use capabilities::{SqlCapabilities, sql_capabilities};
pub(in crate::db) use codec::{decode_persisted_schema_snapshot, encode_persisted_schema_snapshot};
#[cfg(test)]
pub(in crate::db) use codec::{
    persisted_schema_snapshot_decode_count_for_tests,
    reset_persisted_schema_snapshot_decode_count_for_tests,
};
pub(in crate::db) use describe::{
    describe_entity_fields, describe_entity_fields_with_persisted_schema, describe_entity_model,
    describe_entity_model_with_persisted_schema,
};
pub(in crate::db) use fingerprint::{
    accepted_commit_schema_fingerprint, accepted_schema_cache_fingerprint,
    accepted_schema_cache_fingerprint_for_persisted_snapshot,
    accepted_schema_cache_fingerprint_method_version,
};
pub(in crate::db::schema) use fingerprint::{
    accepted_schema_admission_fingerprint, accepted_schema_admission_fingerprint_method_version,
};
pub(in crate::db) use format::{
    show_indexes_for_model, show_indexes_for_model_with_runtime_state,
    show_indexes_for_schema_info_with_runtime_state,
};
pub(in crate::db) use identity::FieldId;
pub(in crate::db) use info::{
    SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo, SchemaIndexFieldPathInfo,
    SchemaIndexInfo, SchemaInfo,
};
#[cfg(test)]
pub(in crate::db) use info::{
    accepted_schema_info_projection_count_for_tests,
    reset_accepted_schema_info_projection_count_for_tests,
};
pub(in crate::db::schema) use integrity::{
    schema_snapshot_index_integrity_detail, schema_snapshot_integrity_detail,
    schema_snapshot_relation_integrity_detail,
};
pub(in crate::db) use layout::{SchemaFieldSlot, SchemaRowLayout, SchemaVersion};
#[cfg(test)]
pub(in crate::db::schema) use mutation::AcceptedSchemaMutationError;
#[cfg(test)]
pub(in crate::db) use mutation::SchemaDdlSchemaVersionAdmissionError;
#[cfg(test)]
pub(in crate::db::schema) use mutation::SchemaMutation;
#[cfg(test)]
pub(in crate::db::schema) use mutation::SchemaRebuildAction;
#[cfg(test)]
pub(in crate::db::schema) use mutation::{MutationCompatibility, RebuildRequirement};
pub(in crate::db::schema) use mutation::{
    MutationPlan, MutationPublicationBlocker, MutationPublicationPreflight,
    MutationPublicationStatus, SchemaExpressionIndexRebuildRow, SchemaExpressionIndexStagedEntry,
    SchemaExpressionIndexStagedRebuild, SchemaFieldPathIndexRebuildRow, SchemaFieldPathIndexRunner,
    SchemaFieldPathIndexRunnerFailure, SchemaFieldPathIndexRunnerReport,
    SchemaMutationAcceptedSnapshotPublicationSink, SchemaMutationDeveloperReport,
    SchemaMutationExecutionPlan, SchemaMutationExecutionStep, SchemaMutationPublishStatus,
    SchemaMutationRequest, SchemaMutationRunnerCapability, SchemaMutationRunnerContract,
    SchemaMutationRunnerInput, SchemaMutationRunnerPhase, SchemaMutationRuntimeEpoch,
    SchemaMutationRuntimeInvalidationSink, SchemaMutationSupportedExecutionPath,
    SchemaMutationSupportedPathRejection, SchemaMutationValidationStatus,
    schema_mutation_request_for_snapshots,
};
pub(in crate::db) use mutation::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlFieldAdditionCandidateError,
    SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
    SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
    SchemaDdlFieldTypeContract, SchemaDdlIndexDropCandidateError, SchemaDdlMutationAdmissionError,
    SchemaDdlSecondaryIndexAdditionCandidate, SchemaDdlSecondaryIndexAdditionCandidateError,
    SchemaDdlSecondaryIndexExpressionIntent, SchemaDdlSecondaryIndexFieldPathIntent,
    SchemaDdlSecondaryIndexKeyCandidateError, SchemaDdlSecondaryIndexKeyIntent,
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
    SchemaExpressionIndexRebuildTarget, SchemaFieldDefaultTarget, SchemaFieldDropTarget,
    SchemaFieldNullabilityTarget, SchemaFieldPathIndexRebuildKey,
    SchemaFieldPathIndexRebuildTarget, SchemaFieldRenameTarget,
    SchemaSecondaryIndexDropCleanupTarget, build_sql_ddl_field_addition_candidate,
    build_sql_ddl_secondary_index_candidate, derive_sql_ddl_expression_index_accepted_after,
    derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
    derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
    derive_sql_ddl_field_path_index_accepted_after, derive_sql_ddl_field_rename_accepted_after,
    derive_sql_ddl_secondary_index_drop_accepted_after, encode_sql_ddl_add_column_default,
    encode_sql_ddl_alter_column_default, resolve_sql_ddl_field_addition_name_candidate,
    resolve_sql_ddl_field_drop_candidate, resolve_sql_ddl_field_drop_default_candidate,
    resolve_sql_ddl_field_nullability_candidate, resolve_sql_ddl_field_rename_candidate,
    resolve_sql_ddl_field_set_default_candidate, resolve_sql_ddl_field_type_contract,
    resolve_sql_ddl_secondary_index_addition_candidate,
    resolve_sql_ddl_secondary_index_drop_candidate,
};
#[cfg(test)]
pub(in crate::db) use mutation::{
    SchemaDdlMutationAdmission, admit_sql_ddl_expression_index_candidate,
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_path_index_candidate, admit_sql_ddl_field_rename_candidate,
    admit_sql_ddl_secondary_index_drop_candidate,
};
#[cfg(test)]
pub(in crate::db::schema) use mutation::{SchemaMutationDelta, classify_schema_mutation_delta};
pub(in crate::db) use proposal::compiled_schema_proposal_for_model;
pub(in crate::db) use reconcile::{
    ensure_accepted_schema_snapshot, execute_sql_ddl_expression_index_addition,
    execute_sql_ddl_field_addition, execute_sql_ddl_field_default_change,
    execute_sql_ddl_field_drop, execute_sql_ddl_field_nullability_change,
    execute_sql_ddl_field_path_index_addition, execute_sql_ddl_field_rename,
    execute_sql_ddl_secondary_index_drop, reconcile_runtime_schemas,
};
pub(in crate::db) use runtime::{
    AcceptedFieldAbsencePolicy, AcceptedFieldDecodeContract,
    AcceptedGeneratedRowCompatibilityProof, AcceptedRowDecodeContract,
    AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
    OwnedAcceptedFieldDecodeContract, OwnedAcceptedRelationEdgeContract,
};
#[cfg(test)]
pub(in crate::db) use runtime::{
    generated_compatible_row_layout_proof_count_for_tests,
    reset_generated_compatible_row_layout_proof_count_for_tests,
};
pub(in crate::db) use snapshot::{
    AcceptedSchemaSnapshot, PersistedEnumVariant, PersistedFieldKind, PersistedFieldOrigin,
    PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
    PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
    PersistedIndexOrigin, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
    PersistedRelationEdgeSnapshot, PersistedRelationStrength, PersistedSchemaSnapshot,
    SchemaFieldDefault, SchemaFieldWritePolicy,
};
pub use store::SchemaStore;
pub(in crate::db) use store::{
    AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, MAX_SCHEMA_SNAPSHOT_BYTES,
    SchemaStoreAllocationMetadata, SchemaStoreCatalogMetadata,
};
#[cfg(test)]
pub(in crate::db) use store::{
    latest_raw_snapshots_by_entity_call_count_for_tests,
    reset_latest_raw_snapshots_by_entity_call_count_for_tests,
};
pub(in crate::db::schema) use transition::{
    SchemaTransitionDecision, SchemaTransitionPlanKind, decide_schema_transition,
};
pub(crate) use types::{FieldType, ScalarType, field_type_from_model_kind, literal_matches_type};
pub(in crate::db) use types::{
    canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
};
