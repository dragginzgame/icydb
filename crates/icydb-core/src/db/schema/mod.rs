//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod accepted_field_kind;
mod accepted_value_admission;
pub(in crate::db) mod authored_projection;
mod capabilities;
mod codec;
mod describe;
pub(in crate::db) mod enum_catalog;
mod errors;
mod field_kind_semantics;
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
    EntityRelationDescription, EntitySchemaCheckDescription, EntitySchemaDescription,
};
pub use errors::{SchemaLiteralValidationReason, SchemaValidationOperator, ValidateError};

pub(in crate::db) use accepted_field_kind::AcceptedFieldKind;
pub(in crate::db) use accepted_value_admission::AcceptedValueAdmissionContract;
pub(in crate::db) use capabilities::sql_capabilities;
#[cfg(feature = "sql")]
pub(in crate::db) use capabilities::{
    SqlCapabilities, sql_capabilities_for_model_kind, sql_capabilities_with_enum_catalog,
};
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
pub(in crate::db) use enum_catalog::{
    AcceptedEnumCatalog, AcceptedEnumCatalogHandle, AcceptedSchemaAuthority,
    AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedValueContract,
    CandidateSchemaRevision, ValueAdmissionBudget, encode_unit_enum_equality_key,
    output_value_from_runtime,
};
#[cfg(test)]
pub(in crate::db) use enum_catalog::{
    build_initial_accepted_enum_catalog_from_kinds_for_tests,
    empty_accepted_schema_candidate_for_tests,
};
pub(in crate::db) use field_kind_semantics::{
    AcceptedFieldKindCategory, AcceptedFieldKindSemantics, AcceptedScalarClass,
    classify_accepted_field_kind,
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
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use mutation::SchemaDdlSchemaVersionAdmissionError;
pub(in crate::db::schema) use mutation::{
    MutationPlan, MutationPublicationPreflight, SchemaMutationRequest,
    schema_mutation_request_for_snapshots,
};
#[cfg(feature = "sql")]
pub(in crate::db) use mutation::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlFieldAdditionCandidateError,
    SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
    SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
    SchemaDdlFieldTypeContract, SchemaDdlIndexDropCandidateError, SchemaDdlMutationAdmissionError,
    SchemaDdlSecondaryIndexAdditionCandidate, SchemaDdlSecondaryIndexAdditionCandidateError,
    SchemaDdlSecondaryIndexExpressionIntent, SchemaDdlSecondaryIndexExpressionOpIntent,
    SchemaDdlSecondaryIndexFieldPathIntent, SchemaDdlSecondaryIndexKeyCandidateError,
    SchemaDdlSecondaryIndexKeyIntent, SchemaDdlVersionContractPreflightError,
    SchemaFieldDefaultTarget, SchemaFieldDropTarget, SchemaFieldNullabilityTarget,
    SchemaFieldRenameTarget, build_sql_ddl_field_addition_candidate,
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
    resolve_sql_ddl_secondary_index_drop_candidate, validate_schema_ddl_version_contract_preflight,
    validate_sql_ddl_field_default_change_candidate,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use mutation::{
    SchemaDdlMutationAdmission, admit_sql_ddl_expression_index_candidate,
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_path_index_candidate, admit_sql_ddl_field_rename_candidate,
    admit_sql_ddl_secondary_index_drop_candidate,
};
pub(in crate::db) use mutation::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
    SchemaExpressionIndexRebuildTarget,
};
pub(in crate::db) use mutation::{
    SchemaFieldPathIndexRebuildKey, SchemaFieldPathIndexRebuildTarget, SchemaUserIndexDomainRow,
    StagedUserIndexDomainError, StagedUserIndexDomainReplacement,
};
#[cfg(test)]
pub(in crate::db::schema) use mutation::{SchemaMutationDelta, classify_schema_mutation_delta};
pub(in crate::db) use proposal::compiled_schema_proposal_for_model;
#[cfg(test)]
pub(in crate::db) use reconcile::{
    bootstrap_test_accepted_schema_snapshot, publish_test_accepted_schema_snapshot,
};
pub(in crate::db) use reconcile::{
    ensure_accepted_catalog_snapshot_selection, ensure_accepted_schema_snapshot,
    reconcile_runtime_schemas,
};
#[cfg(feature = "sql")]
pub(in crate::db) use reconcile::{
    execute_admin_sql_ddl_expression_index_addition, execute_admin_sql_ddl_field_addition,
    execute_admin_sql_ddl_field_default_change, execute_admin_sql_ddl_field_drop,
    execute_admin_sql_ddl_field_nullability_change,
    execute_admin_sql_ddl_field_path_index_addition, execute_admin_sql_ddl_field_rename,
    execute_admin_sql_ddl_secondary_index_drop,
};
#[cfg(feature = "sql")]
pub(in crate::db) use runtime::AcceptedRowLayoutRuntimeField;
pub(in crate::db) use runtime::{
    AcceptedFieldAbsencePolicy, AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract,
    AcceptedGeneratedRowCompatibilityProof, AcceptedRowDecodeContract,
    AcceptedRowLayoutRuntimeContract, OwnedAcceptedFieldDecodeContract,
    OwnedAcceptedRelationEdgeContract, accepted_insert_field_is_omittable,
};
#[cfg(test)]
pub(in crate::db) use runtime::{
    generated_compatible_row_layout_proof_count_for_tests,
    reset_generated_compatible_row_layout_proof_count_for_tests,
};
pub(in crate::db) use snapshot::{
    AcceptedSchemaSnapshot, PersistedFieldOrigin, PersistedFieldSnapshot,
    PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexOrigin,
    PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot,
    PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldWritePolicy,
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

#[cfg(test)]
pub(in crate::db) fn validate_raw_schema_snapshot_format_for_tests(
    bytes: Vec<u8>,
) -> Result<(), crate::error::InternalError> {
    store::validate_raw_schema_snapshot_bytes_for_tests(bytes)
}

#[cfg(test)]
pub(in crate::db) fn validate_accepted_enum_catalog_format_for_tests(
    bytes: &[u8],
) -> Result<(), crate::error::InternalError> {
    enum_catalog::decode_accepted_enum_catalog(bytes).map(drop)
}

#[cfg(test)]
pub(in crate::db) fn validate_accepted_schema_bundle_format_for_tests(
    bytes: &[u8],
) -> Result<(), crate::error::InternalError> {
    enum_catalog::decode_accepted_schema_revision_bundle(bytes).map(drop)
}
pub(in crate::db::schema) use transition::{
    SchemaTransitionDecision, SchemaTransitionPlanKind, decide_schema_transition,
};
#[cfg(any(test, feature = "sql"))]
#[cfg(feature = "sql")]
pub(in crate::db) use types::canonicalize_strict_sql_literal_for_persisted_kind;
pub(in crate::db) use types::field_type_from_persisted_kind;
#[cfg(feature = "sql")]
pub(in crate::db) use types::input_value_from_strict_sql_literal_for_persisted_kind;
pub(crate) use types::{FieldType, ScalarType, field_type_from_model_kind, literal_matches_type};
