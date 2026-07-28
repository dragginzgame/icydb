//! Module: db::schema
//! Responsibility: runtime schema-contract utilities (introspection, validation, hashing).
//! Does not own: query planning policy, execution routing, or storage diagnostics.
//! Boundary: exposes schema-facing contracts consumed by session/query/commit paths.

mod accepted_field_kind;
mod accepted_value_admission;
mod application;
mod application_lowering;
mod application_receipt;
mod application_store;
#[cfg(any(test, feature = "query"))]
mod capabilities;
mod check;
mod codec;
mod composite_catalog;
mod constraint;
mod constraint_activation_runner;
mod constraint_validation;
mod describe;
pub(in crate::db) mod enum_catalog;
mod errors;
mod field_kind_semantics;
mod fingerprint;
#[cfg(any(test, feature = "query"))]
mod format;
mod identity;
mod info;
mod inspection_plan;
mod integrity;
mod layout;
mod live_schema_checkpoint;
mod mutation;
mod runtime;
mod snapshot;
mod source_binding;
#[cfg(feature = "sql")]
mod sql_ddl;
mod storage;
mod store;
#[cfg(any(test, feature = "query"))]
mod transition;
mod types;
mod wire;

/// Maximum zero-based nesting depth accepted by schema contracts and value codecs.
///
/// A root value starts at depth zero, so valid recursive nodes occupy depths
/// `0..MAX_ACCEPTED_RECURSIVE_DEPTH`. Contract construction, admission, and
/// canonical persistence must all enforce this same boundary.
pub(in crate::db) const MAX_ACCEPTED_RECURSIVE_DEPTH_U16: u16 = 64;
/// `usize` form used by schema-tree construction and persisted decoding.
pub(in crate::db) const MAX_ACCEPTED_RECURSIVE_DEPTH: usize =
    MAX_ACCEPTED_RECURSIVE_DEPTH_U16 as usize;

pub use describe::{
    ConstraintValidationProgressDescription, EntityConstraintDescription, EntityFieldDescription,
    EntityIndexDescription, EntityRelationCardinality, EntityRelationDescription,
    EntitySchemaDescription,
};
pub use errors::{SchemaLiteralValidationReason, SchemaValidationOperator, ValidateError};

pub(in crate::db) use accepted_field_kind::AcceptedFieldKind;
pub(in crate::db) use accepted_value_admission::AcceptedValueAdmissionContract;
pub use application::{SchemaApplicationStore, SchemaApplicationTarget};
pub(in crate::db) use application::{
    abort_schema_application, apply_schema, continue_schema_application,
    schema_application_receipt, schema_application_target,
};
pub(in crate::db) use application_lowering::lower_field_type;
pub(in crate::db::schema) use application_lowering::{
    ExistingProposalStore, ProposalStoreTarget, lower_existing_schema_proposal,
    lower_initial_schema_proposal,
};
pub(in crate::db) use application_receipt::SchemaApplicationRecord;
pub(in crate::db) use application_receipt::{SchemaChangeActivation, derive_schema_change_job_id};
pub use application_receipt::{
    SchemaChangeJob, SchemaChangeJobId, SchemaChangeOutcome, SchemaChangeProgress,
    SchemaChangeProgressStatus, SchemaChangeReceipt, SchemaChangeValidationPhase,
};
pub(in crate::db) use application_store::{
    ApplicationRecordKey, SchemaApplicationRecordOp, apply_schema_application_record_op,
    preflight_schema_application_record_op, verify_schema_application_record_op,
    with_schema_application_store,
};
#[cfg(feature = "sql")]
pub(in crate::db) use capabilities::{SqlCapabilities, sql_capabilities_with_enum_catalog};
pub(in crate::db) use check::{
    AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
    AcceptedCheckValueExprV1, AcceptedRowConstraintEvaluationError, CompiledAcceptedRowConstraints,
    accepted_row_constraint_write_error, render_accepted_check_expr_sql,
};
#[cfg(feature = "sql")]
pub(in crate::db) use check::{AcceptedCheckExprV1Error, bind_sql_check_expr};
pub(in crate::db::schema) use check::{
    bind_source_check_expr, source_literal_input, validate_accepted_check_literals,
};
#[cfg(test)]
pub(in crate::db) use codec::encode_unchecked_persisted_schema_snapshot_for_tests;
pub(in crate::db) use codec::{
    MAX_SCHEMA_SNAPSHOT_BYTES, decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
};
pub(in crate::db) use composite_catalog::AcceptedCompositeCatalog;
#[cfg(feature = "sql")]
pub(in crate::db) use constraint::AcceptedConstraintCatalogError;
#[cfg(feature = "sql")]
pub(in crate::db) use constraint::validate_constraint_name;
#[doc(hidden)]
pub use constraint::validate_generated_constraint_name;
pub(in crate::db) use constraint::{
    AcceptedConstraintCatalog, AcceptedConstraintIdentity, AcceptedConstraintKind,
    AcceptedConstraintSnapshot, ConstraintActivationFingerprint, ConstraintActivationKind,
    ConstraintActivationSnapshot, ConstraintActivationState, ConstraintOrigin,
};
pub(in crate::db) use constraint_activation_runner::ConstraintValidationProgress;
#[cfg(feature = "sql")]
pub(in crate::db) use constraint_activation_runner::validate_unpublished_check_candidate_exact;
pub(in crate::db) use constraint_activation_runner::{
    UnpublishedCheckValidation, advance_accepted_check_constraint_activation,
    validate_unpublished_check_candidate_bounded,
};
#[cfg(feature = "sql")]
pub(in crate::db) use constraint_activation_runner::{
    advance_check_constraint_activation, advance_not_null_constraint_activation,
    advance_unique_constraint_activation,
};
pub(in crate::db) use constraint_validation::{
    ConstraintStoreRevision, ConstraintValidationFinding, ConstraintValidationJob,
    ConstraintValidationPhase, ConstraintValidationReceipt, MAX_CONSTRAINT_VALIDATION_JOB_BYTES,
    accepted_constraint_field_paths, decode_constraint_validation_job,
    encode_constraint_validation_job,
};
pub(in crate::db) use describe::describe_accepted_entity_with_persisted_schema;
#[cfg(feature = "sql")]
pub(in crate::db) use describe::describe_entity_fields_with_persisted_schema;
#[cfg(any(test, feature = "query"))]
pub(in crate::db) use enum_catalog::AcceptedSchemaAuthority;
pub(in crate::db::schema) use enum_catalog::AcceptedStoreCatalogScope;
pub(in crate::db) use enum_catalog::{
    AcceptedEnumCatalog, AcceptedSchemaFingerprint, AcceptedSchemaRevision,
    AcceptedSchemaRevisionBundle, AcceptedValueCatalogHandle, AcceptedValueContract,
    CandidateSchemaRevision, ValueAdmissionBudget, encode_unit_enum_equality_key,
    output_value_from_runtime,
};
#[cfg(test)]
pub(in crate::db) use enum_catalog::{
    TestEnumDefinition, TestEnumVariant, accepted_schema_candidate_for_tests,
    accepted_schema_candidate_with_field_bindings_for_tests, build_accepted_enum_catalog_for_tests,
    empty_accepted_enum_catalog_for_tests, empty_accepted_schema_candidate_for_tests,
};
#[cfg(any(test, feature = "query"))]
pub(in crate::db) use field_kind_semantics::AcceptedFieldKindSemantics;
#[cfg(any(test, feature = "query"))]
pub(in crate::db) use field_kind_semantics::AcceptedScalarClass;
pub(in crate::db) use field_kind_semantics::{
    AcceptedFieldKindCategory, classify_accepted_field_kind,
};
pub(in crate::db) use fingerprint::{
    accepted_commit_schema_fingerprint, accepted_schema_cache_fingerprint,
    accepted_schema_cache_fingerprint_for_persisted_snapshot,
    accepted_schema_cache_fingerprint_method_version,
};
#[cfg(any(test, feature = "query"))]
pub(in crate::db::schema) use fingerprint::{
    accepted_schema_admission_fingerprint, accepted_schema_admission_fingerprint_method_version,
};
#[cfg(feature = "sql")]
pub(in crate::db) use format::show_indexes_for_schema_info_with_runtime_state;
pub(in crate::db) use identity::{
    ConstraintId, ConstraintIdAllocator, FieldId, RelationId, SchemaIndexId,
};
pub(in crate::db) use info::{
    SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo, SchemaIndexFieldPathInfo,
    SchemaIndexInfo, SchemaInfo, schema_expression_index_info_from_accepted_index,
    schema_index_info_from_accepted_index,
};
pub(in crate::db) use inspection_plan::AcceptedInspectionPlan;
pub(in crate::db::schema) use integrity::{
    schema_snapshot_constraint_integrity_detail, schema_snapshot_index_integrity_detail,
    schema_snapshot_integrity_detail, schema_snapshot_relation_integrity_detail,
};
pub(in crate::db) use layout::{RowLayoutVersion, SchemaFieldSlot, SchemaRowLayout, SchemaVersion};
pub(in crate::db) use live_schema_checkpoint::{
    apply_live_schema_checkpoint, load_live_schema_checkpoint, preflight_live_schema_checkpoint,
    verify_live_schema_checkpoint,
};
#[cfg(any(test, feature = "query"))]
pub(in crate::db::schema) use mutation::{
    MAX_SCHEMA_PROJECTION_ENTRIES, MutationPlan, MutationPublicationPreflight,
    SchemaMutationRequest, SchemaTransitionSourceBudget, schema_mutation_request_for_snapshots,
};
pub(in crate::db) use mutation::{
    MAX_SCHEMA_PROJECTION_WORK_UNITS, MAX_SCHEMA_STAGED_RAW_BYTES, UniqueConstraintProjection,
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
    SchemaFieldDropTarget, SchemaFieldNullabilityTarget, SchemaFieldRenameTarget,
    SchemaInsertDefaultTarget, build_sql_ddl_field_addition_candidate,
    build_sql_ddl_secondary_index_candidate, derive_sql_ddl_expression_index_accepted_after,
    derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
    derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
    derive_sql_ddl_field_nullability_persisted_after,
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
pub(in crate::db) use mutation::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
    SchemaExpressionIndexRebuildTarget,
};
pub(in crate::db) use mutation::{
    SchemaFieldPathIndexRebuildKey, SchemaFieldPathIndexRebuildTarget, StagedUserIndexDomainError,
};
#[cfg(feature = "sql")]
pub(in crate::db) use mutation::{
    SchemaUserIndexDomainRow, StagedUserIndexDomainReplacement,
    StagedUserIndexDomainReplacementBuilder,
};
pub(in crate::db::schema) use mutation::{
    derive_dense_field_removal_candidate, derive_dense_index_removal_candidate,
    derive_relation_removal_candidate, prove_empty_user_index_domain,
};
pub(in crate::db) use runtime::{
    AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract, AcceptedInsertOmissionPolicy,
    AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeContract, OwnedAcceptedFieldDecodeContract,
    OwnedAcceptedRelationEdgeContract,
};
#[cfg(feature = "sql")]
pub(in crate::db) use runtime::{
    AcceptedRowLayoutRuntimeField, accepted_insert_field_is_omittable,
};
#[cfg(feature = "sql")]
pub(in crate::db) use snapshot::AcceptedFieldDependencyError;
pub(in crate::db) use snapshot::{
    AcceptedSchemaSnapshot, PersistedFieldOrigin, PersistedFieldSnapshot,
    PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexOrigin,
    PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot,
    PersistedSchemaSnapshot, SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaInsertDefault,
};
pub(in crate::db::schema) use source_binding::{
    AcceptedNamedTypeIdentity, decode_accepted_source_bindings, encode_accepted_source_bindings,
};
pub(in crate::db) use source_binding::{AcceptedSourceBindingCatalog, AcceptedTypedAdapterNames};
#[cfg(feature = "sql")]
pub(in crate::db) use sql_ddl::{
    SqlDdlFieldNullabilityOutcome, execute_admin_sql_ddl_check_addition,
    execute_admin_sql_ddl_check_drop, execute_admin_sql_ddl_expression_index_addition,
    execute_admin_sql_ddl_field_addition, execute_admin_sql_ddl_field_default_change,
    execute_admin_sql_ddl_field_drop, execute_admin_sql_ddl_field_nullability_change,
    execute_admin_sql_ddl_field_path_index_addition, execute_admin_sql_ddl_field_rename,
    execute_admin_sql_ddl_not_null_activation_abort, execute_admin_sql_ddl_secondary_index_drop,
    execute_admin_sql_ddl_unique_index_activation,
    execute_admin_sql_ddl_unique_index_activation_abort,
};
pub(in crate::db) use storage::{
    CompositeCodec, FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec,
    ScalarCodec,
};
pub use store::SchemaStore;
pub(in crate::db) use store::{
    AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, SchemaStoreAllocationMetadata,
    SchemaStoreCatalogMetadata, load_accepted_schema_snapshot,
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
#[cfg(feature = "sql")]
pub(in crate::db::schema) use transition::{
    SchemaTransitionDecision, SchemaTransitionPlanKind, decide_schema_transition,
};
pub(crate) use types::FieldType;
#[cfg(any(test, feature = "query"))]
pub(in crate::db) use types::canonicalize_filter_literal_for_persisted_kind;
#[cfg(feature = "sql")]
pub(in crate::db) use types::canonicalize_strict_sql_literal_for_persisted_kind;
pub(in crate::db) use types::field_type_from_persisted_kind;
#[cfg(feature = "sql")]
pub(in crate::db) use types::input_value_from_strict_sql_literal_for_persisted_kind;
#[cfg(any(test, feature = "query"))]
pub(crate) use types::{ScalarType, literal_matches_type};
