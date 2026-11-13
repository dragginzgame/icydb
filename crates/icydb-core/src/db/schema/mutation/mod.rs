//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, physical rebuild execution, or schema-store writes.
//! Boundary: describes accepted snapshot changes before publication persists them.

use crate::db::schema::PersistedFieldSnapshot;

mod budget;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use budget::MAX_SCHEMA_PROJECTION_ENTRIES;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use budget::SchemaTransitionSourceBudget;
pub(in crate::db) use budget::{MAX_SCHEMA_PROJECTION_WORK_UNITS, MAX_SCHEMA_STAGED_RAW_BYTES};

#[cfg(feature = "sql")]
mod field;
#[cfg(feature = "sql")]
pub(in crate::db) use field::{
    SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
    SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
    SchemaFieldAdditionTarget, SchemaFieldDropTarget, SchemaFieldNullabilityTarget,
    SchemaFieldRenameTarget, SchemaInsertDefaultTarget,
    derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
    derive_sql_ddl_field_default_persisted_after, derive_sql_ddl_field_drop_accepted_after,
    derive_sql_ddl_field_nullability_accepted_after,
    derive_sql_ddl_field_nullability_persisted_after, derive_sql_ddl_field_rename_accepted_after,
    derive_sql_ddl_field_rename_persisted_after, resolve_sql_ddl_field_drop_candidate,
    resolve_sql_ddl_field_drop_default_candidate, resolve_sql_ddl_field_nullability_candidate,
    resolve_sql_ddl_field_rename_candidate, resolve_sql_ddl_field_set_default_candidate,
    validate_sql_ddl_field_default_change_candidate,
};
#[cfg(feature = "sql")]
mod field_allocation;
#[cfg(feature = "sql")]
pub(in crate::db) use field_allocation::{
    SchemaDdlFieldAdditionCandidateError, build_sql_ddl_field_addition_candidate,
    resolve_sql_ddl_field_addition_name_candidate,
};

#[cfg(feature = "sql")]
mod field_default_encoding;
#[cfg(feature = "sql")]
pub(in crate::db) use field_default_encoding::{
    encode_sql_ddl_add_column_default, encode_sql_ddl_alter_column_default,
};

mod field_removal;
pub(in crate::db::schema) use field_removal::derive_dense_field_removal_candidate;

#[cfg(feature = "sql")]
mod field_type;
#[cfg(feature = "sql")]
pub(in crate::db) use field_type::{
    SchemaDdlFieldTypeContract, resolve_sql_ddl_field_type_contract,
};

#[cfg(feature = "sql")]
mod ddl_admission;
#[cfg(feature = "sql")]
pub(in crate::db) use ddl_admission::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlIndexDropCandidateError,
    SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
    SchemaDdlVersionContractPreflightError, validate_schema_ddl_version_contract_preflight,
};

#[cfg(any(test, feature = "sql"))]
mod delta;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::schema) use delta::schema_mutation_request_for_snapshots;

#[cfg(feature = "sql")]
mod index_candidate;
#[cfg(feature = "sql")]
pub(in crate::db) use index_candidate::{
    SchemaDdlSecondaryIndexAdditionCandidate, SchemaDdlSecondaryIndexAdditionCandidateError,
    SchemaDdlSecondaryIndexExpressionIntent, SchemaDdlSecondaryIndexExpressionOpIntent,
    SchemaDdlSecondaryIndexFieldPathIntent, SchemaDdlSecondaryIndexKeyCandidateError,
    SchemaDdlSecondaryIndexKeyIntent, build_sql_ddl_secondary_index_candidate,
    resolve_sql_ddl_secondary_index_addition_candidate,
};

mod index;
pub(in crate::db) use index::SchemaExpressionIndexRebuildKey;
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildKey,
    SchemaFieldPathIndexRebuildTarget,
};
#[cfg(feature = "sql")]
pub(in crate::db) use index::{
    derive_sql_ddl_expression_index_accepted_after, derive_sql_ddl_field_path_index_accepted_after,
    derive_sql_ddl_secondary_index_drop_accepted_after,
    resolve_sql_ddl_secondary_index_drop_candidate,
};

mod index_removal;
pub(in crate::db::schema) use index_removal::derive_dense_index_removal_candidate;

mod relation_removal;
pub(in crate::db::schema) use relation_removal::derive_relation_removal_candidate;

mod user_index_domain;
#[cfg(feature = "migration")]
pub(in crate::db) use user_index_domain::MigrationIndexProjection;
pub(in crate::db::schema) use user_index_domain::prove_empty_user_index_domain;
#[cfg(feature = "sql")]
pub(in crate::db) use user_index_domain::{
    SchemaUserIndexDomainRow, StagedUserIndexDomainReplacement,
    StagedUserIndexDomainReplacementBuilder,
};
pub(in crate::db) use user_index_domain::{StagedUserIndexDomainError, UniqueConstraintProjection};

///
/// SchemaMutationRequest
///
/// Catalog-level additions shared by DDL admission and accepted index-target
/// construction. Snapshot admission validates historical fills before the
/// derived operation is carried to publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(any(test, feature = "sql")),
    expect(
        dead_code,
        reason = "catalog mutation vocabulary is shared with SQL DDL planning"
    )
)]
pub(in crate::db::schema) enum SchemaMutationRequest<'a> {
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    AddFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
}

///
/// AcceptedSchemaMutationError
///
/// Fail-closed reason produced while lowering accepted schema metadata into a
/// mutation request. These errors mean the mutation framework cannot describe
/// a safe catalog operation yet; callers must not compensate with generated
/// metadata.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedSchemaMutationError {
    UnsupportedIndexKeyShape,
    EmptyIndexKey,
    ExpressionIndexRequiresExpressionKey,
}

#[cfg(all(test, feature = "sql"))]
mod tests;

// New DDL fields identify catalog roots. Dots are traversal separators in
// field references, so storing one in a root name makes reads and writes disagree.
#[cfg(feature = "sql")]
fn sql_ddl_field_name_is_root(name: &str) -> bool {
    !name.contains('.')
}
