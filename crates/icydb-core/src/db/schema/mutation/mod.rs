//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, physical rebuild execution, or schema-store writes.
//! Boundary: describes accepted snapshot changes before reconciliation persists them.

use crate::db::schema::PersistedFieldSnapshot;

mod budget;
pub(in crate::db) use budget::{
    MAX_SCHEMA_PROJECTION_ENTRIES, MAX_SCHEMA_PROJECTION_WORK_UNITS, MAX_SCHEMA_STAGED_RAW_BYTES,
    SchemaTransitionSourceBudget,
};

#[cfg(feature = "sql")]
mod field;
#[cfg(feature = "sql")]
pub(in crate::db) use field::{
    SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
    SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
    SchemaFieldAdditionTarget, SchemaFieldDropTarget, SchemaFieldNullabilityTarget,
    SchemaFieldRenameTarget, SchemaInsertDefaultTarget,
    derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
    derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
    derive_sql_ddl_field_nullability_persisted_after, derive_sql_ddl_field_rename_accepted_after,
    resolve_sql_ddl_field_drop_candidate, resolve_sql_ddl_field_drop_default_candidate,
    resolve_sql_ddl_field_nullability_candidate, resolve_sql_ddl_field_rename_candidate,
    resolve_sql_ddl_field_set_default_candidate, validate_sql_ddl_field_default_change_candidate,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use field::{
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_rename_candidate,
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

#[cfg(feature = "sql")]
mod field_type;
#[cfg(feature = "sql")]
pub(in crate::db) use field_type::{
    SchemaDdlFieldTypeContract, resolve_sql_ddl_field_type_contract,
};

#[cfg(feature = "sql")]
mod ddl_admission;
#[cfg(feature = "sql")]
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "schema root re-exports DDL schema-version admission diagnostics"
    )
)]
pub(in crate::db) use ddl_admission::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlIndexDropCandidateError,
    SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
    SchemaDdlSchemaVersionAdmissionError, SchemaDdlVersionContractPreflightError,
    validate_schema_ddl_version_contract_preflight,
};

mod delta;
#[cfg(feature = "sql")]
pub(in crate::db::schema) use delta::required_empty_entity_field_addition_matches;
pub(in crate::db::schema) use delta::schema_mutation_request_for_snapshots;
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::schema) use delta::{SchemaMutationDelta, classify_schema_mutation_delta};

mod generated_candidate;
pub(in crate::db::schema) use generated_candidate::{
    GeneratedAcceptedCandidateError, GeneratedConstraintActivationContext,
    derive_generated_accepted_candidate,
};

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
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
};
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildKey,
    SchemaFieldPathIndexRebuildTarget,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use index::{
    admit_sql_ddl_expression_index_candidate, admit_sql_ddl_field_path_index_candidate,
    admit_sql_ddl_secondary_index_drop_candidate,
};
#[cfg(feature = "sql")]
pub(in crate::db) use index::{
    derive_sql_ddl_expression_index_accepted_after, derive_sql_ddl_field_path_index_accepted_after,
    derive_sql_ddl_secondary_index_drop_accepted_after,
    resolve_sql_ddl_secondary_index_drop_candidate,
};

mod user_index_domain;
pub(in crate::db) use user_index_domain::{
    SchemaUserIndexDomainRow, StagedUserIndexDomainError, StagedUserIndexDomainReplacement,
    StagedUserIndexDomainReplacementBuilder, UniqueConstraintProjection,
};

///
/// SchemaMutationRequest
///
/// Internal request vocabulary that lowers catalog-level mutation intent into
/// a deterministic `MutationPlan`. SQL DDL and generated proposal comparison
/// must route through this type instead of constructing plans ad hoc.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRequest<'a> {
    ExactMatch,
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

///
/// MutationPlan
///
/// Deterministic schema-owned plan for moving one accepted snapshot to the
/// next. Every variant is a current publication or physical-execution shape;
/// rejected snapshot deltas never become mutation plans.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPlan {
    MetadataOnly,
    FieldPathIndexRebuild {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    ExpressionIndexRebuild {
        target: SchemaExpressionIndexRebuildTarget,
    },
}

/// Schema-owned publication boundary for a current mutation plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationPreflight {
    PublishableNow,
    RequiresPhysicalWork,
}

impl MutationPlan {
    /// Build the no-op plan for equal accepted snapshots.
    pub(in crate::db::schema) const fn exact_match() -> Self {
        Self::MetadataOnly
    }

    /// Plan a field-path index rebuild from accepted index metadata.
    const fn field_path_index_addition(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self::FieldPathIndexRebuild { target }
    }

    /// Plan an accepted deterministic expression-index rebuild.
    const fn expression_index_addition(target: SchemaExpressionIndexRebuildTarget) -> Self {
        Self::ExpressionIndexRebuild { target }
    }

    /// Return the sole publication decision for this plan.
    #[must_use]
    pub(in crate::db::schema) const fn publication_preflight(
        &self,
    ) -> MutationPublicationPreflight {
        match self {
            Self::MetadataOnly => MutationPublicationPreflight::PublishableNow,
            Self::FieldPathIndexRebuild { .. } | Self::ExpressionIndexRebuild { .. } => {
                MutationPublicationPreflight::RequiresPhysicalWork
            }
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn field_path_index_target(
        &self,
    ) -> Option<&SchemaFieldPathIndexRebuildTarget> {
        match self {
            Self::FieldPathIndexRebuild { target } => Some(target),
            Self::MetadataOnly | Self::ExpressionIndexRebuild { .. } => None,
        }
    }

    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) const fn expression_index_target(
        &self,
    ) -> Option<&SchemaExpressionIndexRebuildTarget> {
        match self {
            Self::ExpressionIndexRebuild { target } => Some(target),
            Self::MetadataOnly | Self::FieldPathIndexRebuild { .. } => None,
        }
    }
}

impl From<SchemaMutationRequest<'_>> for MutationPlan {
    fn from(request: SchemaMutationRequest<'_>) -> Self {
        match request {
            SchemaMutationRequest::ExactMatch => Self::exact_match(),
            SchemaMutationRequest::AppendOnlyFields(_) => Self::MetadataOnly,
            SchemaMutationRequest::AddFieldPathIndex { target } => {
                Self::field_path_index_addition(target)
            }
            SchemaMutationRequest::AddExpressionIndex { target } => {
                Self::expression_index_addition(target)
            }
        }
    }
}

#[cfg(all(test, feature = "sql"))]
mod tests;
