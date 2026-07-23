//! DDL-facing schema mutation admission diagnostics.

use super::{
    AcceptedSchemaMutationError, SchemaExpressionIndexRebuildTarget, SchemaFieldAdditionTarget,
    SchemaFieldDropTarget, SchemaFieldNullabilityTarget, SchemaFieldPathIndexRebuildTarget,
    SchemaFieldRenameTarget, SchemaInsertDefaultTarget,
};
use crate::db::schema::{
    AcceptedConstraintCatalogError, AcceptedSchemaSnapshot, SchemaVersion,
    transition::{
        SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionClassification,
        SchemaAdmissionRejectionReason, schema_admission_rejection,
    },
};
use crate::error::SchemaDdlAdmissionError;

///
/// SchemaDdlMutationAdmission
///
/// Schema-owned proof that one DDL candidate passed the current catalog-native
/// admission path. It exposes only the concrete target consumed by execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaDdlMutationAdmission {
    pub(in crate::db::schema::mutation) target: SchemaDdlMutationTarget,
}

///
/// SchemaDdlIndexDropCandidateError
///
/// Schema-owned rejection reason for resolving a SQL DDL `DROP INDEX`
/// candidate against accepted catalog authority.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlIndexDropCandidateError {
    Generated,
    Unknown,
    Unsupported,
}

impl SchemaDdlMutationAdmission {
    /// Borrow the admitted field-path index rebuild target.
    #[must_use]
    pub(in crate::db) const fn field_path_target(
        &self,
    ) -> Option<&SchemaFieldPathIndexRebuildTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldPathAddition(target) => Some(target),
            SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Borrow the admitted expression index rebuild target.
    #[must_use]
    pub(in crate::db) const fn expression_target(
        &self,
    ) -> Option<&SchemaExpressionIndexRebuildTarget> {
        match &self.target {
            SchemaDdlMutationTarget::ExpressionAddition(target) => Some(target),
            SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Return whether this admission represents a secondary-index drop.
    #[must_use]
    pub(in crate::db) const fn is_secondary_drop(&self) -> bool {
        match &self.target {
            SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_) => false,
            SchemaDdlMutationTarget::SecondaryDrop => true,
        }
    }

    /// Borrow the admitted additive-field target.
    #[must_use]
    pub(in crate::db) const fn field_addition_target(&self) -> Option<&SchemaFieldAdditionTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldAddition(target) => Some(target),
            SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Borrow the admitted field-default metadata target.
    #[must_use]
    pub(in crate::db) const fn field_default_target(&self) -> Option<&SchemaInsertDefaultTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldDefaultChange(target) => Some(target),
            SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Borrow the admitted field-nullability metadata target.
    #[must_use]
    pub(in crate::db) const fn field_nullability_target(
        &self,
    ) -> Option<&SchemaFieldNullabilityTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldNullabilityChange(target) => Some(target),
            SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Borrow the admitted field-rename metadata target.
    #[must_use]
    pub(in crate::db) const fn field_rename_target(&self) -> Option<&SchemaFieldRenameTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldRename(target) => Some(target),
            SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldDrop(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }

    /// Borrow the admitted field-drop metadata target.
    #[must_use]
    pub(in crate::db) const fn field_drop_target(&self) -> Option<&SchemaFieldDropTarget> {
        match &self.target {
            SchemaDdlMutationTarget::FieldDrop(target) => Some(target),
            SchemaDdlMutationTarget::FieldAddition(_)
            | SchemaDdlMutationTarget::FieldDefaultChange(_)
            | SchemaDdlMutationTarget::FieldNullabilityChange(_)
            | SchemaDdlMutationTarget::FieldRename(_)
            | SchemaDdlMutationTarget::FieldPathAddition(_)
            | SchemaDdlMutationTarget::ExpressionAddition(_)
            | SchemaDdlMutationTarget::SecondaryDrop => None,
        }
    }
}

///
/// SchemaDdlMutationTarget
///
/// Schema-owned target admitted for one SQL DDL mutation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlMutationTarget {
    FieldAddition(SchemaFieldAdditionTarget),
    FieldDefaultChange(SchemaInsertDefaultTarget),
    FieldDrop(SchemaFieldDropTarget),
    FieldNullabilityChange(SchemaFieldNullabilityTarget),
    FieldRename(SchemaFieldRenameTarget),
    FieldPathAddition(SchemaFieldPathIndexRebuildTarget),
    ExpressionAddition(SchemaExpressionIndexRebuildTarget),
    SecondaryDrop,
}

///
/// SchemaDdlAcceptedSnapshotDerivation
///
/// Accepted-after schema derivation for a DDL candidate that has already been
/// admitted through the schema mutation path.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaDdlAcceptedSnapshotDerivation {
    pub(in crate::db::schema::mutation) accepted_after: AcceptedSchemaSnapshot,
    pub(in crate::db::schema::mutation) admission: SchemaDdlMutationAdmission,
}

impl SchemaDdlAcceptedSnapshotDerivation {
    /// Borrow the accepted-after schema snapshot.
    #[must_use]
    pub(in crate::db) const fn accepted_after(&self) -> &AcceptedSchemaSnapshot {
        &self.accepted_after
    }

    /// Borrow the schema mutation admission proof for this accepted-after snapshot.
    #[must_use]
    pub(in crate::db) const fn admission(&self) -> &SchemaDdlMutationAdmission {
        &self.admission
    }

    /// Retag the candidate snapshot with the source-declared next schema
    /// version and run the schema-owned version/fingerprint admission gate.
    pub(in crate::db) fn with_declared_schema_version(
        self,
        accepted_before: &AcceptedSchemaSnapshot,
        schema_version: SchemaVersion,
    ) -> Result<Self, SchemaDdlMutationAdmissionError> {
        let accepted_after = AcceptedSchemaSnapshot::try_new(
            self.accepted_after
                .persisted_snapshot()
                .clone_with_version(schema_version),
        )
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
        let comparison = SchemaAdmissionIdentityComparison::from_snapshots(
            accepted_before.persisted_snapshot(),
            accepted_after.persisted_snapshot(),
        )
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
        if let Some(rejection) = schema_admission_rejection(comparison) {
            return Err(SchemaDdlMutationAdmissionError::SchemaVersionAdmission(
                SchemaDdlSchemaVersionAdmissionError::from_schema_admission(
                    rejection.admission().expect("ddl admission invariant"),
                ),
            ));
        }

        Ok(Self {
            accepted_after,
            admission: self.admission,
        })
    }
}

///
/// SchemaDdlMutationAdmissionError
///
/// Fail-closed reason from schema-owned DDL mutation admission.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlMutationAdmissionError {
    AcceptedIndex(AcceptedSchemaMutationError),
    AcceptedAfterRejected,
    ConstraintCatalog(AcceptedConstraintCatalogError),
    RowLayoutVersionExhausted,
    SchemaVersionAdmission(SchemaDdlSchemaVersionAdmissionError),
    UnsupportedExecutionPath,
}

impl SchemaDdlMutationAdmissionError {
    pub(in crate::db) const fn schema_ddl_admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::AcceptedIndex(_) => SchemaDdlAdmissionError::UnsupportedTransitionClass,
            Self::AcceptedAfterRejected | Self::ConstraintCatalog(_) => {
                SchemaDdlAdmissionError::ValidationFailed
            }
            Self::RowLayoutVersionExhausted => SchemaDdlAdmissionError::RowLayoutVersionExhausted,
            Self::SchemaVersionAdmission(reason) => reason.schema_ddl_admission_error(),
            Self::UnsupportedExecutionPath => SchemaDdlAdmissionError::PhysicalRunnerMissing,
        }
    }
}

///
/// SchemaDdlSchemaVersionAdmissionError
///
/// Stable DDL-facing admission-matrix reason for rejected schema-version and
/// fingerprint transitions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSchemaVersionAdmissionError {
    FingerprintMethodMismatch,

    AcceptedSchemaChangeWithoutVersionBump,

    EmptyVersionBump,

    VersionGap { expected_next: u32 },

    VersionRollback,
}

///
/// SchemaDdlVersionContractPreflightError
///
/// Stable pre-fingerprint admission reason for rejected DDL schema-version
/// contracts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlVersionContractPreflightError {
    MissingExpectedSchemaVersion,

    MissingNextSchemaVersion,

    StaleExpectedSchemaVersion { expected: u32, accepted: u32 },

    EmptySchemaVersionBump { requested: u32 },
}

impl SchemaDdlSchemaVersionAdmissionError {
    const fn schema_ddl_admission_error(self) -> SchemaDdlAdmissionError {
        match self {
            Self::FingerprintMethodMismatch => SchemaDdlAdmissionError::FingerprintMethodMismatch,
            Self::AcceptedSchemaChangeWithoutVersionBump => {
                SchemaDdlAdmissionError::AcceptedSchemaChangeWithoutVersionBump
            }
            Self::EmptyVersionBump => SchemaDdlAdmissionError::EmptyVersionBump,
            Self::VersionGap { .. } => SchemaDdlAdmissionError::VersionGap,
            Self::VersionRollback => SchemaDdlAdmissionError::VersionRollback,
        }
    }

    pub(super) const fn from_schema_admission(
        classification: SchemaAdmissionRejectionClassification,
    ) -> Self {
        match classification.reason() {
            SchemaAdmissionRejectionReason::FingerprintMethodMismatch => {
                Self::FingerprintMethodMismatch
            }
            SchemaAdmissionRejectionReason::MissingVersionBump => {
                Self::AcceptedSchemaChangeWithoutVersionBump
            }
            SchemaAdmissionRejectionReason::EmptyVersionBump => Self::EmptyVersionBump,
            SchemaAdmissionRejectionReason::VersionGap => Self::VersionGap {
                expected_next: classification
                    .expected_next()
                    .expect("ddl admission invariant"),
            },
            SchemaAdmissionRejectionReason::VersionRollback => Self::VersionRollback,
        }
    }
}

/// Validate the declaration-level schema-version contract before DDL derives a
/// candidate accepted snapshot.
pub(in crate::db) fn validate_schema_ddl_version_contract_preflight(
    accepted_version: SchemaVersion,
    expected_schema_version: Option<SchemaVersion>,
    next_schema_version: Option<SchemaVersion>,
    mutates_accepted_schema: bool,
) -> Result<(), SchemaDdlVersionContractPreflightError> {
    if let Some(expected) = expected_schema_version
        && expected != accepted_version
    {
        return Err(
            SchemaDdlVersionContractPreflightError::StaleExpectedSchemaVersion {
                expected: expected.get(),
                accepted: accepted_version.get(),
            },
        );
    }

    if expected_schema_version.is_none() {
        return Err(SchemaDdlVersionContractPreflightError::MissingExpectedSchemaVersion);
    }

    if !mutates_accepted_schema {
        if let Some(requested) = next_schema_version {
            return Err(
                SchemaDdlVersionContractPreflightError::EmptySchemaVersionBump {
                    requested: requested.get(),
                },
            );
        }

        return Ok(());
    }

    if next_schema_version.is_none() {
        return Err(SchemaDdlVersionContractPreflightError::MissingNextSchemaVersion);
    }

    Ok(())
}
