//! DDL-facing schema mutation admission diagnostics.

use super::SchemaDdlMutationAdmissionError;
use crate::db::schema::transition::{
    SchemaAdmissionRejectionClassification, SchemaAdmissionRejectionReason,
};
use crate::error::SchemaDdlAdmissionError;
use thiserror::Error as ThisError;

impl SchemaDdlMutationAdmissionError {
    pub(in crate::db) const fn schema_ddl_admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::AcceptedIndex(_) => SchemaDdlAdmissionError::UnsupportedTransitionClass,
            Self::AcceptedAfterRejected => SchemaDdlAdmissionError::ValidationFailed,
            Self::SchemaVersionAdmission(reason, _) => reason.schema_ddl_admission_error(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum SchemaDdlSchemaVersionAdmissionError {
    #[error("schema fingerprint method changed")]
    FingerprintMethodMismatch,

    #[error("schema changed without schema_version bump")]
    AcceptedSchemaChangeWithoutVersionBump,

    #[error("schema_version bumped without schema shape change")]
    EmptyVersionBump,

    #[error("schema_version jumped")]
    VersionGap { expected_next: u32 },

    #[error("schema_version moved backwards")]
    VersionRollback,
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
                    .expect("version-gap admission must carry expected_next"),
            },
            SchemaAdmissionRejectionReason::VersionRollback => Self::VersionRollback,
        }
    }
}
