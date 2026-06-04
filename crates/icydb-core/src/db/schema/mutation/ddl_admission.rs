//! DDL-facing schema mutation admission diagnostics.

use super::SchemaDdlMutationAdmissionError;
use crate::db::schema::{
    SchemaVersion,
    transition::{SchemaAdmissionRejectionClassification, SchemaAdmissionRejectionReason},
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

///
/// SchemaDdlVersionContractPreflightError
///
/// Stable pre-fingerprint admission reason for rejected DDL schema-version
/// contracts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum SchemaDdlVersionContractPreflightError {
    #[error("DDL requires EXPECT SCHEMA VERSION")]
    MissingExpectedSchemaVersion,

    #[error("DDL requires SET SCHEMA VERSION")]
    MissingNextSchemaVersion,

    #[error(
        "DDL expected accepted schema version {expected}, but accepted schema version is {accepted}"
    )]
    StaleExpectedSchemaVersion { expected: u32, accepted: u32 },

    #[error("DDL no-op cannot SET SCHEMA VERSION {requested}")]
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
                    .expect("version-gap admission must carry expected_next"),
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
