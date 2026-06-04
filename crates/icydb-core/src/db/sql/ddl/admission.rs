//! SQL DDL admission diagnostics mapped to stable query-facing error variants.

use super::{SqlDdlBindError, SqlDdlLoweringError, SqlDdlPrepareError};
use crate::error::SchemaDdlAdmissionError;

impl SqlDdlPrepareError {
    pub(in crate::db) fn admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::Bind(error) => error.admission_error(),
            Self::Lowering(error) => error.admission_error(),
        }
    }
}

impl SqlDdlBindError {
    fn admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::MissingExpectedSchemaVersion => {
                SchemaDdlAdmissionError::MissingExpectedSchemaVersion
            }
            Self::MissingNextSchemaVersion => SchemaDdlAdmissionError::MissingNextSchemaVersion,
            Self::StaleExpectedSchemaVersion { .. } => {
                SchemaDdlAdmissionError::StaleExpectedSchemaVersion
            }
            Self::NonPositiveSchemaVersion {
                clause: "SET SCHEMA VERSION",
            } => SchemaDdlAdmissionError::InvalidNextSchemaVersion,
            Self::NonPositiveSchemaVersion { .. } => {
                SchemaDdlAdmissionError::InvalidExpectedSchemaVersion
            }
            Self::EmptySchemaVersionBump { .. } => SchemaDdlAdmissionError::EmptyVersionBump,
            Self::InvalidFilteredIndexPredicate { .. }
            | Self::InvalidAlterTableAddColumnDefault { .. }
            | Self::InvalidAlterTableAlterColumnDefault { .. }
            | Self::DuplicateIndexName { .. }
            | Self::DuplicateFieldPathIndex { .. }
            | Self::DuplicateColumn { .. }
            | Self::UnknownFieldPath { .. }
            | Self::UnknownIndex { .. }
            | Self::UnknownColumn { .. }
            | Self::EntityMismatch { .. }
            | Self::MissingEntityName
            | Self::NotDdl => SchemaDdlAdmissionError::ValidationFailed,
            Self::FieldPathNotIndexable { .. }
            | Self::FieldPathNotAcceptedCatalogBacked { .. }
            | Self::GeneratedIndexDropRejected { .. }
            | Self::UnsupportedDropIndex { .. }
            | Self::UnsupportedAlterTableAddColumnNotNull { .. }
            | Self::UnsupportedAlterTableAddColumnType { .. }
            | Self::UnsupportedAlterTableDropDefaultRequired { .. }
            | Self::GeneratedFieldDefaultChangeRejected { .. }
            | Self::GeneratedFieldNullabilityChangeRejected { .. }
            | Self::PrimaryKeyFieldDropRejected { .. }
            | Self::GeneratedFieldDropRejected { .. }
            | Self::IndexedFieldDropRejected { .. }
            | Self::GeneratedFieldRenameRejected { .. } => {
                SchemaDdlAdmissionError::UnsupportedTransitionClass
            }
        }
    }
}

impl SqlDdlLoweringError {
    const fn admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::UnsupportedStatement => SchemaDdlAdmissionError::UnsupportedTransitionClass,
            Self::MutationAdmission(error) => error.schema_ddl_admission_error(),
        }
    }
}
