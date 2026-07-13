//! SQL DDL admission diagnostics mapped to stable query-facing error variants.

use super::{
    BoundSqlDdlRequest, BoundSqlDdlStatement, SqlDdlBindError, SqlDdlLoweringError,
    SqlDdlPrepareError,
};
use crate::db::{
    schema::{
        AcceptedSchemaSnapshot, SchemaDdlVersionContractPreflightError, SchemaVersion,
        validate_schema_ddl_version_contract_preflight,
    },
    sql::parser::{SqlDdlSchemaVersionContract, SqlDdlStatement},
};
use crate::error::SchemaDdlAdmissionError;

///
/// BoundSqlDdlSchemaVersionContract
///
/// Accepted-catalog DDL version intent after raw parser values have been
/// checked for positive schema-version numbers.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlSchemaVersionContract {
    expected_schema_version: Option<SchemaVersion>,
    next_schema_version: Option<SchemaVersion>,
}

impl BoundSqlDdlSchemaVersionContract {
    /// Return the declared accepted-before schema version.
    #[must_use]
    pub(in crate::db) const fn expected_schema_version(self) -> Option<SchemaVersion> {
        self.expected_schema_version
    }

    /// Return the declared accepted-after schema version.
    #[must_use]
    pub(in crate::db) const fn next_schema_version(self) -> Option<SchemaVersion> {
        self.next_schema_version
    }
}

impl SqlDdlPrepareError {
    pub(in crate::db) fn admission_error(&self) -> SchemaDdlAdmissionError {
        match self {
            Self::Bind(error) => error.admission_error(),
            Self::Lowering(error) => error.admission_error(),
        }
    }
}

pub(in crate::db) const fn ddl_version_contract(
    ddl: &SqlDdlStatement,
) -> SqlDdlSchemaVersionContract {
    match ddl {
        SqlDdlStatement::CreateIndex(statement) => statement.schema_version_contract,
        SqlDdlStatement::DropIndex(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableAddColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableAlterColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableDropColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableRenameColumn(statement) => statement.schema_version_contract,
    }
}

pub(in crate::db) fn bind_sql_ddl_schema_version_contract(
    contract: SqlDdlSchemaVersionContract,
) -> Result<BoundSqlDdlSchemaVersionContract, SqlDdlBindError> {
    Ok(BoundSqlDdlSchemaVersionContract {
        expected_schema_version: bind_sql_ddl_schema_version(
            "EXPECT SCHEMA VERSION",
            contract.expected_schema_version,
        )?,
        next_schema_version: bind_sql_ddl_schema_version(
            "SET SCHEMA VERSION",
            contract.next_schema_version,
        )?,
    })
}

fn bind_sql_ddl_schema_version(
    clause: &'static str,
    value: Option<u32>,
) -> Result<Option<SchemaVersion>, SqlDdlBindError> {
    value
        .map(|raw| {
            if raw == 0 {
                Err(SqlDdlBindError::NonPositiveSchemaVersion { clause })
            } else {
                Ok(SchemaVersion::new(raw))
            }
        })
        .transpose()
}

pub(in crate::db) fn validate_bound_sql_ddl_version_contract(
    bound: &BoundSqlDdlRequest,
    accepted_before: &AcceptedSchemaSnapshot,
) -> Result<(), SqlDdlBindError> {
    let contract = bound.schema_version_contract();
    let accepted_version = accepted_before.persisted_snapshot().version();
    validate_schema_ddl_version_contract_preflight(
        accepted_version,
        contract.expected_schema_version(),
        contract.next_schema_version(),
        !matches!(bound.statement(), BoundSqlDdlStatement::NoOp(_)),
    )
    .map_err(sql_ddl_version_contract_preflight_error)
}

const fn sql_ddl_version_contract_preflight_error(
    error: SchemaDdlVersionContractPreflightError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlVersionContractPreflightError::MissingExpectedSchemaVersion => {
            SqlDdlBindError::MissingExpectedSchemaVersion
        }
        SchemaDdlVersionContractPreflightError::MissingNextSchemaVersion => {
            SqlDdlBindError::MissingNextSchemaVersion
        }
        SchemaDdlVersionContractPreflightError::StaleExpectedSchemaVersion {
            expected,
            accepted,
        } => SqlDdlBindError::StaleExpectedSchemaVersion { expected, accepted },
        SchemaDdlVersionContractPreflightError::EmptySchemaVersionBump { requested } => {
            SqlDdlBindError::EmptySchemaVersionBump { requested }
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
            Self::InvalidAlterTableAddColumnDefault { .. } => {
                SchemaDdlAdmissionError::InvalidAddColumnDefault
            }
            Self::InvalidAlterTableAlterColumnDefault { .. } => {
                SchemaDdlAdmissionError::InvalidAlterColumnDefault
            }
            Self::InvalidFilteredIndexPredicate
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
            | Self::UnsupportedDropIndex { .. }
            | Self::UnsupportedAlterTableAddColumnNotNull { .. }
            | Self::UnsupportedAlterTableAddColumnType { .. }
            | Self::PrimaryKeyFieldDropRejected { .. }
            | Self::GeneratedFieldDropRejected { .. }
            | Self::IndexedFieldDropRejected { .. }
            | Self::IndexedFieldDefaultChangeRejected { .. }
            | Self::GeneratedFieldRenameRejected { .. } => {
                SchemaDdlAdmissionError::UnsupportedTransitionClass
            }
            Self::GeneratedIndexDropRejected { .. } => {
                SchemaDdlAdmissionError::GeneratedIndexDropRejected
            }
            Self::UnsupportedAlterTableDropDefaultRequired { .. } => {
                SchemaDdlAdmissionError::RequiredDropDefaultUnsupported
            }
            Self::GeneratedFieldDefaultChangeRejected { .. } => {
                SchemaDdlAdmissionError::GeneratedFieldDefaultChangeRejected
            }
            Self::GeneratedFieldNullabilityChangeRejected { .. } => {
                SchemaDdlAdmissionError::GeneratedFieldNullabilityChangeRejected
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
