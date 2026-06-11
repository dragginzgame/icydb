//! Module: db::sql::ddl
//! Responsibility: bind parsed SQL DDL to accepted schema catalog contracts.
//! Does not own: mutation planning, physical index rebuilds, or SQL execution.
//! Boundary: translates parser-owned DDL syntax into catalog-native requests.

mod admission;
pub(in crate::db) use admission::BoundSqlDdlSchemaVersionContract;
use admission::{
    bind_sql_ddl_schema_version_contract, ddl_version_contract,
    validate_bound_sql_ddl_version_contract,
};
mod field;
pub(in crate::db) use field::{
    BoundSqlAddColumnRequest, BoundSqlAlterColumnDefaultRequest,
    BoundSqlAlterColumnNullabilityRequest, BoundSqlDropColumnRequest, BoundSqlRenameColumnRequest,
};
use field::{
    bind_alter_table_add_column_statement, bind_alter_table_alter_column_statement,
    bind_alter_table_drop_column_statement, bind_alter_table_rename_column_statement,
};

mod index;
pub(in crate::db) use index::{BoundSqlCreateIndexRequest, BoundSqlDropIndexRequest};
use index::{bind_create_index_statement, bind_drop_index_statement};

mod report;
use report::ddl_preparation_report;
pub use report::{SqlDdlExecutionStatus, SqlDdlMutationKind, SqlDdlPreparationReport};

use crate::db::{
    schema::{
        AcceptedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation,
        SchemaDdlMutationAdmissionError, SchemaInfo,
        derive_sql_ddl_expression_index_accepted_after,
        derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
        derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
        derive_sql_ddl_field_path_index_accepted_after, derive_sql_ddl_field_rename_accepted_after,
        derive_sql_ddl_secondary_index_drop_accepted_after,
    },
    sql::parser::{SqlDdlStatement, SqlStatement},
};

#[cfg(test)]
use crate::db::schema::{
    SchemaDdlMutationAdmission, admit_sql_ddl_expression_index_candidate,
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_path_index_candidate, admit_sql_ddl_field_rename_candidate,
    admit_sql_ddl_secondary_index_drop_candidate,
};

///
/// PreparedSqlDdlCommand
///
/// Fully prepared SQL DDL command. This is intentionally not executable yet:
/// it packages the accepted-catalog binding, accepted-after derivation, and
/// schema mutation admission proof for the future execution boundary.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PreparedSqlDdlCommand {
    bound: BoundSqlDdlRequest,
    derivation: Option<SchemaDdlAcceptedSnapshotDerivation>,
    report: SqlDdlPreparationReport,
}

impl PreparedSqlDdlCommand {
    /// Borrow the accepted-catalog-bound DDL request.
    #[must_use]
    pub(in crate::db) const fn bound(&self) -> &BoundSqlDdlRequest {
        &self.bound
    }

    /// Borrow the accepted-after derivation proof.
    #[must_use]
    pub(in crate::db) const fn derivation(&self) -> Option<&SchemaDdlAcceptedSnapshotDerivation> {
        self.derivation.as_ref()
    }

    /// Borrow the developer-facing preparation report.
    #[must_use]
    pub(in crate::db) const fn report(&self) -> &SqlDdlPreparationReport {
        &self.report
    }

    /// Return whether this prepared command needs schema or storage mutation.
    #[must_use]
    pub(in crate::db) const fn mutates_schema(&self) -> bool {
        self.derivation.is_some()
    }
}

///
/// BoundSqlDdlRequest
///
/// Accepted-catalog SQL DDL request after parser syntax has been resolved
/// against one runtime schema snapshot.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlRequest {
    statement: BoundSqlDdlStatement,
    schema_version_contract: BoundSqlDdlSchemaVersionContract,
}

impl BoundSqlDdlRequest {
    /// Borrow the bound statement payload.
    #[must_use]
    pub(in crate::db) const fn statement(&self) -> &BoundSqlDdlStatement {
        &self.statement
    }

    /// Borrow the source-declared DDL schema-version contract.
    #[must_use]
    pub(in crate::db) const fn schema_version_contract(&self) -> BoundSqlDdlSchemaVersionContract {
        self.schema_version_contract
    }
}

///
/// BoundSqlDdlStatement
///
/// Catalog-resolved DDL statement vocabulary.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum BoundSqlDdlStatement {
    AddColumn(BoundSqlAddColumnRequest),
    AlterColumnDefault(BoundSqlAlterColumnDefaultRequest),
    AlterColumnNullability(BoundSqlAlterColumnNullabilityRequest),
    DropColumn(BoundSqlDropColumnRequest),
    RenameColumn(BoundSqlRenameColumnRequest),
    CreateIndex(BoundSqlCreateIndexRequest),
    DropIndex(BoundSqlDropIndexRequest),
    NoOp(BoundSqlDdlNoOpRequest),
}

///
/// BoundSqlDdlNoOpRequest
///
/// Catalog-resolved idempotent DDL request that is already satisfied or absent.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlNoOpRequest {
    mutation_kind: SqlDdlMutationKind,
    index_name: String,
    entity_name: String,
    target_store: String,
    field_path: Vec<String>,
}

impl BoundSqlDdlNoOpRequest {
    /// Return the user-facing mutation family this no-op belongs to.
    #[must_use]
    pub(in crate::db) const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }

    /// Borrow the requested index name.
    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted index store path, or `-` when no target exists.
    #[must_use]
    pub(in crate::db) const fn target_store(&self) -> &str {
        self.target_store.as_str()
    }

    /// Borrow the target field path, empty when no target exists.
    #[must_use]
    pub(in crate::db) const fn field_path(&self) -> &[String] {
        self.field_path.as_slice()
    }
}

///
/// SqlDdlBindError
///
/// Typed fail-closed reasons for SQL DDL catalog binding.
///
#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) enum SqlDdlBindError {
    NotDdl,

    MissingEntityName,

    EntityMismatch {
        sql_entity: String,
        expected_entity: String,
    },

    UnknownFieldPath {
        entity_name: String,
        field_path: String,
    },

    FieldPathNotIndexable {
        field_path: String,
    },

    FieldPathNotAcceptedCatalogBacked {
        field_path: String,
    },

    InvalidFilteredIndexPredicate,

    DuplicateIndexName {
        index_name: String,
    },

    DuplicateFieldPathIndex {
        field_path: String,
        existing_index: String,
    },

    UnknownIndex {
        entity_name: String,
        index_name: String,
    },

    GeneratedIndexDropRejected {
        index_name: String,
    },

    UnsupportedDropIndex {
        index_name: String,
    },

    InvalidAlterTableAddColumnDefault {
        entity_name: String,
        column_name: String,
    },

    UnsupportedAlterTableAddColumnNotNull {
        entity_name: String,
        column_name: String,
    },

    DuplicateColumn {
        entity_name: String,
        column_name: String,
    },

    UnsupportedAlterTableAddColumnType {
        entity_name: String,
        column_name: String,
        column_type: String,
    },

    UnknownColumn {
        entity_name: String,
        column_name: String,
    },

    InvalidAlterTableAlterColumnDefault {
        entity_name: String,
        column_name: String,
    },

    UnsupportedAlterTableDropDefaultRequired {
        entity_name: String,
        column_name: String,
    },

    GeneratedFieldDefaultChangeRejected {
        entity_name: String,
        column_name: String,
    },

    GeneratedFieldNullabilityChangeRejected {
        entity_name: String,
        column_name: String,
    },

    PrimaryKeyFieldDropRejected {
        entity_name: String,
        column_name: String,
    },

    GeneratedFieldDropRejected {
        entity_name: String,
        column_name: String,
    },

    IndexedFieldDropRejected {
        entity_name: String,
        column_name: String,
        index_name: String,
    },

    GeneratedFieldRenameRejected {
        entity_name: String,
        column_name: String,
    },

    NonPositiveSchemaVersion {
        clause: &'static str,
    },

    MissingExpectedSchemaVersion,

    MissingNextSchemaVersion,

    StaleExpectedSchemaVersion {
        expected: u32,
        accepted: u32,
    },

    EmptySchemaVersionBump {
        requested: u32,
    },
}

///
/// SqlDdlLoweringError
///
/// Typed fail-closed reasons while lowering bound DDL into schema mutation
/// admission.
///
#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) enum SqlDdlLoweringError {
    UnsupportedStatement,

    MutationAdmission(SchemaDdlMutationAdmissionError),
}

///
/// SqlDdlPrepareError
///
/// Typed fail-closed preparation errors for SQL DDL.
///
#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) enum SqlDdlPrepareError {
    Bind(SqlDdlBindError),

    Lowering(SqlDdlLoweringError),
}

impl From<SqlDdlBindError> for SqlDdlPrepareError {
    fn from(value: SqlDdlBindError) -> Self {
        Self::Bind(value)
    }
}

impl From<SqlDdlLoweringError> for SqlDdlPrepareError {
    fn from(value: SqlDdlLoweringError) -> Self {
        Self::Lowering(value)
    }
}

/// Prepare one parsed SQL DDL statement through every pre-execution proof.
pub(in crate::db) fn prepare_sql_ddl_statement(
    statement: &SqlStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
    index_store_path: &'static str,
) -> Result<PreparedSqlDdlCommand, SqlDdlPrepareError> {
    let bound = bind_sql_ddl_statement(statement, accepted_before, schema, index_store_path)?;
    validate_bound_sql_ddl_version_contract(&bound, accepted_before)?;
    let derivation = if matches!(bound.statement(), BoundSqlDdlStatement::NoOp(_)) {
        None
    } else {
        Some(derive_bound_sql_ddl_accepted_after(
            accepted_before,
            &bound,
        )?)
    };
    let report = ddl_preparation_report(&bound);

    Ok(PreparedSqlDdlCommand {
        bound,
        derivation,
        report,
    })
}

/// Bind one parsed SQL DDL statement against accepted catalog metadata.
pub(in crate::db) fn bind_sql_ddl_statement(
    statement: &SqlStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
    index_store_path: &'static str,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let SqlStatement::Ddl(ddl) = statement else {
        return Err(SqlDdlBindError::NotDdl);
    };

    let mut bound = match ddl {
        SqlDdlStatement::CreateIndex(statement) => {
            bind_create_index_statement(statement, accepted_before, schema, index_store_path)
        }
        SqlDdlStatement::DropIndex(statement) => {
            bind_drop_index_statement(statement, accepted_before, schema)
        }
        SqlDdlStatement::AlterTableAddColumn(statement) => {
            bind_alter_table_add_column_statement(statement, accepted_before, schema)
        }
        SqlDdlStatement::AlterTableAlterColumn(statement) => {
            bind_alter_table_alter_column_statement(statement, accepted_before, schema)
        }
        SqlDdlStatement::AlterTableDropColumn(statement) => {
            bind_alter_table_drop_column_statement(statement, accepted_before, schema)
        }
        SqlDdlStatement::AlterTableRenameColumn(statement) => {
            bind_alter_table_rename_column_statement(statement, accepted_before, schema)
        }
    }?;
    bound.schema_version_contract =
        bind_sql_ddl_schema_version_contract(ddl_version_contract(ddl))?;

    Ok(bound)
}

/// Lower one bound SQL DDL request through schema mutation admission.
#[cfg(test)]
pub(in crate::db) fn lower_bound_sql_ddl_to_schema_mutation_admission(
    request: &BoundSqlDdlRequest,
) -> Result<SchemaDdlMutationAdmission, SqlDdlLoweringError> {
    match request.statement() {
        BoundSqlDdlStatement::AddColumn(add) => {
            Ok(admit_sql_ddl_field_addition_candidate(add.field()))
        }
        BoundSqlDdlStatement::AlterColumnDefault(alter) => {
            Ok(admit_sql_ddl_field_default_candidate(alter.field()))
        }
        BoundSqlDdlStatement::AlterColumnNullability(alter) => {
            Ok(admit_sql_ddl_field_nullability_candidate(alter.field()))
        }
        BoundSqlDdlStatement::DropColumn(drop) => {
            Ok(admit_sql_ddl_field_drop_candidate(drop.field()))
        }
        BoundSqlDdlStatement::RenameColumn(rename) => Ok(admit_sql_ddl_field_rename_candidate(
            rename.field(),
            rename.new_name(),
        )),
        BoundSqlDdlStatement::CreateIndex(create) => {
            if create.candidate_index().key().is_field_path_only() {
                admit_sql_ddl_field_path_index_candidate(create.candidate_index())
            } else {
                admit_sql_ddl_expression_index_candidate(create.candidate_index())
            }
        }
        BoundSqlDdlStatement::DropIndex(drop) => {
            admit_sql_ddl_secondary_index_drop_candidate(drop.dropped_index())
        }
        BoundSqlDdlStatement::NoOp(_) => return Err(SqlDdlLoweringError::UnsupportedStatement),
    }
    .map_err(SqlDdlLoweringError::MutationAdmission)
}

/// Derive the accepted-after schema snapshot for one bound SQL DDL request.
pub(in crate::db) fn derive_bound_sql_ddl_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    request: &BoundSqlDdlRequest,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SqlDdlLoweringError> {
    let next_schema_version = request
        .schema_version_contract()
        .next_schema_version()
        .ok_or(SqlDdlLoweringError::UnsupportedStatement)?;
    let derivation = match request.statement() {
        BoundSqlDdlStatement::AddColumn(add) => {
            derive_sql_ddl_field_addition_accepted_after(accepted_before, add.field().clone())
        }
        BoundSqlDdlStatement::AlterColumnDefault(alter) => {
            derive_sql_ddl_field_default_accepted_after(
                accepted_before,
                alter.field_name(),
                alter.default().clone(),
            )
        }
        BoundSqlDdlStatement::AlterColumnNullability(alter) => {
            derive_sql_ddl_field_nullability_accepted_after(
                accepted_before,
                alter.field_name(),
                alter.nullable(),
            )
        }
        BoundSqlDdlStatement::DropColumn(drop) => {
            derive_sql_ddl_field_drop_accepted_after(accepted_before, drop.field_name())
        }
        BoundSqlDdlStatement::RenameColumn(rename) => derive_sql_ddl_field_rename_accepted_after(
            accepted_before,
            rename.old_name(),
            rename.new_name(),
        ),
        BoundSqlDdlStatement::CreateIndex(create) => {
            if create.candidate_index().key().is_field_path_only() {
                derive_sql_ddl_field_path_index_accepted_after(
                    accepted_before,
                    create.candidate_index().clone(),
                )
            } else {
                derive_sql_ddl_expression_index_accepted_after(
                    accepted_before,
                    create.candidate_index().clone(),
                )
            }
        }
        BoundSqlDdlStatement::DropIndex(drop) => {
            derive_sql_ddl_secondary_index_drop_accepted_after(
                accepted_before,
                drop.dropped_index(),
            )
        }
        BoundSqlDdlStatement::NoOp(_) => return Err(SqlDdlLoweringError::UnsupportedStatement),
    }
    .map_err(SqlDdlLoweringError::MutationAdmission)?;

    derivation
        .with_declared_schema_version(accepted_before, next_schema_version)
        .map_err(SqlDdlLoweringError::MutationAdmission)
}
