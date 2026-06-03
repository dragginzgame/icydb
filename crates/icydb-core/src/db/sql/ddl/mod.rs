//! Module: db::sql::ddl
//! Responsibility: bind parsed SQL DDL to accepted schema catalog contracts.
//! Does not own: mutation planning, physical index rebuilds, or SQL execution.
//! Boundary: translates parser-owned DDL syntax into catalog-native requests.

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
use index::{bind_create_index_statement, bind_drop_index_statement, ddl_key_item_report};

use crate::db::{
    schema::{
        AcceptedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation,
        SchemaDdlMutationAdmissionError, SchemaInfo, SchemaVersion,
        derive_sql_ddl_expression_index_accepted_after,
        derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
        derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
        derive_sql_ddl_field_path_index_accepted_after, derive_sql_ddl_field_rename_accepted_after,
        derive_sql_ddl_secondary_index_drop_accepted_after,
    },
    sql::parser::{SqlDdlSchemaVersionContract, SqlDdlStatement, SqlStatement},
};
use thiserror::Error as ThisError;

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
/// SqlDdlPreparationReport
///
/// Compact report for a DDL command that has passed all pre-execution
/// frontend and schema-mutation checks.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlDdlPreparationReport {
    mutation_kind: SqlDdlMutationKind,
    target_index: String,
    target_store: String,
    field_path: Vec<String>,
    execution_status: SqlDdlExecutionStatus,
    rows_scanned: usize,
    index_keys_written: usize,
}

impl SqlDdlPreparationReport {
    /// Return the prepared DDL mutation kind.
    #[must_use]
    pub const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }

    /// Borrow the target accepted index name.
    #[must_use]
    pub const fn target_index(&self) -> &str {
        self.target_index.as_str()
    }

    /// Borrow the target accepted index store path.
    #[must_use]
    pub const fn target_store(&self) -> &str {
        self.target_store.as_str()
    }

    /// Borrow the target field path.
    #[must_use]
    pub const fn field_path(&self) -> &[String] {
        self.field_path.as_slice()
    }

    /// Return the execution status captured by this DDL report.
    #[must_use]
    pub const fn execution_status(&self) -> SqlDdlExecutionStatus {
        self.execution_status
    }

    /// Return rows scanned by DDL execution.
    #[must_use]
    pub const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    /// Return index keys written by DDL execution.
    #[must_use]
    pub const fn index_keys_written(&self) -> usize {
        self.index_keys_written
    }

    pub(in crate::db) const fn with_execution_status(
        mut self,
        execution_status: SqlDdlExecutionStatus,
    ) -> Self {
        self.execution_status = execution_status;
        self
    }

    pub(in crate::db) const fn with_execution_metrics(
        mut self,
        rows_scanned: usize,
        index_keys_written: usize,
    ) -> Self {
        self.rows_scanned = rows_scanned;
        self.index_keys_written = index_keys_written;
        self
    }
}

///
/// SqlDdlMutationKind
///
/// Developer-facing SQL DDL mutation kind.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDdlMutationKind {
    AddDefaultedField,
    AddNullableField,
    SetFieldDefault,
    DropFieldDefault,
    SetFieldNotNull,
    DropFieldNotNull,
    DropField,
    RenameField,
    AddFieldPathIndex,
    AddExpressionIndex,
    DropSecondaryIndex,
}

impl SqlDdlMutationKind {
    /// Return the stable diagnostic label for this DDL mutation kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AddDefaultedField => "add_defaulted_field",
            Self::AddNullableField => "add_nullable_field",
            Self::SetFieldDefault => "set_field_default",
            Self::DropFieldDefault => "drop_field_default",
            Self::SetFieldNotNull => "set_field_not_null",
            Self::DropFieldNotNull => "drop_field_not_null",
            Self::DropField => "drop_field",
            Self::RenameField => "rename_field",
            Self::AddFieldPathIndex => "add_field_path_index",
            Self::AddExpressionIndex => "add_expression_index",
            Self::DropSecondaryIndex => "drop_secondary_index",
        }
    }
}

///
/// SqlDdlExecutionStatus
///
/// SQL DDL execution state at the current boundary.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDdlExecutionStatus {
    PreparedOnly,
    Published,
    NoOp,
}

impl SqlDdlExecutionStatus {
    /// Return the stable diagnostic label for this execution status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedOnly => "prepared_only",
            Self::Published => "published",
            Self::NoOp => "no_op",
        }
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
#[derive(Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum SqlDdlBindError {
    #[error("SQL DDL binder requires a DDL statement")]
    NotDdl,

    #[error("accepted schema does not expose an entity name")]
    MissingEntityName,

    #[error("SQL entity '{sql_entity}' does not match accepted entity '{expected_entity}'")]
    EntityMismatch {
        sql_entity: String,
        expected_entity: String,
    },

    #[error("unknown field path '{field_path}' for accepted entity '{entity_name}'")]
    UnknownFieldPath {
        entity_name: String,
        field_path: String,
    },

    #[error("field path '{field_path}' is not indexable")]
    FieldPathNotIndexable { field_path: String },

    #[error("field path '{field_path}' depends on generated-only metadata")]
    FieldPathNotAcceptedCatalogBacked { field_path: String },

    #[error("invalid filtered index predicate: {detail}")]
    InvalidFilteredIndexPredicate { detail: String },

    #[error("index name '{index_name}' already exists in the accepted schema")]
    DuplicateIndexName { index_name: String },

    #[error("accepted schema already has index '{existing_index}' for field path '{field_path}'")]
    DuplicateFieldPathIndex {
        field_path: String,
        existing_index: String,
    },

    #[error("unknown index '{index_name}' for accepted entity '{entity_name}'")]
    UnknownIndex {
        entity_name: String,
        index_name: String,
    },

    #[error(
        "index '{index_name}' is generated by the entity model and cannot be dropped with SQL DDL; remove the index from the entity schema macro instead"
    )]
    GeneratedIndexDropRejected { index_name: String },

    #[error(
        "index '{index_name}' is not a supported DDL-droppable secondary index; SQL DDL can currently drop only indexes created through SQL DDL"
    )]
    UnsupportedDropIndex { index_name: String },

    #[error(
        "SQL DDL ALTER TABLE ADD COLUMN DEFAULT value is not encodable for accepted entity '{entity_name}' column '{column_name}': {detail}"
    )]
    InvalidAlterTableAddColumnDefault {
        entity_name: String,
        column_name: String,
        detail: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ADD COLUMN NOT NULL is not executable yet for accepted entity '{entity_name}' column '{column_name}'"
    )]
    UnsupportedAlterTableAddColumnNotNull {
        entity_name: String,
        column_name: String,
    },

    #[error("field '{column_name}' already exists in accepted entity '{entity_name}'")]
    DuplicateColumn {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ADD COLUMN type '{column_type}' is not supported yet for accepted entity '{entity_name}' column '{column_name}'"
    )]
    UnsupportedAlterTableAddColumnType {
        entity_name: String,
        column_name: String,
        column_type: String,
    },

    #[error("unknown column '{column_name}' for accepted entity '{entity_name}'")]
    UnknownColumn {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ALTER COLUMN SET DEFAULT value is not encodable for accepted entity '{entity_name}' column '{column_name}': {detail}"
    )]
    InvalidAlterTableAlterColumnDefault {
        entity_name: String,
        column_name: String,
        detail: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ALTER COLUMN DROP DEFAULT is not executable yet for required accepted entity '{entity_name}' column '{column_name}'"
    )]
    UnsupportedAlterTableDropDefaultRequired {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ALTER COLUMN DEFAULT cannot change generated accepted field '{column_name}' on entity '{entity_name}'; change the Rust schema default instead"
    )]
    GeneratedFieldDefaultChangeRejected {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE ALTER COLUMN NULLABILITY cannot change generated accepted field '{column_name}' on entity '{entity_name}'; change the Rust schema nullability instead"
    )]
    GeneratedFieldNullabilityChangeRejected {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE DROP COLUMN cannot drop primary-key field '{column_name}' on entity '{entity_name}'"
    )]
    PrimaryKeyFieldDropRejected {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE DROP COLUMN cannot change generated accepted field '{column_name}' on entity '{entity_name}'; remove the field from the Rust schema instead"
    )]
    GeneratedFieldDropRejected {
        entity_name: String,
        column_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE DROP COLUMN cannot drop accepted field '{column_name}' on entity '{entity_name}' while index '{index_name}' depends on it; drop dependent DDL-owned indexes first"
    )]
    IndexedFieldDropRejected {
        entity_name: String,
        column_name: String,
        index_name: String,
    },

    #[error(
        "SQL DDL ALTER TABLE RENAME COLUMN cannot change generated accepted field '{column_name}' on entity '{entity_name}'; rename the field in the Rust schema instead"
    )]
    GeneratedFieldRenameRejected {
        entity_name: String,
        column_name: String,
    },

    #[error("SQL DDL {clause} must be a positive schema version")]
    NonPositiveSchemaVersion { clause: &'static str },

    #[error("mutating SQL DDL requires EXPECT SCHEMA VERSION")]
    MissingExpectedSchemaVersion,

    #[error("mutating SQL DDL requires SET SCHEMA VERSION")]
    MissingNextSchemaVersion,

    #[error(
        "SQL DDL expected accepted schema version {expected}, but accepted schema version is {accepted}"
    )]
    StaleExpectedSchemaVersion { expected: u32, accepted: u32 },

    #[error("SQL DDL no-op cannot SET SCHEMA VERSION {requested}")]
    EmptySchemaVersionBump { requested: u32 },
}

///
/// SqlDdlLoweringError
///
/// Typed fail-closed reasons while lowering bound DDL into schema mutation
/// admission.
///
#[derive(Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum SqlDdlLoweringError {
    #[error("SQL DDL lowering requires a supported DDL statement")]
    UnsupportedStatement,

    #[error("schema mutation admission rejected DDL candidate: {0:?}")]
    MutationAdmission(SchemaDdlMutationAdmissionError),
}

///
/// SqlDdlPrepareError
///
/// Typed fail-closed preparation errors for SQL DDL.
///
#[derive(Debug, Eq, PartialEq, ThisError)]
pub(in crate::db) enum SqlDdlPrepareError {
    #[error("{0}")]
    Bind(#[from] SqlDdlBindError),

    #[error("{0}")]
    Lowering(#[from] SqlDdlLoweringError),
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
            bind_create_index_statement(statement, schema, index_store_path)
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
        BoundSqlDdlStatement::RenameColumn(rename) => {
            let after = rename
                .field()
                .clone_with_name(rename.new_name().to_string());
            Ok(admit_sql_ddl_field_rename_candidate(rename.field(), &after))
        }
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

const fn ddl_version_contract(ddl: &SqlDdlStatement) -> SqlDdlSchemaVersionContract {
    match ddl {
        SqlDdlStatement::CreateIndex(statement) => statement.schema_version_contract,
        SqlDdlStatement::DropIndex(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableAddColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableAlterColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableDropColumn(statement) => statement.schema_version_contract,
        SqlDdlStatement::AlterTableRenameColumn(statement) => statement.schema_version_contract,
    }
}

fn bind_sql_ddl_schema_version_contract(
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

fn validate_bound_sql_ddl_version_contract(
    bound: &BoundSqlDdlRequest,
    accepted_before: &AcceptedSchemaSnapshot,
) -> Result<(), SqlDdlBindError> {
    let contract = bound.schema_version_contract();
    let accepted_version = accepted_before.persisted_snapshot().version();
    if let Some(expected) = contract.expected_schema_version()
        && expected != accepted_version
    {
        return Err(SqlDdlBindError::StaleExpectedSchemaVersion {
            expected: expected.get(),
            accepted: accepted_version.get(),
        });
    }
    if matches!(bound.statement(), BoundSqlDdlStatement::NoOp(_)) {
        if let Some(requested) = contract.next_schema_version() {
            return Err(SqlDdlBindError::EmptySchemaVersionBump {
                requested: requested.get(),
            });
        }

        return Ok(());
    }
    if contract.expected_schema_version().is_none() {
        return Err(SqlDdlBindError::MissingExpectedSchemaVersion);
    }
    if contract.next_schema_version().is_none() {
        return Err(SqlDdlBindError::MissingNextSchemaVersion);
    }

    Ok(())
}

fn ddl_preparation_report(bound: &BoundSqlDdlRequest) -> SqlDdlPreparationReport {
    match bound.statement() {
        BoundSqlDdlStatement::AddColumn(add) => SqlDdlPreparationReport {
            mutation_kind: if add.field().default().is_none() {
                SqlDdlMutationKind::AddNullableField
            } else {
                SqlDdlMutationKind::AddDefaultedField
            },
            target_index: add.field().name().to_string(),
            target_store: add.entity_name().to_string(),
            field_path: vec![add.field().name().to_string()],
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::AlterColumnDefault(alter) => SqlDdlPreparationReport {
            mutation_kind: alter.mutation_kind(),
            target_index: alter.field_name().to_string(),
            target_store: alter.entity_name().to_string(),
            field_path: vec![alter.field_name().to_string()],
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::AlterColumnNullability(alter) => SqlDdlPreparationReport {
            mutation_kind: alter.mutation_kind(),
            target_index: alter.field_name().to_string(),
            target_store: alter.entity_name().to_string(),
            field_path: vec![alter.field_name().to_string()],
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::DropColumn(drop) => SqlDdlPreparationReport {
            mutation_kind: SqlDdlMutationKind::DropField,
            target_index: drop.field_name().to_string(),
            target_store: drop.entity_name().to_string(),
            field_path: vec![drop.field_name().to_string()],
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::RenameColumn(rename) => SqlDdlPreparationReport {
            mutation_kind: SqlDdlMutationKind::RenameField,
            target_index: rename.new_name().to_string(),
            target_store: rename.entity_name().to_string(),
            field_path: vec![rename.old_name().to_string(), rename.new_name().to_string()],
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::CreateIndex(create) => {
            let target = create.candidate_index();

            SqlDdlPreparationReport {
                mutation_kind: if target.key().is_field_path_only() {
                    SqlDdlMutationKind::AddFieldPathIndex
                } else {
                    SqlDdlMutationKind::AddExpressionIndex
                },
                target_index: target.name().to_string(),
                target_store: target.store().to_string(),
                field_path: ddl_key_item_report(create.key_items()),
                execution_status: SqlDdlExecutionStatus::PreparedOnly,
                rows_scanned: 0,
                index_keys_written: 0,
            }
        }
        BoundSqlDdlStatement::DropIndex(drop) => SqlDdlPreparationReport {
            mutation_kind: SqlDdlMutationKind::DropSecondaryIndex,
            target_index: drop.index_name().to_string(),
            target_store: drop.dropped_index().store().to_string(),
            field_path: drop.field_path().to_vec(),
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
        BoundSqlDdlStatement::NoOp(no_op) => SqlDdlPreparationReport {
            mutation_kind: no_op.mutation_kind(),
            target_index: no_op.index_name().to_string(),
            target_store: no_op.target_store().to_string(),
            field_path: no_op.field_path().to_vec(),
            execution_status: SqlDdlExecutionStatus::PreparedOnly,
            rows_scanned: 0,
            index_keys_written: 0,
        },
    }
}
