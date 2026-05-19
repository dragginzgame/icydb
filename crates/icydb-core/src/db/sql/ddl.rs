//! Module: db::sql::ddl
//! Responsibility: bind parsed SQL DDL to accepted schema catalog contracts.
//! Does not own: mutation planning, physical index rebuilds, or SQL execution.
//! Boundary: translates parser-owned DDL syntax into catalog-native requests.

#![allow(
    dead_code,
    reason = "DDL binding exposes prepare-only diagnostics and test-only inspection accessors"
)]

use crate::db::{
    data::encode_runtime_value_for_accepted_field_contract,
    predicate::parse_sql_predicate,
    query::predicate::validate_predicate,
    schema::{
        AcceptedFieldDecodeContract, AcceptedSchemaSnapshot, FieldId, PersistedFieldKind,
        PersistedFieldOrigin, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, SchemaDdlAcceptedSnapshotDerivation,
        SchemaDdlIndexDropCandidateError, SchemaDdlMutationAdmission,
        SchemaDdlMutationAdmissionError, SchemaExpressionIndexInfo,
        SchemaExpressionIndexKeyItemInfo, SchemaFieldDefault, SchemaFieldSlot,
        SchemaFieldWritePolicy, SchemaInfo, admit_sql_ddl_expression_index_candidate,
        admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
        admit_sql_ddl_field_nullability_candidate, admit_sql_ddl_field_path_index_candidate,
        admit_sql_ddl_secondary_index_drop_candidate,
        canonicalize_strict_sql_literal_for_persisted_kind,
        derive_sql_ddl_expression_index_accepted_after,
        derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
        derive_sql_ddl_field_nullability_accepted_after,
        derive_sql_ddl_field_path_index_accepted_after,
        derive_sql_ddl_secondary_index_drop_accepted_after,
        resolve_sql_ddl_secondary_index_drop_candidate,
    },
    sql::{
        identifier::identifiers_tail_match,
        parser::{
            SqlAlterColumnAction, SqlAlterTableAddColumnStatement,
            SqlAlterTableAlterColumnStatement, SqlAlterTableDropColumnStatement,
            SqlCreateIndexExpressionKey, SqlCreateIndexKeyItem, SqlCreateIndexStatement,
            SqlCreateIndexUniqueness, SqlDdlStatement, SqlDropIndexStatement, SqlStatement,
        },
    },
};
use crate::model::field::{FieldStorageDecode, LeafCodec, ScalarCodec};
use thiserror::Error as ThisError;

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
}

impl BoundSqlDdlRequest {
    /// Borrow the bound statement payload.
    #[must_use]
    pub(in crate::db) const fn statement(&self) -> &BoundSqlDdlStatement {
        &self.statement
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
    CreateIndex(BoundSqlCreateIndexRequest),
    DropIndex(BoundSqlDropIndexRequest),
    NoOp(BoundSqlDdlNoOpRequest),
}

///
/// BoundSqlAddColumnRequest
///
/// Catalog-resolved additive field DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAddColumnRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
}

impl BoundSqlAddColumnRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted DDL-owned field snapshot to publish.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }
}

///
/// BoundSqlAlterColumnDefaultRequest
///
/// Catalog-resolved field-default metadata DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAlterColumnDefaultRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
    default: SchemaFieldDefault,
    mutation_kind: SqlDdlMutationKind,
}

impl BoundSqlAlterColumnDefaultRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted field whose default will change.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }

    /// Borrow the default contract to publish.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> &SchemaFieldDefault {
        &self.default
    }

    /// Return the field-default mutation kind.
    #[must_use]
    pub(in crate::db) const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }
}

///
/// BoundSqlAlterColumnNullabilityRequest
///
/// Catalog-resolved field-nullability metadata DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAlterColumnNullabilityRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
    nullable: bool,
    mutation_kind: SqlDdlMutationKind,
}

impl BoundSqlAlterColumnNullabilityRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted field whose nullability will change.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }

    /// Return the nullable contract to publish.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the field-nullability mutation kind.
    #[must_use]
    pub(in crate::db) const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }
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
/// BoundSqlCreateIndexRequest
///
/// Catalog-resolved request for adding one secondary index.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlCreateIndexRequest {
    index_name: String,
    entity_name: String,
    key_items: Vec<BoundSqlDdlCreateIndexKey>,
    field_paths: Vec<BoundSqlDdlFieldPath>,
    candidate_index: PersistedIndexSnapshot,
}

impl BoundSqlCreateIndexRequest {
    /// Borrow the requested index name.
    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field-path targets.
    #[must_use]
    pub(in crate::db) const fn field_paths(&self) -> &[BoundSqlDdlFieldPath] {
        self.field_paths.as_slice()
    }

    /// Borrow the accepted key targets in DDL key order.
    #[must_use]
    pub(in crate::db) const fn key_items(&self) -> &[BoundSqlDdlCreateIndexKey] {
        self.key_items.as_slice()
    }

    /// Borrow the candidate accepted index snapshot for mutation admission.
    #[must_use]
    pub(in crate::db) const fn candidate_index(&self) -> &PersistedIndexSnapshot {
        &self.candidate_index
    }
}

///
/// BoundSqlDropIndexRequest
///
/// Catalog-resolved request for dropping one DDL-published secondary index.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDropIndexRequest {
    index_name: String,
    entity_name: String,
    dropped_index: PersistedIndexSnapshot,
    field_path: Vec<String>,
}

impl BoundSqlDropIndexRequest {
    /// Borrow the requested index name.
    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted index snapshot that will be removed.
    #[must_use]
    pub(in crate::db) const fn dropped_index(&self) -> &PersistedIndexSnapshot {
        &self.dropped_index
    }

    /// Borrow the dropped field-path target.
    #[must_use]
    pub(in crate::db) const fn field_path(&self) -> &[String] {
        self.field_path.as_slice()
    }
}

///
/// BoundSqlDdlFieldPath
///
/// Accepted field-path target for SQL DDL binding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlFieldPath {
    root: String,
    segments: Vec<String>,
    accepted_path: Vec<String>,
}

impl BoundSqlDdlFieldPath {
    /// Borrow the top-level field name.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &str {
        self.root.as_str()
    }

    /// Borrow nested path segments below the top-level field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.segments.as_slice()
    }

    /// Borrow the full accepted field path used by index metadata.
    #[must_use]
    pub(in crate::db) const fn accepted_path(&self) -> &[String] {
        self.accepted_path.as_slice()
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

    #[error("accepted schema does not expose an entity path")]
    MissingEntityPath,

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
        "SQL DDL ALTER TABLE ADD COLUMN is not executable yet for accepted entity '{entity_name}' column '{column_name}'"
    )]
    UnsupportedAlterTableAddColumn {
        entity_name: String,
        column_name: String,
    },

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
        "SQL DDL ALTER TABLE ALTER COLUMN {action} is not executable yet for accepted entity '{entity_name}' column '{column_name}'"
    )]
    UnsupportedAlterTableAlterColumn {
        entity_name: String,
        column_name: String,
        action: String,
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
        "SQL DDL ALTER TABLE DROP COLUMN is not executable yet for accepted entity '{entity_name}' column '{column_name}'; retained-slot field removal policy is not enabled yet"
    )]
    UnsupportedAlterTableDropColumn {
        entity_name: String,
        column_name: String,
    },
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

    match ddl {
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
    }
}

fn bind_create_index_statement(
    statement: &SqlCreateIndexStatement,
    schema: &SchemaInfo,
    index_store_path: &'static str,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let key_items = statement
        .key_items
        .iter()
        .map(|key_item| bind_create_index_key_item(key_item, entity_name, schema))
        .collect::<Result<Vec<_>, _>>()?;
    let field_paths = create_index_field_path_report_items(key_items.as_slice());
    if let Some(existing_index) = find_field_path_index_by_name(schema, statement.name.as_str()) {
        if key_items_are_field_path_only(key_items.as_slice())
            && statement.if_not_exists
            && existing_field_path_index_matches_request(
                existing_index,
                field_paths.as_slice(),
                statement.predicate_sql.as_deref(),
                statement.uniqueness,
            )
        {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::AddFieldPathIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: existing_index.store().to_string(),
                    field_path: ddl_field_path_report(field_paths.as_slice()),
                }),
            });
        }

        return Err(SqlDdlBindError::DuplicateIndexName {
            index_name: statement.name.clone(),
        });
    }
    let predicate_sql =
        validated_create_index_predicate_sql(statement.predicate_sql.as_deref(), schema)?;
    if let Some(existing_index) = find_expression_index_by_name(schema, statement.name.as_str()) {
        if statement.if_not_exists
            && existing_expression_index_matches_request(
                existing_index,
                key_items.as_slice(),
                predicate_sql.as_deref(),
                statement.uniqueness,
            )
        {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::AddExpressionIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: existing_index.store().to_string(),
                    field_path: ddl_key_item_report(key_items.as_slice()),
                }),
            });
        }

        return Err(SqlDdlBindError::DuplicateIndexName {
            index_name: statement.name.clone(),
        });
    }
    if key_items_are_field_path_only(key_items.as_slice()) {
        reject_duplicate_field_path_index(
            field_paths.as_slice(),
            predicate_sql.as_deref(),
            schema,
        )?;
    } else {
        reject_duplicate_expression_index(key_items.as_slice(), predicate_sql.as_deref(), schema)?;
    }
    let candidate_index = candidate_index_snapshot(
        statement.name.as_str(),
        key_items.as_slice(),
        predicate_sql.as_deref(),
        statement.uniqueness,
        schema,
        index_store_path,
    )?;

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::CreateIndex(BoundSqlCreateIndexRequest {
            index_name: statement.name.clone(),
            entity_name: entity_name.to_string(),
            key_items,
            field_paths,
            candidate_index,
        }),
    })
}

fn bind_drop_index_statement(
    statement: &SqlDropIndexStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if let Some(sql_entity) = statement.entity.as_deref()
        && !identifiers_tail_match(sql_entity, entity_name)
    {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: sql_entity.to_string(),
            expected_entity: entity_name.to_string(),
        });
    }
    let drop_candidate = resolve_sql_ddl_secondary_index_drop_candidate(
        accepted_before,
        &statement.name,
    )
    .map_err(|error| match error {
        SchemaDdlIndexDropCandidateError::Generated => {
            SqlDdlBindError::GeneratedIndexDropRejected {
                index_name: statement.name.clone(),
            }
        }
        SchemaDdlIndexDropCandidateError::Unknown => SqlDdlBindError::UnknownIndex {
            entity_name: entity_name.to_string(),
            index_name: statement.name.clone(),
        },
        SchemaDdlIndexDropCandidateError::Unsupported => SqlDdlBindError::UnsupportedDropIndex {
            index_name: statement.name.clone(),
        },
    });
    let (dropped_index, field_path) = match drop_candidate {
        Ok((dropped_index, field_path)) => (dropped_index, field_path),
        Err(SqlDdlBindError::UnknownIndex { .. }) if statement.if_exists => {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::DropSecondaryIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: "-".to_string(),
                    field_path: Vec::new(),
                }),
            });
        }
        Err(error) => return Err(error),
    };
    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::DropIndex(BoundSqlDropIndexRequest {
            index_name: statement.name.clone(),
            entity_name: entity_name.to_string(),
            dropped_index,
            field_path,
        }),
    })
}

fn bind_alter_table_add_column_statement(
    statement: &SqlAlterTableAddColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    if schema
        .field_nullable(statement.column_name.as_str())
        .is_some()
    {
        return Err(SqlDdlBindError::DuplicateColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    let (kind, storage_decode, leaf_codec) = persisted_field_contract_for_sql_column_type(
        statement.column_type.as_str(),
    )
    .ok_or_else(|| SqlDdlBindError::UnsupportedAlterTableAddColumnType {
        entity_name: entity_name.to_string(),
        column_name: statement.column_name.clone(),
        column_type: statement.column_type.clone(),
    })?;
    let default = schema_field_default_for_sql_default(
        entity_name,
        statement.column_name.as_str(),
        statement.default.as_ref(),
        &kind,
        statement.nullable,
        storage_decode,
        leaf_codec,
    )?;
    if !statement.nullable && default.is_none() {
        return Err(SqlDdlBindError::UnsupportedAlterTableAddColumnNotNull {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }
    let field = PersistedFieldSnapshot::new_with_write_policy_and_origin(
        next_sql_ddl_field_id(accepted_before),
        statement.column_name.clone(),
        next_sql_ddl_field_slot(accepted_before),
        kind,
        Vec::new(),
        statement.nullable,
        default,
        SchemaFieldWritePolicy::from_model_policies(None, None),
        PersistedFieldOrigin::SqlDdl,
        storage_decode,
        leaf_codec,
    );

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AddColumn(BoundSqlAddColumnRequest {
            entity_name: entity_name.to_string(),
            field,
        }),
    })
}

fn alter_table_alter_column_bind_error(
    statement: &SqlAlterTableAlterColumnStatement,
    schema: &SchemaInfo,
) -> SqlDdlBindError {
    let Some(entity_name) = schema.entity_name() else {
        return SqlDdlBindError::MissingEntityName;
    };

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        };
    }

    if schema
        .field_nullable(statement.column_name.as_str())
        .is_none()
    {
        return SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        };
    }

    SqlDdlBindError::UnsupportedAlterTableAlterColumn {
        entity_name: entity_name.to_string(),
        column_name: statement.column_name.clone(),
        action: statement.action.label().to_string(),
    }
}

fn bind_alter_table_alter_column_statement(
    statement: &SqlAlterTableAlterColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let Some(entity_name) = schema.entity_name() else {
        return Err(SqlDdlBindError::MissingEntityName);
    };

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == statement.column_name)
        .ok_or_else(|| SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        })?;

    match &statement.action {
        SqlAlterColumnAction::SetDefault(default) => {
            reject_generated_field_default_change(entity_name, field)?;
            let default =
                schema_field_default_for_alter_column_default(entity_name, field, default)?;
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                field,
                default,
                SqlDdlMutationKind::SetFieldDefault,
            ))
        }
        SqlAlterColumnAction::DropDefault => {
            if !field.default().is_none() {
                reject_generated_field_default_change(entity_name, field)?;
            }
            if !field.nullable() && !field.default().is_none() {
                return Err(SqlDdlBindError::UnsupportedAlterTableDropDefaultRequired {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                });
            }
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                field,
                SchemaFieldDefault::None,
                SqlDdlMutationKind::DropFieldDefault,
            ))
        }
        SqlAlterColumnAction::SetNotNull => Ok(bind_alter_table_alter_column_nullability(
            entity_name,
            field,
            false,
            SqlDdlMutationKind::SetFieldNotNull,
        )?),
        SqlAlterColumnAction::DropNotNull => Ok(bind_alter_table_alter_column_nullability(
            entity_name,
            field,
            true,
            SqlDdlMutationKind::DropFieldNotNull,
        )?),
    }
}

fn bind_alter_table_drop_column_statement(
    statement: &SqlAlterTableDropColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let accepted = accepted_before.persisted_snapshot();
    let Some(field) = accepted
        .fields()
        .iter()
        .find(|field| field.name() == statement.column_name)
    else {
        if statement.if_exists {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::DropField,
                    index_name: statement.column_name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: "-".to_string(),
                    field_path: vec![statement.column_name.clone()],
                }),
            });
        }

        return Err(SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    };

    if accepted.primary_key_field_id() == field.id() {
        return Err(SqlDdlBindError::PrimaryKeyFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    if let Some(index) = accepted
        .indexes()
        .iter()
        .find(|index| index.key().references_field(field.id()))
    {
        return Err(SqlDdlBindError::IndexedFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
            index_name: index.name().to_string(),
        });
    }

    Err(SqlDdlBindError::UnsupportedAlterTableDropColumn {
        entity_name: entity_name.to_string(),
        column_name: statement.column_name.clone(),
    })
}

fn bind_alter_table_alter_column_default(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    default: SchemaFieldDefault,
    mutation_kind: SqlDdlMutationKind,
) -> BoundSqlDdlRequest {
    if field.default() == &default {
        return BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind,
                index_name: field.name().to_string(),
                entity_name: entity_name.to_string(),
                target_store: entity_name.to_string(),
                field_path: vec![field.name().to_string()],
            }),
        };
    }

    BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AlterColumnDefault(BoundSqlAlterColumnDefaultRequest {
            entity_name: entity_name.to_string(),
            field: field.clone(),
            default,
            mutation_kind,
        }),
    }
}

fn reject_generated_field_default_change(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
) -> Result<(), SqlDdlBindError> {
    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldDefaultChangeRejected {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
        });
    }

    Ok(())
}

fn bind_alter_table_alter_column_nullability(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    nullable: bool,
    mutation_kind: SqlDdlMutationKind,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    if field.nullable() == nullable {
        return Ok(BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind,
                index_name: field.name().to_string(),
                entity_name: entity_name.to_string(),
                target_store: entity_name.to_string(),
                field_path: vec![field.name().to_string()],
            }),
        });
    }

    reject_generated_field_nullability_change(entity_name, field)?;

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AlterColumnNullability(
            BoundSqlAlterColumnNullabilityRequest {
                entity_name: entity_name.to_string(),
                field: field.clone(),
                nullable,
                mutation_kind,
            },
        ),
    })
}

fn reject_generated_field_nullability_change(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
) -> Result<(), SqlDdlBindError> {
    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldNullabilityChangeRejected {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
        });
    }

    Ok(())
}

fn schema_field_default_for_sql_default(
    entity_name: &str,
    column_name: &str,
    default: Option<&crate::value::Value>,
    kind: &PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> Result<SchemaFieldDefault, SqlDdlBindError> {
    let Some(default) = default else {
        return Ok(SchemaFieldDefault::None);
    };
    if matches!(default, crate::value::Value::Null) {
        return Err(SqlDdlBindError::InvalidAlterTableAddColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
            detail: "NULL cannot be used as an accepted database default".to_string(),
        });
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, default)
        .unwrap_or_else(|| default.clone());
    let contract =
        AcceptedFieldDecodeContract::new(column_name, kind, nullable, storage_decode, leaf_codec);
    let payload = encode_runtime_value_for_accepted_field_contract(contract, &normalized).map_err(
        |error| SqlDdlBindError::InvalidAlterTableAddColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
            detail: error.to_string(),
        },
    )?;

    Ok(SchemaFieldDefault::SlotPayload(payload))
}

fn schema_field_default_for_alter_column_default(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    default: &crate::value::Value,
) -> Result<SchemaFieldDefault, SqlDdlBindError> {
    if matches!(default, crate::value::Value::Null) {
        return Err(SqlDdlBindError::InvalidAlterTableAlterColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
            detail: "NULL cannot be used as an accepted database default".to_string(),
        });
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(field.kind(), default)
        .unwrap_or_else(|| default.clone());
    let contract = AcceptedFieldDecodeContract::new(
        field.name(),
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    );
    let payload = encode_runtime_value_for_accepted_field_contract(contract, &normalized).map_err(
        |error| SqlDdlBindError::InvalidAlterTableAlterColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
            detail: error.to_string(),
        },
    )?;

    Ok(SchemaFieldDefault::SlotPayload(payload))
}

fn next_sql_ddl_field_id(accepted_before: &AcceptedSchemaSnapshot) -> FieldId {
    let next = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .map(|field| field.id().get())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("accepted field IDs should not be exhausted");

    FieldId::new(next)
}

fn next_sql_ddl_field_slot(accepted_before: &AcceptedSchemaSnapshot) -> SchemaFieldSlot {
    let next = accepted_before
        .persisted_snapshot()
        .row_layout()
        .field_to_slot()
        .iter()
        .map(|(_, slot)| slot.get())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("accepted row slots should not be exhausted");

    SchemaFieldSlot::new(next)
}

fn persisted_field_contract_for_sql_column_type(
    column_type: &str,
) -> Option<(PersistedFieldKind, FieldStorageDecode, LeafCodec)> {
    let normalized = column_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "bool" | "boolean" => Some((
            PersistedFieldKind::Bool,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        )),
        "int" | "integer" => Some((
            PersistedFieldKind::Int,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "nat" | "natural" => Some((
            PersistedFieldKind::Nat,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "text" | "string" => Some((
            PersistedFieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        )),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum BoundSqlDdlCreateIndexKey {
    FieldPath(BoundSqlDdlFieldPath),
    Expression(BoundSqlDdlExpressionKey),
}

///
/// BoundSqlDdlExpressionKey
///
/// Accepted expression-index key target for SQL DDL binding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlExpressionKey {
    op: PersistedIndexExpressionOp,
    source: BoundSqlDdlFieldPath,
    canonical_sql: String,
}

impl BoundSqlDdlExpressionKey {
    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    /// Borrow the accepted source field path.
    #[must_use]
    pub(in crate::db) const fn source(&self) -> &BoundSqlDdlFieldPath {
        &self.source
    }

    /// Borrow the SQL-facing canonical expression text.
    #[must_use]
    pub(in crate::db) const fn canonical_sql(&self) -> &str {
        self.canonical_sql.as_str()
    }
}

fn bind_create_index_key_item(
    key_item: &SqlCreateIndexKeyItem,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlCreateIndexKey, SqlDdlBindError> {
    match key_item {
        SqlCreateIndexKeyItem::FieldPath(field_path) => {
            bind_create_index_field_path(field_path.as_str(), entity_name, schema)
                .map(BoundSqlDdlCreateIndexKey::FieldPath)
        }
        SqlCreateIndexKeyItem::Expression(expression) => {
            bind_create_index_expression_key(expression, entity_name, schema)
        }
    }
}

fn bind_create_index_expression_key(
    expression: &SqlCreateIndexExpressionKey,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlCreateIndexKey, SqlDdlBindError> {
    let source = bind_create_index_field_path(expression.field_path.as_str(), entity_name, schema)?;

    Ok(BoundSqlDdlCreateIndexKey::Expression(
        BoundSqlDdlExpressionKey {
            op: expression_op_from_sql_function(expression.function),
            source,
            canonical_sql: expression.canonical_sql(),
        },
    ))
}

const fn expression_op_from_sql_function(
    function: crate::db::sql::parser::SqlCreateIndexExpressionFunction,
) -> PersistedIndexExpressionOp {
    match function {
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Lower => {
            PersistedIndexExpressionOp::Lower
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Upper => {
            PersistedIndexExpressionOp::Upper
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Trim => {
            PersistedIndexExpressionOp::Trim
        }
    }
}

fn key_items_are_field_path_only(key_items: &[BoundSqlDdlCreateIndexKey]) -> bool {
    key_items
        .iter()
        .all(|key_item| matches!(key_item, BoundSqlDdlCreateIndexKey::FieldPath(_)))
}

fn create_index_field_path_report_items(
    key_items: &[BoundSqlDdlCreateIndexKey],
) -> Vec<BoundSqlDdlFieldPath> {
    key_items
        .iter()
        .map(|key_item| match key_item {
            BoundSqlDdlCreateIndexKey::FieldPath(field_path) => field_path.clone(),
            BoundSqlDdlCreateIndexKey::Expression(expression) => expression.source().clone(),
        })
        .collect()
}

fn bind_create_index_field_path(
    field_path: &str,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlFieldPath, SqlDdlBindError> {
    let mut path = field_path
        .split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty());
    let Some(root) = path.next() else {
        return Err(SqlDdlBindError::UnknownFieldPath {
            entity_name: entity_name.to_string(),
            field_path: field_path.to_string(),
        });
    };
    let segments = path.map(str::to_string).collect::<Vec<_>>();

    let capabilities = if segments.is_empty() {
        schema.sql_capabilities(root)
    } else {
        schema.nested_sql_capabilities(root, segments.as_slice())
    }
    .ok_or_else(|| SqlDdlBindError::UnknownFieldPath {
        entity_name: entity_name.to_string(),
        field_path: field_path.to_string(),
    })?;

    if !capabilities.orderable() {
        return Err(SqlDdlBindError::FieldPathNotIndexable {
            field_path: field_path.to_string(),
        });
    }

    let mut accepted_path = Vec::with_capacity(segments.len() + 1);
    accepted_path.push(root.to_string());
    accepted_path.extend(segments.iter().cloned());

    Ok(BoundSqlDdlFieldPath {
        root: root.to_string(),
        segments,
        accepted_path,
    })
}

fn find_field_path_index_by_name<'a>(
    schema: &'a SchemaInfo,
    index_name: &str,
) -> Option<&'a crate::db::schema::SchemaIndexInfo> {
    schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == index_name)
}

fn existing_field_path_index_matches_request(
    index: &crate::db::schema::SchemaIndexInfo,
    field_paths: &[BoundSqlDdlFieldPath],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
) -> bool {
    let fields = index.fields();

    index.unique() == matches!(uniqueness, SqlCreateIndexUniqueness::Unique)
        && index.predicate_sql() == predicate_sql
        && fields.len() == field_paths.len()
        && fields
            .iter()
            .zip(field_paths)
            .all(|(field, requested)| field.path() == requested.accepted_path())
}

fn find_expression_index_by_name<'a>(
    schema: &'a SchemaInfo,
    index_name: &str,
) -> Option<&'a SchemaExpressionIndexInfo> {
    schema
        .expression_indexes()
        .iter()
        .find(|index| index.name() == index_name)
}

fn existing_expression_index_matches_request(
    index: &SchemaExpressionIndexInfo,
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
) -> bool {
    let existing_key_items = index.key_items();

    index.unique() == matches!(uniqueness, SqlCreateIndexUniqueness::Unique)
        && index.predicate_sql() == predicate_sql
        && existing_key_items.len() == key_items.len()
        && existing_key_items
            .iter()
            .zip(key_items)
            .all(existing_expression_key_item_matches_request)
}

fn existing_expression_key_item_matches_request(
    existing: (
        &SchemaExpressionIndexKeyItemInfo,
        &BoundSqlDdlCreateIndexKey,
    ),
) -> bool {
    let (existing, requested) = existing;
    match (existing, requested) {
        (
            SchemaExpressionIndexKeyItemInfo::FieldPath(existing),
            BoundSqlDdlCreateIndexKey::FieldPath(requested),
        ) => existing.path() == requested.accepted_path(),
        (
            SchemaExpressionIndexKeyItemInfo::Expression(existing),
            BoundSqlDdlCreateIndexKey::Expression(requested),
        ) => existing_expression_component_matches_request(
            existing.op(),
            existing.source().path(),
            existing.canonical_text(),
            requested,
        ),
        _ => false,
    }
}

fn existing_expression_component_matches_request(
    existing_op: PersistedIndexExpressionOp,
    existing_path: &[String],
    existing_canonical_text: &str,
    requested: &BoundSqlDdlExpressionKey,
) -> bool {
    let requested_path = requested.source().accepted_path();
    let requested_canonical_text = format!("expr:v1:{}", requested.canonical_sql());

    existing_op == requested.op()
        && existing_path == requested_path
        && existing_canonical_text == requested_canonical_text
}

fn reject_duplicate_expression_index(
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    let Some(existing_index) = schema.expression_indexes().iter().find(|index| {
        existing_expression_index_matches_request(
            index,
            key_items,
            predicate_sql,
            if index.unique() {
                SqlCreateIndexUniqueness::Unique
            } else {
                SqlCreateIndexUniqueness::NonUnique
            },
        )
    }) else {
        return Ok(());
    };

    Err(SqlDdlBindError::DuplicateFieldPathIndex {
        field_path: ddl_key_item_report(key_items).join(","),
        existing_index: existing_index.name().to_string(),
    })
}

fn reject_duplicate_field_path_index(
    field_paths: &[BoundSqlDdlFieldPath],
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    let Some(existing_index) = schema.field_path_indexes().iter().find(|index| {
        let fields = index.fields();
        index.predicate_sql() == predicate_sql
            && fields.len() == field_paths.len()
            && fields
                .iter()
                .zip(field_paths)
                .all(|(field, requested)| field.path() == requested.accepted_path())
    }) else {
        return Ok(());
    };

    Err(SqlDdlBindError::DuplicateFieldPathIndex {
        field_path: ddl_field_path_report(field_paths).join(","),
        existing_index: existing_index.name().to_string(),
    })
}

fn candidate_index_snapshot(
    index_name: &str,
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
    schema: &SchemaInfo,
    index_store_path: &'static str,
) -> Result<PersistedIndexSnapshot, SqlDdlBindError> {
    let key = if key_items_are_field_path_only(key_items) {
        PersistedIndexKeySnapshot::FieldPath(
            key_items
                .iter()
                .map(|key_item| {
                    let BoundSqlDdlCreateIndexKey::FieldPath(field_path) = key_item else {
                        unreachable!("field-path-only index checked before field-path lowering");
                    };

                    accepted_index_field_path_snapshot(schema, field_path)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        PersistedIndexKeySnapshot::Items(
            key_items
                .iter()
                .map(|key_item| match key_item {
                    BoundSqlDdlCreateIndexKey::FieldPath(field_path) => {
                        accepted_index_field_path_snapshot(schema, field_path)
                            .map(PersistedIndexKeyItemSnapshot::FieldPath)
                    }
                    BoundSqlDdlCreateIndexKey::Expression(expression) => {
                        accepted_index_expression_snapshot(schema, expression)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    Ok(PersistedIndexSnapshot::new_sql_ddl(
        schema.next_secondary_index_ordinal(),
        index_name.to_string(),
        index_store_path.to_string(),
        matches!(uniqueness, SqlCreateIndexUniqueness::Unique),
        key,
        predicate_sql.map(str::to_string),
    ))
}

fn accepted_index_field_path_snapshot(
    schema: &SchemaInfo,
    field_path: &BoundSqlDdlFieldPath,
) -> Result<crate::db::schema::PersistedIndexFieldPathSnapshot, SqlDdlBindError> {
    schema
        .accepted_index_field_path_snapshot(field_path.root(), field_path.segments())
        .ok_or_else(|| SqlDdlBindError::FieldPathNotAcceptedCatalogBacked {
            field_path: field_path.accepted_path().join("."),
        })
}

fn accepted_index_expression_snapshot(
    schema: &SchemaInfo,
    expression: &BoundSqlDdlExpressionKey,
) -> Result<PersistedIndexKeyItemSnapshot, SqlDdlBindError> {
    let source = accepted_index_field_path_snapshot(schema, expression.source())?;
    let Some(output_kind) = expression_output_kind(expression.op(), source.kind()) else {
        return Err(SqlDdlBindError::FieldPathNotIndexable {
            field_path: expression.source().accepted_path().join("."),
        });
    };

    Ok(PersistedIndexKeyItemSnapshot::Expression(Box::new(
        PersistedIndexExpressionSnapshot::new(
            expression.op(),
            source.clone(),
            source.kind().clone(),
            output_kind,
            format!("expr:v1:{}", expression.canonical_sql()),
        ),
    )))
}

fn expression_output_kind(
    op: PersistedIndexExpressionOp,
    source_kind: &PersistedFieldKind,
) -> Option<PersistedFieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            if matches!(source_kind, PersistedFieldKind::Text { .. }) {
                Some(source_kind.clone())
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Date => {
            if matches!(
                source_kind,
                PersistedFieldKind::Date | PersistedFieldKind::Timestamp
            ) {
                Some(PersistedFieldKind::Date)
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            if matches!(
                source_kind,
                PersistedFieldKind::Date | PersistedFieldKind::Timestamp
            ) {
                Some(PersistedFieldKind::Int)
            } else {
                None
            }
        }
    }
}

fn validated_create_index_predicate_sql(
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<Option<String>, SqlDdlBindError> {
    let Some(predicate_sql) = predicate_sql else {
        return Ok(None);
    };
    let predicate = parse_sql_predicate(predicate_sql).map_err(|error| {
        SqlDdlBindError::InvalidFilteredIndexPredicate {
            detail: error.to_string(),
        }
    })?;
    validate_predicate(schema, &predicate).map_err(|error| {
        SqlDdlBindError::InvalidFilteredIndexPredicate {
            detail: error.to_string(),
        }
    })?;

    Ok(Some(predicate_sql.to_string()))
}

fn ddl_field_path_report(field_paths: &[BoundSqlDdlFieldPath]) -> Vec<String> {
    match field_paths {
        [field_path] => field_path.accepted_path().to_vec(),
        _ => vec![
            field_paths
                .iter()
                .map(|field_path| field_path.accepted_path().join("."))
                .collect::<Vec<_>>()
                .join(","),
        ],
    }
}

fn ddl_key_item_report(key_items: &[BoundSqlDdlCreateIndexKey]) -> Vec<String> {
    match key_items {
        [key_item] => vec![ddl_key_item_text(key_item)],
        _ => vec![
            key_items
                .iter()
                .map(ddl_key_item_text)
                .collect::<Vec<_>>()
                .join(","),
        ],
    }
}

fn ddl_key_item_text(key_item: &BoundSqlDdlCreateIndexKey) -> String {
    match key_item {
        BoundSqlDdlCreateIndexKey::FieldPath(field_path) => field_path.accepted_path().join("."),
        BoundSqlDdlCreateIndexKey::Expression(expression) => expression.canonical_sql().to_string(),
    }
}

/// Lower one bound SQL DDL request through schema mutation admission.
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
    match request.statement() {
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
    .map_err(SqlDdlLoweringError::MutationAdmission)
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
