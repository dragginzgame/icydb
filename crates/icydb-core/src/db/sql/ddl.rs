//! Module: db::sql::ddl
//! Responsibility: bind parsed SQL DDL to accepted schema catalog contracts.
//! Does not own: mutation planning, physical index rebuilds, or SQL execution.
//! Boundary: translates parser-owned DDL syntax into catalog-native requests.

#![allow(
    dead_code,
    reason = "0.155 stages accepted-catalog DDL binding before execution is enabled"
)]

use crate::db::{
    schema::{
        AcceptedSchemaSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmission,
        SchemaDdlMutationAdmissionError, SchemaInfo, admit_sql_ddl_field_path_index_candidate,
        derive_sql_ddl_field_path_index_accepted_after,
    },
    sql::{
        identifier::identifiers_tail_match,
        parser::{SqlCreateIndexStatement, SqlDdlStatement, SqlStatement},
    },
};
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
    derivation: SchemaDdlAcceptedSnapshotDerivation,
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
    pub(in crate::db) const fn derivation(&self) -> &SchemaDdlAcceptedSnapshotDerivation {
        &self.derivation
    }

    /// Borrow the developer-facing preparation report.
    #[must_use]
    pub(in crate::db) const fn report(&self) -> &SqlDdlPreparationReport {
        &self.report
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

    /// Return the execution status. 0.155.1 only prepares DDL.
    #[must_use]
    pub const fn execution_status(&self) -> SqlDdlExecutionStatus {
        self.execution_status
    }
}

///
/// SqlDdlMutationKind
///
/// Developer-facing SQL DDL mutation kind.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDdlMutationKind {
    AddNonUniqueFieldPathIndex,
}

impl SqlDdlMutationKind {
    /// Return the stable diagnostic label for this DDL mutation kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AddNonUniqueFieldPathIndex => "add_non_unique_field_path_index",
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
}

impl SqlDdlExecutionStatus {
    /// Return the stable diagnostic label for this execution status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedOnly => "prepared_only",
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
    CreateIndex(BoundSqlCreateIndexRequest),
}

///
/// BoundSqlCreateIndexRequest
///
/// Catalog-resolved request for the only 0.155 DDL shape: one non-unique
/// field-path secondary index.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlCreateIndexRequest {
    index_name: String,
    entity_name: String,
    field_path: BoundSqlDdlFieldPath,
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

    /// Borrow the accepted field-path target.
    #[must_use]
    pub(in crate::db) const fn field_path(&self) -> &BoundSqlDdlFieldPath {
        &self.field_path
    }

    /// Borrow the candidate accepted index snapshot for mutation admission.
    #[must_use]
    pub(in crate::db) const fn candidate_index(&self) -> &PersistedIndexSnapshot {
        &self.candidate_index
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

    #[error("index name '{index_name}' already exists in the accepted schema")]
    DuplicateIndexName { index_name: String },

    #[error("accepted schema already has index '{existing_index}' for field path '{field_path}'")]
    DuplicateFieldPathIndex {
        field_path: String,
        existing_index: String,
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
    #[error("SQL DDL lowering requires a CREATE INDEX statement")]
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
) -> Result<PreparedSqlDdlCommand, SqlDdlPrepareError> {
    let bound = bind_sql_ddl_statement(statement, schema)?;
    let derivation = derive_bound_sql_ddl_accepted_after(accepted_before, &bound)?;
    let report = ddl_preparation_report(&bound, &derivation);

    Ok(PreparedSqlDdlCommand {
        bound,
        derivation,
        report,
    })
}

/// Bind one parsed SQL DDL statement against accepted catalog metadata.
pub(in crate::db) fn bind_sql_ddl_statement(
    statement: &SqlStatement,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let SqlStatement::Ddl(ddl) = statement else {
        return Err(SqlDdlBindError::NotDdl);
    };

    match ddl {
        SqlDdlStatement::CreateIndex(statement) => bind_create_index_statement(statement, schema),
    }
}

fn bind_create_index_statement(
    statement: &SqlCreateIndexStatement,
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

    reject_duplicate_index_name(statement.name.as_str(), schema)?;
    let field_path =
        bind_create_index_field_path(statement.field_path.as_str(), entity_name, schema)?;
    reject_duplicate_field_path_index(&field_path, schema)?;
    let candidate_index = candidate_index_snapshot(statement.name.as_str(), &field_path, schema)?;

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::CreateIndex(BoundSqlCreateIndexRequest {
            index_name: statement.name.clone(),
            entity_name: entity_name.to_string(),
            field_path,
            candidate_index,
        }),
    })
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

fn reject_duplicate_index_name(
    index_name: &str,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    if schema
        .field_path_indexes()
        .iter()
        .any(|index| index.name() == index_name)
        || schema
            .expression_indexes()
            .iter()
            .any(|index| index.name() == index_name)
    {
        return Err(SqlDdlBindError::DuplicateIndexName {
            index_name: index_name.to_string(),
        });
    }

    Ok(())
}

fn reject_duplicate_field_path_index(
    field_path: &BoundSqlDdlFieldPath,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    let Some(existing_index) = schema.field_path_indexes().iter().find(|index| {
        let fields = index.fields();
        fields.len() == 1 && fields[0].path() == field_path.accepted_path()
    }) else {
        return Ok(());
    };

    Err(SqlDdlBindError::DuplicateFieldPathIndex {
        field_path: field_path.accepted_path().join("."),
        existing_index: existing_index.name().to_string(),
    })
}

fn candidate_index_snapshot(
    index_name: &str,
    field_path: &BoundSqlDdlFieldPath,
    schema: &SchemaInfo,
) -> Result<PersistedIndexSnapshot, SqlDdlBindError> {
    let key = schema
        .accepted_index_field_path_snapshot(field_path.root(), field_path.segments())
        .ok_or_else(|| SqlDdlBindError::FieldPathNotAcceptedCatalogBacked {
            field_path: field_path.accepted_path().join("."),
        })?;
    let store = schema
        .ddl_index_store_path(index_name)
        .ok_or(SqlDdlBindError::MissingEntityPath)?;

    Ok(PersistedIndexSnapshot::new(
        schema.next_secondary_index_ordinal(),
        index_name.to_string(),
        store,
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![key]),
        None,
    ))
}

/// Lower one bound SQL DDL request through schema mutation admission.
pub(in crate::db) fn lower_bound_sql_ddl_to_schema_mutation_admission(
    request: &BoundSqlDdlRequest,
) -> Result<SchemaDdlMutationAdmission, SqlDdlLoweringError> {
    let BoundSqlDdlStatement::CreateIndex(create) = request.statement();

    admit_sql_ddl_field_path_index_candidate(create.candidate_index())
        .map_err(SqlDdlLoweringError::MutationAdmission)
}

/// Derive the accepted-after schema snapshot for one bound SQL DDL request.
pub(in crate::db) fn derive_bound_sql_ddl_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    request: &BoundSqlDdlRequest,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SqlDdlLoweringError> {
    let BoundSqlDdlStatement::CreateIndex(create) = request.statement();

    derive_sql_ddl_field_path_index_accepted_after(
        accepted_before,
        create.candidate_index().clone(),
    )
    .map_err(SqlDdlLoweringError::MutationAdmission)
}

fn ddl_preparation_report(
    bound: &BoundSqlDdlRequest,
    derivation: &SchemaDdlAcceptedSnapshotDerivation,
) -> SqlDdlPreparationReport {
    let BoundSqlDdlStatement::CreateIndex(create) = bound.statement();
    let target = derivation.admission().target();

    SqlDdlPreparationReport {
        mutation_kind: SqlDdlMutationKind::AddNonUniqueFieldPathIndex,
        target_index: target.name().to_string(),
        target_store: target.store().to_string(),
        field_path: create.field_path().accepted_path().to_vec(),
        execution_status: SqlDdlExecutionStatus::PreparedOnly,
    }
}
