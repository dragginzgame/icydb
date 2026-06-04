use super::{BoundSqlDdlRequest, BoundSqlDdlStatement};
use crate::db::sql::ddl::index::ddl_key_item_report;

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

pub(in crate::db) fn ddl_preparation_report(bound: &BoundSqlDdlRequest) -> SqlDdlPreparationReport {
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
