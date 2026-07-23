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
    constraint_validation: Option<SqlConstraintValidationPage>,
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

    /// Borrow the bounded constraint-validation page, when this DDL advanced one.
    #[must_use]
    pub const fn constraint_validation(&self) -> Option<&SqlConstraintValidationPage> {
        self.constraint_validation.as_ref()
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

    pub(in crate::db) fn with_constraint_validation(
        mut self,
        constraint_validation: SqlConstraintValidationPage,
    ) -> Self {
        self.constraint_validation = Some(constraint_validation);
        self
    }
}

/// One bounded constraint-validation finding returned to the DDL caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlConstraintValidationFinding {
    primary_key: Vec<u8>,
    field_ids: Vec<u32>,
    error_code: u16,
}

impl SqlConstraintValidationFinding {
    /// Build one typed finding from canonical persisted identities.
    #[must_use]
    pub(in crate::db) const fn new(
        primary_key: Vec<u8>,
        field_ids: Vec<u32>,
        error_code: u16,
    ) -> Self {
        Self {
            primary_key,
            field_ids,
            error_code,
        }
    }

    /// Borrow the canonical persisted primary-key bytes.
    #[must_use]
    pub const fn primary_key(&self) -> &[u8] {
        self.primary_key.as_slice()
    }

    /// Borrow the sorted implicated accepted field identities.
    #[must_use]
    pub const fn field_ids(&self) -> &[u32] {
        self.field_ids.as_slice()
    }

    /// Return the stable diagnostic code for this finding.
    #[must_use]
    pub const fn error_code(&self) -> u16 {
        self.error_code
    }
}

/// Current engine-owned state of one bounded constraint-validation job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstraintValidationState {
    Forward,
    Verify,
    Restarted,
    Validated,
}

impl SqlConstraintValidationState {
    /// Return the stable outward state label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Verify => "verify",
            Self::Restarted => "restarted",
            Self::Validated => "validated",
        }
    }
}

/// Revision proof status associated with one validation response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstraintValidationRevisionStatus {
    Tracking,
    Captured,
    Invalidated,
    Complete,
}

impl SqlConstraintValidationRevisionStatus {
    /// Return the stable outward revision-status label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tracking => "tracking",
            Self::Captured => "captured",
            Self::Invalidated => "invalidated",
            Self::Complete => "complete",
        }
    }
}

/// Typed response for one bounded `VALIDATE CONSTRAINT` step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlConstraintValidationPage {
    constraint_id: u32,
    activation_epoch: Option<u64>,
    page_sequence: Option<u64>,
    state: SqlConstraintValidationState,
    revision_status: SqlConstraintValidationRevisionStatus,
    rows_scanned: u64,
    findings: Vec<SqlConstraintValidationFinding>,
    complete: bool,
}

impl SqlConstraintValidationPage {
    /// Build the terminal response for an already or newly validated constraint.
    #[must_use]
    pub(in crate::db) const fn validated(constraint_id: u32, rows_scanned: u64) -> Self {
        Self {
            constraint_id,
            activation_epoch: None,
            page_sequence: None,
            state: SqlConstraintValidationState::Validated,
            revision_status: SqlConstraintValidationRevisionStatus::Complete,
            rows_scanned,
            findings: Vec::new(),
            complete: true,
        }
    }

    /// Build one non-terminal response from the durable job snapshot.
    #[must_use]
    pub(in crate::db) const fn pending(
        constraint_id: u32,
        activation_epoch: u64,
        state: SqlConstraintValidationState,
        revision_status: SqlConstraintValidationRevisionStatus,
        rows_scanned: u64,
    ) -> Self {
        Self {
            constraint_id,
            activation_epoch: Some(activation_epoch),
            page_sequence: None,
            state,
            revision_status,
            rows_scanned,
            findings: Vec::new(),
            complete: false,
        }
    }

    pub(in crate::db) fn with_findings(
        mut self,
        page_sequence: u64,
        findings: Vec<SqlConstraintValidationFinding>,
    ) -> Self {
        self.page_sequence = Some(page_sequence);
        self.findings = findings;
        self
    }

    /// Return the accepted constraint identity.
    #[must_use]
    pub const fn constraint_id(&self) -> u32 {
        self.constraint_id
    }

    /// Return the durable activation epoch, when validation remains active.
    #[must_use]
    pub const fn activation_epoch(&self) -> Option<u64> {
        self.activation_epoch
    }

    /// Return the acknowledgement identity of a retained finding page.
    #[must_use]
    pub const fn page_sequence(&self) -> Option<u64> {
        self.page_sequence
    }

    /// Return the current bounded validation state.
    #[must_use]
    pub const fn state(&self) -> SqlConstraintValidationState {
        self.state
    }

    /// Return the current revision-proof status.
    #[must_use]
    pub const fn revision_status(&self) -> SqlConstraintValidationRevisionStatus {
        self.revision_status
    }

    /// Return the cumulative classified-row count for this job.
    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    /// Borrow the retained bounded finding page.
    #[must_use]
    pub const fn findings(&self) -> &[SqlConstraintValidationFinding] {
        self.findings.as_slice()
    }

    /// Return whether validation and promotion are complete.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

///
/// SqlDdlMutationKind
///
/// Developer-facing SQL DDL mutation kind.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDdlMutationKind {
    AddCheckConstraint,
    AddDefaultedField,
    AddField,
    SetFieldDefault,
    DropFieldDefault,
    SetFieldNotNull,
    DropFieldNotNull,
    DropField,
    RenameField,
    AddFieldPathIndex,
    AddExpressionIndex,
    DropSecondaryIndex,
    DropCheckConstraint,
    ValidateConstraint,
}

impl SqlDdlMutationKind {
    /// Return the stable diagnostic label for this DDL mutation kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AddCheckConstraint => "add_check_constraint",
            Self::AddDefaultedField => "add_defaulted_field",
            Self::AddField => "add_field",
            Self::SetFieldDefault => "set_field_default",
            Self::DropFieldDefault => "drop_field_default",
            Self::SetFieldNotNull => "set_field_not_null",
            Self::DropFieldNotNull => "drop_field_not_null",
            Self::DropField => "drop_field",
            Self::RenameField => "rename_field",
            Self::AddFieldPathIndex => "add_field_path_index",
            Self::AddExpressionIndex => "add_expression_index",
            Self::DropSecondaryIndex => "drop_secondary_index",
            Self::DropCheckConstraint => "drop_check_constraint",
            Self::ValidateConstraint => "validate_constraint",
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
    ActivationPublished,
    ValidationStarted,
    ValidationAdvanced,
    ValidationFindings,
    ValidationRestarted,
    Validated,
    NoOp,
}

impl SqlDdlExecutionStatus {
    /// Return the stable diagnostic label for this execution status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedOnly => "prepared_only",
            Self::Published => "published",
            Self::ActivationPublished => "activation_published",
            Self::ValidationStarted => "validation_started",
            Self::ValidationAdvanced => "validation_advanced",
            Self::ValidationFindings => "validation_findings",
            Self::ValidationRestarted => "validation_restarted",
            Self::Validated => "validated",
            Self::NoOp => "no_op",
        }
    }
}

pub(in crate::db) fn ddl_preparation_report(bound: &BoundSqlDdlRequest) -> SqlDdlPreparationReport {
    match bound.statement() {
        BoundSqlDdlStatement::AddCheckConstraint(add) => prepared_report(
            SqlDdlMutationKind::AddCheckConstraint,
            add.constraint_name(),
            add.entity_name(),
            add.expression()
                .dependencies()
                .into_iter()
                .map(|field_id| field_id.get().to_string())
                .collect(),
        ),
        BoundSqlDdlStatement::AddColumn(add) => prepared_report(
            if add.field().insert_default().is_none() {
                SqlDdlMutationKind::AddField
            } else {
                SqlDdlMutationKind::AddDefaultedField
            },
            add.field().name(),
            add.entity_name(),
            vec![add.field().name().to_string()],
        ),
        BoundSqlDdlStatement::AlterColumnDefault(alter) => prepared_report(
            alter.mutation_kind(),
            alter.field_name(),
            alter.entity_name(),
            vec![alter.field_name().to_string()],
        ),
        BoundSqlDdlStatement::AlterColumnNullability(alter) => prepared_report(
            alter.mutation_kind(),
            alter.field_name(),
            alter.entity_name(),
            vec![alter.field_name().to_string()],
        ),
        BoundSqlDdlStatement::DropColumn(drop) => prepared_report(
            SqlDdlMutationKind::DropField,
            drop.field_name(),
            drop.entity_name(),
            vec![drop.field_name().to_string()],
        ),
        BoundSqlDdlStatement::RenameColumn(rename) => prepared_report(
            SqlDdlMutationKind::RenameField,
            rename.new_name(),
            rename.entity_name(),
            vec![rename.old_name().to_string(), rename.new_name().to_string()],
        ),
        BoundSqlDdlStatement::CreateIndex(create) => {
            let target = create.candidate_index();

            prepared_report(
                if target.key().is_field_path_only() {
                    SqlDdlMutationKind::AddFieldPathIndex
                } else {
                    SqlDdlMutationKind::AddExpressionIndex
                },
                target.name(),
                target.store(),
                ddl_key_item_report(create.key_items()),
            )
        }
        BoundSqlDdlStatement::DropIndex(drop) => prepared_report(
            SqlDdlMutationKind::DropSecondaryIndex,
            drop.index_name(),
            drop.dropped_index().store(),
            drop.field_path().to_vec(),
        ),
        BoundSqlDdlStatement::DropConstraint(drop) => prepared_report(
            SqlDdlMutationKind::DropCheckConstraint,
            drop.constraint_name(),
            drop.entity_name(),
            Vec::new(),
        ),
        BoundSqlDdlStatement::ValidateConstraint(validate) => prepared_report(
            SqlDdlMutationKind::ValidateConstraint,
            validate.constraint_name(),
            validate.entity_name(),
            Vec::new(),
        ),
        BoundSqlDdlStatement::NoOp(no_op) => prepared_report(
            no_op.mutation_kind(),
            no_op.index_name(),
            no_op.target_store(),
            no_op.field_path().to_vec(),
        ),
    }
}

fn prepared_report(
    mutation_kind: SqlDdlMutationKind,
    target_index: &str,
    target_store: &str,
    field_path: Vec<String>,
) -> SqlDdlPreparationReport {
    SqlDdlPreparationReport {
        mutation_kind,
        target_index: target_index.to_string(),
        target_store: target_store.to_string(),
        field_path,
        execution_status: SqlDdlExecutionStatus::PreparedOnly,
        rows_scanned: 0,
        index_keys_written: 0,
        constraint_validation: None,
    }
}
