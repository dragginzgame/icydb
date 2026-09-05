//! Module: db::session::sql::ddl
//! Responsibility: prepare and publish SQL DDL through accepted schema
//! authority.
//! Does not own: SQL parsing surface classification or general SQL execution.
//! Boundary: keeps DDL publication and physical schema mutation work out of
//! the SQL facade.

use crate::{
    db::{
        DbSession, QueryError,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, ConstraintValidationProgress,
            SchemaDdlAcceptedSnapshotDerivation, SqlDdlFieldNullabilityOutcome,
            advance_check_constraint_activation, advance_not_null_constraint_activation,
            advance_unique_constraint_activation, constraint_validation_finding_output,
            ensure_schema_migration_ready_for_schema_changes, execute_admin_sql_ddl_check_addition,
            execute_admin_sql_ddl_check_drop, execute_admin_sql_ddl_expression_index_addition,
            execute_admin_sql_ddl_field_addition, execute_admin_sql_ddl_field_default_change,
            execute_admin_sql_ddl_field_drop, execute_admin_sql_ddl_field_nullability_change,
            execute_admin_sql_ddl_field_path_index_addition, execute_admin_sql_ddl_field_rename,
            execute_admin_sql_ddl_not_null_activation_abort,
            execute_admin_sql_ddl_secondary_index_drop,
            execute_admin_sql_ddl_unique_index_activation,
            execute_admin_sql_ddl_unique_index_activation_abort,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
                SqlConstraintValidationState, SqlDdlExecutionStatus, SqlStatementDispatch,
                SqlStatementResult, sql_statement_dispatch,
            },
        },
        sql::ddl::{
            BoundSqlCreateIndexRequest, BoundSqlDdlStatement, BoundSqlValidationConstraintKind,
            PreparedSqlDdlCommand, prepare_sql_ddl_statement,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

fn constraint_validation_report(
    constraint_id: u32,
    entity_tag: crate::types::EntityTag,
    accepted_schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    accepted: &AcceptedSchemaSnapshot,
    activation_epoch: Option<u64>,
    progress: ConstraintValidationProgress,
) -> Result<(SqlDdlExecutionStatus, SqlConstraintValidationPage), QueryError> {
    match progress {
        ConstraintValidationProgress::Started => Ok((
            SqlDdlExecutionStatus::ValidationStarted,
            pending_constraint_validation_page(
                constraint_id,
                activation_epoch,
                SqlConstraintValidationState::Forward,
                SqlConstraintValidationRevisionStatus::Tracking,
                0,
            )?,
        )),
        ConstraintValidationProgress::Advanced {
            phase,
            rows_scanned,
        } => {
            let (state, revision_status) = validation_phase_status(phase);
            Ok((
                SqlDdlExecutionStatus::ValidationAdvanced,
                pending_constraint_validation_page(
                    constraint_id,
                    activation_epoch,
                    state,
                    revision_status,
                    rows_scanned,
                )?,
            ))
        }
        ConstraintValidationProgress::Findings {
            receipt,
            phase,
            rows_scanned,
        } => {
            let (state, revision_status) = validation_phase_status(phase);
            let constraint_id = crate::db::schema::ConstraintId::new(constraint_id)
                .ok_or_else(InternalError::store_corruption)
                .map_err(QueryError::execute)?;
            let activation = accepted
                .persisted_snapshot()
                .constraint_catalog()
                .activation(constraint_id)
                .ok_or_else(InternalError::store_corruption)
                .map_err(QueryError::execute)?;
            let findings = receipt
                .findings()
                .iter()
                .map(|finding| {
                    constraint_validation_finding_output(
                        accepted_schema_fingerprint,
                        entity_tag,
                        activation,
                        finding,
                    )
                })
                .collect::<Result<Vec<_>, InternalError>>()
                .map_err(QueryError::execute)?;
            Ok((
                SqlDdlExecutionStatus::ValidationFindings,
                pending_constraint_validation_page(
                    constraint_id.get(),
                    activation_epoch,
                    state,
                    revision_status,
                    rows_scanned,
                )?
                .with_findings(receipt.page_sequence(), findings),
            ))
        }
        ConstraintValidationProgress::Restarted { rows_scanned } => Ok((
            SqlDdlExecutionStatus::ValidationRestarted,
            pending_constraint_validation_page(
                constraint_id,
                activation_epoch,
                SqlConstraintValidationState::Restarted,
                SqlConstraintValidationRevisionStatus::Invalidated,
                rows_scanned,
            )?,
        )),
        ConstraintValidationProgress::Promoted { rows_scanned } => Ok((
            SqlDdlExecutionStatus::Validated,
            SqlConstraintValidationPage::validated(constraint_id, rows_scanned),
        )),
    }
}

fn pending_constraint_validation_page(
    constraint_id: u32,
    activation_epoch: Option<u64>,
    state: SqlConstraintValidationState,
    revision_status: SqlConstraintValidationRevisionStatus,
    rows_scanned: u64,
) -> Result<SqlConstraintValidationPage, QueryError> {
    let activation_epoch = activation_epoch
        .ok_or_else(|| QueryError::execute(crate::error::InternalError::store_invariant()))?;
    Ok(SqlConstraintValidationPage::pending(
        constraint_id,
        activation_epoch,
        state,
        revision_status,
        rows_scanned,
    ))
}

const fn validation_phase_status(
    phase: crate::db::schema::ConstraintValidationPhase,
) -> (
    SqlConstraintValidationState,
    SqlConstraintValidationRevisionStatus,
) {
    match phase {
        crate::db::schema::ConstraintValidationPhase::Forward => (
            SqlConstraintValidationState::Forward,
            SqlConstraintValidationRevisionStatus::Tracking,
        ),
        crate::db::schema::ConstraintValidationPhase::Verify => (
            SqlConstraintValidationState::Verify,
            SqlConstraintValidationRevisionStatus::Captured,
        ),
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn prepare_sql_ddl_command(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<(AcceptedSchemaCatalogContext, PreparedSqlDdlCommand), QueryError> {
        let statement = dispatch.statement();
        let entity_name = dispatch
            .entity_name()
            .ok_or_else(QueryError::unsupported_query)?;
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(entity_name))
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info();
        let prepared = match prepare_sql_ddl_statement(
            statement,
            catalog.snapshot(),
            schema_info,
            catalog.identity().store_path(),
        ) {
            Ok(prepared) => prepared,
            Err(err) => return Err(QueryError::from_sql_ddl_prepare_error(err)),
        };

        Ok((catalog, prepared))
    }

    /// Execute one administrative SQL DDL statement.
    ///
    /// Supported DDL routes through schema-owned physical work and
    /// accepted-snapshot publication. The caller must own administrative
    /// authorization before accepting caller-controlled SQL.
    pub fn execute_admin_sql_ddl(&self, sql: &str) -> Result<SqlStatementResult, QueryError> {
        let dispatch = sql_statement_dispatch(sql)?;

        self.execute_admin_sql_ddl_dispatch(&dispatch)
    }

    /// Execute one administrative DDL command from a parsed dispatch.
    #[doc(hidden)]
    pub fn execute_admin_sql_ddl_dispatch(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<SqlStatementResult, QueryError> {
        let (accepted_before, prepared) = self.prepare_sql_ddl_command(dispatch)?;
        if !prepared.mutates_schema() {
            return Ok(SqlStatementResult::Ddl(
                prepared
                    .report()
                    .clone()
                    .with_execution_status(SqlDdlExecutionStatus::NoOp),
            ));
        }

        let store = self
            .db
            .recovered_store(accepted_before.identity().store_path())
            .map_err(QueryError::execute)?;
        ensure_schema_migration_ready_for_schema_changes().map_err(QueryError::execute)?;

        if let Some(result) =
            self.execute_prepared_constraint_ddl(store, &accepted_before, &prepared)
        {
            return result;
        }

        let Some(derivation) = prepared.derivation() else {
            return Err(QueryError::unsupported_query());
        };

        let (rows_scanned, index_keys_written) = Self::execute_prepared_sql_ddl_mutation(
            store,
            accepted_before.snapshot(),
            accepted_before.identity(),
            derivation,
            &prepared,
        )?;
        self.invalidate_accepted_schema_runtime_root();

        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published)
                .with_execution_metrics(rows_scanned, index_keys_written),
        ))
    }

    fn execute_prepared_constraint_ddl(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
    ) -> Option<Result<SqlStatementResult, QueryError>> {
        match prepared.bound().statement() {
            BoundSqlDdlStatement::AddCheckConstraint(add) => {
                Some(self.execute_prepared_add_check(store, accepted_before, prepared, add))
            }
            BoundSqlDdlStatement::DropConstraint(drop) => {
                Some(self.execute_prepared_drop_check(store, accepted_before, prepared, drop))
            }
            BoundSqlDdlStatement::ValidateConstraint(validate) => {
                Some(self.execute_prepared_validate_constraint(accepted_before, prepared, validate))
            }
            BoundSqlDdlStatement::AlterColumnNullability(alter) => Some(
                self.execute_prepared_field_nullability(store, accepted_before, prepared, alter),
            ),
            BoundSqlDdlStatement::CreateIndex(create) if create.candidate_index().unique() => {
                Some(self.execute_prepared_unique_index_activation(
                    store,
                    accepted_before,
                    prepared,
                    create,
                ))
            }
            BoundSqlDdlStatement::DropIndex(drop) if drop.pending_activation_id().is_some() => {
                Some(self.execute_prepared_unique_index_activation_abort(
                    store,
                    accepted_before,
                    prepared,
                    drop,
                ))
            }
            BoundSqlDdlStatement::AddColumn(_)
            | BoundSqlDdlStatement::AlterColumnDefault(_)
            | BoundSqlDdlStatement::DropColumn(_)
            | BoundSqlDdlStatement::RenameColumn(_)
            | BoundSqlDdlStatement::CreateIndex(_)
            | BoundSqlDdlStatement::DropIndex(_)
            | BoundSqlDdlStatement::NoOp(_) => None,
        }
    }

    fn execute_prepared_add_check(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        add: &crate::db::sql::ddl::BoundSqlAddCheckConstraintRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let next_schema_version = prepared
            .bound()
            .schema_version_contract()
            .next_schema_version()
            .ok_or_else(QueryError::unsupported_query)?;
        let (rows_scanned, _constraint_id) = execute_admin_sql_ddl_check_addition(
            store,
            accepted_before.identity().entity_tag(),
            accepted_before.identity().entity_path(),
            accepted_before.snapshot(),
            accepted_before.identity(),
            add,
            next_schema_version,
        )
        .map_err(QueryError::from_sql_ddl_execution_error)?;
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(if add.not_valid() {
                    SqlDdlExecutionStatus::ActivationPublished
                } else {
                    SqlDdlExecutionStatus::Validated
                })
                .with_execution_metrics(rows_scanned, 0),
        ))
    }

    fn execute_prepared_unique_index_activation(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        create: &BoundSqlCreateIndexRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let next_schema_version = prepared
            .bound()
            .schema_version_contract()
            .next_schema_version()
            .ok_or_else(QueryError::unsupported_query)?;
        execute_admin_sql_ddl_unique_index_activation(
            store,
            accepted_before.identity().entity_tag(),
            accepted_before.identity().entity_path(),
            accepted_before.snapshot(),
            accepted_before.identity(),
            create,
            next_schema_version,
        )
        .map_err(QueryError::from_sql_ddl_execution_error)?;
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::ActivationPublished),
        ))
    }

    fn execute_prepared_unique_index_activation_abort(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        drop: &crate::db::sql::ddl::BoundSqlDropIndexRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let next_schema_version = prepared
            .bound()
            .schema_version_contract()
            .next_schema_version()
            .ok_or_else(QueryError::unsupported_query)?;
        execute_admin_sql_ddl_unique_index_activation_abort(
            store,
            accepted_before.identity().entity_tag(),
            accepted_before.identity().entity_path(),
            accepted_before.snapshot(),
            accepted_before.identity(),
            drop,
            next_schema_version,
        )
        .map_err(QueryError::from_sql_ddl_execution_error)?;
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published),
        ))
    }

    fn execute_prepared_drop_check(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        drop: &crate::db::sql::ddl::BoundSqlDropConstraintRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let next_schema_version = prepared
            .bound()
            .schema_version_contract()
            .next_schema_version()
            .ok_or_else(QueryError::unsupported_query)?;
        execute_admin_sql_ddl_check_drop(
            store,
            accepted_before.identity().entity_tag(),
            accepted_before.identity().entity_path(),
            accepted_before.snapshot(),
            accepted_before.identity(),
            drop,
            next_schema_version,
        )
        .map_err(QueryError::from_sql_ddl_execution_error)?;
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published),
        ))
    }

    fn execute_prepared_validate_constraint(
        &self,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        validate: &crate::db::sql::ddl::BoundSqlValidateConstraintRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let (status, validation_page) = if validate.already_validated() {
            (
                SqlDdlExecutionStatus::Validated,
                SqlConstraintValidationPage::validated(validate.constraint_id().get(), 0),
            )
        } else {
            constraint_validation_report(
                validate.constraint_id().get(),
                accepted_before.identity().entity_tag(),
                accepted_before.fingerprint(),
                accepted_before.snapshot(),
                validate.activation_epoch(),
                match validate.kind() {
                    BoundSqlValidationConstraintKind::Check => advance_check_constraint_activation(
                        &self.db,
                        accepted_before.identity().entity_tag(),
                        validate.constraint_id(),
                        validate.after_page_sequence(),
                    ),
                    BoundSqlValidationConstraintKind::NotNull => {
                        advance_not_null_constraint_activation(
                            &self.db,
                            accepted_before.identity().entity_tag(),
                            validate.constraint_id(),
                            validate.after_page_sequence(),
                        )
                    }
                    BoundSqlValidationConstraintKind::Unique => {
                        advance_unique_constraint_activation(
                            &self.db,
                            accepted_before.identity().entity_tag(),
                            validate.constraint_id(),
                            validate.after_page_sequence(),
                        )
                    }
                }
                .map_err(QueryError::execute)?,
            )?
        };
        let rows_scanned = usize::try_from(validation_page.rows_scanned()).unwrap_or(usize::MAX);
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(status)
                .with_execution_metrics(rows_scanned, 0)
                .with_constraint_validation(validation_page),
        ))
    }

    fn execute_prepared_field_nullability(
        &self,
        store: StoreHandle,
        accepted_before: &AcceptedSchemaCatalogContext,
        prepared: &PreparedSqlDdlCommand,
        alter: &crate::db::sql::ddl::BoundSqlAlterColumnNullabilityRequest,
    ) -> Result<SqlStatementResult, QueryError> {
        let status = if let Some(constraint_id) = alter.pending_activation_id() {
            let next_schema_version = prepared
                .bound()
                .schema_version_contract()
                .next_schema_version()
                .ok_or_else(QueryError::unsupported_query)?;
            execute_admin_sql_ddl_not_null_activation_abort(
                store,
                accepted_before.identity().entity_tag(),
                accepted_before.identity().entity_path(),
                accepted_before.snapshot(),
                accepted_before.identity(),
                next_schema_version,
                alter.field().id(),
                constraint_id,
            )
            .map_err(QueryError::from_sql_ddl_execution_error)?;
            SqlDdlExecutionStatus::Published
        } else {
            let derivation = prepared
                .derivation()
                .ok_or_else(QueryError::unsupported_query)?;
            match execute_admin_sql_ddl_field_nullability_change(
                store,
                accepted_before.identity().entity_tag(),
                accepted_before.identity().entity_path(),
                accepted_before.snapshot(),
                accepted_before.identity(),
                derivation,
            )
            .map_err(QueryError::from_sql_ddl_execution_error)?
            {
                SqlDdlFieldNullabilityOutcome::Published => SqlDdlExecutionStatus::Published,
                SqlDdlFieldNullabilityOutcome::ActivationPublished { .. } => {
                    SqlDdlExecutionStatus::ActivationPublished
                }
            }
        };
        self.invalidate_accepted_schema_runtime_root();
        Ok(SqlStatementResult::Ddl(
            prepared.report().clone().with_execution_status(status),
        ))
    }

    fn execute_prepared_sql_ddl_mutation(
        store: StoreHandle,
        accepted_before: &AcceptedSchemaSnapshot,
        accepted_before_identity: AcceptedCatalogIdentity,
        derivation: &SchemaDdlAcceptedSnapshotDerivation,
        prepared: &PreparedSqlDdlCommand,
    ) -> Result<(usize, usize), QueryError> {
        let metrics = match prepared.bound().statement() {
            BoundSqlDdlStatement::AddColumn(_) => {
                execute_admin_sql_ddl_field_addition(
                    store,
                    accepted_before_identity.entity_tag(),
                    accepted_before_identity.entity_path(),
                    accepted_before,
                    accepted_before_identity.clone(),
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::AlterColumnDefault(_) => {
                execute_admin_sql_ddl_field_default_change(
                    store,
                    accepted_before_identity.entity_tag(),
                    accepted_before_identity.entity_path(),
                    accepted_before,
                    accepted_before_identity.clone(),
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::DropColumn(_) => {
                let rows_scanned = execute_admin_sql_ddl_field_drop(
                    store,
                    accepted_before_identity.entity_tag(),
                    accepted_before_identity.entity_path(),
                    accepted_before,
                    accepted_before_identity.clone(),
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (rows_scanned, 0)
            }
            BoundSqlDdlStatement::RenameColumn(_) => {
                execute_admin_sql_ddl_field_rename(
                    store,
                    accepted_before_identity.entity_tag(),
                    accepted_before_identity.entity_path(),
                    accepted_before,
                    accepted_before_identity.clone(),
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::CreateIndex(create) => Self::execute_prepared_create_index(
                store,
                accepted_before,
                accepted_before_identity,
                derivation,
                create,
            )?,
            BoundSqlDdlStatement::DropIndex(drop) => {
                if drop.pending_activation_id().is_some() {
                    return Err(QueryError::unsupported_query());
                }
                execute_admin_sql_ddl_secondary_index_drop(
                    store,
                    accepted_before_identity.entity_tag(),
                    accepted_before_identity.entity_path(),
                    accepted_before,
                    accepted_before_identity.clone(),
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::AddCheckConstraint(_)
            | BoundSqlDdlStatement::AlterColumnNullability(_)
            | BoundSqlDdlStatement::DropConstraint(_)
            | BoundSqlDdlStatement::ValidateConstraint(_) => {
                return Err(QueryError::unsupported_query());
            }
            BoundSqlDdlStatement::NoOp(_) => (0, 0),
        };

        Ok(metrics)
    }

    fn execute_prepared_create_index(
        store: StoreHandle,
        accepted_before: &AcceptedSchemaSnapshot,
        accepted_before_identity: AcceptedCatalogIdentity,
        derivation: &SchemaDdlAcceptedSnapshotDerivation,
        create: &BoundSqlCreateIndexRequest,
    ) -> Result<(usize, usize), QueryError> {
        if create.candidate_index().unique() {
            return Err(QueryError::unsupported_query());
        }
        let execute = if create.candidate_index().key().is_field_path_only() {
            execute_admin_sql_ddl_field_path_index_addition
        } else {
            execute_admin_sql_ddl_expression_index_addition
        };

        execute(
            store,
            accepted_before_identity.entity_tag(),
            accepted_before_identity.entity_path(),
            accepted_before,
            accepted_before_identity.clone(),
            derivation,
        )
        .map_err(QueryError::from_sql_ddl_execution_error)
    }
}
