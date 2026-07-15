//! Module: db::session::sql::ddl
//! Responsibility: prepare and publish SQL DDL through accepted schema
//! authority.
//! Does not own: SQL parsing surface classification or general SQL execution.
//! Boundary: keeps DDL publication and physical schema mutation work out of
//! the SQL facade.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation,
            execute_admin_sql_ddl_expression_index_addition, execute_admin_sql_ddl_field_addition,
            execute_admin_sql_ddl_field_default_change, execute_admin_sql_ddl_field_drop,
            execute_admin_sql_ddl_field_nullability_change,
            execute_admin_sql_ddl_field_path_index_addition, execute_admin_sql_ddl_field_rename,
            execute_admin_sql_ddl_secondary_index_drop,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::{SqlDdlExecutionStatus, SqlDdlPreparationReport, SqlStatementResult},
        },
        sql::{
            ddl::{BoundSqlDdlStatement, PreparedSqlDdlCommand, prepare_sql_ddl_statement},
            parser::parse_sql_with_attribution,
        },
    },
    traits::{CanisterKind, Path},
};

impl<C: CanisterKind> DbSession<C> {
    /// Prepare one SQL DDL statement against the accepted schema catalog.
    ///
    /// This is a non-executing surface: it proves the statement can bind,
    /// derive an accepted-after snapshot, and pass schema mutation admission,
    /// then returns a prepared-only report without mutating schema or index
    /// storage.
    pub fn prepare_sql_ddl<E>(&self, sql: &str) -> Result<SqlDdlPreparationReport, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (_, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;

        Ok(prepared.report().clone())
    }

    fn prepare_sql_ddl_command<E>(
        &self,
        sql: &str,
    ) -> Result<(AcceptedSchemaCatalogContext, PreparedSqlDdlCommand), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (statement, _) =
            parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info_for::<E>();
        let prepared = match prepare_sql_ddl_statement(
            &statement,
            catalog.snapshot(),
            &schema_info,
            E::Store::PATH,
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
    pub fn execute_admin_sql_ddl<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (accepted_before, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;
        if !prepared.mutates_schema() {
            return Ok(SqlStatementResult::Ddl(
                prepared
                    .report()
                    .clone()
                    .with_execution_status(SqlDdlExecutionStatus::NoOp),
            ));
        }

        let Some(derivation) = prepared.derivation() else {
            return Err(QueryError::unsupported_query());
        };
        let store = self
            .db
            .recovered_store(E::Store::PATH)
            .map_err(QueryError::execute)?;

        let (rows_scanned, index_keys_written) = Self::execute_prepared_sql_ddl_mutation::<E>(
            store,
            accepted_before.snapshot(),
            accepted_before.identity(),
            derivation,
            &prepared,
        )?;
        self.invalidate_accepted_schema_query_cache_for_entity::<E>();

        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published)
                .with_execution_metrics(rows_scanned, index_keys_written),
        ))
    }

    fn execute_prepared_sql_ddl_mutation<E>(
        store: StoreHandle,
        accepted_before: &AcceptedSchemaSnapshot,
        accepted_before_identity: AcceptedCatalogIdentity,
        derivation: &SchemaDdlAcceptedSnapshotDerivation,
        prepared: &PreparedSqlDdlCommand,
    ) -> Result<(usize, usize), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let metrics = match prepared.bound().statement() {
            BoundSqlDdlStatement::AddColumn(_) => {
                execute_admin_sql_ddl_field_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::AlterColumnDefault(_) => {
                execute_admin_sql_ddl_field_default_change(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::AlterColumnNullability(_) => {
                let rows_scanned = execute_admin_sql_ddl_field_nullability_change(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (rows_scanned, 0)
            }
            BoundSqlDdlStatement::DropColumn(_) => {
                let rows_scanned = execute_admin_sql_ddl_field_drop(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (rows_scanned, 0)
            }
            BoundSqlDdlStatement::RenameColumn(_) => {
                execute_admin_sql_ddl_field_rename(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::CreateIndex(create)
                if create.candidate_index().key().is_field_path_only() =>
            {
                execute_admin_sql_ddl_field_path_index_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?
            }
            BoundSqlDdlStatement::CreateIndex(_) => {
                execute_admin_sql_ddl_expression_index_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?
            }
            BoundSqlDdlStatement::DropIndex(_) => {
                execute_admin_sql_ddl_secondary_index_drop(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            BoundSqlDdlStatement::NoOp(_) => (0, 0),
        };

        Ok(metrics)
    }
}
