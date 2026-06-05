//! Module: db::session::sql::execute::metadata
//! Responsibility: shape metadata SQL commands into public SQL statement results.
//! Does not own: SQL parsing, metadata collection, or compiled-command dispatch.
//! Boundary: keeps DESCRIBE/SHOW response envelopes out of the execution hub.

use crate::{
    db::{DbSession, PersistedRow, QueryError, session::sql::SqlStatementResult},
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn describe_entity_sql_statement_result<E>(
        &self,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.try_describe_entity::<E>()
            .map(SqlStatementResult::Describe)
            .map_err(QueryError::execute)
    }

    pub(super) fn show_indexes_sql_statement_result<E>(
        &self,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.try_show_indexes::<E>()
            .map(SqlStatementResult::ShowIndexes)
            .map_err(QueryError::execute)
    }

    pub(super) fn show_columns_sql_statement_result<E>(
        &self,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.try_show_columns::<E>()
            .map(SqlStatementResult::ShowColumns)
            .map_err(QueryError::execute)
    }

    pub(super) fn show_entities_sql_statement_result(
        &self,
        verbose: bool,
    ) -> Result<SqlStatementResult, QueryError> {
        self.try_show_entities()
            .map(|entities| SqlStatementResult::ShowEntities { entities, verbose })
            .map_err(QueryError::execute)
    }

    pub(super) fn show_stores_sql_statement_result(&self, verbose: bool) -> SqlStatementResult {
        SqlStatementResult::ShowStores {
            stores: self.show_stores(),
            verbose,
        }
    }

    pub(super) fn show_memory_sql_statement_result(&self) -> SqlStatementResult {
        SqlStatementResult::ShowMemory(self.show_memory())
    }
}
