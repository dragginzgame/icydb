//! Module: db::session::sql::execute::metadata
//! Responsibility: shape metadata SQL commands into public SQL statement results.
//! Does not own: SQL parsing, metadata collection, or non-metadata command dispatch.
//! Boundary: keeps DESCRIBE/SHOW command routing and response envelopes out
//! of the execution hub.

use crate::db::{
    schema::{
        describe_entity_fields_with_persisted_schema, describe_entity_model_with_persisted_schema,
    },
    session::{
        AcceptedSchemaCatalogContext,
        sql::{CompiledSqlCommand, SqlCacheAttribution},
    },
};
use crate::{
    db::{
        DbSession, EntityCatalogDescription, PersistedRow, QueryError,
        session::sql::SqlStatementResult,
    },
    traits::{CanisterKind, EntityValue},
};

fn filter_show_entity_catalog(
    entities: Vec<EntityCatalogDescription>,
    entity: &str,
) -> Vec<EntityCatalogDescription> {
    let has_exact_match = entities.iter().any(|entry| entry.entity_name() == entity);

    entities
        .into_iter()
        .filter(|entry| {
            if has_exact_match {
                entry.entity_name() == entity
            } else {
                entry.entity_name().eq_ignore_ascii_case(entity)
            }
        })
        .collect()
}

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

    fn describe_entity_sql_statement_result_with_catalog<E>(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> SqlStatementResult
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        SqlStatementResult::Describe(describe_entity_model_with_persisted_schema(
            E::MODEL,
            catalog.snapshot(),
        ))
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

    fn show_columns_sql_statement_result_with_catalog(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> SqlStatementResult {
        SqlStatementResult::ShowColumns(describe_entity_fields_with_persisted_schema(
            catalog.snapshot(),
        ))
    }

    pub(super) fn show_entities_sql_statement_result(
        &self,
        entity: Option<&str>,
        verbose: bool,
    ) -> Result<SqlStatementResult, QueryError> {
        self.try_show_entities()
            .map(|entities| match entity {
                Some(entity) => filter_show_entity_catalog(entities, entity),
                None => entities,
            })
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

    pub(super) fn execute_metadata_compiled_sql_with_default_cache<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_metadata_compiled_sql_with_cache::<E>(compiled, None)
    }

    pub(super) fn execute_metadata_compiled_sql_with_catalog_cache<E>(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_metadata_compiled_sql_with_cache::<E>(compiled, Some(catalog))
    }

    fn execute_metadata_compiled_sql_with_cache<E>(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let result = match compiled {
            CompiledSqlCommand::DescribeEntity => match catalog {
                Some(catalog) => {
                    Ok(Self::describe_entity_sql_statement_result_with_catalog::<E>(catalog))
                }
                None => self.describe_entity_sql_statement_result::<E>(),
            },
            CompiledSqlCommand::ShowIndexesEntity => self.show_indexes_sql_statement_result::<E>(),
            CompiledSqlCommand::ShowColumnsEntity => match catalog {
                Some(catalog) => Ok(Self::show_columns_sql_statement_result_with_catalog(
                    catalog,
                )),
                None => self.show_columns_sql_statement_result::<E>(),
            },
            CompiledSqlCommand::ShowEntities { entity, verbose } => {
                self.show_entities_sql_statement_result(entity.as_deref(), *verbose)
            }
            CompiledSqlCommand::ShowStores { verbose } => {
                Ok(self.show_stores_sql_statement_result(*verbose))
            }
            CompiledSqlCommand::ShowMemory => Ok(self.show_memory_sql_statement_result()),
            CompiledSqlCommand::Select { .. }
            | CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::GlobalAggregate { .. }
            | CompiledSqlCommand::Insert(_)
            | CompiledSqlCommand::Update(_) => return None,
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(_) => return None,
        };

        Some(SqlCacheAttribution::with_default(result))
    }
}
