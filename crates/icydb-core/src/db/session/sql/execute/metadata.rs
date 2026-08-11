//! Module: db::session::sql::execute::metadata
//! Responsibility: shape metadata SQL commands into public SQL statement results.
//! Does not own: SQL parsing, metadata collection, or non-metadata command dispatch.
//! Boundary: keeps DESCRIBE/SHOW command routing and response envelopes out
//! of the execution hub.

use crate::db::sql::parser::SqlDescribeMode;
use crate::db::{
    SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput,
    schema::{
        AcceptedEntityDescriptionMetadata, describe_accepted_entity_with_persisted_schema,
        describe_compact_columns_with_persisted_schema,
        describe_entity_fields_with_persisted_schema,
        describe_entity_relations_with_persisted_schema,
    },
    session::{
        AcceptedSchemaCatalogContext,
        sql::{CompiledSqlCommand, SqlCacheAttribution},
    },
};
use crate::{
    db::{DbSession, EntityCatalogDescription, QueryError, session::sql::SqlStatementResult},
    traits::CanisterKind,
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
    fn describe_entity_sql_statement_result_with_catalog(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        mode: SqlDescribeMode,
    ) -> Result<SqlStatementResult, QueryError> {
        if mode == SqlDescribeMode::Compact {
            return describe_compact_columns_with_persisted_schema(
                catalog.snapshot(),
                catalog.value_catalog_handle(),
            )
            .map(|columns| {
                SqlStatementResult::Describe(SqlDescribeOutput::Compact {
                    entity: catalog.snapshot().entity_name().to_string(),
                    columns,
                })
            })
            .map_err(QueryError::execute);
        }
        let validation_jobs = self
            .constraint_validation_jobs_for_accepted_catalog(catalog)
            .map_err(QueryError::execute)?;
        let identity = self
            .identity_description_for_accepted_catalog(catalog)
            .map_err(QueryError::execute)?;
        describe_accepted_entity_with_persisted_schema(
            catalog.snapshot(),
            catalog.value_catalog_handle(),
            validation_jobs.as_slice(),
            AcceptedEntityDescriptionMetadata::new(
                identity,
                catalog.identity().entity_tag().value(),
                catalog.fingerprint_method_version(),
                catalog.fingerprint(),
            ),
            |target_path| catalog.relation_target_description(target_path),
        )
        .map(|description| SqlStatementResult::Describe(SqlDescribeOutput::Verbose { description }))
        .map_err(QueryError::execute)
    }

    fn show_constraints_sql_statement_result_with_catalog(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let validation_jobs = self
            .constraint_validation_jobs_for_accepted_catalog(catalog)
            .map_err(QueryError::execute)?;
        describe_accepted_entity_with_persisted_schema(
            catalog.snapshot(),
            catalog.value_catalog_handle(),
            validation_jobs.as_slice(),
            AcceptedEntityDescriptionMetadata::new(
                None,
                catalog.identity().entity_tag().value(),
                catalog.fingerprint_method_version(),
                catalog.fingerprint(),
            ),
            |target_path| catalog.relation_target_description(target_path),
        )
        .map(|description| SqlStatementResult::ShowConstraints(description.constraints().to_vec()))
        .map_err(QueryError::execute)
    }

    fn show_columns_sql_statement_result_with_catalog(
        catalog: &AcceptedSchemaCatalogContext,
        mode: SqlDescribeMode,
    ) -> Result<SqlStatementResult, QueryError> {
        if mode == SqlDescribeMode::Compact {
            return describe_compact_columns_with_persisted_schema(
                catalog.snapshot(),
                catalog.value_catalog_handle(),
            )
            .map(|columns| {
                SqlStatementResult::ShowColumns(SqlShowColumnsOutput::Compact {
                    entity: catalog.snapshot().entity_name().to_string(),
                    columns,
                })
            })
            .map_err(QueryError::execute);
        }
        describe_entity_fields_with_persisted_schema(
            catalog.snapshot(),
            catalog.value_catalog_handle(),
        )
        .map(|columns| {
            SqlStatementResult::ShowColumns(SqlShowColumnsOutput::Verbose {
                entity: catalog.snapshot().entity_name().to_string(),
                columns,
            })
        })
        .map_err(QueryError::execute)
    }

    fn show_relations_sql_statement_result_with_catalog(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let relations =
            describe_entity_relations_with_persisted_schema(catalog.snapshot(), &|target_path| {
                catalog.relation_target_description(target_path)
            })
            .map_err(QueryError::execute)?;
        SqlShowRelationsOutput::new(catalog.snapshot().entity_name().to_string(), relations)
            .map(SqlStatementResult::ShowRelations)
            .map_err(QueryError::execute)
    }

    fn show_indexes_sql_statement_result_with_catalog(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> SqlStatementResult {
        SqlStatementResult::ShowIndexes(self.show_indexes_for_store_schema_info(
            catalog.identity().store_path(),
            catalog.accepted_schema_info(),
            catalog.snapshot().persisted_snapshot(),
        ))
    }

    pub(super) fn show_entities_sql_statement_result(
        &self,
        entity: Option<&str>,
        verbose: bool,
    ) -> Result<SqlStatementResult, QueryError> {
        self.show_entities()
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

    pub(super) fn execute_accepted_metadata_compiled_sql_with_catalog_cache(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>> {
        self.execute_metadata_compiled_sql_with_cache(compiled, Some(catalog))
    }

    fn execute_metadata_compiled_sql_with_cache(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>> {
        let result = match compiled {
            CompiledSqlCommand::DescribeEntity { mode } => {
                self.describe_entity_sql_statement_result_with_catalog(catalog?, *mode)
            }
            CompiledSqlCommand::ShowConstraintsEntity => {
                self.show_constraints_sql_statement_result_with_catalog(catalog?)
            }
            CompiledSqlCommand::ShowIndexesEntity => {
                Ok(self.show_indexes_sql_statement_result_with_catalog(catalog?))
            }
            CompiledSqlCommand::ShowColumnsEntity { mode } => {
                Self::show_columns_sql_statement_result_with_catalog(catalog?, *mode)
            }
            CompiledSqlCommand::ShowRelationsEntity => {
                Self::show_relations_sql_statement_result_with_catalog(catalog?)
            }
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
            #[cfg(feature = "sql")]
            CompiledSqlCommand::Explain(_) => return None,
        };

        Some(SqlCacheAttribution::with_default(result))
    }
}
