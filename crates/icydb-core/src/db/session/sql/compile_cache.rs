//! Module: db::session::sql::compile_cache
//! Responsibility: compiled SQL command cache lookup, miss compilation, and
//! insertion orchestration.
//! Does not own: parsed-statement semantic compilation or SQL execution.
//! Boundary: keeps the public query/mutation compile surfaces on one cache shell.

use crate::{
    db::{
        DbSession, QueryError,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompileAttributionBuilder,
                SqlCompilePhaseAttribution, SqlCompiledCommandCacheContext,
                SqlCompiledCommandCacheKey, SqlCompiledCommandExecutionContext,
                SqlCompiledCommandSurface, measured, sql_compiled_command_cache_miss_reason,
            },
        },
        sql::parser::parse_sql_with_attribution,
    },
    metrics::sink::{
        CacheKind, CacheOutcome, SqlCompileRejectPhase, record_cache_entries,
        record_cache_event_for_path, record_cache_miss_reason_for_path,
        record_sql_compile_reject_for_path,
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db) fn compile_sql_query_with_execution_context(
        &self,
        entity_name: Option<&str>,
        sql: &str,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name)
            .map_err(QueryError::execute)?;
        self.compile_sql_surface_with_catalog(sql, SqlCompiledCommandSurface::Query, catalog)
    }

    pub(in crate::db) fn compile_sql_mutation_with_execution_context(
        &self,
        sql: &str,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let entity_name = crate::db::session::sql::sql_statement_entity_name(sql)?;
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name.as_deref())
            .map_err(QueryError::execute)?;
        self.compile_sql_surface_with_catalog(sql, SqlCompiledCommandSurface::Mutation, catalog)
    }

    fn compile_sql_surface_with_catalog(
        &self,
        sql: &str,
        surface: SqlCompiledCommandSurface,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let entity_path = catalog.identity().entity_path_handle();
        let (cache_key_local_instructions, context) = measured(|| {
            Ok(SqlCompiledCommandCacheContext::from_catalog(
                surface, sql, catalog,
            ))
        })?;
        let mut attribution = SqlCompileAttributionBuilder::default();
        attribution.record_cache_key(cache_key_local_instructions);
        let (cache_key, catalog) = context.into_cache_inputs();
        let (compiled, cache_attribution, phase_attribution, accepted_authority) = self
            .compile_sql_statement_with_cache(
                cache_key,
                &catalog,
                attribution,
                sql,
                surface,
                entity_path.as_ref(),
            )?;
        let context =
            SqlCompiledCommandExecutionContext::new(compiled, catalog, accepted_authority, surface);

        Ok((context, cache_attribution, phase_attribution))
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        catalog: &AcceptedSchemaCatalogContext,
        mut attribution: SqlCompileAttributionBuilder,
        sql: &str,
        surface: SqlCompiledCommandSurface,
        entity_path: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
            Option<crate::db::executor::EntityAuthority>,
        ),
        QueryError,
    > {
        let (cache_lookup_local_instructions, (cached, entries, miss_reason)) = measured(|| {
            let cache_state = self.with_sql_compiled_command_cache(|cache| {
                let cached = cache.get(&cache_key).cloned();
                let miss_reason = cached
                    .is_none()
                    .then(|| sql_compiled_command_cache_miss_reason(cache, &cache_key));

                (cached, cache.len(), miss_reason)
            });
            Ok::<_, QueryError>(cache_state)
        })?;
        attribution.record_cache_lookup(cache_lookup_local_instructions);
        record_cache_entries(CacheKind::SqlCompiledCommand, entries);
        if let Some(compiled) = cached {
            record_cache_event_for_path(
                CacheKind::SqlCompiledCommand,
                CacheOutcome::Hit,
                entity_path,
            );
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                attribution.finish(),
                None,
            ));
        }
        record_cache_event_for_path(
            CacheKind::SqlCompiledCommand,
            CacheOutcome::Miss,
            entity_path,
        );
        if let Some(reason) = miss_reason {
            record_cache_miss_reason_for_path(CacheKind::SqlCompiledCommand, reason, entity_path);
        }

        let authority = catalog.accepted_entity_authority();
        let schema = catalog.accepted_schema_info();

        let parse_result =
            measured(|| parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error));
        let (parse_local_instructions, (parsed, parse_attribution)) = match parse_result {
            Ok(parsed) => parsed,
            Err(error) => {
                record_sql_compile_reject_for_path(SqlCompileRejectPhase::Parse, entity_path);
                return Err(error);
            }
        };
        attribution.record_parse(parse_local_instructions, parse_attribution);
        let compile_result = Self::compile_sql_statement_measured(&parsed, surface, schema);
        let (artifacts, compile_attribution) = match compile_result {
            Ok(compiled) => compiled,
            Err(error) => {
                record_sql_compile_reject_for_path(SqlCompileRejectPhase::Semantic, entity_path);
                return Err(error);
            }
        };
        attribution.record_core_compile(compile_attribution);
        let compiled = artifacts.command;

        let (cache_insert_local_instructions, entries) = measured(|| {
            let entries = self.with_sql_compiled_command_cache(|cache| {
                cache.insert(cache_key, compiled.clone());
                cache.len()
            });
            Ok::<_, QueryError>(entries)
        })?;
        attribution.record_cache_insert(cache_insert_local_instructions);
        record_cache_entries(CacheKind::SqlCompiledCommand, entries);
        record_cache_event_for_path(
            CacheKind::SqlCompiledCommand,
            CacheOutcome::Insert,
            entity_path,
        );

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            attribution.finish(),
            Some(authority),
        ))
    }
}
