//! Module: db::session::sql::compile_cache
//! Responsibility: compiled SQL command cache lookup, miss compilation, and
//! insertion orchestration.
//! Does not own: parsed-statement semantic compilation or SQL execution.
//! Boundary: keeps the public query/mutation compile surfaces on one cache shell.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompileAttributionBuilder,
                SqlCompilePhaseAttribution, SqlCompiledCommandCacheKey,
                SqlCompiledCommandExecutionContext, SqlCompiledCommandSurface, measured,
                sql_compiled_command_cache_miss_reason,
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
    // Compile one SQL query-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    #[cfg(test)]
    pub(in crate::db) fn compile_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_query_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    #[cfg(test)]
    pub(in crate::db::session::sql) fn compile_sql_query_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_query_with_execution_context::<E>(sql).map(
            |(context, cache_attribution, phase_attribution)| {
                (context.into_command(), cache_attribution, phase_attribution)
            },
        )
    }

    pub(in crate::db) fn compile_sql_query_with_execution_context<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_surface_with_execution_context::<E>(sql, SqlCompiledCommandSurface::Query)
    }

    // Compile one SQL mutation-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    #[cfg(test)]
    pub(in crate::db) fn compile_sql_mutation<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_mutation_with_execution_context::<E>(sql)
            .map(|(context, _, _)| context.into_command())
    }

    #[cfg(test)]
    pub(in crate::db::session::sql) fn compile_sql_mutation_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_mutation_with_execution_context::<E>(sql)
            .map(|(context, cache_attribution, phase_attribution)| {
                (context.into_command(), cache_attribution, phase_attribution)
            })
    }

    pub(in crate::db) fn compile_sql_mutation_with_execution_context<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        self.compile_sql_surface_with_execution_context::<E>(
            sql,
            SqlCompiledCommandSurface::Mutation,
        )
    }

    // Reuse one internal compile shell for both outward SQL surfaces so query
    // and mutation no longer duplicate cache-key construction and surface
    // validation plumbing before they reach the real compile/cache owner.
    fn compile_sql_surface_with_execution_context<E>(
        &self,
        sql: &str,
        surface: SqlCompiledCommandSurface,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        let cache_context =
            measured(|| self.sql_compiled_command_cache_context_for_entity::<E>(surface, sql));
        let (cache_key_local_instructions, context) = match cache_context {
            Ok(context) => context,
            Err(error) => {
                record_sql_compile_reject_for_path(SqlCompileRejectPhase::CacheKey, E::PATH);
                return Err(error);
            }
        };
        let mut attribution = SqlCompileAttributionBuilder::default();
        attribution.record_cache_key(cache_key_local_instructions);
        let (cache_key, catalog) = context.into_cache_inputs();

        let (compiled, cache_attribution, phase_attribution, accepted_authority) = self
            .compile_sql_statement_with_cache::<E>(
                cache_key,
                &catalog,
                attribution,
                sql,
                surface,
            )?;
        let context =
            SqlCompiledCommandExecutionContext::new(compiled, catalog, accepted_authority, surface);

        Ok((context, cache_attribution, phase_attribution))
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache<E>(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        catalog: &AcceptedSchemaCatalogContext,
        mut attribution: SqlCompileAttributionBuilder,
        sql: &str,
        surface: SqlCompiledCommandSurface,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
            Option<crate::db::executor::EntityAuthority>,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
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
            record_cache_event_for_path(CacheKind::SqlCompiledCommand, CacheOutcome::Hit, E::PATH);
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                attribution.finish(),
                None,
            ));
        }
        record_cache_event_for_path(CacheKind::SqlCompiledCommand, CacheOutcome::Miss, E::PATH);
        if let Some(reason) = miss_reason {
            record_cache_miss_reason_for_path(CacheKind::SqlCompiledCommand, reason, E::PATH);
        }

        let (authority, schema) = catalog
            .accepted_entity_authority_and_schema_info_for::<E>()
            .map_err(QueryError::execute)?;

        let parse_result =
            measured(|| parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error));
        let (parse_local_instructions, (parsed, parse_attribution)) = match parse_result {
            Ok(parsed) => parsed,
            Err(error) => {
                record_sql_compile_reject_for_path(SqlCompileRejectPhase::Parse, E::PATH);
                return Err(error);
            }
        };
        attribution.record_parse(parse_local_instructions, parse_attribution);
        let compile_result =
            Self::compile_sql_statement_measured(&parsed, surface, authority.clone(), &schema);
        let (artifacts, compile_attribution) = match compile_result {
            Ok(compiled) => compiled,
            Err(error) => {
                record_sql_compile_reject_for_path(SqlCompileRejectPhase::Semantic, E::PATH);
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
        record_cache_event_for_path(CacheKind::SqlCompiledCommand, CacheOutcome::Insert, E::PATH);

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            attribution.finish(),
            Some(authority),
        ))
    }
}
