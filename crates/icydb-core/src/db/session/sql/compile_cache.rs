//! Module: db::session::sql::compile_cache
//! Responsibility: compiled SQL command cache lookup, miss compilation, and
//! insertion orchestration.
//! Does not own: parsed-statement semantic compilation or SQL execution.
//! Boundary: keeps the public query/mutation compile surfaces on one cache shell.

#[cfg(all(test, feature = "diagnostics"))]
use crate::db::session::sql::sql_statement_dispatch;
use crate::{
    db::{
        DbSession, QueryError, SqlStatementDispatch,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompileAttributionBuilder,
                SqlCompilePhaseAttribution, SqlCompiledCommandCacheContext,
                SqlCompiledCommandCacheKey, SqlCompiledCommandExecutionContext,
                SqlCompiledCommandSurface, measured, sql_statement_entity_name_from_statement,
            },
        },
        sql::parser::{SqlParsePhaseAttribution, SqlStatement, parse_sql_with_attribution},
    },
    error::InternalError,
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::db) fn compile_sql_query_with_execution_context(
        &self,
        sql: &str,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            String,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let dispatch = sql_statement_dispatch(sql)?;
        self.compile_sql_query_with_dispatch_execution_context(&dispatch)
    }

    #[inline]
    pub(in crate::db::session::sql) fn compile_sql_query_with_dispatch_execution_context(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<
        (
            SqlCompiledCommandExecutionContext,
            String,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let parsed = dispatch.statement();
        let (entity_name, attribution) = Self::compilation_input_from_parsed(
            parsed,
            dispatch.parse_local_instructions(),
            dispatch.parse_attribution(),
        );
        let catalog = match entity_name.as_deref() {
            Some(entity_name) => self
                .find_accepted_schema_catalog_context_for_entity_name(entity_name)
                .map_err(QueryError::execute)?
                .ok_or_else(|| QueryError::execute(InternalError::sql_query_entity_not_found()))?,
            None => self
                .accepted_schema_catalog_context_for_entity_name(None)
                .map_err(QueryError::execute)?,
        };
        let (context, cache_attribution, phase_attribution) = self
            .compile_sql_surface_with_catalog(
                dispatch.sql(),
                parsed,
                SqlCompiledCommandSurface::Query,
                catalog,
                attribution,
            )?;

        Ok((
            context,
            entity_name.unwrap_or_default(),
            cache_attribution,
            phase_attribution,
        ))
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
        let (parsed, entity_name, attribution) = Self::parse_sql_for_compilation(sql)?;
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name.as_deref())
            .map_err(QueryError::execute)?;
        self.compile_sql_surface_with_catalog(
            sql,
            &parsed,
            SqlCompiledCommandSurface::Mutation,
            catalog,
            attribution,
        )
    }

    fn parse_sql_for_compilation(
        sql: &str,
    ) -> Result<(SqlStatement, Option<String>, SqlCompileAttributionBuilder), QueryError> {
        let (parse_local_instructions, (parsed, parse_attribution)) =
            measured(|| parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error))?;

        let (entity_name, attribution) = Self::compilation_input_from_parsed(
            &parsed,
            parse_local_instructions,
            parse_attribution,
        );

        Ok((parsed, entity_name, attribution))
    }

    #[inline]
    fn compilation_input_from_parsed(
        parsed: &SqlStatement,
        parse_local_instructions: u64,
        parse_attribution: SqlParsePhaseAttribution,
    ) -> (Option<String>, SqlCompileAttributionBuilder) {
        let entity_name = sql_statement_entity_name_from_statement(parsed).map(str::to_string);
        let mut attribution = SqlCompileAttributionBuilder::default();
        attribution.record_parse(parse_local_instructions, parse_attribution);

        (entity_name, attribution)
    }

    fn compile_sql_surface_with_catalog(
        &self,
        sql: &str,
        parsed: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        catalog: AcceptedSchemaCatalogContext,
        mut attribution: SqlCompileAttributionBuilder,
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
        attribution.record_cache_key(cache_key_local_instructions);
        let (cache_key, catalog) = context.into_cache_inputs();
        let (compiled, cache_attribution, phase_attribution, accepted_authority) = self
            .compile_sql_statement_with_cache(
                cache_key,
                &catalog,
                attribution,
                parsed,
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
        parsed: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        _entity_path: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
            Option<crate::db::executor::EntityAuthority>,
        ),
        QueryError,
    > {
        let (cache_lookup_local_instructions, cached) = measured(|| {
            let cache_state =
                self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
            Ok::<_, QueryError>(cache_state)
        })?;
        attribution.record_cache_lookup(cache_lookup_local_instructions);
        if let Some(compiled) = cached {
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                attribution.finish(),
                None,
            ));
        }
        let authority = catalog.accepted_entity_authority();
        let schema = catalog.accepted_schema_info();

        let (artifacts, compile_attribution) =
            Self::compile_sql_statement_measured(parsed, surface, schema)?;
        attribution.record_core_compile(compile_attribution);
        let compiled = artifacts.command;

        let (cache_insert_local_instructions, ()) = measured(|| {
            self.with_sql_compiled_command_cache(|cache| {
                cache.insert(cache_key, compiled.clone());
            });
            Ok::<_, QueryError>(())
        })?;
        attribution.record_cache_insert(cache_insert_local_instructions);

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            attribution.finish(),
            Some(authority),
        ))
    }
}
