//! Module: db::session::sql::compile_cache
//! Responsibility: compiled SQL command cache lookup, miss compilation, and
//! insertion orchestration.
//! Does not own: parsed-statement semantic compilation or SQL execution.
//! Boundary: keeps the public query/mutation compile surfaces on one cache shell.

use crate::{
    db::{
        DbSession, QueryError, SqlStatementDispatch,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCompiledCommandCacheContext, SqlCompiledCommandCacheKey,
                SqlCompiledCommandExecutionContext, SqlCompiledCommandSurface,
                sql_statement_entity_name_from_statement,
            },
        },
        sql::parser::{SqlStatement, parse_sql},
    },
    error::InternalError,
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    #[cfg(test)]
    pub(in crate::db) fn compile_sql_query_for_tests(
        &self,
        sql: &str,
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        let dispatch = crate::db::sql_statement_dispatch(sql)?;

        self.compile_sql_query_with_dispatch_execution_context(&dispatch)
    }

    #[inline]
    pub(in crate::db::session::sql) fn compile_sql_query_with_dispatch_execution_context(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        let parsed = dispatch.statement();
        let entity_name = sql_statement_entity_name_from_statement(parsed).map(str::to_string);
        let catalog = match entity_name.as_deref() {
            Some(entity_name) => self
                .find_accepted_schema_catalog_context_for_entity_name(entity_name)
                .map_err(QueryError::execute)?
                .ok_or_else(|| QueryError::execute(InternalError::sql_query_entity_not_found()))?,
            None => self
                .accepted_schema_catalog_context_for_entity_name(None)
                .map_err(QueryError::execute)?,
        };
        let context = self.compile_sql_surface_with_catalog(
            dispatch.sql(),
            parsed,
            SqlCompiledCommandSurface::Query,
            catalog,
        )?;

        Ok((context, entity_name.unwrap_or_default()))
    }

    pub(in crate::db) fn compile_sql_mutation_with_execution_context(
        &self,
        sql: &str,
    ) -> Result<SqlCompiledCommandExecutionContext, QueryError> {
        let parsed = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;
        let entity_name = sql_statement_entity_name_from_statement(&parsed).map(str::to_string);
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name.as_deref())
            .map_err(QueryError::execute)?;
        self.compile_sql_surface_with_catalog(
            sql,
            &parsed,
            SqlCompiledCommandSurface::Mutation,
            catalog,
        )
    }

    fn compile_sql_surface_with_catalog(
        &self,
        sql: &str,
        parsed: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Result<SqlCompiledCommandExecutionContext, QueryError> {
        let entity_path = catalog.identity().entity_path_handle();
        let context = SqlCompiledCommandCacheContext::from_catalog(surface, sql, catalog);
        let (cache_key, catalog) = context.into_cache_inputs();
        let (compiled, accepted_authority) = self.compile_sql_statement_with_cache(
            cache_key,
            &catalog,
            parsed,
            surface,
            entity_path.as_ref(),
        )?;
        let context =
            SqlCompiledCommandExecutionContext::new(compiled, catalog, accepted_authority, surface);

        Ok(context)
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        catalog: &AcceptedSchemaCatalogContext,
        parsed: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        _entity_path: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            Option<crate::db::executor::EntityAuthority>,
        ),
        QueryError,
    > {
        let cached = self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
        if let Some(compiled) = cached {
            return Ok((compiled, None));
        }
        let authority = catalog.accepted_entity_authority();
        let schema = catalog.accepted_schema_info();

        let compiled = Self::compile_sql_statement(parsed, surface, schema)?;

        self.with_sql_compiled_command_cache(|cache| {
            cache.insert(cache_key, compiled.clone());
        });

        Ok((compiled, Some(authority)))
    }
}
