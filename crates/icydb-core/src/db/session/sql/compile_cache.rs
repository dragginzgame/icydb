//! Module: db::session::sql::compile_cache
//! Responsibility: compiled SQL command cache lookup, miss compilation, and
//! insertion orchestration.
//! Does not own: parsed-statement semantic compilation or SQL execution.
//! Boundary: keeps the public query/mutation compile surfaces on one cache shell.

use std::rc::Rc;

use crate::{
    db::{
        DbSession, QueryError, SqlStatementDispatch,
        executor::{
            AcceptedInspectionPlanLoadError, AcceptedSchemaCatalogContext,
            AcceptedSchemaRuntimeRoot,
        },
        session::sql::{
            CompiledSqlCommand, SqlCompiledCommandCacheEntry, SqlCompiledCommandCacheKey,
            SqlCompiledCommandExecutionContext, SqlCompiledCommandSurface,
            cache::{SqlCachedCommand, compiled_command_retained_bytes},
            sql_statement_entity_name_from_statement,
        },
        sql::{lowering::validate_sql_bindings, parser::SqlStatement},
        sql_shared::validate_sql_input_bytes,
    },
    error::InternalError,
    traits::CanisterKind,
    value::InputValue,
};

impl<C: CanisterKind> DbSession<C> {
    #[cfg(test)]
    pub(in crate::db) fn compile_sql_query_for_tests(
        &self,
        sql: &str,
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        self.compile_sql_query_text_execution_context(sql)
    }

    // Exact resident text has already passed syntax/input admission. Revalidate
    // lifecycle and the complete accepted root on every hit before resolving its
    // entity from current authority. Misses keep the ordinary parser/compiler.
    pub(in crate::db::session::sql) fn compile_sql_query_text_execution_context(
        &self,
        sql: &str,
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        validate_sql_input_bytes(sql).map_err(QueryError::from_sql_parse_error)?;
        let key = SqlCompiledCommandCacheKey::new(SqlCompiledCommandSurface::Query, sql);
        let cached = self.with_sql_compiled_command_cache(|cache| cache.get(&key).cloned());
        if let Some(cached) = cached
            && let SqlCachedCommand::Literal(command) = cached.command
        {
            let root = self
                .accepted_schema_runtime_root()
                .map_err(AcceptedInspectionPlanLoadError::into_internal)
                .map_err(QueryError::execute)?;
            if cached.root == root.identity() {
                let catalog = sql_query_catalog_context(root, cached.entity_name.as_deref())?;
                self.with_sql_compiled_command_cache(|cache| cache.refresh(&key));
                let entity = cached
                    .entity_name
                    .as_deref()
                    .unwrap_or_default()
                    .to_string();
                return Ok((
                    SqlCompiledCommandExecutionContext::new(
                        command,
                        catalog,
                        SqlCompiledCommandSurface::Query,
                    ),
                    entity,
                ));
            }
        }
        let dispatch = crate::db::sql_statement_dispatch(sql)?;
        self.compile_sql_query_with_dispatch_execution_context(&dispatch, &[])
    }

    #[inline]
    pub(in crate::db::session::sql) fn compile_sql_query_with_dispatch_execution_context(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
        bindings: &[InputValue],
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        let parsed = dispatch.statement();
        validate_sql_bindings(parsed, bindings)?;
        let entity_name = sql_statement_entity_name_from_statement(parsed).map(str::to_string);
        let root = self
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
            .map_err(QueryError::execute)?;
        let catalog = sql_query_catalog_context(root, entity_name.as_deref())?;
        let context = self.compile_sql_surface_with_catalog(
            dispatch.sql(),
            parsed,
            SqlCompiledCommandSurface::Query,
            catalog,
            bindings,
        )?;

        Ok((context, entity_name.unwrap_or_default()))
    }

    pub(in crate::db) fn compile_sql_mutation_with_execution_context(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<SqlCompiledCommandExecutionContext, QueryError> {
        let parsed = dispatch.statement();
        crate::db::sql::input::validate_sql_statement_input(parsed, &[])?;
        let entity_name = dispatch.entity_name();
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name)
            .map_err(QueryError::execute)?;
        self.compile_sql_surface_with_catalog(
            dispatch.sql(),
            parsed,
            SqlCompiledCommandSurface::Mutation,
            catalog,
            &[],
        )
    }

    fn compile_sql_surface_with_catalog(
        &self,
        sql: &str,
        parsed: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        catalog: AcceptedSchemaCatalogContext,
        bindings: &[InputValue],
    ) -> Result<SqlCompiledCommandExecutionContext, QueryError> {
        if !bindings.is_empty() {
            let key = SqlCompiledCommandCacheKey::new(surface, sql);
            let cached = self.with_sql_compiled_command_cache(|cache| cache.get(&key).cloned());
            if let Some(cached) = cached
                && cached.root == catalog.runtime_root_identity()
                && let SqlCachedCommand::Parameterized(template) = cached.command
            {
                let compiled = self.bind_sql_parameter_template(
                    &template,
                    catalog.accepted_schema_info(),
                    bindings,
                )?;
                self.with_sql_compiled_command_cache(|cache| cache.refresh(&key));
                return Ok(SqlCompiledCommandExecutionContext::new(
                    compiled, catalog, surface,
                ));
            }
            let (template, compiled) = self.compile_sql_parameter_template(
                parsed,
                surface,
                catalog.accepted_schema_info(),
                bindings,
            )?;
            let resident = SqlCompiledCommandCacheEntry {
                root: catalog.runtime_root_identity(),
                entity_name: sql_statement_entity_name_from_statement(parsed).map(Rc::from),
                command: SqlCachedCommand::Parameterized(Rc::new(template)),
            };
            if let Some(bytes) = compiled_command_retained_bytes(&key, &resident) {
                self.with_sql_compiled_command_cache(|cache| {
                    cache.insert_weighted(key, resident, bytes);
                });
            }
            return Ok(SqlCompiledCommandExecutionContext::new(
                compiled, catalog, surface,
            ));
        }
        let cache_key = SqlCompiledCommandCacheKey::new(surface, sql);
        let compiled =
            self.compile_sql_statement_with_cache(cache_key, &catalog, parsed, surface)?;
        let context = SqlCompiledCommandExecutionContext::new(compiled, catalog, surface);

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
    ) -> Result<CompiledSqlCommand, QueryError> {
        let cached = self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
        if let Some(cached) = cached
            && cached.root == catalog.runtime_root_identity()
            && let SqlCachedCommand::Literal(command) = cached.command
        {
            self.with_sql_compiled_command_cache(|cache| cache.refresh(&cache_key));
            return Ok(command);
        }
        let schema = catalog.accepted_schema_info();

        let compiled = self.compile_sql_statement(parsed, surface, schema, &[])?;

        // Queries are owned by the clone, so execution-local memoization cannot
        // grow the resident command after its retained-byte admission.
        let resident = SqlCompiledCommandCacheEntry {
            root: catalog.runtime_root_identity(),
            entity_name: sql_statement_entity_name_from_statement(parsed).map(Rc::from),
            command: SqlCachedCommand::Literal(compiled.clone()),
        };
        if let Some(bytes) = compiled_command_retained_bytes(&cache_key, &resident) {
            self.with_sql_compiled_command_cache(|cache| {
                cache.insert_weighted(cache_key, resident, bytes);
            });
        }

        Ok(compiled)
    }
}

// Resolve the parsed (or resident) entity label from the same freshly admitted
// root used to verify compilation authority. No second root capture is needed.
fn sql_query_catalog_context(
    root: Rc<AcceptedSchemaRuntimeRoot>,
    entity_name: Option<&str>,
) -> Result<AcceptedSchemaCatalogContext, QueryError> {
    let entity = match entity_name {
        Some(name) => root
            .entity_for_name(name)
            .ok_or_else(|| QueryError::execute(InternalError::sql_query_entity_not_found()))?,
        None => root.first_entity().ok_or_else(|| {
            QueryError::execute(InternalError::unsupported_entity_path(entity_name))
        })?,
    };
    Ok(AcceptedSchemaCatalogContext::new(root, entity))
}
