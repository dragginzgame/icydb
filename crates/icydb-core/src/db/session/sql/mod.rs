//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod execute;
mod explain;
mod projection;

#[cfg(feature = "perf-attribution")]
use candid::CandidType;
#[cfg(feature = "perf-attribution")]
use serde::Deserialize;
use std::{cell::RefCell, collections::HashMap};

use crate::db::sql::parser::{SqlDeleteStatement, SqlInsertStatement, SqlUpdateStatement};
use crate::{
    db::{
        DbSession, GroupedRow, PersistedRow, QueryError,
        commit::CommitSchemaFingerprint,
        executor::EntityAuthority,
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
        schema::commit_schema_fingerprint_for_entity,
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSelectShape, LoweredSqlCommand,
            SqlGlobalAggregateCommandCore,
        },
        sql::parser::{SqlStatement, parse_sql},
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(test)]
use crate::db::{
    MissingRowPolicy, PagedGroupedExecutionWithTrace,
    sql::lowering::{
        LoweredSelectQueryShape, bind_lowered_sql_query, lower_sql_command_from_prepared_statement,
        prepare_sql_statement,
    },
};

#[cfg(feature = "structural-read-metrics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

/// Unified SQL statement payload returned by shared SQL lane execution.
#[derive(Debug)]
pub enum SqlStatementResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        rows: Vec<Vec<crate::value::Value>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

///
/// SqlQueryExecutionAttribution
///
/// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
/// split at the new compile/execute seam.
/// This keeps future cache validation focused on one concrete question:
/// whether repeated queries stop paying compile cost while execute cost stays
/// otherwise comparable.
///

#[cfg(feature = "perf-attribution")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SqlCompiledCommandSurface {
    Query,
    Update,
}

///
/// SqlCompiledCommandCacheKey
///
/// SqlCompiledCommandCacheKey pins one compiled SQL artifact to the exact
/// session-local semantic boundary that produced it.
/// The key is intentionally conservative: surface kind, entity path, schema
/// fingerprint, and raw SQL text must all match before execution can reuse a
/// prior compile result.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledCommandCacheKey {
    surface: SqlCompiledCommandSurface,
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    sql: String,
}

impl SqlCompiledCommandCacheKey {
    fn query_for_entity<E>(sql: &str) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity::<E>(SqlCompiledCommandSurface::Query, sql)
    }

    fn update_for_entity<E>(sql: &str) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity::<E>(SqlCompiledCommandSurface::Update, sql)
    }

    fn for_entity<E>(surface: SqlCompiledCommandSurface, sql: &str) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self {
            surface,
            entity_path: E::PATH,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            sql: sql.to_string(),
        }
    }
}

pub(in crate::db) type SqlCompiledCommandCache =
    HashMap<SqlCompiledCommandCacheKey, CompiledSqlCommand>;

// Keep the compile artifact session-owned and generic-free so the SQL surface
// can separate semantic compilation from execution without coupling the seam to
// typed entity binding or executor scratch state.
#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select(LoweredSelectShape),
    Delete {
        query: LoweredBaseQueryShape,
        statement: SqlDeleteStatement,
    },
    GlobalAggregate {
        command: SqlGlobalAggregateCommandCore,
        label_override: Option<String>,
    },
    Explain(LoweredSqlCommand),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

#[cfg(feature = "perf-attribution")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_sql_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "perf-attribution")]
fn measure_sql_stage<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_sql_local_instruction_counter();
    let result = run();
    let delta = read_sql_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

impl<C: CanisterKind> DbSession<C> {
    // Lazily allocate one session-local compiled SQL cache so cold sessions do
    // not pay any map setup cost until SQL compilation is actually used.
    fn sql_compiled_command_cache(&self) -> &RefCell<SqlCompiledCommandCache> {
        self.sql_compiled_command_cache
            .get_or_init(|| RefCell::new(SqlCompiledCommandCache::new()))
    }

    #[cfg(test)]
    pub(in crate::db) fn sql_compiled_command_cache_len(&self) -> usize {
        self.sql_compiled_command_cache().borrow().len()
    }

    // Resolve planner-visible indexes and build one execution-ready
    // structural plan at the session SQL boundary.
    pub(in crate::db::session::sql) fn build_structural_plan_with_visible_indexes_for_authority(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(VisibleIndexes<'_>, AccessPlannedQuery), QueryError> {
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = query.build_plan_with_visible_indexes(&visible_indexes)?;

        Ok((visible_indexes, plan))
    }

    // Keep the public SQL query surface aligned with its name and with
    // query-shaped canister entrypoints.
    fn ensure_sql_query_statement_supported(statement: &SqlStatement) -> Result<(), QueryError> {
        match statement {
            SqlStatement::Select(_)
            | SqlStatement::Explain(_)
            | SqlStatement::Describe(_)
            | SqlStatement::ShowIndexes(_)
            | SqlStatement::ShowColumns(_)
            | SqlStatement::ShowEntities(_) => Ok(()),
            SqlStatement::Insert(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
            )),
            SqlStatement::Update(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
            )),
            SqlStatement::Delete(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
            )),
        }
    }

    // Keep the public SQL mutation surface aligned with state-changing SQL
    // while preserving one explicit read/introspection owner.
    fn ensure_sql_update_statement_supported(statement: &SqlStatement) -> Result<(), QueryError> {
        match statement {
            SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => Ok(()),
            SqlStatement::Select(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SELECT; use execute_sql_query::<E>()",
            )),
            SqlStatement::Explain(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects EXPLAIN; use execute_sql_query::<E>()",
            )),
            SqlStatement::Describe(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects DESCRIBE; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowIndexes(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW INDEXES; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowColumns(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW COLUMNS; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowEntities(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW ENTITIES; use execute_sql_query::<E>()",
            )),
        }
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_query::<E>(sql)?;

        self.execute_compiled_sql::<E>(&compiled)
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "perf-attribution")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure the compile side of the new seam, including parse,
        // surface validation, and semantic command construction.
        let (compile_local_instructions, compiled) =
            measure_sql_stage(|| self.compile_sql_query::<E>(sql));
        let compiled = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let (execute_local_instructions, result) =
            measure_sql_stage(|| self.execute_compiled_sql::<E>(&compiled));
        let result = result?;
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            result,
            SqlQueryExecutionAttribution {
                compile_local_instructions,
                execute_local_instructions,
                total_local_instructions,
            },
        ))
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_update::<E>(sql)?;

        self.execute_compiled_sql::<E>(&compiled)
    }

    #[cfg(test)]
    pub(in crate::db) fn execute_grouped_sql_query_for_tests<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = parse_sql_statement(sql)?;

        let lowered = lower_sql_command_from_prepared_statement(
            prepare_sql_statement(parsed, E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper requires grouped SELECT",
            ));
        };
        if query.select_shape() != Some(LoweredSelectQueryShape::Grouped) {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper requires grouped SELECT",
            ));
        }
        let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        self.execute_grouped(&query, cursor_token)
    }

    // Compile one SQL query-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_statement_with_cache::<E>(
            SqlCompiledCommandCacheKey::query_for_entity::<E>(sql),
            sql,
            Self::ensure_sql_query_statement_supported,
        )
    }

    // Compile one SQL update-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_update<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_statement_with_cache::<E>(
            SqlCompiledCommandCacheKey::update_for_entity::<E>(sql),
            sql,
            Self::ensure_sql_update_statement_supported,
        )
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache<E>(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        sql: &str,
        ensure_surface_supported: fn(&SqlStatement) -> Result<(), QueryError>,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        {
            let cache = self.sql_compiled_command_cache().borrow();
            if let Some(compiled) = cache.get(&cache_key) {
                return Ok(compiled.clone());
            }
        }

        let parsed = parse_sql_statement(sql)?;
        ensure_surface_supported(&parsed)?;
        let compiled = Self::compile_sql_statement_inner::<E>(&parsed)?;

        self.sql_compiled_command_cache()
            .borrow_mut()
            .insert(cache_key, compiled.clone());

        Ok(compiled)
    }

    // Compile one already-parsed SQL statement into the session-owned semantic
    // command artifact used by the explicit compile -> execute seam.
    pub(in crate::db) fn compile_sql_statement_inner<E>(
        sql_statement: &SqlStatement,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Self::compile_sql_statement_for_authority(sql_statement, EntityAuthority::for_type::<E>())
    }
}
