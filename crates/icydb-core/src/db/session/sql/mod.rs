//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod execute;
mod explain;
mod parameter;
mod projection;

#[cfg(feature = "diagnostics")]
use candid::CandidType;
use icydb_utils::Xxh3;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;
use std::{cell::RefCell, collections::HashMap, hash::BuildHasherDefault, sync::Arc};

type CacheBuildHasher = BuildHasherDefault<Xxh3>;

// Bump these when SQL cache-key meaning changes in a way that must force
// existing in-heap entries to miss instead of aliasing old semantics.
const SQL_COMPILED_COMMAND_CACHE_METHOD_VERSION: u8 = 1;

#[cfg(test)]
pub(in crate::db) use self::parameter::PreparedSqlExecutionTemplateKind;
#[allow(unused_imports)]
pub(in crate::db) use self::parameter::PreparedSqlQuery;
#[cfg(feature = "diagnostics")]
use crate::db::DataStore;
#[cfg(feature = "diagnostics")]
use crate::db::executor::GroupedCountAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::projection::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[allow(unused_imports)]
pub(in crate::db) use crate::db::sql::lowering::{
    PreparedSqlParameterContract, PreparedSqlParameterTypeFamily,
};
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
use crate::db::sql::parser::{SqlDeleteStatement, SqlInsertStatement, SqlUpdateStatement};
use crate::{
    db::{
        DbSession, GroupedRow, PersistedRow, QueryError,
        commit::CommitSchemaFingerprint,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
        schema::commit_schema_fingerprint_for_entity,
        session::query::QueryPlanCacheAttribution,
        session::sql::projection::{
            projection_fixed_scales_from_projection_spec, projection_labels_from_projection_spec,
        },
        sql::lowering::{LoweredBaseQueryShape, LoweredSqlCommand, SqlGlobalAggregateCommandCore},
        sql::parser::{SqlStatement, parse_sql_with_attribution},
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;
#[cfg(feature = "diagnostics")]
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
        fixed_scales: Vec<Option<u32>>,
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
        fixed_scales: Vec<Option<u32>>,
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

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile_cache_key_local_instructions: u64,
    pub compile_cache_lookup_local_instructions: u64,
    pub compile_parse_local_instructions: u64,
    pub compile_parse_tokenize_local_instructions: u64,
    pub compile_parse_select_local_instructions: u64,
    pub compile_parse_expr_local_instructions: u64,
    pub compile_parse_predicate_local_instructions: u64,
    pub compile_aggregate_lane_check_local_instructions: u64,
    pub compile_prepare_local_instructions: u64,
    pub compile_lower_local_instructions: u64,
    pub compile_bind_local_instructions: u64,
    pub compile_cache_insert_local_instructions: u64,
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count_borrowed_hash_computations: u64,
    pub grouped_count_bucket_candidate_checks: u64,
    pub grouped_count_existing_group_hits: u64,
    pub grouped_count_new_group_inserts: u64,
    pub grouped_count_row_materialization_local_instructions: u64,
    pub grouped_count_group_lookup_local_instructions: u64,
    pub grouped_count_existing_group_update_local_instructions: u64,
    pub grouped_count_new_group_insert_local_instructions: u64,
    pub pure_covering_decode_local_instructions: u64,
    pub pure_covering_row_assembly_local_instructions: u64,
    pub store_get_calls: u64,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub sql_compiled_command_cache_hits: u64,
    pub sql_compiled_command_cache_misses: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

// SqlExecutePhaseAttribution keeps the execute side split into select-plan
// work, physical store/index access, and narrower runtime execution so shell
// tooling can show all three.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlExecutePhaseAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: GroupedCountAttribution,
}

///
/// SqlCompilePhaseAttribution
///
/// SqlCompilePhaseAttribution keeps the SQL-front-end compile miss path split
/// into the concrete stages that still exist after the shared lower-cache
/// collapse.
/// This lets perf audits distinguish cache lookup, parsing, prepared-statement
/// normalization, lowered-command construction, structural binding, and cache
/// insertion cost instead of treating compile as one opaque bucket.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SqlCompilePhaseAttribution {
    pub cache_key: u64,
    pub cache_lookup: u64,
    pub parse: u64,
    pub parse_tokenize: u64,
    pub parse_select: u64,
    pub parse_expr: u64,
    pub parse_predicate: u64,
    pub aggregate_lane_check: u64,
    pub prepare: u64,
    pub lower: u64,
    pub bind: u64,
    pub cache_insert: u64,
}

impl SqlCompilePhaseAttribution {
    #[must_use]
    const fn cache_hit(cache_key: u64, cache_lookup: u64) -> Self {
        Self {
            cache_key,
            cache_lookup,
            parse: 0,
            parse_tokenize: 0,
            parse_select: 0,
            parse_expr: 0,
            parse_predicate: 0,
            aggregate_lane_check: 0,
            prepare: 0,
            lower: 0,
            bind: 0,
            cache_insert: 0,
        }
    }
}

#[cfg(feature = "diagnostics")]
impl SqlExecutePhaseAttribution {
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
            store_local_instructions,
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions),
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: GroupedCountAttribution::none(),
        }
    }
}

// SqlCacheAttribution keeps the surviving SQL-front-end compile cache separate
// from the shared lower query-plan cache so perf audits can tell which
// boundary actually produced reuse on one query path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SqlCacheAttribution {
    pub sql_compiled_command_cache_hits: u64,
    pub sql_compiled_command_cache_misses: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

impl SqlCacheAttribution {
    #[must_use]
    const fn none() -> Self {
        Self {
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 0,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 0,
        }
    }

    #[must_use]
    const fn sql_compiled_command_cache_hit() -> Self {
        Self {
            sql_compiled_command_cache_hits: 1,
            ..Self::none()
        }
    }

    #[must_use]
    const fn sql_compiled_command_cache_miss() -> Self {
        Self {
            sql_compiled_command_cache_misses: 1,
            ..Self::none()
        }
    }

    #[must_use]
    const fn from_shared_query_plan_cache(attribution: QueryPlanCacheAttribution) -> Self {
        Self {
            shared_query_plan_cache_hits: attribution.hits,
            shared_query_plan_cache_misses: attribution.misses,
            ..Self::none()
        }
    }

    #[must_use]
    const fn merge(self, other: Self) -> Self {
        Self {
            sql_compiled_command_cache_hits: self
                .sql_compiled_command_cache_hits
                .saturating_add(other.sql_compiled_command_cache_hits),
            sql_compiled_command_cache_misses: self
                .sql_compiled_command_cache_misses
                .saturating_add(other.sql_compiled_command_cache_misses),
            shared_query_plan_cache_hits: self
                .shared_query_plan_cache_hits
                .saturating_add(other.shared_query_plan_cache_hits),
            shared_query_plan_cache_misses: self
                .shared_query_plan_cache_misses
                .saturating_add(other.shared_query_plan_cache_misses),
        }
    }
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
    cache_method_version: u8,
    surface: SqlCompiledCommandSurface,
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    sql: String,
}

///
/// SqlProjectionContract
///
/// SqlProjectionContract is the outward SQL projection contract
/// derived from one shared lower prepared plan.
/// SQL execution keeps this wrapper so statement shaping stays owner-local
/// while all prepared-plan reuse lives entirely below the SQL boundary.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl SqlProjectionContract {
    #[must_use]
    pub(in crate::db) const fn new(columns: Vec<String>, fixed_scales: Vec<Option<u32>>) -> Self {
        Self {
            columns,
            fixed_scales,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
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
            cache_method_version: SQL_COMPILED_COMMAND_CACHE_METHOD_VERSION,
            surface,
            entity_path: E::PATH,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            sql: sql.to_string(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }
}

#[cfg(test)]
impl SqlCompiledCommandCacheKey {
    pub(in crate::db) fn query_for_entity_with_method_version<E>(
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity_with_method_version::<E>(
            SqlCompiledCommandSurface::Query,
            sql,
            cache_method_version,
        )
    }

    pub(in crate::db) fn update_for_entity_with_method_version<E>(
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity_with_method_version::<E>(
            SqlCompiledCommandSurface::Update,
            sql,
            cache_method_version,
        )
    }

    fn for_entity_with_method_version<E>(
        surface: SqlCompiledCommandSurface,
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self {
            cache_method_version,
            surface,
            entity_path: E::PATH,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            sql: sql.to_string(),
        }
    }
}

pub(in crate::db) type SqlCompiledCommandCache =
    HashMap<SqlCompiledCommandCacheKey, CompiledSqlCommand, CacheBuildHasher>;

thread_local! {
    // Keep SQL-facing caches in canister-lifetime heap state keyed by the
    // store registry identity so update calls can warm query-facing SQL reuse
    // without leaking entries across unrelated registries in tests.
    static SQL_COMPILED_COMMAND_CACHES: RefCell<HashMap<usize, SqlCompiledCommandCache, CacheBuildHasher>> =
        RefCell::new(HashMap::default());
}

// Keep the compile artifact session-owned and generic-free so the SQL surface
// can separate semantic compilation from execution without coupling the seam to
// typed entity binding or executor scratch state.
#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select {
        query: Arc<StructuralQuery>,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    },
    Delete {
        query: LoweredBaseQueryShape,
        statement: SqlDeleteStatement,
    },
    GlobalAggregate {
        command: Box<SqlGlobalAggregateCommandCore>,
    },
    Explain(LoweredSqlCommand),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

impl CompiledSqlCommand {
    fn new_select(query: StructuralQuery, compiled_cache_key: SqlCompiledCommandCacheKey) -> Self {
        Self::Select {
            query: Arc::new(query),
            compiled_cache_key,
        }
    }
}

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
#[cfg(test)]
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

// Keep the diagnostics-oriented parser split as a separate helper so normal
// callers can keep using the plain parser surface without threading
// attribution through unrelated paths.
fn parse_sql_statement_with_attribution(
    sql: &str,
) -> Result<
    (
        SqlStatement,
        crate::db::sql::parser::SqlParsePhaseAttribution,
    ),
    QueryError,
> {
    parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_sql_local_instruction_counter() -> u64 {
    #[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(all(feature = "diagnostics", target_arch = "wasm32")))]
    {
        0
    }
}

pub(in crate::db::session::sql) fn measure_sql_stage<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    #[cfg(feature = "diagnostics")]
    let start = read_sql_local_instruction_counter();

    let result = run();

    #[cfg(feature = "diagnostics")]
    let delta = read_sql_local_instruction_counter().saturating_sub(start);

    #[cfg(not(feature = "diagnostics"))]
    let delta = 0;

    (delta, result)
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_cache_scope_id(&self) -> usize {
        self.db.cache_scope_id()
    }

    fn with_sql_compiled_command_cache<R>(
        &self,
        f: impl FnOnce(&mut SqlCompiledCommandCache) -> R,
    ) -> R {
        let scope_id = self.sql_cache_scope_id();

        SQL_COMPILED_COMMAND_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_default();

            f(cache)
        })
    }

    #[cfg(test)]
    pub(in crate::db) fn sql_compiled_command_cache_len(&self) -> usize {
        self.with_sql_compiled_command_cache(|cache| cache.len())
    }

    #[cfg(test)]
    pub(in crate::db) fn clear_sql_caches_for_tests(&self) {
        self.with_sql_compiled_command_cache(SqlCompiledCommandCache::clear);
    }

    // Build one SQL-owned projection contract from one shared lower prepared
    // plan so every SQL SELECT path keeps statement shaping local while the
    // shared lower cache remains the only prepared-plan owner.
    fn sql_select_projection_contract_from_shared_prepared_plan(
        authority: EntityAuthority,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> SqlProjectionContract {
        let projection = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let columns = projection_labels_from_projection_spec(&projection);
        let fixed_scales = projection_fixed_scales_from_projection_spec(&projection);

        SqlProjectionContract::new(columns, fixed_scales)
    }

    // Resolve one SQL SELECT entirely through the shared lower query-plan
    // cache and derive only the outward SQL projection contract locally.
    fn sql_select_prepared_plan(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        cache_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self.cached_shared_query_plan_for_authority(
            authority,
            cache_schema_fingerprint,
            query,
        )?;
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        Ok((
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
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
    #[cfg(feature = "diagnostics")]
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
            measure_sql_stage(|| self.compile_sql_query_with_cache_attribution::<E>(sql));
        let (compiled, compile_cache_attribution, compile_phase_attribution) = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let store_get_calls_before = DataStore::current_get_call_count();
        let pure_covering_decode_before = current_pure_covering_decode_local_instructions();
        let pure_covering_row_assembly_before =
            current_pure_covering_row_assembly_local_instructions();
        let (result, execute_cache_attribution, execute_phase_attribution) =
            self.execute_compiled_sql_with_phase_attribution::<E>(&compiled)?;
        let store_get_calls =
            DataStore::current_get_call_count().saturating_sub(store_get_calls_before);
        let pure_covering_decode_local_instructions =
            current_pure_covering_decode_local_instructions()
                .saturating_sub(pure_covering_decode_before);
        let pure_covering_row_assembly_local_instructions =
            current_pure_covering_row_assembly_local_instructions()
                .saturating_sub(pure_covering_row_assembly_before);
        let execute_local_instructions = execute_phase_attribution
            .planner_local_instructions
            .saturating_add(execute_phase_attribution.store_local_instructions)
            .saturating_add(execute_phase_attribution.executor_local_instructions);
        let cache_attribution = compile_cache_attribution.merge(execute_cache_attribution);
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            result,
            SqlQueryExecutionAttribution {
                compile_local_instructions,
                compile_cache_key_local_instructions: compile_phase_attribution.cache_key,
                compile_cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                compile_parse_local_instructions: compile_phase_attribution.parse,
                compile_parse_tokenize_local_instructions: compile_phase_attribution.parse_tokenize,
                compile_parse_select_local_instructions: compile_phase_attribution.parse_select,
                compile_parse_expr_local_instructions: compile_phase_attribution.parse_expr,
                compile_parse_predicate_local_instructions: compile_phase_attribution
                    .parse_predicate,
                compile_aggregate_lane_check_local_instructions: compile_phase_attribution
                    .aggregate_lane_check,
                compile_prepare_local_instructions: compile_phase_attribution.prepare,
                compile_lower_local_instructions: compile_phase_attribution.lower,
                compile_bind_local_instructions: compile_phase_attribution.bind,
                compile_cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                planner_local_instructions: execute_phase_attribution.planner_local_instructions,
                store_local_instructions: execute_phase_attribution.store_local_instructions,
                executor_local_instructions: execute_phase_attribution.executor_local_instructions,
                grouped_stream_local_instructions: execute_phase_attribution
                    .grouped_stream_local_instructions,
                grouped_fold_local_instructions: execute_phase_attribution
                    .grouped_fold_local_instructions,
                grouped_finalize_local_instructions: execute_phase_attribution
                    .grouped_finalize_local_instructions,
                grouped_count_borrowed_hash_computations: execute_phase_attribution
                    .grouped_count
                    .borrowed_hash_computations,
                grouped_count_bucket_candidate_checks: execute_phase_attribution
                    .grouped_count
                    .bucket_candidate_checks,
                grouped_count_existing_group_hits: execute_phase_attribution
                    .grouped_count
                    .existing_group_hits,
                grouped_count_new_group_inserts: execute_phase_attribution
                    .grouped_count
                    .new_group_inserts,
                grouped_count_row_materialization_local_instructions: execute_phase_attribution
                    .grouped_count
                    .row_materialization_local_instructions,
                grouped_count_group_lookup_local_instructions: execute_phase_attribution
                    .grouped_count
                    .group_lookup_local_instructions,
                grouped_count_existing_group_update_local_instructions: execute_phase_attribution
                    .grouped_count
                    .existing_group_update_local_instructions,
                grouped_count_new_group_insert_local_instructions: execute_phase_attribution
                    .grouped_count
                    .new_group_insert_local_instructions,
                pure_covering_decode_local_instructions,
                pure_covering_row_assembly_local_instructions,
                store_get_calls,
                response_decode_local_instructions: 0,
                execute_local_instructions,
                total_local_instructions,
                sql_compiled_command_cache_hits: cache_attribution.sql_compiled_command_cache_hits,
                sql_compiled_command_cache_misses: cache_attribution
                    .sql_compiled_command_cache_misses,
                shared_query_plan_cache_hits: cache_attribution.shared_query_plan_cache_hits,
                shared_query_plan_cache_misses: cache_attribution.shared_query_plan_cache_misses,
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

    // Compile one SQL query-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_query_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_query_with_cache_attribution<E>(
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_key_local_instructions, cache_key) = measure_sql_stage(|| {
            Ok::<_, QueryError>(SqlCompiledCommandCacheKey::query_for_entity::<E>(sql))
        });
        let cache_key = cache_key?;

        self.compile_sql_statement_with_cache::<E>(
            cache_key,
            cache_key_local_instructions,
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
        self.compile_sql_update_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_update_with_cache_attribution<E>(
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_key_local_instructions, cache_key) = measure_sql_stage(|| {
            Ok::<_, QueryError>(SqlCompiledCommandCacheKey::update_for_entity::<E>(sql))
        });
        let cache_key = cache_key?;

        self.compile_sql_statement_with_cache::<E>(
            cache_key,
            cache_key_local_instructions,
            sql,
            Self::ensure_sql_update_statement_supported,
        )
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache<E>(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        cache_key_local_instructions: u64,
        sql: &str,
        ensure_surface_supported: fn(&SqlStatement) -> Result<(), QueryError>,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_lookup_local_instructions, cached) = measure_sql_stage(|| {
            let cached =
                self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
            Ok::<_, QueryError>(cached)
        });
        let cached = cached?;
        if let Some(compiled) = cached {
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                SqlCompilePhaseAttribution::cache_hit(
                    cache_key_local_instructions,
                    cache_lookup_local_instructions,
                ),
            ));
        }

        let (parse_local_instructions, parsed) =
            measure_sql_stage(|| parse_sql_statement_with_attribution(sql));
        let (parsed, parse_attribution) = parsed?;
        let parse_select_local_instructions = parse_local_instructions
            .saturating_sub(parse_attribution.tokenize)
            .saturating_sub(parse_attribution.expr)
            .saturating_sub(parse_attribution.predicate);
        ensure_surface_supported(&parsed)?;
        let authority = EntityAuthority::for_type::<E>();
        let (
            compiled,
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ) = Self::compile_sql_statement_for_authority(&parsed, authority, cache_key.clone())?;

        let (cache_insert_local_instructions, cache_insert) = measure_sql_stage(|| {
            self.with_sql_compiled_command_cache(|cache| {
                cache.insert(cache_key, compiled.clone());
            });
            Ok::<_, QueryError>(())
        });
        cache_insert?;

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            SqlCompilePhaseAttribution {
                cache_key: cache_key_local_instructions,
                cache_lookup: cache_lookup_local_instructions,
                parse: parse_local_instructions,
                parse_tokenize: parse_attribution.tokenize,
                parse_select: parse_select_local_instructions,
                parse_expr: parse_attribution.expr,
                parse_predicate: parse_attribution.predicate,
                aggregate_lane_check: aggregate_lane_check_local_instructions,
                prepare: prepare_local_instructions,
                lower: lower_local_instructions,
                bind: bind_local_instructions,
                cache_insert: cache_insert_local_instructions,
            },
        ))
    }
}
