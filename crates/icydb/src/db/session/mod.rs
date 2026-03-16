pub mod delete;
pub mod load;
mod macros;

use crate::{
    db::{
        EntityFieldDescription, EntitySchemaDescription, SqlStatementRoute, StorageReport,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::{
            PagedGroupedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
        },
    },
    error::Error,
    metrics::MetricsSink,
    traits::{CanisterKind, EntityKind, EntityValue, Update, UpdateView},
    types::Id,
};
use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::{FluentLoadQuery, PagedLoadQuery};

///
/// DbSession
///
/// Public facade for session-scoped query execution and policy.
/// Wraps the core session and converts core errors into `icydb::Error`.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    // ------------------------------------------------------------------
    // Session configuration
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn new(session: core::db::DbSession<C>) -> Self {
        Self { inner: session }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    // ------------------------------------------------------------------
    // Query entry points
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery {
            inner: self.inner.load::<E>(),
        }
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
    }

    /// Build one typed query intent from one reduced SQL statement.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.query_from_sql::<E>(sql)?)
    }

    /// Parse one reduced SQL statement into canonical route metadata.
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, Error> {
        Ok(self.inner.sql_statement_route(sql)?)
    }

    /// Derive canonical projection column labels for one reduced SQL `SELECT` statement.
    pub fn sql_projection_columns<E>(&self, sql: &str) -> Result<Vec<String>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.sql_projection_columns::<E>(sql)?)
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<Response<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(Response::from_core(self.inner.execute_sql::<E>(sql)?))
    }

    /// Execute one reduced SQL `SELECT` statement and return projection-shaped rows.
    pub fn execute_sql_projection<E>(&self, sql: &str) -> Result<ProjectionResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(ProjectionResponse::from_core(
            self.inner.execute_sql_projection::<E>(sql)?,
        ))
    }

    /// Execute one reduced SQL global aggregate `SELECT` statement.
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<crate::value::Value, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.execute_sql_aggregate::<E>(sql)?)
    }

    /// Execute one reduced SQL grouped `SELECT` statement with optional continuation cursor.
    pub fn execute_sql_grouped<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedResponse, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let execution = self.inner.execute_sql_grouped::<E>(sql, cursor_token)?;
        let next_cursor = execution.continuation_cursor().map(core::db::encode_cursor);

        Ok(PagedGroupedResponse::new(
            execution.rows().to_vec(),
            next_cursor,
            execution.execution_trace().copied(),
        ))
    }

    /// Explain one reduced SQL statement.
    pub fn explain_sql<E>(&self, sql: &str) -> Result<String, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.explain_sql::<E>(sql)?)
    }

    /// Execute one reduced SQL `DESCRIBE` statement.
    pub fn describe_sql<E>(&self, sql: &str) -> Result<EntitySchemaDescription, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.describe_sql::<E>(sql)?)
    }

    /// Execute one reduced SQL `SHOW INDEXES` statement.
    pub fn show_indexes_sql<E>(&self, sql: &str) -> Result<Vec<String>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.show_indexes_sql::<E>(sql)?)
    }

    /// Execute one reduced SQL `SHOW COLUMNS` statement.
    pub fn show_columns_sql<E>(&self, sql: &str) -> Result<Vec<EntityFieldDescription>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.show_columns_sql::<E>(sql)?)
    }

    /// Execute one reduced SQL `SHOW ENTITIES` statement.
    pub fn show_entities_sql(&self, sql: &str) -> Result<Vec<String>, Error> {
        Ok(self.inner.show_entities_sql(sql)?)
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete::<E>(),
        }
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> SessionDeleteQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete_with_consistency::<E>(consistency),
        }
    }

    /// Return one stable, human-readable index listing for the entity schema.
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.show_indexes::<E>()
    }

    /// Return one stable list of field descriptors for the entity schema.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.show_columns::<E>()
    }

    /// Return one stable list of runtime-registered entity names.
    #[must_use]
    pub fn show_entities(&self) -> Vec<String> {
        self.inner.show_entities()
    }

    /// Return one structured schema description for the entity.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.describe_entity::<E>()
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, Error> {
        Ok(self.inner.storage_report(name_to_path)?)
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(Response::from_core(self.inner.execute_query(query)?))
    }

    /// Build one trace payload for a query without executing it.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.trace_query(query)?)
    }

    /// Execute one grouped query page with optional continuation cursor.
    pub fn execute_grouped<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedResponse, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let execution = self.inner.execute_grouped(query, cursor_token)?;
        let next_cursor = execution.continuation_cursor().map(core::db::encode_cursor);

        Ok(PagedGroupedResponse::new(
            execution.rows().to_vec(),
            next_cursor,
            execution.execution_trace().copied(),
        ))
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::new(self.inner.insert(entity)?))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.insert_many_atomic(entities)?,
        ))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.insert_many_non_atomic(entities)?,
        ))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::new(self.inner.replace(entity)?))
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.replace_many_atomic(entities)?,
        ))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.replace_many_non_atomic(entities)?,
        ))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::new(self.inner.update(entity)?))
    }

    /// Load one entity by id, apply a merge patch, and persist it.
    ///
    /// Patch semantics are handled at this facade boundary so callers do not
    /// need to interact with core patch errors directly.
    pub fn patch_by_id<E>(&self, id: Id<E>, patch: Update<E>) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue + UpdateView,
    {
        let mut entity = self.load::<E>().by_id(id).entity()?;

        UpdateView::merge(&mut entity, patch)?;

        self.update(entity)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.update_many_atomic(entities)?,
        ))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.update_many_non_atomic(entities)?,
        ))
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.insert_view::<E>(view)?)
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.replace_view::<E>(view)?)
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.update_view::<E>(view)?)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
        macros::{canister, entity, store},
        traits::{Path as _, Sanitizer as _},
    };
    use canic_cdk::structures::{
        DefaultMemoryImpl,
        memory::{MemoryId, MemoryManager, VirtualMemory},
    };
    use icydb_core as core;
    use std::cell::RefCell;

    fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
        let manager = MemoryManager::init(DefaultMemoryImpl::default());
        manager.get(MemoryId::new(id))
    }

    ///
    /// FacadeSqlCanister
    ///
    #[canister(memory_min = 240, memory_max = 250, commit_memory_id = 240)]
    pub struct FacadeSqlCanister {}

    ///
    /// FacadeSqlStore
    ///
    #[store(
        ident = "FACADE_SQL_STORE",
        canister = "FacadeSqlCanister",
        data_memory_id = 241,
        index_memory_id = 242
    )]
    pub struct FacadeSqlStore {}

    ///
    /// FacadeSqlEntity
    ///
    #[entity(
        store = "FacadeSqlStore",
        pk(field = "id"),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                default = "crate::types::Ulid::generate"
            ),
            field(ident = "name", value(item(prim = "Text"))),
            field(ident = "age", value(item(prim = "Nat64")))
        )
    )]
    pub struct FacadeSqlEntity {}

    thread_local! {
        static FACADE_SQL_DATA_STORE: RefCell<core::db::DataStore> =
            RefCell::new(core::db::DataStore::init(test_memory(241)));
        static FACADE_SQL_INDEX_STORE: RefCell<core::db::IndexStore> =
            RefCell::new(core::db::IndexStore::init(test_memory(242)));
        static FACADE_SQL_STORE_REGISTRY: core::db::StoreRegistry = {
            let mut registry = core::db::StoreRegistry::new();
            registry
                .register_store(
                    FacadeSqlStore::PATH,
                    &FACADE_SQL_DATA_STORE,
                    &FACADE_SQL_INDEX_STORE,
                )
                .expect("facade SQL test store registration should succeed");
            registry
        };
    }

    fn facade_session() -> DbSession<FacadeSqlCanister> {
        let core_session = core::db::DbSession::<FacadeSqlCanister>::new_with_hooks(
            &FACADE_SQL_STORE_REGISTRY,
            &[],
        );
        DbSession::new(core_session)
    }

    fn reset_facade_sql_store() {
        FACADE_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
        FACADE_SQL_INDEX_STORE.with(|store| store.borrow_mut().clear());
    }

    fn fresh_facade_session() -> DbSession<FacadeSqlCanister> {
        reset_facade_sql_store();
        facade_session()
    }

    fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 3] {
        [
            (
                "SELECT * FROM FacadeSqlEntity JOIN other ON FacadeSqlEntity.id = other.id",
                "JOIN",
            ),
            ("SELECT \"name\" FROM FacadeSqlEntity", "quoted identifiers"),
            ("SELECT * FROM FacadeSqlEntity alias", "table aliases"),
        ]
    }

    fn assert_facade_query_unsupported_runtime(err: Error) {
        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported)
        );
        assert_eq!(err.origin(), ErrorOrigin::Query);
    }

    fn assert_unsupported_sql_runtime_result<T>(result: Result<T, Error>, surface: &str) {
        match result {
            Ok(_) => panic!("unsupported SQL should fail through {surface}"),
            Err(err) => assert_facade_query_unsupported_runtime(err),
        }
    }

    fn assert_explain_contains_tokens(explain: &str, tokens: &[&str], context: &str) {
        for token in tokens {
            assert!(
                explain.contains(token),
                "facade explain matrix case missing token `{token}`: {context}",
            );
        }
    }

    #[test]
    fn facade_query_from_sql_matrix_lowers_expected_modes_and_grouping() {
        let session = fresh_facade_session();

        // Phase 1: define SQL matrix inputs and expected shape contracts.
        let cases = vec![
            (
                "SELECT * FROM FacadeSqlEntity ORDER BY age ASC LIMIT 1",
                true,
                false,
            ),
            (
                "DELETE FROM FacadeSqlEntity ORDER BY age ASC LIMIT 1",
                false,
                false,
            ),
            (
                "SELECT age, COUNT(*) \
                 FROM FacadeSqlEntity \
                 GROUP BY age \
                 ORDER BY age ASC LIMIT 10",
                true,
                true,
            ),
            (
                "SELECT age, COUNT(*) \
                 FROM FacadeSqlEntity \
                 GROUP BY age \
                 HAVING COUNT(*) IS NOT NULL \
                 ORDER BY age ASC LIMIT 10",
                true,
                true,
            ),
        ];

        // Phase 2: compile SQL to query intent and assert mode/grouping contracts.
        for (sql, expect_load_mode, expect_grouped) in cases {
            let query = session
                .query_from_sql::<FacadeSqlEntity>(sql)
                .expect("facade query_from_sql matrix case should lower");
            let is_load_mode = matches!(query.mode(), core::db::QueryMode::Load(_));
            let explain = query
                .explain()
                .expect("facade query_from_sql matrix explain should build")
                .render_text_canonical();
            let is_grouped = explain.contains("grouping=Grouped");

            assert_eq!(
                is_load_mode, expect_load_mode,
                "facade query mode case: {sql}"
            );
            assert_eq!(
                is_grouped, expect_grouped,
                "facade query grouping case: {sql}"
            );
        }
    }

    #[test]
    fn facade_sql_statement_route_describe_classifies_entity() {
        let session = fresh_facade_session();

        let route = session
            .sql_statement_route("DESCRIBE public.FacadeSqlEntity")
            .expect("facade SQL statement route should classify DESCRIBE");

        assert_eq!(
            route,
            SqlStatementRoute::Describe {
                entity: "public.FacadeSqlEntity".to_string(),
            }
        );
        assert_eq!(route.entity(), "public.FacadeSqlEntity");
        assert!(route.is_describe());
        assert!(!route.is_explain());
        assert!(!route.is_show_indexes());
        assert!(!route.is_show_columns());
        assert!(!route.is_show_entities());
    }

    #[test]
    fn facade_sql_statement_route_show_indexes_classifies_entity() {
        let session = fresh_facade_session();

        let route = session
            .sql_statement_route("SHOW INDEXES public.FacadeSqlEntity")
            .expect("facade SQL statement route should classify SHOW INDEXES");

        assert_eq!(
            route,
            SqlStatementRoute::ShowIndexes {
                entity: "public.FacadeSqlEntity".to_string(),
            }
        );
        assert_eq!(route.entity(), "public.FacadeSqlEntity");
        assert!(route.is_show_indexes());
        assert!(!route.is_describe());
        assert!(!route.is_explain());
        assert!(!route.is_show_columns());
        assert!(!route.is_show_entities());
    }

    #[test]
    fn facade_sql_statement_route_show_columns_classifies_entity() {
        let session = fresh_facade_session();

        let route = session
            .sql_statement_route("SHOW COLUMNS public.FacadeSqlEntity")
            .expect("facade SQL statement route should classify SHOW COLUMNS");

        assert_eq!(
            route,
            SqlStatementRoute::ShowColumns {
                entity: "public.FacadeSqlEntity".to_string(),
            }
        );
        assert_eq!(route.entity(), "public.FacadeSqlEntity");
        assert!(route.is_show_columns());
        assert!(!route.is_show_indexes());
        assert!(!route.is_describe());
        assert!(!route.is_explain());
        assert!(!route.is_show_entities());
    }

    #[test]
    fn facade_sql_statement_route_show_entities_classifies_surface() {
        let session = fresh_facade_session();

        let route = session
            .sql_statement_route("SHOW ENTITIES")
            .expect("facade SQL statement route should classify SHOW ENTITIES");

        assert_eq!(route, SqlStatementRoute::ShowEntities);
        assert!(route.is_show_entities());
        assert_eq!(route.entity(), "");
        assert!(!route.is_show_indexes());
        assert!(!route.is_show_columns());
        assert!(!route.is_describe());
        assert!(!route.is_explain());
    }

    #[test]
    fn facade_describe_sql_matches_describe_entity_payload() {
        let session = fresh_facade_session();

        let from_sql = session
            .describe_sql::<FacadeSqlEntity>("DESCRIBE FacadeSqlEntity")
            .expect("facade describe_sql should succeed");
        let from_typed = session.describe_entity::<FacadeSqlEntity>();

        assert_eq!(
            from_sql, from_typed,
            "facade describe_sql should return the same canonical payload as describe_entity",
        );
    }

    #[test]
    fn facade_show_indexes_sql_matches_show_indexes_payload() {
        let session = fresh_facade_session();

        let from_sql = session
            .show_indexes_sql::<FacadeSqlEntity>("SHOW INDEXES FacadeSqlEntity")
            .expect("facade show_indexes_sql should succeed");
        let from_typed = session.show_indexes::<FacadeSqlEntity>();

        assert_eq!(
            from_sql, from_typed,
            "facade show_indexes_sql should return the same canonical payload as show_indexes",
        );
    }

    #[test]
    fn facade_show_columns_sql_matches_show_columns_payload() {
        let session = fresh_facade_session();

        let from_sql = session
            .show_columns_sql::<FacadeSqlEntity>("SHOW COLUMNS FacadeSqlEntity")
            .expect("facade show_columns_sql should succeed");
        let from_typed = session.show_columns::<FacadeSqlEntity>();

        assert_eq!(
            from_sql, from_typed,
            "facade show_columns_sql should return the same canonical payload as show_columns",
        );
    }

    #[test]
    fn facade_show_entities_sql_matches_show_entities_payload() {
        let session = fresh_facade_session();

        let from_sql = session
            .show_entities_sql("SHOW ENTITIES")
            .expect("facade show_entities_sql should succeed");
        let from_typed = session.show_entities();

        assert_eq!(
            from_sql, from_typed,
            "facade show_entities_sql should return the same canonical payload as show_entities",
        );
    }

    #[test]
    fn facade_explain_sql_plan_matrix_queries_include_expected_tokens() {
        let session = fresh_facade_session();

        // Phase 1: define EXPLAIN plan SQL matrix cases.
        let cases = vec![
            (
                "EXPLAIN SELECT * FROM FacadeSqlEntity ORDER BY age LIMIT 1",
                vec!["mode=Load", "access="],
            ),
            (
                "EXPLAIN DELETE FROM FacadeSqlEntity ORDER BY age LIMIT 1",
                vec!["mode=Delete", "access="],
            ),
            (
                "EXPLAIN SELECT age, COUNT(*) \
                 FROM FacadeSqlEntity \
                 GROUP BY age \
                 ORDER BY age ASC LIMIT 10",
                vec!["mode=Load", "grouping=Grouped"],
            ),
            (
                "EXPLAIN SELECT COUNT(*) FROM FacadeSqlEntity",
                vec!["mode=Load", "access="],
            ),
        ];

        // Phase 2: execute each EXPLAIN plan case and assert stable tokens.
        for (sql, tokens) in cases {
            let explain = session
                .explain_sql::<FacadeSqlEntity>(sql)
                .expect("facade EXPLAIN plan matrix case should succeed");
            assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
        }
    }

    #[test]
    fn facade_explain_sql_execution_matrix_queries_include_expected_tokens() {
        let session = fresh_facade_session();

        // Phase 1: define EXPLAIN EXECUTION SQL matrix cases.
        let cases = vec![
            (
                "EXPLAIN EXECUTION SELECT * FROM FacadeSqlEntity ORDER BY age LIMIT 1",
                vec!["node_id=0", "layer="],
            ),
            (
                "EXPLAIN EXECUTION SELECT age, COUNT(*) \
                 FROM FacadeSqlEntity \
                 GROUP BY age \
                 ORDER BY age ASC LIMIT 10",
                vec!["node_id=0", "execution_mode="],
            ),
            (
                "EXPLAIN EXECUTION SELECT COUNT(*) FROM FacadeSqlEntity",
                vec!["AggregateCount execution_mode=", "node_id=0"],
            ),
        ];

        // Phase 2: execute each EXPLAIN EXECUTION case and assert stable tokens.
        for (sql, tokens) in cases {
            let explain = session
                .explain_sql::<FacadeSqlEntity>(sql)
                .expect("facade EXPLAIN EXECUTION matrix case should succeed");
            assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
        }
    }

    #[test]
    fn facade_explain_sql_json_matrix_queries_include_expected_tokens() {
        let session = fresh_facade_session();

        // Phase 1: define EXPLAIN JSON SQL matrix cases.
        let cases = vec![
            (
                "EXPLAIN JSON SELECT * FROM FacadeSqlEntity ORDER BY age LIMIT 1",
                vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
            ),
            (
                "EXPLAIN JSON DELETE FROM FacadeSqlEntity ORDER BY age LIMIT 1",
                vec!["\"mode\":{\"type\":\"Delete\"", "\"access\":"],
            ),
            (
                "EXPLAIN JSON SELECT age, COUNT(*) \
                 FROM FacadeSqlEntity \
                 GROUP BY age \
                 ORDER BY age ASC LIMIT 10",
                vec!["\"mode\":{\"type\":\"Load\"", "\"grouping\""],
            ),
            (
                "EXPLAIN JSON SELECT COUNT(*) FROM FacadeSqlEntity",
                vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
            ),
        ];

        // Phase 2: execute each EXPLAIN JSON case and assert stable tokens.
        for (sql, tokens) in cases {
            let explain = session
                .explain_sql::<FacadeSqlEntity>(sql)
                .expect("facade EXPLAIN JSON matrix case should succeed");
            assert!(
                explain.starts_with('{') && explain.ends_with('}'),
                "facade EXPLAIN JSON matrix output should be one JSON object payload: {sql}",
            );
            assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
        }
    }

    #[test]
    fn facade_query_from_sql_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            assert_unsupported_sql_runtime_result(
                session.query_from_sql::<FacadeSqlEntity>(sql),
                "facade query_from_sql",
            );
        }
    }

    #[test]
    fn facade_query_from_sql_rejects_non_query_statement_lanes_matrix() {
        let session = fresh_facade_session();

        // Phase 1: define statement lanes that must stay outside query_from_sql.
        let cases = [
            (
                "EXPLAIN SELECT * FROM FacadeSqlEntity",
                "facade query_from_sql EXPLAIN",
            ),
            ("DESCRIBE FacadeSqlEntity", "facade query_from_sql DESCRIBE"),
            (
                "SHOW INDEXES FacadeSqlEntity",
                "facade query_from_sql SHOW INDEXES",
            ),
            (
                "SHOW COLUMNS FacadeSqlEntity",
                "facade query_from_sql SHOW COLUMNS",
            ),
            ("SHOW ENTITIES", "facade query_from_sql SHOW ENTITIES"),
        ];

        // Phase 2: assert each lane remains fail-closed through unsupported runtime errors.
        for (sql, context) in cases {
            assert_unsupported_sql_runtime_result(
                session.query_from_sql::<FacadeSqlEntity>(sql),
                context,
            );
        }
    }

    #[test]
    fn facade_execute_sql_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            assert_unsupported_sql_runtime_result(
                session.execute_sql::<FacadeSqlEntity>(sql),
                "facade execute_sql",
            );
        }
    }

    #[test]
    fn facade_execute_sql_projection_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            assert_unsupported_sql_runtime_result(
                session.execute_sql_projection::<FacadeSqlEntity>(sql),
                "facade execute_sql_projection",
            );
        }
    }

    #[test]
    fn facade_execute_sql_grouped_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            assert_unsupported_sql_runtime_result(
                session.execute_sql_grouped::<FacadeSqlEntity>(sql, None),
                "facade execute_sql_grouped",
            );
        }
    }

    #[test]
    fn facade_execute_sql_aggregate_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            assert_unsupported_sql_runtime_result(
                session.execute_sql_aggregate::<FacadeSqlEntity>(sql),
                "facade execute_sql_aggregate",
            );
        }
    }

    #[test]
    fn facade_explain_sql_preserves_unsupported_runtime_contract() {
        let session = fresh_facade_session();

        for (sql, _feature) in unsupported_sql_feature_cases() {
            let explain_sql = format!("EXPLAIN {sql}");
            assert_unsupported_sql_runtime_result(
                session.explain_sql::<FacadeSqlEntity>(explain_sql.as_str()),
                "facade explain_sql",
            );
        }
    }

    #[test]
    fn facade_explain_sql_rejects_non_explain_statement_lanes_matrix() {
        let session = fresh_facade_session();

        // Phase 1: define statement lanes that must stay outside explain_sql.
        let cases = [
            ("DESCRIBE FacadeSqlEntity", "facade explain_sql DESCRIBE"),
            (
                "SHOW INDEXES FacadeSqlEntity",
                "facade explain_sql SHOW INDEXES",
            ),
            (
                "SHOW COLUMNS FacadeSqlEntity",
                "facade explain_sql SHOW COLUMNS",
            ),
            ("SHOW ENTITIES", "facade explain_sql SHOW ENTITIES"),
        ];

        // Phase 2: assert each lane remains fail-closed through unsupported runtime errors.
        for (sql, context) in cases {
            assert_unsupported_sql_runtime_result(
                session.explain_sql::<FacadeSqlEntity>(sql),
                context,
            );
        }
    }

    #[test]
    fn facade_introspection_sql_surfaces_reject_wrong_lanes_matrix() {
        let session = fresh_facade_session();

        // Phase 1: define wrong-lane cases for each introspection surface.
        let describe_cases = [
            (
                "SELECT * FROM FacadeSqlEntity",
                "facade describe_sql SELECT",
            ),
            (
                "SHOW INDEXES FacadeSqlEntity",
                "facade describe_sql SHOW INDEXES",
            ),
            (
                "SHOW COLUMNS FacadeSqlEntity",
                "facade describe_sql SHOW COLUMNS",
            ),
            ("SHOW ENTITIES", "facade describe_sql SHOW ENTITIES"),
        ];
        let show_indexes_cases = [
            (
                "SELECT * FROM FacadeSqlEntity",
                "facade show_indexes_sql SELECT",
            ),
            (
                "DESCRIBE FacadeSqlEntity",
                "facade show_indexes_sql DESCRIBE",
            ),
            (
                "SHOW COLUMNS FacadeSqlEntity",
                "facade show_indexes_sql SHOW COLUMNS",
            ),
            ("SHOW ENTITIES", "facade show_indexes_sql SHOW ENTITIES"),
        ];
        let show_columns_cases = [
            (
                "SELECT * FROM FacadeSqlEntity",
                "facade show_columns_sql SELECT",
            ),
            (
                "DESCRIBE FacadeSqlEntity",
                "facade show_columns_sql DESCRIBE",
            ),
            (
                "SHOW INDEXES FacadeSqlEntity",
                "facade show_columns_sql SHOW INDEXES",
            ),
            ("SHOW ENTITIES", "facade show_columns_sql SHOW ENTITIES"),
        ];
        let show_entities_cases = [
            (
                "SELECT * FROM FacadeSqlEntity",
                "facade show_entities_sql SELECT",
            ),
            (
                "DESCRIBE FacadeSqlEntity",
                "facade show_entities_sql DESCRIBE",
            ),
            (
                "SHOW INDEXES FacadeSqlEntity",
                "facade show_entities_sql SHOW INDEXES",
            ),
            (
                "SHOW COLUMNS FacadeSqlEntity",
                "facade show_entities_sql SHOW COLUMNS",
            ),
        ];

        // Phase 2: assert each introspection surface stays fail-closed for wrong lanes.
        for (sql, context) in describe_cases {
            assert_unsupported_sql_runtime_result(
                session.describe_sql::<FacadeSqlEntity>(sql),
                context,
            );
        }
        for (sql, context) in show_indexes_cases {
            assert_unsupported_sql_runtime_result(
                session.show_indexes_sql::<FacadeSqlEntity>(sql),
                context,
            );
        }
        for (sql, context) in show_columns_cases {
            assert_unsupported_sql_runtime_result(
                session.show_columns_sql::<FacadeSqlEntity>(sql),
                context,
            );
        }
        for (sql, context) in show_entities_cases {
            assert_unsupported_sql_runtime_result(session.show_entities_sql(sql), context);
        }
    }
}
