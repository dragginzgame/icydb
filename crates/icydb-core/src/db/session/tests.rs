use super::*;
use crate::{
    db::{
        Db, ProjectionResponse,
        commit::{ensure_recovered, init_commit_store_for_tests},
        cursor::CursorPlanError,
        data::DataStore,
        index::IndexStore,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{Expr, ProjectionField},
        registry::StoreRegistry,
    },
    error::{ErrorClass, ErrorDetail, ErrorOrigin, QueryErrorDetail},
    model::field::FieldKind,
    testing::test_memory,
    traits::Path,
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

crate::test_canister! {
    ident = SessionSqlCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = SessionSqlStore,
    canister = SessionSqlCanister,
}

thread_local! {
    static SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(160)));
    static SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(161)));
    static SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            SessionSqlStore::PATH,
            &SESSION_SQL_DATA_STORE,
            &SESSION_SQL_INDEX_STORE,
        )
        .expect("SQL session test store registration should succeed");
        reg
    };
}

static SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new(&SESSION_SQL_STORE_REGISTRY);

///
/// SessionSqlEntity
///
/// Test entity used to lock end-to-end reduced SQL session behavior.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct SessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

crate::test_entity_schema! {
    ident = SessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionSqlEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

// Reset all session SQL fixture state between tests to preserve deterministic assertions.
fn reset_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&SESSION_SQL_DB).expect("write-side recovery should succeed");
    SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
    SESSION_SQL_INDEX_STORE.with(|store| store.borrow_mut().clear());
}

fn sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(SESSION_SQL_DB)
}

fn unsupported_sql_surface_query_error(message: &'static str) -> QueryError {
    QueryError::execute(crate::error::InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
        message,
    ))
}

///
/// SessionSqlLegacySurfaceExt
///
/// Test-only compatibility adapters that map removed lane-specific SQL
/// entrypoints to the unified SQL dispatch surface.
///

trait SessionSqlLegacySurfaceExt {
    fn sql_projection_columns<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn execute_sql_projection<E>(&self, sql: &str) -> Result<ProjectionResponse<E>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn explain_sql<E>(&self, sql: &str) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn describe_sql<E>(&self, sql: &str) -> Result<EntitySchemaDescription, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn show_indexes_sql<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn show_columns_sql<E>(&self, sql: &str) -> Result<Vec<EntityFieldDescription>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue;

    fn show_entities_sql(&self, sql: &str) -> Result<Vec<String>, QueryError>;
}

impl SessionSqlLegacySurfaceExt for DbSession<SessionSqlCanister> {
    fn sql_projection_columns<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::Projection { columns, .. } => Ok(columns),
            SqlDispatchResult::Explain(_)
            | SqlDispatchResult::Describe(_)
            | SqlDispatchResult::ShowIndexes(_)
            | SqlDispatchResult::ShowColumns(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "sql_projection_columns only supports SELECT statements",
            )),
        }
    }

    fn execute_sql_projection<E>(&self, sql: &str) -> Result<ProjectionResponse<E>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::Projection { projection, .. } => Ok(projection),
            SqlDispatchResult::Explain(_)
            | SqlDispatchResult::Describe(_)
            | SqlDispatchResult::ShowIndexes(_)
            | SqlDispatchResult::ShowColumns(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "execute_sql_projection only supports SELECT statements",
            )),
        }
    }

    fn explain_sql<E>(&self, sql: &str) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::Explain(explain) => Ok(explain),
            SqlDispatchResult::Projection { .. }
            | SqlDispatchResult::Describe(_)
            | SqlDispatchResult::ShowIndexes(_)
            | SqlDispatchResult::ShowColumns(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "explain_sql requires an EXPLAIN statement",
            )),
        }
    }

    fn describe_sql<E>(&self, sql: &str) -> Result<EntitySchemaDescription, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::Describe(description) => Ok(description),
            SqlDispatchResult::Projection { .. }
            | SqlDispatchResult::Explain(_)
            | SqlDispatchResult::ShowIndexes(_)
            | SqlDispatchResult::ShowColumns(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "describe_sql requires a DESCRIBE statement",
            )),
        }
    }

    fn show_indexes_sql<E>(&self, sql: &str) -> Result<Vec<String>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::ShowIndexes(indexes) => Ok(indexes),
            SqlDispatchResult::Projection { .. }
            | SqlDispatchResult::Explain(_)
            | SqlDispatchResult::Describe(_)
            | SqlDispatchResult::ShowColumns(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "show_indexes_sql requires a SHOW INDEXES statement",
            )),
        }
    }

    fn show_columns_sql<E>(&self, sql: &str) -> Result<Vec<EntityFieldDescription>, QueryError>
    where
        E: EntityKind<Canister = SessionSqlCanister> + EntityValue,
    {
        let payload = self.execute_sql_dispatch::<E>(sql)?;

        match payload {
            SqlDispatchResult::ShowColumns(columns) => Ok(columns),
            SqlDispatchResult::Projection { .. }
            | SqlDispatchResult::Explain(_)
            | SqlDispatchResult::Describe(_)
            | SqlDispatchResult::ShowIndexes(_)
            | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_surface_query_error(
                "show_columns_sql requires a SHOW COLUMNS statement",
            )),
        }
    }

    fn show_entities_sql(&self, sql: &str) -> Result<Vec<String>, QueryError> {
        let route = self.sql_statement_route(sql)?;
        if !route.is_show_entities() {
            return Err(unsupported_sql_surface_query_error(
                "show_entities_sql requires a SHOW ENTITIES or SHOW TABLES statement",
            ));
        }

        Ok(self.show_entities())
    }
}

// Seed one deterministic SQL fixture dataset used by matrix tests.
fn seed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, u64)],
) {
    for (name, age) in rows {
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: (*name).to_string(),
                age: *age,
            })
            .expect("seed insert should succeed");
    }
}

// Execute one scalar SQL query and return `(name, age)` tuples in response order.
fn execute_sql_name_age_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Vec<(String, u64)> {
    session
        .execute_sql::<SessionSqlEntity>(sql)
        .expect("scalar SQL execution should succeed")
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect()
}

// Assert one explain payload contains every required token for one case.
fn assert_explain_contains_tokens(explain: &str, tokens: &[&str], context: &str) {
    for token in tokens {
        assert!(
            explain.contains(token),
            "explain matrix case missing token `{token}`: {context}",
        );
    }
}

// Assert query-surface cursor errors remain wrapped under QueryError::Plan(PlanError::Cursor).
fn assert_query_error_is_cursor_plan(
    err: QueryError,
    predicate: impl FnOnce(&CursorPlanError) -> bool,
) {
    assert!(matches!(
        err,
        QueryError::Plan(plan_err)
            if matches!(
                plan_err.as_ref(),
                PlanError::Cursor(inner) if predicate(inner.as_ref())
            )
    ));
}

// Assert both session conversion paths preserve the same cursor-plan variant payload.
fn assert_cursor_mapping_parity(
    build: impl Fn() -> CursorPlanError,
    predicate: impl Fn(&CursorPlanError) -> bool + Copy,
) {
    let mapped_via_executor = map_executor_plan_error(ExecutorPlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_executor, predicate);

    let mapped_via_plan = QueryError::from(PlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_plan, predicate);
}

// Assert SQL parser unsupported-feature labels remain preserved through
// query-facing execution error detail payloads.
fn assert_sql_unsupported_feature_detail(err: QueryError, expected_feature: &'static str) {
    let QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(internal)) =
        err
    else {
        panic!("expected query execution unsupported error variant");
    };

    assert_eq!(internal.class(), ErrorClass::Unsupported);
    assert_eq!(internal.origin(), ErrorOrigin::Query);
    assert!(
        matches!(
            internal.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                if *feature == expected_feature
        ),
        "unsupported SQL feature detail label should be preserved",
    );
}

// Assert one SQL surface result fails with the unsupported execution boundary.
fn assert_unsupported_sql_surface_result<T>(result: Result<T, QueryError>, context: &str) {
    let Err(err) = result else {
        panic!("{context}");
    };
    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported SQL surface case should map to unsupported execution class: {context}",
    );
}

fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 5] {
    [
        (
            "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            "JOIN",
        ),
        (
            "SELECT \"name\" FROM SessionSqlEntity",
            "quoted identifiers",
        ),
        ("SELECT * FROM SessionSqlEntity alias", "table aliases"),
        (
            "SELECT * FROM SessionSqlEntity WHERE name LIKE 'Al%'",
            "LIKE predicates beyond LOWER(field) LIKE 'prefix%'",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE '%Al'",
            "LOWER(field) LIKE patterns beyond trailing '%' prefix form",
        ),
    ]
}

#[test]
fn session_cursor_error_mapping_parity_boundary_arity() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_window_mismatch() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_window_mismatch(8, 3),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorWindowMismatch {
                    expected_offset: 8,
                    actual_offset: 3
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_decode_reason() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::invalid_continuation_cursor(
                crate::db::codec::cursor::CursorDecodeError::OddLength,
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::InvalidContinuationCursor {
                    reason: crate::db::codec::cursor::CursorDecodeError::OddLength
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_primary_key_type_mismatch() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                "id",
                "ulid",
                Some(crate::value::Value::Text("not-a-ulid".to_string())),
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    field,
                    expected,
                    value: Some(crate::value::Value::Text(value))
                } if field == "id" && expected == "ulid" && value == "not-a-ulid"
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_matrix_preserves_cursor_variants() {
    // Keep one matrix-level canary test name so cross-module audit references remain stable.
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}

#[test]
fn execute_sql_select_star_honors_order_limit_offset() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "older".to_string(),
            age: 37,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "younger".to_string(),
            age: 19,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 1",
        )
        .expect("SELECT * should execute");

    assert_eq!(response.count(), 1, "window should return one row");
    let row = response
        .iter()
        .next()
        .expect("windowed result should include one row");
    assert_eq!(
        row.entity_ref().name,
        "older",
        "ordered window should return the second age-ordered row",
    );
}

#[test]
fn execute_sql_scalar_matrix_queries_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows for scalar matrix cases.
    seed_session_sql_entities(
        &session,
        &[
            ("scalar-matrix-a", 10),
            ("scalar-matrix-b", 20),
            ("scalar-matrix-c", 30),
            ("scalar-matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven scalar SQL cases.
    let cases = vec![
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                ("scalar-matrix-c".to_string(), 30_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![
                ("scalar-matrix-b".to_string(), 20_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT DISTINCT * \
             FROM SessionSqlEntity \
             WHERE age >= 30 \
             ORDER BY age DESC",
            vec![
                ("scalar-matrix-d".to_string(), 40_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT * \
             FROM public.SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                ("scalar-matrix-a".to_string(), 10_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
    ];

    // Phase 3: assert scalar row payload order and values for each query.
    for (sql, expected_rows) in cases {
        let actual_rows = execute_sql_name_age_rows(&session, sql);
        assert_eq!(actual_rows, expected_rows, "scalar matrix case: {sql}");
    }
}

#[test]
fn execute_sql_delete_honors_predicate_order_and_limit() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "first-minor".to_string(),
            age: 16,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "second-minor".to_string(),
            age: 17,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "adult".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let deleted = session
        .execute_sql::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
        )
        .expect("DELETE should execute");

    assert_eq!(deleted.count(), 1, "delete limit should remove one row");
    assert_eq!(
        deleted
            .iter()
            .next()
            .expect("deleted row should exist")
            .entity_ref()
            .age,
        16,
        "ordered delete should remove the youngest matching row first",
    );

    let remaining = session
        .load::<SessionSqlEntity>()
        .order_by("age")
        .execute()
        .expect("post-delete load should succeed");
    let remaining_ages = remaining
        .iter()
        .map(|row| row.entity_ref().age)
        .collect::<Vec<_>>();

    assert_eq!(
        remaining_ages,
        vec![17, 42],
        "delete window semantics should preserve non-deleted rows",
    );
}

#[test]
fn execute_sql_delete_matrix_queries_match_deleted_and_remaining_rows() {
    // Phase 1: define one shared seed dataset and table-driven DELETE cases.
    let seed_rows = [
        ("delete-matrix-a", 10_u64),
        ("delete-matrix-b", 20_u64),
        ("delete-matrix-c", 30_u64),
        ("delete-matrix-d", 40_u64),
    ];
    let cases = vec![
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1",
            vec![("delete-matrix-b".to_string(), 20_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age DESC LIMIT 2",
            vec![
                ("delete-matrix-d".to_string(), 40_u64),
                ("delete-matrix-c".to_string(), 30_u64),
            ],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 100 \
             ORDER BY age ASC LIMIT 1",
            vec![],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
    ];

    // Phase 2: execute each DELETE case from a fresh seeded store.
    for (sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted = session
            .execute_sql::<SessionSqlEntity>(sql)
            .expect("delete matrix SQL execution should succeed");
        let deleted_rows = deleted
            .iter()
            .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
            .collect::<Vec<_>>();
        let remaining_rows =
            execute_sql_name_age_rows(&session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC");

        assert_eq!(
            deleted_rows, expected_deleted,
            "delete matrix deleted rows: {sql}"
        );
        assert_eq!(
            remaining_rows, expected_remaining,
            "delete matrix remaining rows: {sql}",
        );
    }
}

#[test]
fn query_from_sql_rejects_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define statement lanes that must stay outside query_from_sql.
    let cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "query_from_sql must reject EXPLAIN statements",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "query_from_sql must reject DESCRIBE statements",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "query_from_sql must reject SHOW INDEXES statements",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "query_from_sql must reject SHOW COLUMNS statements",
        ),
        (
            "SHOW ENTITIES",
            "query_from_sql must reject SHOW ENTITIES statements",
        ),
        (
            "SHOW TABLES",
            "query_from_sql must reject SHOW TABLES statements",
        ),
    ];

    // Phase 2: assert each lane remains fail-closed through unsupported execution.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(
            session.query_from_sql::<SessionSqlEntity>(sql),
            context,
        );
    }
}

#[test]
fn sql_statement_route_select_classifies_query_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1")
        .expect("select SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::Query {
            entity: "SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "SessionSqlEntity");
    assert!(!route.is_explain());
    assert!(!route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn sql_statement_route_describe_classifies_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("DESCRIBE public.SessionSqlEntity")
        .expect("describe SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::Describe {
            entity: "public.SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "public.SessionSqlEntity");
    assert!(!route.is_explain());
    assert!(route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn sql_statement_route_show_indexes_classifies_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SHOW INDEXES public.SessionSqlEntity")
        .expect("show indexes SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::ShowIndexes {
            entity: "public.SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "public.SessionSqlEntity");
    assert!(!route.is_explain());
    assert!(!route.is_describe());
    assert!(route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn sql_statement_route_show_columns_classifies_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SHOW COLUMNS public.SessionSqlEntity")
        .expect("show columns SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::ShowColumns {
            entity: "public.SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "public.SessionSqlEntity");
    assert!(!route.is_explain());
    assert!(!route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn sql_statement_route_show_entities_classifies_surface() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SHOW ENTITIES")
        .expect("show entities SQL statement should parse");

    assert_eq!(route, SqlStatementRoute::ShowEntities);
    assert!(route.is_show_entities());
    assert_eq!(route.entity(), "");
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_describe());
    assert!(!route.is_explain());
}

#[test]
fn sql_statement_route_show_tables_classifies_show_entities_surface() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SHOW TABLES")
        .expect("show tables SQL statement should parse");

    assert_eq!(route, SqlStatementRoute::ShowEntities);
    assert!(route.is_show_entities());
    assert_eq!(route.entity(), "");
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_describe());
    assert!(!route.is_explain());
}

#[test]
fn sql_statement_route_explain_classifies_wrapped_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("EXPLAIN JSON DELETE FROM SessionSqlEntity WHERE age > 20 LIMIT 1")
        .expect("explain SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::Explain {
            entity: "SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "SessionSqlEntity");
    assert!(route.is_explain());
    assert!(!route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn describe_sql_returns_same_payload_as_describe_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql = session
        .describe_sql::<SessionSqlEntity>("DESCRIBE SessionSqlEntity")
        .expect("describe_sql should succeed");
    let from_typed = session.describe_entity::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "describe_sql should project through canonical describe_entity payload",
    );
}

#[test]
fn describe_sql_rejects_non_describe_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside describe_sql.
    let cases = [
        (
            "SELECT * FROM SessionSqlEntity",
            "describe_sql should reject SELECT statements",
        ),
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "describe_sql should reject EXPLAIN statements",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "describe_sql should reject SHOW INDEXES statements",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "describe_sql should reject SHOW COLUMNS statements",
        ),
        (
            "SHOW ENTITIES",
            "describe_sql should reject SHOW ENTITIES statements",
        ),
        (
            "SHOW TABLES",
            "describe_sql should reject SHOW TABLES statements",
        ),
    ];

    // Phase 2: assert each non-describe lane remains fail-closed.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(
            session.describe_sql::<SessionSqlEntity>(sql),
            context,
        );
    }
}

#[test]
fn show_indexes_sql_returns_same_payload_as_show_indexes() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql = session
        .show_indexes_sql::<SessionSqlEntity>("SHOW INDEXES SessionSqlEntity")
        .expect("show_indexes_sql should succeed");
    let from_typed = session.show_indexes::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "show_indexes_sql should project through canonical show_indexes payload",
    );
}

#[test]
fn show_indexes_sql_rejects_non_show_indexes_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_indexes_sql.
    let cases = [
        (
            "SELECT * FROM SessionSqlEntity",
            "show_indexes_sql should reject SELECT statements",
        ),
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "show_indexes_sql should reject EXPLAIN statements",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "show_indexes_sql should reject DESCRIBE statements",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "show_indexes_sql should reject SHOW COLUMNS statements",
        ),
        (
            "SHOW ENTITIES",
            "show_indexes_sql should reject SHOW ENTITIES statements",
        ),
        (
            "SHOW TABLES",
            "show_indexes_sql should reject SHOW TABLES statements",
        ),
    ];

    // Phase 2: assert each non-show-indexes lane remains fail-closed.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(
            session.show_indexes_sql::<SessionSqlEntity>(sql),
            context,
        );
    }
}

#[test]
fn show_columns_sql_returns_same_payload_as_show_columns() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql = session
        .show_columns_sql::<SessionSqlEntity>("SHOW COLUMNS SessionSqlEntity")
        .expect("show_columns_sql should succeed");
    let from_typed = session.show_columns::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "show_columns_sql should project through canonical show_columns payload",
    );
}

#[test]
fn show_columns_sql_rejects_non_show_columns_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_columns_sql.
    let cases = [
        (
            "SELECT * FROM SessionSqlEntity",
            "show_columns_sql should reject SELECT statements",
        ),
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "show_columns_sql should reject EXPLAIN statements",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "show_columns_sql should reject DESCRIBE statements",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "show_columns_sql should reject SHOW INDEXES statements",
        ),
        (
            "SHOW ENTITIES",
            "show_columns_sql should reject SHOW ENTITIES statements",
        ),
        (
            "SHOW TABLES",
            "show_columns_sql should reject SHOW TABLES statements",
        ),
    ];

    // Phase 2: assert each non-show-columns lane remains fail-closed.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(
            session.show_columns_sql::<SessionSqlEntity>(sql),
            context,
        );
    }
}

#[test]
fn show_entities_sql_returns_runtime_entity_names() {
    reset_session_sql_store();
    let session = sql_session();

    let entities = session
        .show_entities_sql("SHOW ENTITIES")
        .expect("show_entities_sql should succeed");

    assert_eq!(
        entities,
        session.show_entities(),
        "show_entities_sql should project through canonical show_entities payload",
    );
}

#[test]
fn show_entities_sql_show_tables_alias_returns_runtime_entity_names() {
    reset_session_sql_store();
    let session = sql_session();

    let entities = session
        .show_entities_sql("SHOW TABLES")
        .expect("show_entities_sql SHOW TABLES alias should succeed");

    assert_eq!(
        entities,
        session.show_entities(),
        "show_entities_sql SHOW TABLES alias should project through canonical show_entities payload",
    );
}

#[test]
fn show_entities_sql_rejects_non_show_entities_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_entities_sql.
    let cases = [
        (
            "SELECT * FROM SessionSqlEntity",
            "show_entities_sql should reject SELECT statements",
        ),
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "show_entities_sql should reject EXPLAIN statements",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "show_entities_sql should reject DESCRIBE statements",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "show_entities_sql should reject SHOW INDEXES statements",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "show_entities_sql should reject SHOW COLUMNS statements",
        ),
    ];

    // Phase 2: assert each non-show-entities lane remains fail-closed.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(session.show_entities_sql(sql), context);
    }
}

#[test]
fn explain_sql_rejects_non_explain_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside explain_sql.
    let cases = [
        (
            "DESCRIBE SessionSqlEntity",
            "explain_sql should reject DESCRIBE statements",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "explain_sql should reject SHOW INDEXES statements",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "explain_sql should reject SHOW COLUMNS statements",
        ),
        (
            "SHOW ENTITIES",
            "explain_sql should reject SHOW ENTITIES statements",
        ),
        (
            "SHOW TABLES",
            "explain_sql should reject SHOW TABLES statements",
        ),
    ];

    // Phase 2: assert each non-explain lane remains fail-closed.
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(
            session.explain_sql::<SessionSqlEntity>(sql),
            context,
        );
    }
}

#[test]
fn sql_statement_route_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .sql_statement_route(sql)
            .expect_err("unsupported SQL feature should fail through sql_statement_route");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn query_from_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .query_from_sql::<SessionSqlEntity>(sql)
            .expect_err("unsupported SQL feature should fail through query_from_sql");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn execute_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .execute_sql::<SessionSqlEntity>(sql)
            .expect_err("unsupported SQL feature should fail through execute_sql");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn execute_sql_projection_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .execute_sql_projection::<SessionSqlEntity>(sql)
            .expect_err("unsupported SQL feature should fail through execute_sql_projection");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn execute_sql_grouped_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
            .expect_err("unsupported SQL feature should fail through execute_sql_grouped");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn execute_sql_aggregate_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let err = session
            .execute_sql_aggregate::<SessionSqlEntity>(sql)
            .expect_err("unsupported SQL feature should fail through execute_sql_aggregate");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn explain_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature) in unsupported_sql_feature_cases() {
        let explain_sql = format!("EXPLAIN {sql}");
        let err = session
            .explain_sql::<SessionSqlEntity>(explain_sql.as_str())
            .expect_err("unsupported SQL feature should fail through explain_sql");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn query_from_sql_select_field_projection_lowers_to_scalar_field_selection() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>("SELECT name, age FROM SessionSqlEntity")
        .expect("field-list SQL query should lower");
    let projection = query
        .plan()
        .expect("field-list SQL plan should build")
        .projection_spec();
    let field_names = projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("field-list SQL projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn query_from_sql_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("grouped aggregate projection SQL query should lower");
    assert!(
        query.has_grouping(),
        "grouped aggregate SQL projection lowering should produce grouped query intent",
    );
}

#[test]
fn query_from_sql_lower_like_prefix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE 'Al%'",
        )
        .expect("LOWER(field) LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn execute_sql_select_field_projection_currently_returns_entity_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projected-row".to_string(),
            age: 29,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("field-list SQL projection should execute");
    let row = response
        .iter()
        .next()
        .expect("field-list SQL projection response should contain one row");

    assert_eq!(
        row.entity_ref().name,
        "projected-row",
        "field-list SQL projection should still return entity rows in this baseline",
    );
    assert_eq!(
        row.entity_ref().age,
        29,
        "field-list SQL projection should preserve full entity payload until projection response shaping is introduced",
    );
}

#[test]
fn sql_projection_columns_select_field_list_returns_canonical_labels() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = session
        .sql_projection_columns::<SessionSqlEntity>("SELECT name, age FROM SessionSqlEntity")
        .expect("field-list SQL projection columns should derive");

    assert_eq!(columns, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn sql_projection_columns_select_star_returns_entity_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = session
        .sql_projection_columns::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity")
        .expect("star SQL projection columns should derive");

    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "age".to_string()]
    );
}

#[test]
fn sql_projection_columns_rejects_delete_statements() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .sql_projection_columns::<SessionSqlEntity>("DELETE FROM SessionSqlEntity WHERE age > 10")
        .expect_err("delete SQL should be rejected for projection-column derivation");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "projection column derivation should reject non-SELECT SQL",
    );
}

#[test]
fn execute_sql_projection_select_field_list_returns_projection_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-surface".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("projection SQL execution should succeed");
    let row = response
        .iter()
        .next()
        .expect("projection SQL response should contain one row");

    assert_eq!(response.count(), 1);
    assert_eq!(
        row.values(),
        [Value::Text("projection-surface".to_string())],
        "projection SQL response should carry only projected field values in declaration order",
    );
}

#[test]
fn execute_sql_projection_select_star_returns_all_fields_in_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-star".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("projection SQL star execution should succeed");
    let row = response
        .iter()
        .next()
        .expect("projection SQL star response should contain one row");

    assert_eq!(response.count(), 1);
    assert_eq!(
        row.values().len(),
        3,
        "SELECT * projection response should include all model fields",
    );
    assert_eq!(row.values()[0], Value::Ulid(row.id().key()));
    assert_eq!(row.values()[1], Value::Text("projection-star".to_string()));
    assert_eq!(row.values()[2], Value::Uint(41));
}

#[test]
fn execute_sql_select_schema_qualified_entity_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "schema-qualified".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT * FROM public.SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("schema-qualified entity SQL should execute");

    assert_eq!(response.len(), 1);
}

#[test]
fn execute_sql_projection_select_table_qualified_fields_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-projection".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 40 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
        )
        .expect("table-qualified projection SQL should execute");
    let row = response
        .iter()
        .next()
        .expect("table-qualified projection SQL response should contain one row");

    assert_eq!(response.count(), 1);
    assert_eq!(
        row.values(),
        [Value::Text("qualified-projection".to_string())]
    );
}

#[test]
fn execute_sql_projection_select_field_list_honors_order_limit_offset_window() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic age-ordered rows.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-c".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-d".to_string(),
            age: 40,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute one projection query with explicit window controls.
    let response = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT name, age \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("projection SQL window execution should succeed");
    let rows = response.iter().collect::<Vec<_>>();

    // Phase 3: assert projected row payloads follow ordered window semantics.
    assert_eq!(response.count(), 2);
    assert_eq!(
        rows[0].values(),
        [
            Value::Text("projection-window-c".to_string()),
            Value::Uint(30)
        ],
    );
    assert_eq!(
        rows[1].values(),
        [
            Value::Text("projection-window-b".to_string()),
            Value::Uint(20)
        ],
    );
}

#[test]
fn execute_sql_projection_rejects_delete_statements() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_projection::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
        )
        .expect_err("projection SQL execution should reject delete statements");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "projection SQL delete usage should fail as unsupported",
    );
}

#[test]
fn execute_sql_select_field_projection_unknown_field_fails_with_plan_error() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT missing_field FROM SessionSqlEntity")
        .expect_err("unknown projected fields should fail planner validation");

    assert!(
        matches!(err, QueryError::Plan(_)),
        "unknown projected fields should surface planner-domain query errors: {err:?}",
    );
}

#[test]
fn execute_sql_rejects_aggregate_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect_err("global aggregate SQL projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL projection should fail at reduced lowering boundary",
    );
}

#[test]
fn execute_sql_rejects_table_alias_forms_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity alias")
        .expect_err("table aliases should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "table alias usage should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT \"name\" FROM SessionSqlEntity")
        .expect_err("quoted identifiers should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "quoted identifiers should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn execute_sql_select_distinct_star_executes() {
    reset_session_sql_store();
    let session = sql_session();

    let id_a = Ulid::generate();
    let id_b = Ulid::generate();
    session
        .insert(SessionSqlEntity {
            id: id_a,
            name: "distinct-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: id_b,
            name: "distinct-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>("SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC")
        .expect("SELECT DISTINCT * should execute");
    assert_eq!(response.len(), 2);
}

#[test]
fn execute_sql_projection_select_distinct_with_pk_field_list_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "distinct-pk-a".to_string(),
            age: 25,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "distinct-pk-b".to_string(),
            age: 25,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT DISTINCT id, age FROM SessionSqlEntity ORDER BY id ASC",
        )
        .expect("SELECT DISTINCT field-list with PK should execute");
    assert_eq!(response.len(), 2);
    assert_eq!(response[0].values().len(), 2);
}

#[test]
fn execute_sql_rejects_distinct_without_pk_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT DISTINCT age FROM SessionSqlEntity")
        .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "distinct SQL gating should map to unsupported execution error boundary",
    );
}

#[test]
fn execute_sql_aggregate_count_star_and_count_field_return_uint() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-b".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let count_rows = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("COUNT(*) SQL aggregate should execute");
    let count_field = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT COUNT(age) FROM SessionSqlEntity")
        .expect("COUNT(field) SQL aggregate should execute");
    assert_eq!(count_rows, Value::Uint(2));
    assert_eq!(count_field, Value::Uint(2));
}

#[test]
fn execute_sql_aggregate_sum_with_table_qualified_field_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-aggregate-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-aggregate-b".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(SessionSqlEntity.age) FROM SessionSqlEntity",
        )
        .expect("table-qualified aggregate SQL should execute");

    assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(52u64)));
}

#[test]
fn execute_sql_aggregate_rejects_distinct_aggregate_qualifier() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT COUNT(DISTINCT age) FROM SessionSqlEntity",
        )
        .expect_err("aggregate DISTINCT qualifier should remain unsupported");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "aggregate DISTINCT qualifier should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn execute_sql_aggregate_sum_avg_min_max_return_expected_values() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "sumavg-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "sumavg-b".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT SUM(age) FROM SessionSqlEntity")
        .expect("SUM(field) SQL aggregate should execute");
    let avg = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT AVG(age) FROM SessionSqlEntity")
        .expect("AVG(field) SQL aggregate should execute");
    let min = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT MIN(age) FROM SessionSqlEntity")
        .expect("MIN(field) SQL aggregate should execute");
    let max = session
        .execute_sql_aggregate::<SessionSqlEntity>("SELECT MAX(age) FROM SessionSqlEntity")
        .expect("MAX(field) SQL aggregate should execute");
    let empty_sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(age) FROM SessionSqlEntity WHERE age < 0",
        )
        .expect("SUM(field) SQL aggregate empty-window execution should succeed");
    let empty_min = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MIN(age) FROM SessionSqlEntity WHERE age < 0",
        )
        .expect("MIN(field) SQL aggregate empty-window execution should succeed");
    let empty_max = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MAX(age) FROM SessionSqlEntity WHERE age < 0",
        )
        .expect("MAX(field) SQL aggregate empty-window execution should succeed");

    assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(52u64)));
    assert_eq!(avg, Value::Decimal(crate::types::Decimal::from(26u64)));
    assert_eq!(min, Value::Uint(20));
    assert_eq!(max, Value::Uint(32));
    assert_eq!(empty_sum, Value::Null);
    assert_eq!(empty_min, Value::Null);
    assert_eq!(empty_max, Value::Null);
}

#[test]
fn execute_sql_aggregate_honors_order_limit_offset_window() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-c".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");

    let count = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("COUNT(*) SQL aggregate window execution should succeed");
    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 1 OFFSET 1",
        )
        .expect("SUM(field) SQL aggregate window execution should succeed");
    let avg = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2 OFFSET 1",
        )
        .expect("AVG(field) SQL aggregate window execution should succeed");

    assert_eq!(count, Value::Uint(2));
    assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(20u64)));
    assert_eq!(avg, Value::Decimal(crate::types::Decimal::from(25u64)));
}

#[test]
fn execute_sql_aggregate_offset_beyond_window_returns_empty_aggregate_semantics() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed a small scalar window.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "beyond-window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "beyond-window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute aggregates where OFFSET removes all visible rows.
    let count = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("COUNT(*) aggregate with offset beyond window should execute");
    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("SUM aggregate with offset beyond window should execute");
    let avg = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("AVG aggregate with offset beyond window should execute");
    let min = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MIN(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("MIN aggregate with offset beyond window should execute");
    let max = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MAX(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("MAX aggregate with offset beyond window should execute");

    // Phase 3: assert empty-window aggregate semantics.
    assert_eq!(count, Value::Uint(0));
    assert_eq!(sum, Value::Null);
    assert_eq!(avg, Value::Null);
    assert_eq!(min, Value::Null);
    assert_eq!(max, Value::Null);
}

#[test]
fn execute_sql_projection_matrix_queries_match_expected_projected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by matrix projections.
    seed_session_sql_entities(
        &session,
        &[
            ("matrix-a", 10),
            ("matrix-b", 20),
            ("matrix-c", 30),
            ("matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven projection SQL cases.
    let cases = vec![
        (
            "SELECT name, age \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                vec![Value::Text("matrix-c".to_string()), Value::Uint(30)],
                vec![Value::Text("matrix-b".to_string()), Value::Uint(20)],
            ],
        ),
        (
            "SELECT age \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![vec![Value::Uint(20)], vec![Value::Uint(30)]],
        ),
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                vec![Value::Text("matrix-a".to_string())],
                vec![Value::Text("matrix-b".to_string())],
            ],
        ),
    ];

    // Phase 3: assert projected row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let response = session
            .execute_sql_projection::<SessionSqlEntity>(sql)
            .expect("projection matrix SQL execution should succeed");
        let actual_rows = response
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(actual_rows, expected_rows, "projection matrix case: {sql}");
    }
}

#[test]
fn execute_sql_grouped_matrix_queries_match_expected_grouped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by grouped matrix queries.
    seed_session_sql_entities(
        &session,
        &[
            ("group-matrix-a", 10),
            ("group-matrix-b", 10),
            ("group-matrix-c", 20),
            ("group-matrix-d", 30),
            ("group-matrix-e", 30),
            ("group-matrix-f", 30),
        ],
    );

    // Phase 2: execute table-driven grouped SQL cases.
    let cases = vec![
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) > 1 \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NULL \
             ORDER BY age ASC LIMIT 10",
            vec![],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NOT NULL \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
    ];

    // Phase 3: assert grouped row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
            .expect("grouped matrix SQL execution should succeed");
        let actual_rows = execution
            .rows()
            .iter()
            .map(|row| {
                (
                    row.group_key()[0].clone(),
                    row.aggregate_values()[0].clone(),
                )
            })
            .collect::<Vec<_>>();
        let expected_values = expected_rows
            .iter()
            .map(|(group_key, count)| (Value::Uint(*group_key), Value::Uint(*count)))
            .collect::<Vec<_>>();

        assert!(
            execution.continuation_cursor().is_none(),
            "grouped matrix cases should fully materialize under LIMIT 10: {sql}",
        );
        assert_eq!(actual_rows, expected_values, "grouped matrix case: {sql}");
    }
}

#[test]
fn execute_sql_aggregate_matrix_queries_match_expected_values() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by aggregate matrix queries.
    seed_session_sql_entities(
        &session,
        &[
            ("agg-matrix-a", 10),
            ("agg-matrix-b", 10),
            ("agg-matrix-c", 20),
            ("agg-matrix-d", 30),
            ("agg-matrix-e", 30),
            ("agg-matrix-f", 30),
        ],
    );

    // Phase 2: execute table-driven aggregate SQL cases.
    let cases = vec![
        ("SELECT COUNT(*) FROM SessionSqlEntity", Value::Uint(6)),
        (
            "SELECT SUM(age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(130_u64)),
        ),
        (
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2",
            Value::Decimal(crate::types::Decimal::from(30_u64)),
        ),
        (
            "SELECT MIN(age) FROM SessionSqlEntity WHERE age >= 20",
            Value::Uint(20),
        ),
        (
            "SELECT MAX(age) FROM SessionSqlEntity WHERE age <= 20",
            Value::Uint(20),
        ),
        (
            "SELECT COUNT(*) FROM SessionSqlEntity WHERE age < 0",
            Value::Uint(0),
        ),
        (
            "SELECT SUM(age) FROM SessionSqlEntity WHERE age < 0",
            Value::Null,
        ),
    ];

    // Phase 3: assert aggregate outputs for each SQL input.
    for (sql, expected_value) in cases {
        let actual_value = session
            .execute_sql_aggregate::<SessionSqlEntity>(sql)
            .expect("aggregate matrix SQL execution should succeed");

        assert_eq!(actual_value, expected_value, "aggregate matrix case: {sql}");
    }
}

#[test]
fn execute_sql_aggregate_rejects_unsupported_aggregate_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "SELECT age FROM SessionSqlEntity",
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    ] {
        let err = session
            .execute_sql_aggregate::<SessionSqlEntity>(sql)
            .expect_err("unsupported SQL aggregate shape should fail closed");
        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "unsupported SQL aggregate shape should map to unsupported execution error boundary: {sql}",
        );
    }
}

#[test]
fn execute_sql_aggregate_rejects_unknown_target_field() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(missing_field) FROM SessionSqlEntity",
        )
        .expect_err("unknown aggregate target field should fail");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unknown aggregate target field should map to unsupported execution error boundary",
    );
}

#[test]
fn execute_sql_projection_rejects_grouped_aggregate_sql() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_projection::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect_err("projection SQL API should reject grouped aggregate SQL intent");

    assert!(
        matches!(
            err,
            QueryError::Intent(
                crate::db::query::intent::IntentError::GroupedRequiresExecuteGrouped
            )
        ),
        "projection SQL API must reject grouped aggregate SQL with grouped-intent routing error",
    );
}

#[test]
fn execute_sql_grouped_select_count_returns_grouped_aggregate_row() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-c".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let execution = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
            None,
        )
        .expect("grouped SQL aggregate execution should succeed");

    assert!(
        execution.continuation_cursor().is_none(),
        "single-page grouped aggregate execution should not emit continuation cursor",
    );
    assert_eq!(execution.rows().len(), 2);
    assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
    assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
    assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
}

#[test]
fn execute_sql_grouped_select_count_with_qualified_identifiers_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-c".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let execution = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            None,
        )
        .expect("qualified grouped SQL aggregate execution should succeed");

    assert!(execution.continuation_cursor().is_none());
    assert_eq!(execution.rows().len(), 2);
    assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
    assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
    assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
}

#[test]
fn execute_sql_grouped_limit_window_emits_cursor_and_resumes_next_group_page() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed three grouped-key buckets with deterministic counts.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-b".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-c".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-d".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-e".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-f".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute the first grouped page and capture continuation cursor.
    let sql = "SELECT age, COUNT(*) \
               FROM SessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC LIMIT 1";
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, None)
        .expect("first grouped SQL page should execute");
    assert_eq!(first_page.rows().len(), 1);
    assert_eq!(first_page.rows()[0].group_key(), [Value::Uint(10)]);
    assert_eq!(first_page.rows()[0].aggregate_values(), [Value::Uint(2)]);
    let cursor_one = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 3: resume to second grouped page and capture next cursor.
    let second_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_one.as_str()))
        .expect("second grouped SQL page should execute");
    assert_eq!(second_page.rows().len(), 1);
    assert_eq!(second_page.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(second_page.rows()[0].aggregate_values(), [Value::Uint(1)]);
    let cursor_two = crate::db::encode_cursor(
        second_page
            .continuation_cursor()
            .expect("second grouped SQL page should emit continuation cursor"),
    );

    // Phase 4: resume final grouped page and assert no further continuation.
    let third_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_two.as_str()))
        .expect("third grouped SQL page should execute");
    assert_eq!(third_page.rows().len(), 1);
    assert_eq!(third_page.rows()[0].group_key(), [Value::Uint(30)]);
    assert_eq!(third_page.rows()[0].aggregate_values(), [Value::Uint(3)]);
    assert!(
        third_page.continuation_cursor().is_none(),
        "last grouped SQL page should not emit continuation cursor",
    );
}

#[test]
fn execute_sql_grouped_rejects_invalid_cursor_token_payload() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: execute one grouped query with an invalid cursor token payload.
    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 1",
            Some("zz"),
        )
        .expect_err("grouped SQL should fail closed on invalid cursor token payload");

    // Phase 2: assert decode failures stay in cursor-plan error taxonomy.
    assert_query_error_is_cursor_plan(err, |inner| {
        matches!(inner, CursorPlanError::InvalidContinuationCursor { .. })
    });
}

#[test]
fn execute_sql_grouped_rejects_cursor_token_from_different_query_signature() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed grouped buckets and capture one valid continuation cursor.
    seed_session_sql_entities(
        &session,
        &[
            ("cursor-signature-a", 10),
            ("cursor-signature-b", 20),
            ("cursor-signature-c", 30),
        ],
    );
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 1",
            None,
        )
        .expect("first grouped SQL page should execute");
    let cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 2: replay cursor against a signature-incompatible grouped SQL shape.
    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age DESC LIMIT 1",
            Some(cursor.as_str()),
        )
        .expect_err("grouped SQL should reject cursor tokens from incompatible query signatures");

    // Phase 3: assert mismatch stays in cursor-plan signature error taxonomy.
    assert_query_error_is_cursor_plan(err, |inner| {
        matches!(
            inner,
            CursorPlanError::ContinuationCursorSignatureMismatch { .. }
        )
    });
}

#[test]
fn execute_sql_grouped_rejects_scalar_sql_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_grouped::<SessionSqlEntity>("SELECT name FROM SessionSqlEntity", None)
        .expect_err("grouped SQL API should reject non-grouped SQL queries");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "grouped SQL API should fail closed for non-grouped SQL shapes",
    );
}

#[test]
fn execute_sql_rejects_grouped_sql_intent_without_grouped_api() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age")
        .expect_err("scalar SQL API should reject grouped SQL intent");

    assert!(
        matches!(
            err,
            QueryError::Intent(
                crate::db::query::intent::IntentError::GroupedRequiresExecuteGrouped
            )
        ),
        "scalar SQL API must preserve grouped explicit-entrypoint contract",
    );
}

#[test]
fn execute_sql_rejects_unsupported_group_by_projection_shape() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity GROUP BY age")
        .expect_err("group-by projection mismatch should fail closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported grouped SQL projection shapes should fail at reduced lowering boundary",
    );
}

#[test]
fn explain_sql_plan_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN plan SQL cases.
    let cases = vec![
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["mode=Load", "access="],
        ),
        (
            "EXPLAIN SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            vec!["mode=Load", "distinct=true"],
        ),
        (
            "EXPLAIN SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["mode=Load", "grouping=Grouped"],
        ),
        (
            "EXPLAIN DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["mode=Delete", "access="],
        ),
        (
            "EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["mode=Load", "access="],
        ),
    ];

    // Phase 2: execute each EXPLAIN plan query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = session
            .explain_sql::<SessionSqlEntity>(sql)
            .expect("EXPLAIN plan matrix query should succeed");
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_execution_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN EXECUTION SQL cases.
    let cases = vec![
        (
            "EXPLAIN EXECUTION SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["node_id=0", "layer="],
        ),
        (
            "EXPLAIN EXECUTION SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["node_id=0", "execution_mode="],
        ),
        (
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["AggregateCount execution_mode=", "node_id=0"],
        ),
    ];

    // Phase 2: execute each EXPLAIN EXECUTION query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = session
            .explain_sql::<SessionSqlEntity>(sql)
            .expect("EXPLAIN EXECUTION matrix query should succeed");
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_json_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN JSON SQL cases.
    let cases = vec![
        (
            "EXPLAIN JSON SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
        ),
        (
            "EXPLAIN JSON SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            vec!["\"mode\":{\"type\":\"Load\"", "\"distinct\":true"],
        ),
        (
            "EXPLAIN JSON SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["\"mode\":{\"type\":\"Load\"", "\"grouping\""],
        ),
        (
            "EXPLAIN JSON DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["\"mode\":{\"type\":\"Delete\"", "\"access\":"],
        ),
        (
            "EXPLAIN JSON SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
        ),
    ];

    // Phase 2: execute each EXPLAIN JSON query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = session
            .explain_sql::<SessionSqlEntity>(sql)
            .expect("EXPLAIN JSON matrix query should succeed");
        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "explain JSON matrix output should be one JSON object payload: {sql}",
        );
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_execution_returns_descriptor_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
        )
        .expect("EXPLAIN EXECUTION should succeed");

    assert!(
        explain.contains("node_id=0"),
        "execution explain output should include the root descriptor node id",
    );
    assert!(
        explain.contains("layer="),
        "execution explain output should include execution layer annotations",
    );
}

#[test]
fn explain_sql_plan_returns_logical_plan_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
        )
        .expect("EXPLAIN should succeed");

    assert!(
        explain.contains("mode=Load"),
        "logical explain text should include query mode projection",
    );
    assert!(
        explain.contains("access="),
        "logical explain text should include projected access shape",
    );
}

#[test]
fn explain_sql_plan_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
        )
        .expect("qualified grouped EXPLAIN plan SQL should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("unqualified grouped EXPLAIN plan SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same logical EXPLAIN plan output",
    );
}

#[test]
fn explain_sql_execution_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
        )
        .expect("qualified grouped EXPLAIN execution SQL should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("unqualified grouped EXPLAIN execution SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same execution EXPLAIN descriptor output",
    );
}

#[test]
fn explain_sql_json_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
        )
        .expect("qualified grouped EXPLAIN JSON SQL should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("unqualified grouped EXPLAIN JSON SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same EXPLAIN JSON output",
    );
}

#[test]
fn explain_sql_plan_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT * \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
        )
        .expect("qualified EXPLAIN plan SQL should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
        )
        .expect("unqualified EXPLAIN plan SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same logical EXPLAIN plan output",
    );
}

#[test]
fn explain_sql_execution_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
        )
        .expect("qualified EXPLAIN execution SQL should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT name \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
        )
        .expect("unqualified EXPLAIN execution SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same execution EXPLAIN descriptor output",
    );
}

#[test]
fn explain_sql_json_qualified_aggregate_matches_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT SUM(SessionSqlEntity.age) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21",
        )
        .expect("qualified global aggregate EXPLAIN JSON should succeed");
    let unqualified = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT SUM(age) FROM SessionSqlEntity WHERE age >= 21",
        )
        .expect("unqualified global aggregate EXPLAIN JSON should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same global aggregate EXPLAIN JSON output",
    );
}

#[test]
fn explain_sql_plan_select_distinct_star_marks_distinct_true() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
        )
        .expect("EXPLAIN SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("distinct=true"),
        "logical explain text should preserve scalar distinct intent",
    );
}

#[test]
fn explain_sql_execution_select_distinct_star_returns_execution_descriptor_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
        )
        .expect("EXPLAIN EXECUTION SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("node_id=0"),
        "execution explain output should include the root descriptor node id",
    );
}

#[test]
fn explain_sql_json_returns_logical_plan_json() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
        )
        .expect("EXPLAIN JSON should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "logical explain JSON should render one JSON object payload",
    );
    assert!(
        explain.contains("\"mode\":{\"type\":\"Load\""),
        "logical explain JSON should expose structured query mode metadata",
    );
    assert!(
        explain.contains("\"access\":"),
        "logical explain JSON should include projected access metadata",
    );
}

#[test]
fn explain_sql_json_select_distinct_star_marks_distinct_true() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
        )
        .expect("EXPLAIN JSON SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("\"distinct\":true"),
        "logical explain JSON should preserve scalar distinct intent",
    );
}

#[test]
fn explain_sql_json_delete_returns_logical_delete_mode() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN JSON DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
        )
        .expect("EXPLAIN JSON DELETE should succeed");

    assert!(
        explain.contains("\"mode\":{\"type\":\"Delete\""),
        "logical explain JSON should expose delete query mode metadata",
    );
}

#[test]
fn explain_sql_plan_global_aggregate_returns_logical_plan_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>("EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("global aggregate SQL explain plan should succeed");

    assert!(
        explain.contains("mode=Load"),
        "global aggregate SQL explain plan should project logical load mode",
    );
    assert!(
        explain.contains("access="),
        "global aggregate SQL explain plan should include logical access projection",
    );
}

#[test]
fn explain_sql_execution_global_aggregate_returns_execution_descriptor_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>("EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("global aggregate SQL explain execution should succeed");

    assert!(
        explain.contains("AggregateCount execution_mode="),
        "global aggregate SQL explain execution should include aggregate terminal node heading",
    );
    assert!(
        explain.contains("node_id=0"),
        "global aggregate SQL explain execution should include root node id",
    );
}

#[test]
fn explain_sql_json_global_aggregate_returns_logical_plan_json() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = session
        .explain_sql::<SessionSqlEntity>("EXPLAIN JSON SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("global aggregate SQL explain json should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "global aggregate SQL explain json should render one JSON object payload",
    );
    assert!(
        explain.contains("\"mode\":{\"type\":\"Load\""),
        "global aggregate SQL explain json should expose logical query mode metadata",
    );
}

#[test]
fn explain_sql_global_aggregate_rejects_unknown_target_field() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .explain_sql::<SessionSqlEntity>(
            "EXPLAIN EXECUTION SELECT SUM(missing_field) FROM SessionSqlEntity",
        )
        .expect_err("global aggregate SQL explain should reject unknown target fields");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL explain should map unknown target field to unsupported execution error boundary",
    );
}

#[test]
fn explain_sql_rejects_distinct_without_pk_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .explain_sql::<SessionSqlEntity>("EXPLAIN SELECT DISTINCT age FROM SessionSqlEntity")
        .expect_err("EXPLAIN SELECT DISTINCT without PK projection should remain fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported DISTINCT explain shape should map to unsupported execution error boundary",
    );
}

#[test]
fn explain_sql_rejects_non_explain_statements() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .explain_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity")
        .expect_err("explain_sql must reject non-EXPLAIN statements");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "non-EXPLAIN input must fail as unsupported explain usage",
    );
}
