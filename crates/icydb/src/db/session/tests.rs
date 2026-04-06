use super::*;
use crate::{
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    macros::{canister, entity, store},
    traits::{Path as _, Sanitizer as _},
};
use canic_cdk::structures::{DefaultMemoryImpl, memory::VirtualMemory};
use canic_memory::api::MemoryApi;
use icydb_core as core;
use std::cell::RefCell;

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

fn test_memory(id: u8, label: &str) -> VirtualMemory<DefaultMemoryImpl> {
    MemoryApi::bootstrap_owner_range(env!("CARGO_PKG_NAME"), 240, 250)
        .expect("facade SQL tests should bootstrap their reserved memory range");
    MemoryApi::register(id, env!("CARGO_PKG_NAME"), label)
        .expect("facade SQL tests should register memory slots within their reserved range")
}

thread_local! {
    static FACADE_SQL_DATA_STORE: RefCell<core::db::DataStore> =
        RefCell::new(core::db::DataStore::init(test_memory(241, "FacadeSqlDataStore")));
    static FACADE_SQL_INDEX_STORE: RefCell<core::db::IndexStore> =
        RefCell::new(core::db::IndexStore::init(test_memory(242, "FacadeSqlIndexStore")));
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

const fn facade_session() -> DbSession<FacadeSqlCanister> {
    let core_session =
        core::db::DbSession::<FacadeSqlCanister>::new_with_hooks(&FACADE_SQL_STORE_REGISTRY, &[]);
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

fn unsupported_sql_runtime_error(message: &'static str) -> Error {
    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        message,
    )
}

fn dispatch_explain_sql<E>(
    session: &DbSession<FacadeSqlCanister>,
    sql: &str,
) -> Result<String, Error>
where
    E: crate::db::PersistedRow<Canister = FacadeSqlCanister> + EntityValue,
{
    let parsed = session.parse_sql_statement(sql)?;
    if !parsed.route().is_explain() {
        return Err(unsupported_sql_runtime_error(
            "EXPLAIN dispatch requires an EXPLAIN statement",
        ));
    }

    match session.execute_sql_dispatch_parsed::<E>(&parsed)? {
        SqlQueryResult::Explain { explain, .. } => Ok(explain),
        SqlQueryResult::Projection(_)
        | SqlQueryResult::Describe(_)
        | SqlQueryResult::ShowIndexes { .. }
        | SqlQueryResult::ShowColumns { .. }
        | SqlQueryResult::ShowEntities { .. } => Err(unsupported_sql_runtime_error(
            "EXPLAIN dispatch requires an EXPLAIN statement",
        )),
    }
}

fn dispatch_describe_sql<E>(
    session: &DbSession<FacadeSqlCanister>,
    sql: &str,
) -> Result<EntitySchemaDescription, Error>
where
    E: crate::db::PersistedRow<Canister = FacadeSqlCanister> + EntityValue,
{
    let parsed = session.parse_sql_statement(sql)?;
    if !parsed.route().is_describe() {
        return Err(unsupported_sql_runtime_error(
            "DESCRIBE dispatch requires a DESCRIBE statement",
        ));
    }

    match session.execute_sql_dispatch_parsed::<E>(&parsed)? {
        SqlQueryResult::Describe(description) => Ok(description),
        SqlQueryResult::Projection(_)
        | SqlQueryResult::Explain { .. }
        | SqlQueryResult::ShowIndexes { .. }
        | SqlQueryResult::ShowColumns { .. }
        | SqlQueryResult::ShowEntities { .. } => Err(unsupported_sql_runtime_error(
            "DESCRIBE dispatch requires a DESCRIBE statement",
        )),
    }
}

fn dispatch_show_indexes_sql<E>(
    session: &DbSession<FacadeSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, Error>
where
    E: crate::db::PersistedRow<Canister = FacadeSqlCanister> + EntityValue,
{
    let parsed = session.parse_sql_statement(sql)?;
    if !parsed.route().is_show_indexes() {
        return Err(unsupported_sql_runtime_error(
            "SHOW INDEXES dispatch requires a SHOW INDEXES statement",
        ));
    }

    match session.execute_sql_dispatch_parsed::<E>(&parsed)? {
        SqlQueryResult::ShowIndexes { indexes, .. } => Ok(indexes),
        SqlQueryResult::Projection(_)
        | SqlQueryResult::Explain { .. }
        | SqlQueryResult::Describe(_)
        | SqlQueryResult::ShowColumns { .. }
        | SqlQueryResult::ShowEntities { .. } => Err(unsupported_sql_runtime_error(
            "SHOW INDEXES dispatch requires a SHOW INDEXES statement",
        )),
    }
}

fn dispatch_show_columns_sql<E>(
    session: &DbSession<FacadeSqlCanister>,
    sql: &str,
) -> Result<Vec<EntityFieldDescription>, Error>
where
    E: crate::db::PersistedRow<Canister = FacadeSqlCanister> + EntityValue,
{
    let parsed = session.parse_sql_statement(sql)?;
    if !parsed.route().is_show_columns() {
        return Err(unsupported_sql_runtime_error(
            "SHOW COLUMNS dispatch requires a SHOW COLUMNS statement",
        ));
    }

    match session.execute_sql_dispatch_parsed::<E>(&parsed)? {
        SqlQueryResult::ShowColumns { columns, .. } => Ok(columns),
        SqlQueryResult::Projection(_)
        | SqlQueryResult::Explain { .. }
        | SqlQueryResult::Describe(_)
        | SqlQueryResult::ShowIndexes { .. }
        | SqlQueryResult::ShowEntities { .. } => Err(unsupported_sql_runtime_error(
            "SHOW COLUMNS dispatch requires a SHOW COLUMNS statement",
        )),
    }
}

fn dispatch_show_entities_sql(
    session: &DbSession<FacadeSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, Error> {
    let route = session.sql_statement_route(sql)?;
    if !route.is_show_entities() {
        return Err(unsupported_sql_runtime_error(
            "SHOW ENTITIES dispatch requires a SHOW ENTITIES statement",
        ));
    }

    Ok(session.show_entities())
}

const fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 7] {
    [
        (
            "SELECT * FROM FacadeSqlEntity JOIN other ON FacadeSqlEntity.id = other.id",
            "JOIN",
        ),
        ("SELECT \"name\" FROM FacadeSqlEntity", "quoted identifiers"),
        ("SELECT * FROM FacadeSqlEntity alias", "table aliases"),
        (
            "SELECT * FROM FacadeSqlEntity WHERE name LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM FacadeSqlEntity WHERE LOWER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM FacadeSqlEntity WHERE UPPER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM FacadeSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al')",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        ),
    ]
}

fn assert_facade_query_unsupported_runtime(err: Error, context: &str) {
    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: {context}",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: {context}",
    );
}

fn assert_unsupported_sql_runtime_result<T>(result: Result<T, Error>, surface: &str) {
    match result {
        Ok(_) => panic!("unsupported SQL should fail through {surface}"),
        Err(err) => assert_facade_query_unsupported_runtime(err, surface),
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
fn facade_query_from_sql_lower_like_prefix_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>(
            "SELECT * FROM FacadeSqlEntity WHERE LOWER(name) LIKE 'Al%'",
        )
        .expect("facade LOWER(field) LIKE prefix SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade LOWER(field) LIKE prefix SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade LOWER(field) LIKE prefix SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade LOWER(field) LIKE prefix SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_strict_like_prefix_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>("SELECT * FROM FacadeSqlEntity WHERE name LIKE 'Al%'")
        .expect("facade strict LIKE prefix SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade strict LIKE prefix SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade strict LIKE prefix SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade strict LIKE prefix SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_direct_starts_with_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>(
            "SELECT * FROM FacadeSqlEntity WHERE STARTS_WITH(name, 'Al')",
        )
        .expect("facade direct STARTS_WITH SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade direct STARTS_WITH SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade direct STARTS_WITH SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade direct STARTS_WITH SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_direct_lower_starts_with_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>(
            "SELECT * FROM FacadeSqlEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
        )
        .expect("facade direct LOWER(field) STARTS_WITH SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade direct LOWER(field) STARTS_WITH SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade direct LOWER(field) STARTS_WITH SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade direct LOWER(field) STARTS_WITH SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_direct_upper_starts_with_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>(
            "SELECT * FROM FacadeSqlEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
        )
        .expect("facade direct UPPER(field) STARTS_WITH SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade direct UPPER(field) STARTS_WITH SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade direct UPPER(field) STARTS_WITH SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade direct UPPER(field) STARTS_WITH SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_upper_like_prefix_lowers_to_load_query_intent() {
    let session = fresh_facade_session();

    let query = session
        .query_from_sql::<FacadeSqlEntity>(
            "SELECT * FROM FacadeSqlEntity WHERE UPPER(name) LIKE 'AL%'",
        )
        .expect("facade UPPER(field) LIKE prefix SQL query should lower");
    assert!(
        matches!(query.mode(), core::db::QueryMode::Load(_)),
        "facade UPPER(field) LIKE prefix SQL should lower to load query mode",
    );
    let explain = query
        .explain()
        .expect("facade UPPER(field) LIKE prefix SQL explain should build")
        .render_text_canonical();
    assert!(
        explain.contains("StartsWith") || explain.contains("starts_with"),
        "facade UPPER(field) LIKE prefix SQL explain should preserve starts-with intent",
    );
}

#[test]
fn facade_query_from_sql_delete_direct_starts_with_family_matches_like_intent() {
    let session = fresh_facade_session();

    // Phase 1: define the accepted direct family and the equivalent LIKE forms
    // the public facade should continue to lower to on the structural query lane.
    let cases = [
        (
            "DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM FacadeSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "facade strict direct STARTS_WITH delete intent",
        ),
        (
            "DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "DELETE FROM FacadeSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "facade direct LOWER(field) STARTS_WITH delete intent",
        ),
        (
            "DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM FacadeSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "facade direct UPPER(field) STARTS_WITH delete intent",
        ),
    ];

    // Phase 2: compare the lowered delete intent directly so the facade stays
    // coherent without depending on the local write-fixture harness.
    for (direct_sql, like_sql, context) in cases {
        let direct = session
            .query_from_sql::<FacadeSqlEntity>(direct_sql)
            .expect("facade direct STARTS_WITH delete SQL should lower");
        let like = session
            .query_from_sql::<FacadeSqlEntity>(like_sql)
            .expect("facade LIKE delete SQL should lower");

        assert_eq!(
            direct
                .explain()
                .expect("facade direct STARTS_WITH delete SQL explain should build")
                .render_text_canonical(),
            like.explain()
                .expect("facade LIKE delete SQL explain should build")
                .render_text_canonical(),
            "facade direct STARTS_WITH delete should match the established LIKE delete intent: {context}",
        );
    }
}

#[test]
fn facade_explain_delete_direct_starts_with_family_matches_like_output() {
    let session = fresh_facade_session();

    // Phase 1: compare the public EXPLAIN surface across the accepted direct
    // family and the established LIKE path for delete queries.
    let cases = [
        (
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "facade strict direct STARTS_WITH delete explain",
        ),
        (
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "facade direct LOWER(field) STARTS_WITH delete explain",
        ),
        (
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM FacadeSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "facade direct UPPER(field) STARTS_WITH delete explain",
        ),
    ];

    // Phase 2: assert the public facade emits identical logical EXPLAIN output
    // for both spellings so the bounded family stays contract-coherent.
    for (direct_sql, like_sql, context) in cases {
        let direct = dispatch_explain_sql::<FacadeSqlEntity>(&session, direct_sql)
            .expect("facade direct STARTS_WITH delete EXPLAIN should succeed");
        let like = dispatch_explain_sql::<FacadeSqlEntity>(&session, like_sql)
            .expect("facade LIKE delete EXPLAIN should succeed");

        assert_eq!(
            direct, like,
            "facade direct STARTS_WITH delete EXPLAIN should match the established LIKE output: {context}",
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

    let from_sql = dispatch_describe_sql::<FacadeSqlEntity>(&session, "DESCRIBE FacadeSqlEntity")
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

    let from_sql =
        dispatch_show_indexes_sql::<FacadeSqlEntity>(&session, "SHOW INDEXES FacadeSqlEntity")
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

    let from_sql =
        dispatch_show_columns_sql::<FacadeSqlEntity>(&session, "SHOW COLUMNS FacadeSqlEntity")
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

    let from_sql = dispatch_show_entities_sql(&session, "SHOW ENTITIES")
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
        let explain = dispatch_explain_sql::<FacadeSqlEntity>(&session, sql)
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
        let explain = dispatch_explain_sql::<FacadeSqlEntity>(&session, sql)
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
        let explain = dispatch_explain_sql::<FacadeSqlEntity>(&session, sql)
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
fn facade_query_from_sql_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    let session = fresh_facade_session();

    let err = session
        .query_from_sql::<FacadeSqlEntity>(
            "DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
        )
        .expect_err("facade direct STARTS_WITH delete wrapper should stay fail-closed");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade query_from_sql non-casefold direct STARTS_WITH delete",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade query_from_sql non-casefold direct STARTS_WITH delete",
    );
    assert!(
        err.message().contains(
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
        ),
        "facade query_from_sql should preserve the stable unsupported direct STARTS_WITH delete detail",
    );
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
fn facade_explain_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    let session = fresh_facade_session();

    let err = dispatch_explain_sql::<FacadeSqlEntity>(
        &session,
        "EXPLAIN DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
    )
    .expect_err("facade direct STARTS_WITH delete EXPLAIN wrapper should stay fail-closed");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade EXPLAIN DELETE non-casefold direct STARTS_WITH",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade EXPLAIN DELETE non-casefold direct STARTS_WITH",
    );
    assert!(
        err.message().contains(
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
        ),
        "facade EXPLAIN DELETE should preserve the stable unsupported direct STARTS_WITH delete detail",
    );
}

#[test]
fn facade_explain_json_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    let session = fresh_facade_session();

    let err = dispatch_explain_sql::<FacadeSqlEntity>(
        &session,
        "EXPLAIN JSON DELETE FROM FacadeSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
    )
    .expect_err("facade direct STARTS_WITH JSON delete EXPLAIN wrapper should stay fail-closed");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade EXPLAIN JSON DELETE non-casefold direct STARTS_WITH",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade EXPLAIN JSON DELETE non-casefold direct STARTS_WITH",
    );
    assert!(
        err.message().contains(
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
        ),
        "facade EXPLAIN JSON DELETE should preserve the stable unsupported direct STARTS_WITH delete detail",
    );
}

#[test]
fn facade_execute_sql_rejects_non_query_statement_lanes_matrix() {
    let session = fresh_facade_session();

    let cases = [
        (
            "EXPLAIN SELECT * FROM FacadeSqlEntity",
            "execute_sql rejects EXPLAIN",
        ),
        ("DESCRIBE FacadeSqlEntity", "execute_sql rejects DESCRIBE"),
        (
            "SHOW INDEXES FacadeSqlEntity",
            "execute_sql rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS FacadeSqlEntity",
            "execute_sql rejects SHOW COLUMNS",
        ),
        ("SHOW ENTITIES", "execute_sql rejects SHOW ENTITIES"),
    ];

    for (sql, expected) in cases {
        let err = session
            .execute_sql::<FacadeSqlEntity>(sql)
            .expect_err("non-query statement lanes should stay fail-closed for execute_sql");
        assert!(
            err.to_string().contains(expected),
            "facade execute_sql should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

#[test]
fn facade_execute_sql_grouped_rejects_non_query_statement_lanes_matrix() {
    let session = fresh_facade_session();

    let cases = [
        (
            "EXPLAIN SELECT * FROM FacadeSqlEntity",
            "execute_sql_grouped rejects EXPLAIN",
        ),
        (
            "DESCRIBE FacadeSqlEntity",
            "execute_sql_grouped rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES FacadeSqlEntity",
            "execute_sql_grouped rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS FacadeSqlEntity",
            "execute_sql_grouped rejects SHOW COLUMNS",
        ),
        ("SHOW ENTITIES", "execute_sql_grouped rejects SHOW ENTITIES"),
    ];

    for (sql, expected) in cases {
        let Err(err) = session.execute_sql_grouped::<FacadeSqlEntity>(sql, None) else {
            panic!("non-query statement lanes should stay fail-closed for execute_sql_grouped")
        };
        assert!(
            err.to_string().contains(expected),
            "facade execute_sql_grouped should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

#[test]
fn facade_query_from_sql_rejects_computed_text_projection_in_current_lane() {
    let session = fresh_facade_session();

    // Phase 1: keep the public facade on the same structural-only contract
    // as the core `query_from_sql(...)` boundary.
    let err = session
            .query_from_sql::<FacadeSqlEntity>("SELECT TRIM(name) FROM FacadeSqlEntity")
            .expect_err(
                "facade query_from_sql should reject computed text projection on the structural-only lane",
            );

    // Phase 2: assert the facade preserves the actionable boundary message
    // instead of silently widening into dispatch-owned computed semantics.
    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade query_from_sql computed text projection",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade query_from_sql computed text projection",
    );
    assert!(
        err.to_string()
            .contains("query_from_sql does not accept computed text projection"),
        "facade query_from_sql should preserve the computed projection boundary message",
    );
}

#[test]
fn facade_execute_sql_rejects_computed_text_projection_in_current_lane() {
    let session = fresh_facade_session();

    let err = session
        .execute_sql::<FacadeSqlEntity>("SELECT TRIM(name) FROM FacadeSqlEntity")
        .expect_err(
            "facade execute_sql should keep computed text projection on the dispatch-owned lane",
        );

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql computed text projection",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql computed text projection",
    );
    assert!(
        err.to_string()
            .contains("execute_sql rejects computed text projection"),
        "facade execute_sql should preserve the computed projection boundary message",
    );
}

#[test]
fn facade_execute_sql_grouped_rejects_computed_text_projection_in_current_lane() {
    let session = fresh_facade_session();

    let Err(err) = session
        .execute_sql_grouped::<FacadeSqlEntity>("SELECT TRIM(name) FROM FacadeSqlEntity", None)
    else {
        panic!(
            "facade execute_sql_grouped should keep computed text projection on the dispatch-owned lane"
        )
    };

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_grouped computed text projection",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_grouped computed text projection",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects computed text projection"),
        "facade execute_sql_grouped should preserve the computed projection boundary message",
    );
}

#[test]
fn facade_query_from_sql_rejects_global_aggregate_execution_in_current_lane() {
    let session = fresh_facade_session();

    let err = session
            .query_from_sql::<FacadeSqlEntity>("SELECT COUNT(*) FROM FacadeSqlEntity")
            .expect_err(
                "facade query_from_sql should keep global aggregate execution on the dedicated aggregate lane",
            );

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade query_from_sql global aggregate",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade query_from_sql global aggregate",
    );
    assert!(
        err.to_string()
            .contains("query_from_sql rejects global aggregate SELECT"),
        "facade query_from_sql should preserve the dedicated aggregate-lane boundary message",
    );
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
            session.execute_sql_dispatch::<FacadeSqlEntity>(sql),
            "facade execute_sql_projection",
        );
    }
}

#[test]
fn facade_scalar_sql_surfaces_reject_global_aggregate_execution_in_current_lane() {
    let session = fresh_facade_session();
    let sql = "SELECT COUNT(*) FROM FacadeSqlEntity";

    // Phase 1: keep scalar row-shaped execution fail-closed for global
    // aggregate SQL so the dedicated aggregate lane remains explicit.
    let execute_err = session
        .execute_sql::<FacadeSqlEntity>(sql)
        .expect_err("facade execute_sql should reject global aggregate SQL");
    let dispatch_err = session
        .execute_sql_dispatch::<FacadeSqlEntity>(sql)
        .expect_err("facade execute_sql_dispatch should reject global aggregate SQL");

    assert_eq!(
        execute_err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql global aggregate",
    );
    assert_eq!(
        execute_err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql global aggregate",
    );
    assert!(
        execute_err
            .to_string()
            .contains("execute_sql rejects global aggregate SELECT"),
        "facade execute_sql should preserve the dedicated aggregate-lane boundary message",
    );
    assert_eq!(
        dispatch_err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_dispatch global aggregate",
    );
    assert_eq!(
        dispatch_err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_dispatch global aggregate",
    );
    assert!(
        dispatch_err
            .to_string()
            .contains("execute_sql_dispatch rejects global aggregate SELECT"),
        "facade execute_sql_dispatch should preserve the dedicated aggregate-lane boundary message",
    );
}

#[test]
fn facade_execute_sql_grouped_rejects_global_aggregate_execution_in_current_lane() {
    let session = fresh_facade_session();

    let Err(err) = session
        .execute_sql_grouped::<FacadeSqlEntity>("SELECT COUNT(*) FROM FacadeSqlEntity", None)
    else {
        panic!(
            "facade execute_sql_grouped should keep global aggregate execution on the dedicated aggregate lane"
        )
    };

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_grouped global aggregate",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_grouped global aggregate",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects global aggregate SELECT"),
        "facade execute_sql_grouped should preserve the dedicated aggregate-lane boundary message",
    );
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
fn facade_execute_sql_aggregate_rejects_non_aggregate_statement_lanes_matrix() {
    let session = fresh_facade_session();

    let cases = [
        (
            "EXPLAIN SELECT COUNT(*) FROM FacadeSqlEntity",
            "execute_sql_aggregate rejects EXPLAIN",
        ),
        (
            "DESCRIBE FacadeSqlEntity",
            "execute_sql_aggregate rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES FacadeSqlEntity",
            "execute_sql_aggregate rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS FacadeSqlEntity",
            "execute_sql_aggregate rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "execute_sql_aggregate rejects SHOW ENTITIES",
        ),
        (
            "DELETE FROM FacadeSqlEntity ORDER BY age LIMIT 1",
            "execute_sql_aggregate rejects DELETE",
        ),
    ];

    for (sql, expected) in cases {
        let err = session
            .execute_sql_aggregate::<FacadeSqlEntity>(sql)
            .expect_err(
                "non-aggregate statement lanes should stay fail-closed for execute_sql_aggregate",
            );
        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            "unsupported runtime kind mismatch: facade execute_sql_aggregate lane matrix",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "unsupported runtime origin mismatch: facade execute_sql_aggregate lane matrix",
        );
        assert!(
            err.to_string().contains(expected),
            "facade execute_sql_aggregate should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

#[test]
fn facade_execute_sql_aggregate_rejects_non_aggregate_select_shapes_in_current_lane() {
    let session = fresh_facade_session();
    let sql = "SELECT age FROM FacadeSqlEntity";
    let err = session
            .execute_sql_aggregate::<FacadeSqlEntity>(sql)
            .expect_err(
                "non-aggregate or grouped aggregate SELECT should stay fail-closed for execute_sql_aggregate",
            );
    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_aggregate select shape",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_aggregate select shape",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_aggregate requires constrained global aggregate SELECT"),
        "facade execute_sql_aggregate should preserve constrained aggregate-surface guidance: {sql}",
    );
}

#[test]
fn facade_execute_sql_aggregate_rejects_grouped_select_execution_in_current_lane() {
    let session = fresh_facade_session();

    let err = session
        .execute_sql_aggregate::<FacadeSqlEntity>(
            "SELECT age, COUNT(*) FROM FacadeSqlEntity GROUP BY age",
        )
        .expect_err("facade execute_sql_aggregate should reject grouped SQL execution");
    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_aggregate grouped SQL",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_aggregate grouped SQL",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_aggregate rejects grouped SELECT"),
        "facade execute_sql_aggregate should preserve explicit grouped-entrypoint guidance",
    );
}

#[test]
fn facade_scalar_sql_surfaces_reject_grouped_sql_execution_in_current_lane() {
    let session = fresh_facade_session();
    let sql = "SELECT age, COUNT(*) FROM FacadeSqlEntity GROUP BY age";

    let execute_err = session
        .execute_sql::<FacadeSqlEntity>(sql)
        .expect_err("facade execute_sql should reject grouped SQL execution");
    let dispatch_err = session
        .execute_sql_dispatch::<FacadeSqlEntity>(sql)
        .expect_err("facade execute_sql_dispatch should reject grouped SQL execution");

    assert_eq!(
        execute_err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql grouped SQL",
    );
    assert_eq!(
        execute_err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql grouped SQL",
    );
    assert!(
        execute_err
            .to_string()
            .contains("execute_sql rejects grouped SELECT"),
        "facade execute_sql should preserve grouped explicit-entrypoint guidance",
    );
    assert_eq!(
        dispatch_err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_dispatch grouped SQL",
    );
    assert_eq!(
        dispatch_err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_dispatch grouped SQL",
    );
    assert!(
        dispatch_err
            .to_string()
            .contains("execute_sql_dispatch rejects grouped SELECT execution"),
        "facade execute_sql_dispatch should preserve grouped explicit-entrypoint guidance",
    );
}

#[test]
fn facade_execute_sql_grouped_rejects_delete_execution_in_current_lane() {
    let session = fresh_facade_session();

    let err = session.execute_sql_grouped::<FacadeSqlEntity>(
        "DELETE FROM FacadeSqlEntity ORDER BY id LIMIT 1",
        None,
    );
    let Err(err) = err else {
        panic!("facade execute_sql_grouped should reject DELETE execution");
    };

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "unsupported runtime kind mismatch: facade execute_sql_grouped delete SQL",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "unsupported runtime origin mismatch: facade execute_sql_grouped delete SQL",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects DELETE"),
        "facade execute_sql_grouped should preserve explicit DELETE lane guidance",
    );
}

#[test]
fn facade_explain_sql_preserves_unsupported_runtime_contract() {
    let session = fresh_facade_session();

    for (sql, _feature) in unsupported_sql_feature_cases() {
        let explain_sql = format!("EXPLAIN {sql}");
        assert_unsupported_sql_runtime_result(
            dispatch_explain_sql::<FacadeSqlEntity>(&session, explain_sql.as_str()),
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
            dispatch_explain_sql::<FacadeSqlEntity>(&session, sql),
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
            dispatch_describe_sql::<FacadeSqlEntity>(&session, sql),
            context,
        );
    }
    for (sql, context) in show_indexes_cases {
        assert_unsupported_sql_runtime_result(
            dispatch_show_indexes_sql::<FacadeSqlEntity>(&session, sql),
            context,
        );
    }
    for (sql, context) in show_columns_cases {
        assert_unsupported_sql_runtime_result(
            dispatch_show_columns_sql::<FacadeSqlEntity>(&session, sql),
            context,
        );
    }
    for (sql, context) in show_entities_cases {
        assert_unsupported_sql_runtime_result(dispatch_show_entities_sql(&session, sql), context);
    }
}
