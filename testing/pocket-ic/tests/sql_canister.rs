use std::fs;

use canic_testkit::pic::{StandaloneCanisterFixture, install_prebuilt_canister};
use icydb::{
    Error,
    db::sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput},
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
};
use icydb_testing_integration::build_canister;

fn install_sql_canister_fixture() -> StandaloneCanisterFixture {
    // Build the dedicated SQL smoke canister once, then install that wasm into
    // a fresh standalone PocketIC instance with empty init args.
    let wasm_path = build_canister("sql").expect("sql canister should build for PocketIC tests");
    let wasm = fs::read(&wasm_path)
        .unwrap_or_else(|err| panic!("failed to read built sql canister wasm: {err}"));

    install_prebuilt_canister(
        wasm,
        candid::encode_args(()).expect("encode empty init args"),
    )
}

fn reset_sql_fixtures(fixture: &StandaloneCanisterFixture) {
    // Keep each test isolated by resetting and then loading the deterministic
    // baseline fixture set through the live canister update surface.
    let reset: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_reset", ())
        .expect("fixtures_reset should decode");
    reset.expect("fixtures_reset should succeed");

    let load: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_load_default", ())
        .expect("fixtures_load_default should decode");
    load.expect("fixtures_load_default should succeed");
}

fn query_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .pic()
        .query_call(fixture.canister_id(), "query", (sql.to_string(),))
        .expect("sql query canister call should decode")
}

fn update_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .pic()
        .update_call(fixture.canister_id(), "update", (sql.to_string(),))
        .expect("sql update canister call should decode")
}

fn expect_projection(result: SqlQueryResult) -> SqlQueryRowsOutput {
    match result {
        SqlQueryResult::Projection(rows) => rows,
        other => panic!("expected projection payload, got {other:?}"),
    }
}

fn expect_grouped(result: SqlQueryResult) -> SqlGroupedRowsOutput {
    match result {
        SqlQueryResult::Grouped(rows) => rows,
        other => panic!("expected grouped payload, got {other:?}"),
    }
}

#[test]
fn sql_canister_query_endpoint_executes_scalar_and_grouped_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let scalar = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar SQL query should succeed"),
    );
    assert_eq!(
        scalar,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["bob".to_string()], vec!["alice".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve ordered scalar projection payloads",
    );

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT age, COUNT(*) FROM SqlTestUser GROUP BY age ORDER BY age ASC LIMIT 10",
        )
        .expect("grouped SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["age".to_string(), "COUNT(*)".to_string()],
            rows: vec![
                vec!["24".to_string(), "1".to_string()],
                vec!["31".to_string(), "1".to_string()],
                vec!["43".to_string(), "1".to_string()],
            ],
            row_count: 3,
            next_cursor: None,
        },
        "query(sql) should preserve grouped result payloads too",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_scalar_arithmetic_and_round_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT age - 1 FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar arithmetic SQL query should succeed"),
    );
    assert_eq!(
        arithmetic,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["age - 1".to_string()],
            rows: vec![vec!["23".to_string()], vec!["30".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve scalar arithmetic projection payloads at the live canister boundary",
    );

    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(age / 3, 2) FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar ROUND SQL query should succeed"),
    );
    assert_eq!(
        rounded,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["ROUND(age / 3, 2)".to_string()],
            rows: vec![vec!["8.00".to_string()], vec!["10.33".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve scalar ROUND projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_round_field_to_field_arithmetic_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(age + rank, 2) AS total FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("ROUND(field + field) SQL query should succeed"),
    );
    assert_eq!(
        rounded,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["total".to_string()],
            rows: vec![vec!["49.00".to_string()], vec!["59.00".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve ROUND(field + field) projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_field_to_field_arithmetic_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT age + rank AS total FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("field-to-field arithmetic SQL query should succeed"),
    );
    assert_eq!(
        arithmetic,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["total".to_string()],
            rows: vec![vec!["49".to_string()], vec!["59".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve field-to-field arithmetic projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_order_by_bounded_numeric_alias_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age + 1 AS next_age FROM SqlTestUser ORDER BY next_age ASC LIMIT 2",
        )
        .expect("ORDER BY arithmetic alias SQL query should succeed"),
    );
    let field_to_field = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age + rank AS total FROM SqlTestUser ORDER BY total ASC LIMIT 2",
        )
        .expect("ORDER BY field-to-field arithmetic alias SQL query should succeed"),
    );
    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, ROUND(age / 3, 2) AS rounded_age FROM SqlTestUser ORDER BY rounded_age DESC LIMIT 2",
        )
        .expect("ORDER BY ROUND alias SQL query should succeed"),
    );

    assert_eq!(
        arithmetic,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "next_age".to_string()],
            rows: vec![
                vec!["bob".to_string(), "25".to_string()],
                vec!["alice".to_string(), "32".to_string()],
            ],
            row_count: 2,
        },
        "query(sql) should preserve arithmetic alias ordering at the live canister boundary",
    );
    assert_eq!(
        field_to_field,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "total".to_string()],
            rows: vec![
                vec!["bob".to_string(), "49".to_string()],
                vec!["alice".to_string(), "59".to_string()],
            ],
            row_count: 2,
        },
        "query(sql) should preserve field-to-field arithmetic alias ordering at the live canister boundary",
    );
    assert_eq!(
        rounded,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "rounded_age".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "14.33".to_string()],
                vec!["alice".to_string(), "10.33".to_string()],
            ],
            row_count: 2,
        },
        "query(sql) should preserve ROUND alias ordering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_direct_bounded_numeric_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY age + 1 ASC LIMIT 2",
        )
        .expect("direct ORDER BY arithmetic SQL query should succeed"),
    );
    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY ROUND(age / 3, 2) DESC LIMIT 2",
        )
        .expect("direct ORDER BY ROUND SQL query should succeed"),
    );

    assert_eq!(
        arithmetic,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            rows: vec![
                vec!["bob".to_string(), "24".to_string()],
                vec!["alice".to_string(), "31".to_string()],
            ],
            row_count: 2,
        },
        "query(sql) should preserve direct arithmetic ordering at the live canister boundary",
    );
    assert_eq!(
        rounded,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "43".to_string()],
                vec!["alice".to_string(), "31".to_string()],
            ],
            row_count: 2,
        },
        "query(sql) should preserve direct ROUND ordering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_field_to_field_predicate_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age > rank ORDER BY age ASC LIMIT 10",
        )
        .expect("field-to-field predicate SQL query should succeed"),
    );
    assert_eq!(
        filtered,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["alice".to_string()]],
            row_count: 1,
        },
        "query(sql) should preserve field-to-field predicate filtering at the live canister boundary",
    );

    let mixed = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age > 18 AND age > rank ORDER BY age ASC LIMIT 10",
        )
        .expect("mixed literal and field-to-field predicate SQL query should succeed"),
    );
    assert_eq!(
        mixed,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["alice".to_string()]],
            row_count: 1,
        },
        "query(sql) should preserve correct residual filtering when a literal predicate and a field-to-field predicate are combined at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_not_between_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age NOT BETWEEN 25 AND 40 ORDER BY age ASC LIMIT 10",
        )
        .expect("NOT BETWEEN SQL query should succeed"),
    );
    assert_eq!(
        filtered,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["bob".to_string()], vec!["charlie".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve NOT BETWEEN filtering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_not_like_prefix_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE LOWER(name) NOT LIKE 'a%' ORDER BY age ASC LIMIT 10",
        )
        .expect("NOT LIKE SQL query should succeed"),
    );
    assert_eq!(
        filtered,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["bob".to_string()], vec!["charlie".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve bounded NOT LIKE prefix filtering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_preserves_show_tables_alias() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let tables = query_sql(&fixture, "SHOW TABLES").expect("SHOW TABLES should succeed");
    let entities = query_sql(&fixture, "SHOW ENTITIES").expect("SHOW ENTITIES should succeed");

    assert_eq!(
        tables, entities,
        "SHOW TABLES should stay an alias for SHOW ENTITIES at the live canister boundary",
    );
}

#[test]
fn sql_canister_update_endpoint_executes_delete_returning() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let deleted = expect_projection(
        update_sql(
            &fixture,
            "DELETE FROM SqlTestUser WHERE name = 'bob' RETURNING name",
        )
        .expect("DELETE RETURNING should succeed on update(sql)"),
    );
    assert_eq!(
        deleted,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["bob".to_string()]],
            row_count: 1,
        },
        "update(sql) should preserve RETURNING projection payloads",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_mutation_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(
        &fixture,
        "DELETE FROM SqlTestUser WHERE name = 'bob' RETURNING name",
    )
    .expect_err("query(sql) must reject mutation statements");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "wrong-lane SQL must stay an unsupported runtime error at the canister boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "wrong-lane SQL should keep query-owned origin metadata",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_malformed_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(&fixture, "SELECT FROM SqlTestUser")
        .expect_err("query(sql) must reject malformed SQL before execution");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "malformed SQL should stay an unsupported runtime error at the canister boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "malformed SQL should keep query-owned origin metadata",
    );
    assert!(
        err.message().contains("invalid SQL syntax"),
        "malformed SQL should preserve parser-owned invalid-syntax detail",
    );
}
