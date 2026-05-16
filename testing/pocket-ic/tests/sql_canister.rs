use std::fs;

use candid::CandidType;
use canic_testkit::pic::{StandaloneCanisterFixture, install_prebuilt_canister};
use icydb::{
    Error, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{
        EntitySchemaDescription,
        sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput},
    },
};
use icydb_testing_integration::build_canister;
use serde::Deserialize;

// Mirror the generated IcyDB SQL query envelope so these boundary tests can
// keep asserting the ordinary SQL payload while the CLI also receives perf data.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
    planner_instructions: u64,
    store_instructions: u64,
    executor_instructions: u64,
    pure_covering_decode_instructions: u64,
    pure_covering_row_assembly_instructions: u64,
    decode_instructions: u64,
    compiler_instructions: u64,
}

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
        .update_call(fixture.canister_id(), "__icydb_fixtures_reset", ())
        .expect("__icydb_fixtures_reset should decode");
    reset.expect("__icydb_fixtures_reset should succeed");

    let load: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "__icydb_fixtures_load", ())
        .expect("__icydb_fixtures_load should decode");
    load.expect("__icydb_fixtures_load should succeed");
}

fn query_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    let response: Result<SqlQueryPerfResult, Error> = fixture
        .pic()
        .query_call(fixture.canister_id(), "__icydb_query", (sql.to_string(),))
        .expect("sql query canister call should decode");

    response.map(|payload| payload.result)
}

fn query_numeric_types(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> Result<SqlQueryResult, Error> {
    query_sql(fixture, sql)
}

fn ddl_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .pic()
        .update_call(fixture.canister_id(), "__icydb_ddl", (sql.to_string(),))
        .expect("sql DDL canister call should decode")
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

fn expect_explain(result: SqlQueryResult) -> String {
    match result {
        SqlQueryResult::Explain { explain, .. } => explain,
        other => panic!("expected explain payload, got {other:?}"),
    }
}

fn expect_describe(result: SqlQueryResult) -> EntitySchemaDescription {
    match result {
        SqlQueryResult::Describe(description) => description,
        other => panic!("expected DESCRIBE payload, got {other:?}"),
    }
}

fn expect_show_indexes(result: SqlQueryResult) -> Vec<String> {
    match result {
        SqlQueryResult::ShowIndexes { indexes, .. } => indexes,
        other => panic!("expected SHOW INDEXES payload, got {other:?}"),
    }
}

fn assert_numeric_query_error(err: Error, expected_message: &str, context: &str) {
    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "{context} should stay an unsupported runtime error at the canister boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
    assert!(
        err.message().contains(expected_message),
        "{context} should preserve numeric error detail, got: {}",
        err.message(),
    );
}

fn assert_ddl_rejects_without_index_visibility_change(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
    forbidden_visibility_fragment: &str,
) {
    let before = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES SqlTestUser")
            .expect("SHOW INDEXES should read accepted indexes before rejected DDL"),
    );
    let err = ddl_sql(fixture, sql).expect_err("invalid DDL should reject");

    assert_eq!(
        err.kind(),
        &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        "invalid DDL should stay an unsupported runtime error at the canister boundary",
    );
    let after = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES SqlTestUser")
            .expect("SHOW INDEXES should still read accepted indexes after rejected DDL"),
    );
    assert_eq!(
        after, before,
        "rejected DDL must leave accepted index visibility unchanged",
    );
    assert!(
        after
            .iter()
            .all(|index| !index.contains(forbidden_visibility_fragment)),
        "rejected DDL output fragment must not become visible: {after:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let ddl = ddl_sql(
        &fixture,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
    )
    .expect("supported CREATE INDEX DDL should publish through the canister endpoint");

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_non_unique_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES SqlTestUser")
            .expect("SHOW INDEXES should read accepted indexes after DDL publication"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready]"),
        "SHOW INDEXES should expose the DDL-published accepted index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_publication_updates_describe_explain_and_reads() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before_describe = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema before DDL"),
    );
    assert!(
        before_describe
            .indexes()
            .iter()
            .all(|index| index.name() != "sql_test_user_rank_idx"),
        "pre-DDL DESCRIBE must not expose the future DDL index",
    );

    let before_explain = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION \
             SELECT name FROM SqlTestUser \
             WHERE rank >= 25 \
             ORDER BY rank ASC \
             LIMIT 2",
        )
        .expect("EXPLAIN should succeed before DDL"),
    );
    assert!(
        !before_explain.contains("sql_test_user_rank_idx"),
        "pre-DDL EXPLAIN must not select the future DDL index: {before_explain}",
    );

    ddl_sql(
        &fixture,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
    )
    .expect("supported CREATE INDEX DDL should publish before post-DDL visibility checks");

    let after_describe = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after DDL"),
    );
    assert!(
        after_describe.indexes().iter().any(|index| {
            index.name() == "sql_test_user_rank_idx"
                && index.fields().iter().map(String::as_str).eq(["rank"])
                && !index.unique()
        }),
        "post-DDL DESCRIBE should expose the published accepted index: {after_describe:?}",
    );

    let after_explain = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION \
             SELECT name FROM SqlTestUser \
             WHERE rank >= 25 \
             ORDER BY rank ASC \
             LIMIT 2",
        )
        .expect("EXPLAIN should succeed after DDL"),
    );
    assert!(
        after_explain.contains("IndexRange(sql_test_user_rank_idx)"),
        "post-DDL EXPLAIN should select the DDL-published accepted index: {after_explain}",
    );

    let rows = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE rank >= 25 ORDER BY rank ASC LIMIT 2",
        )
        .expect("indexed read should succeed after DDL"),
    );
    assert_eq!(
        rows,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["bob".to_string()], vec!["alice".to_string()]],
            row_count: 2,
        },
        "post-DDL indexed read should observe the accepted-after index without changing row semantics",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unknown_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        "CREATE INDEX sql_test_user_missing_idx ON SqlTestUser (missing)",
        "sql_test_user_missing_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_index_name_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    ddl_sql(
        &fixture,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
    )
    .expect("setup CREATE INDEX should publish before duplicate-name rejection");

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (age)",
        "INDEX sql_test_user_rank_idx (age)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        "CREATE INDEX sql_test_user_duplicate_name_idx ON SqlTestUser (name)",
        "sql_test_user_duplicate_name_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unsupported_create_index_shapes_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for (sql, forbidden_visibility_fragment) in [
        (
            "CREATE UNIQUE INDEX sql_test_user_unique_rank_idx ON SqlTestUser (rank)",
            "sql_test_user_unique_rank_idx",
        ),
        (
            "CREATE INDEX sql_test_user_rank_age_idx ON SqlTestUser (rank, age)",
            "sql_test_user_rank_age_idx",
        ),
        (
            "CREATE INDEX sql_test_user_lower_name_idx ON SqlTestUser (LOWER(name))",
            "sql_test_user_lower_name_idx",
        ),
        (
            "CREATE INDEX sql_test_user_filtered_rank_idx ON SqlTestUser (rank) WHERE age > 20",
            "sql_test_user_filtered_rank_idx",
        ),
    ] {
        assert_ddl_rejects_without_index_visibility_change(
            &fixture,
            sql,
            forbidden_visibility_fragment,
        );
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
fn sql_canister_query_endpoint_executes_global_post_aggregate_value_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let post_aggregate = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded, COUNT(*) + 1 AS count_plus_one, MAX(age) - MIN(age) AS spread \
             FROM SqlTestUser",
        )
        .expect("global post-aggregate SQL query should succeed"),
    );

    assert_eq!(
        post_aggregate,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec![
                "avg_rounded".to_string(),
                "count_plus_one".to_string(),
                "spread".to_string(),
            ],
            rows: vec![vec!["32.67".to_string(), "4".to_string(), "19".to_string(),]],
            row_count: 1,
        },
        "query(sql) should preserve the real reduced values for global post-aggregate projection expressions at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_global_aggregate_having_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let matched = expect_projection(
        query_sql(
            &fixture,
            "SELECT COUNT(*) FROM SqlTestUser HAVING COUNT(*) > 1",
        )
        .expect("global aggregate HAVING SQL query should succeed"),
    );
    assert_eq!(
        matched,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["COUNT(*)".to_string()],
            rows: vec![vec!["3".to_string()]],
            row_count: 1,
        },
        "query(sql) should keep the implicit aggregate row when global HAVING matches",
    );

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded FROM SqlTestUser HAVING AVG(age) > 40",
        )
        .expect("global aggregate HAVING should still return projection payload when filtered"),
    );
    assert_eq!(
        filtered,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["avg_rounded".to_string()],
            rows: vec![],
            row_count: 0,
        },
        "query(sql) should filter away the implicit aggregate row while preserving the projection shape when global HAVING fails",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_aggregate_combo_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT age, AVG(age + 1) AS avg_plus_one \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING AVG(age + 1) > 25 \
             ORDER BY avg_plus_one DESC, age ASC \
             LIMIT 2",
        )
        .expect("grouped aggregate combination SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["age".to_string(), "avg_plus_one".to_string()],
            rows: vec![
                vec!["43".to_string(), "44".to_string()],
                vec!["31".to_string(), "32".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve grouped aggregate-input, HAVING, and Top-K ordering values together at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_wrapped_aggregate_input_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT name, ROUND(AVG(age + 1 * 2), 2) AS avg_boosted \
             FROM SqlTestUser \
             GROUP BY name \
             ORDER BY avg_boosted DESC, name ASC \
             LIMIT 2",
        )
        .expect("grouped wrapped aggregate-input ORDER BY alias SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "avg_boosted".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "45.00".to_string()],
                vec!["alice".to_string(), "33.00".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve wrapped grouped aggregate-input ordering values at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_parenthesized_aggregate_input_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT name, ROUND(AVG((age + age) / 2), 2) AS avg_balanced \
             FROM SqlTestUser \
             GROUP BY name \
             ORDER BY avg_balanced DESC, name ASC \
             LIMIT 2",
        )
        .expect("grouped parenthesized aggregate-input ORDER BY alias SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "avg_balanced".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "43.00".to_string()],
                vec!["alice".to_string(), "31.00".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve parenthesized grouped aggregate-input ordering values at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_keeps_canonical_equivalent_grouped_having_explain_identity() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let left = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT age, COUNT(*) \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING age >= 24 AND COUNT(*) > 0 \
             ORDER BY age ASC \
             LIMIT 10",
        )
        .expect("left grouped HAVING explain query should succeed"),
    );
    let right = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT age, COUNT(*) \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING COUNT(*) > 0 AND age >= 24 \
             ORDER BY age ASC \
             LIMIT 10",
        )
        .expect("right grouped HAVING explain query should succeed"),
    );

    assert_eq!(
        left, right,
        "public SQL explain should keep canonical-equivalent grouped HAVING order on the same outward identity surface",
    );
}

#[test]
fn sql_canister_query_endpoint_surfaces_semantic_reuse_diagnostics_on_verbose_explain() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let first = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT name \
             FROM SqlTestUser \
             WHERE age >= 24 AND age < 50 \
             ORDER BY age ASC \
             LIMIT 2",
        )
        .expect("first verbose explain query should succeed"),
    );
    let second = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT name \
             FROM SqlTestUser \
             WHERE age < 50 AND age >= 24 \
             ORDER BY age ASC \
             LIMIT 2",
        )
        .expect("second verbose explain query should succeed"),
    );

    assert!(
        first.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && first.contains("diag.s.semantic_reuse=miss"),
        "first public SQL verbose explain should report one shared query-plan miss: {first}",
    );
    assert!(
        second.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && second.contains("diag.s.semantic_reuse=miss"),
        "public SQL query entrypoints should surface one honest shared query-plan miss on each isolated query call: {second}",
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
fn sql_canister_query_endpoint_executes_chained_scalar_arithmetic_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let precedence = expect_projection(
        query_sql(
            &fixture,
            "SELECT age + 1 * 2 AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("chained scalar precedence SQL query should succeed"),
    );
    assert_eq!(
        precedence,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["value".to_string()],
            rows: vec![vec!["26".to_string()], vec!["33".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve multiplication precedence inside chained scalar arithmetic at the live canister boundary",
    );

    let associativity = expect_projection(
        query_sql(
            &fixture,
            "SELECT age - 1 - 2 AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("chained scalar associativity SQL query should succeed"),
    );
    assert_eq!(
        associativity,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["value".to_string()],
            rows: vec![vec!["21".to_string()], vec!["28".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve left-associative subtraction inside chained scalar arithmetic at the live canister boundary",
    );

    let parenthesized = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND((age + rank) / 2, 2) AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("parenthesized scalar ROUND SQL query should succeed"),
    );
    assert_eq!(
        parenthesized,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["value".to_string()],
            rows: vec![vec!["24.50".to_string()], vec!["29.50".to_string()]],
            row_count: 2,
        },
        "query(sql) should preserve parenthesized scalar arithmetic before ROUND at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_chained_global_aggregate_expression_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let result = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age + 1 * 2), 2) AS avg_shifted, ROUND(AVG((age + age) / 2), 2) AS avg_balanced FROM SqlTestUser",
        )
        .expect("chained global aggregate expression SQL query should succeed"),
    );
    assert_eq!(
        result,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["avg_shifted".to_string(), "avg_balanced".to_string()],
            rows: vec![vec!["34.67".to_string(), "32.67".to_string()]],
            row_count: 1,
        },
        "query(sql) should preserve chained aggregate-input and parenthesized global post-aggregate values at the live canister boundary",
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
fn sql_canister_numeric_type_endpoint_executes_small_width_numeric_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let small_width = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, nat16_value + 1, nat8_value + nat16_value, int8_value - 1 \
             FROM SqlTestNumericTypes \
             ORDER BY label \
             LIMIT 10",
        )
        .expect("mixed small-width numeric SQL query should succeed"),
    );
    assert_eq!(
        small_width,
        SqlQueryRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "label".to_string(),
                "nat16_value + 1".to_string(),
                "nat8_value + nat16_value".to_string(),
                "int8_value - 1".to_string(),
            ],
            rows: vec![
                vec![
                    "alpha".to_string(),
                    "4".to_string(),
                    "17".to_string(),
                    "-2".to_string(),
                ],
                vec![
                    "beta".to_string(),
                    "8".to_string(),
                    "23".to_string(),
                    "1".to_string(),
                ],
            ],
            row_count: 2,
        },
        "query(sql) should preserve Int8/Nat8/Nat16 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_wide_integer_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let wide_width = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, int16_value + int32_value, int64_value + nat64_value, nat32_value + nat64_value \
             FROM SqlTestNumericTypes \
             ORDER BY nat16_value DESC \
             LIMIT 10",
        )
        .expect("mixed wide numeric SQL query should succeed"),
    );
    assert_eq!(
        wide_width,
        SqlQueryRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "label".to_string(),
                "int16_value + int32_value".to_string(),
                "int64_value + nat64_value".to_string(),
                "nat32_value + nat64_value".to_string(),
            ],
            rows: vec![
                vec![
                    "beta".to_string(),
                    "63".to_string(),
                    "18000".to_string(),
                    "9300".to_string(),
                ],
                vec![
                    "alpha".to_string(),
                    "33".to_string(),
                    "500".to_string(),
                    "1120".to_string(),
                ],
            ],
            row_count: 2,
        },
        "query(sql) should preserve Int16/Int32/Int64 and Nat32/Nat64 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_decimal_float_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let decimal_float = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, ROUND(decimal_value * 100, 2), TRUNC(decimal_value / 3, 2), float64_value / 2, ROUND(float32_value + float64_value, 2) \
             FROM SqlTestNumericTypes \
             ORDER BY decimal_value DESC \
             LIMIT 10",
        )
        .expect("decimal and float numeric SQL query should succeed"),
    );
    assert_eq!(
        decimal_float,
        SqlQueryRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "label".to_string(),
                "ROUND(decimal_value * 100, 2)".to_string(),
                "TRUNC(decimal_value / 3, 2)".to_string(),
                "float64_value / 2".to_string(),
                "ROUND(float32_value + float64_value, 2)".to_string(),
            ],
            rows: vec![
                vec![
                    "beta".to_string(),
                    "25.00".to_string(),
                    "0.08".to_string(),
                    "0.125".to_string(),
                    "0.50".to_string(),
                ],
                vec![
                    "alpha".to_string(),
                    "15.00".to_string(),
                    "0.05".to_string(),
                    "0.25".to_string(),
                    "1.25".to_string(),
                ],
            ],
            row_count: 2,
        },
        "query(sql) should preserve Decimal/Float32/Float64 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_mixed_numeric_aggregate_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let global = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT COUNT(*), SUM(nat16_value), AVG(int32_value), MIN(int16_value), MAX(nat64_value) \
             FROM SqlTestNumericTypes",
        )
        .expect("global mixed numeric aggregate SQL query should succeed"),
    );
    assert_eq!(
        global,
        SqlQueryRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "COUNT(*)".to_string(),
                "SUM(nat16_value)".to_string(),
                "AVG(int32_value)".to_string(),
                "MIN(int16_value)".to_string(),
                "MAX(nat64_value)".to_string(),
            ],
            rows: vec![vec![
                "2".to_string(),
                "10".to_string(),
                "46.5".to_string(),
                "-2".to_string(),
                "9000".to_string(),
            ]],
            row_count: 1,
        },
        "query(sql) should preserve mixed numeric global aggregates at the schema/test SQL canister boundary",
    );

    let grouped = expect_grouped(
        query_numeric_types(
            &fixture,
            "SELECT group_name, SUM(nat32_value), AVG(decimal_value), MAX(float64_value) \
             FROM SqlTestNumericTypes \
             GROUP BY group_name \
             ORDER BY group_name \
             LIMIT 50",
        )
        .expect("grouped mixed numeric aggregate SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "group_name".to_string(),
                "SUM(nat32_value)".to_string(),
                "AVG(decimal_value)".to_string(),
                "MAX(float64_value)".to_string(),
            ],
            rows: vec![
                vec![
                    "fighter".to_string(),
                    "300".to_string(),
                    "0.25".to_string(),
                    "0.25".to_string(),
                ],
                vec![
                    "mage".to_string(),
                    "120".to_string(),
                    "0.15".to_string(),
                    "0.5".to_string(),
                ],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve mixed numeric grouped aggregates at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_reports_numeric_overflow_errors() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for sql in [
        "SELECT label, POWER(nat16_value + nat8_value, 100) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(nat64_value + 1, 20) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(decimal_value + 100, 80) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(int16_value - 1000, 99) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT SUM(POWER(nat16_value, 100)) \
         FROM SqlTestNumericTypes",
        "SELECT group_name, AVG(POWER(nat32_value, 50)) \
         FROM SqlTestNumericTypes \
         GROUP BY group_name \
         ORDER BY group_name \
         LIMIT 50",
    ] {
        let err = query_numeric_types(&fixture, sql)
            .expect_err("overflowing mixed numeric SQL should fail");

        assert_numeric_query_error(err, "numeric overflow", sql);
    }
}

#[test]
fn sql_canister_numeric_type_endpoint_reports_numeric_not_representable_errors() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for sql in [
        "SELECT label, nat16_value / 0 \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, MOD(nat64_value, 0) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, SQRT(int16_value - 1000) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
    ] {
        let err = query_numeric_types(&fixture, sql)
            .expect_err("non-representable mixed numeric SQL should fail");

        assert_numeric_query_error(err, "numeric result is not representable", sql);
    }
}

#[test]
fn sql_canister_query_endpoint_executes_singleton_global_output_order_alias_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let ordered = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded FROM SqlTestUser ORDER BY avg_rounded DESC",
        )
        .expect("singleton global aggregate output ORDER BY alias SQL query should succeed"),
    );
    assert_eq!(
        ordered,
        SqlQueryRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["avg_rounded".to_string()],
            rows: vec![vec!["32.67".to_string()]],
            row_count: 1,
        },
        "query(sql) should treat singleton global aggregate output ordering as an inert no-op while still returning the correct value",
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
