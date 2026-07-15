use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode, ErrorOrigin,
    db::{
        EntitySchemaDescription, RowProjectionOutput,
        sql::{SqlGroupedRowsOutput, SqlQueryResult},
    },
    diagnostic::DiagnosticCode,
};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
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
    // a fresh standalone IC testkit instance with empty init args.
    install_fixture_canister("sql")
}

fn install_sql_bounded_canister_fixture() -> StandaloneCanisterFixture {
    // Reuse the SQL smoke canister code with the bounded generated update
    // policy selected in icydb.toml.
    install_fixture_canister("sql_bounded")
}

fn install_demo_rpg_canister_fixture() -> StandaloneCanisterFixture {
    // The demo RPG canister has one generated entity, making it a useful
    // boundary fixture for proving generated DDL still requires explicit targets.
    install_fixture_canister("demo_rpg")
}

fn reset_sql_fixtures(fixture: &StandaloneCanisterFixture) {
    // Keep each test isolated by resetting and then loading the deterministic
    // baseline fixture set through the live canister update surface.
    reset_icydb_fixtures(fixture);
}

fn seed_oversized_sql_group_name(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_call("seed_oversized_sql_group_name", ())
        .expect("oversized SQL group-name seed call should decode");

    result.expect("oversized SQL group-name seed should succeed");
}

fn query_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    let response: Result<SqlQueryPerfResult, Error> = fixture
        .query_call("icydb_query", (sql.to_string(),))
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
        .update_call("icydb_ddl", (sql.to_string(),))
        .expect("sql DDL canister call should decode")
}

fn update_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .update_call("icydb_update", (sql.to_string(),))
        .expect("sql update canister call should decode")
}

#[derive(Clone, Copy, Debug)]
struct DdlSchemaVersion {
    current: u32,
}

impl DdlSchemaVersion {
    const fn initial() -> Self {
        Self { current: 1 }
    }

    fn publish(
        &mut self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        let result = ddl_sql(fixture, &ddl_transition_sql(sql, self.current));
        if result.is_ok() {
            self.current = self
                .current
                .checked_add(1)
                .expect("test schema version should fit u32");
        }
        result
    }

    fn reject(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        ddl_sql(fixture, &ddl_transition_sql(sql, self.current))
    }

    fn no_op(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        ddl_sql(fixture, &ddl_expected_sql(sql, self.current))
    }
}

fn ddl_transition_sql(sql: &str, expected_schema_version: u32) -> String {
    ddl_contract_sql(
        sql,
        &format!(
            "EXPECT SCHEMA VERSION {expected_schema_version} SET SCHEMA VERSION {}",
            expected_schema_version
                .checked_add(1)
                .expect("test schema version should fit u32"),
        ),
    )
}

fn ddl_expected_sql(sql: &str, expected_schema_version: u32) -> String {
    ddl_contract_sql(
        sql,
        &format!("EXPECT SCHEMA VERSION {expected_schema_version}"),
    )
}

fn ddl_contract_sql(sql: &str, contract: &str) -> String {
    if let Some(where_offset) = sql.find(" WHERE ") {
        format!(
            "{} {contract}{}",
            &sql[..where_offset],
            &sql[where_offset..],
        )
    } else {
        format!("{sql} {contract}")
    }
}

fn expect_projection(result: SqlQueryResult) -> RowProjectionOutput {
    match result {
        SqlQueryResult::Projection(rows) => rows,
        other => panic!("expected projection payload, got {other:?}"),
    }
}

fn first_projected_text(output: &RowProjectionOutput) -> String {
    output
        .rendered_rows()
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .expect("projection should include a first text cell")
}

fn assert_projection_rendered(
    output: &RowProjectionOutput,
    entity: &str,
    columns: &[&str],
    rows: &[&[&str]],
    row_count: u32,
    message: &str,
) {
    assert_eq!(output.entity, entity, "{message}");
    assert_eq!(output.columns, columns, "{message}");
    assert_eq!(output.rendered_rows(), string_rows(rows), "{message}");
    assert_eq!(output.row_count, row_count, "{message}");
}

fn string_rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|value| (*value).to_string()).collect())
        .collect()
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
        other => panic!("expected SHOW INDEXES FROM payload, got {other:?}"),
    }
}

fn sql_test_user_id_by_name(fixture: &StandaloneCanisterFixture, name: &str) -> String {
    let sql = format!("SELECT id FROM SqlTestUser WHERE name = '{name}'");
    let output = expect_projection(
        query_sql(fixture, sql.as_str()).expect("fixture id read should find the named user"),
    );

    assert_eq!(
        output.row_count, 1,
        "named SQL fixture user should be unique",
    );
    first_projected_text(&output)
}

fn sql_test_numeric_type_id_by_label(fixture: &StandaloneCanisterFixture, label: &str) -> String {
    let sql = format!("SELECT id FROM SqlTestNumericTypes WHERE label = '{label}'");
    let output = expect_projection(
        query_sql(fixture, sql.as_str()).expect("fixture id read should find the labeled row"),
    );

    assert_eq!(
        output.row_count, 1,
        "labeled numeric SQL fixture row should be unique",
    );
    first_projected_text(&output)
}

fn assert_ddl_no_op(result: SqlQueryResult, expected_kind: &str, expected_target: &str) {
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = result
    else {
        panic!("no-op DDL should return a DDL payload");
    };

    assert_eq!(mutation_kind, expected_kind);
    assert_eq!(target_index, expected_target);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

fn assert_rename_column_ddl_report(result: SqlQueryResult) {
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        target_store,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
    } = result
    else {
        panic!("RENAME COLUMN should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "rename_field");
    assert_eq!(target_index, "handle");
    assert_eq!(target_store, "SqlTestUser");
    assert_eq!(
        field_path,
        vec!["nickname".to_string(), "handle".to_string()],
    );
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

fn assert_rename_column_schema_visibility(
    before: &EntitySchemaDescription,
    after: &EntitySchemaDescription,
) {
    assert!(
        before
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "setup should expose DDL-owned source field before RENAME COLUMN",
    );
    assert!(
        !after
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "published RENAME COLUMN should remove the old accepted field name",
    );
    assert!(
        after.fields().iter().any(|field| field.name() == "handle"),
        "published RENAME COLUMN should expose the new accepted field name",
    );
}

fn assert_rename_column_index_visibility(indexes: &[String]) {
    assert!(
        indexes
            .iter()
            .any(|index| index
                == "INDEX sql_test_user_nickname_idx (handle) [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update field-path index metadata: {indexes:?}",
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_lower_nickname_idx (expr:v1:LOWER(handle)) [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update expression index metadata: {indexes:?}",
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_filtered_nickname_idx (handle) WHERE handle IS NOT NULL [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update filtered index predicate metadata: {indexes:?}",
    );
}

fn assert_runtime_unsupported_query_error(err: &Error, context: &str) {
    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "{context} should stay an unsupported runtime error at the canister boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
}

fn assert_query_sql_surface_mismatch_error(err: &Error, expected: ErrorCode, context: &str) {
    assert_eq!(
        err.diagnostic_code(),
        DiagnosticCode::QuerySqlSurfaceMismatch,
        "{context} should stay at the compact SQL surface mismatch boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
    assert_eq!(
        err.code(),
        expected,
        "{context} should preserve the numeric SQL surface mismatch leaf code",
    );
}

fn assert_ddl_rejection_error(err: &Error, context: &str) {
    assert!(
        matches!(
            err.origin(),
            ErrorOrigin::Query | ErrorOrigin::Store | ErrorOrigin::Interface
        ),
        "{context} should keep query, store, or interface origin metadata, got {:?}",
        err.origin(),
    );

    match err.diagnostic_code() {
        DiagnosticCode::SchemaDdlAdmission => assert!(
            err.code().raw() >= ErrorCode::SCHEMA_DDL_MISSING_EXPECTED_SCHEMA_VERSION.raw()
                && err.code().raw() <= ErrorCode::SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED.raw(),
            "{context} should preserve a numeric schema DDL admission leaf code",
        ),
        DiagnosticCode::QueryUnsupportedSqlFeature => assert!(
            err.code() != ErrorCode::QUERY_UNSUPPORTED_SQL_FEATURE,
            "{context} should preserve a numeric unsupported SQL feature leaf code",
        ),
        DiagnosticCode::RuntimeUnsupported if err.origin() == ErrorOrigin::Interface => assert!(
            matches!(
                err.code(),
                ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED
                    | ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED
            ),
            "{context} should preserve a numeric generated DDL boundary leaf code",
        ),
        DiagnosticCode::RuntimeUnsupported => {}
        other => panic!(
            "{context} should reject as compact DDL admission, unsupported SQL feature, or unsupported runtime, got {other:?}"
        ),
    }
}

fn assert_numeric_query_error(err: Error, expected_code: ErrorCode, context: &str) {
    assert!(
        matches!(
            expected_code,
            ErrorCode::QUERY_NUMERIC_OVERFLOW | ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE
        ),
        "numeric query assertions must use numeric diagnostic codes",
    );
    assert_eq!(
        err.code(),
        expected_code,
        "{context} should preserve numeric compact diagnostic code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
}

fn assert_ddl_rejects_without_index_visibility_change(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    sql: &str,
    forbidden_visibility_fragment: &str,
) {
    let before = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes before rejected DDL"),
    );
    let err = schema_version
        .reject(fixture, sql)
        .expect_err("invalid DDL should reject");

    assert_ddl_rejection_error(
        &err,
        "invalid DDL should stay at the schema DDL admission boundary",
    );
    let after = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should still read accepted indexes after rejected DDL"),
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

fn assert_ddl_rejects_with_index_visibility_unchanged(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    sql: &str,
) -> Error {
    assert_ddl_rejects_with_entity_index_visibility_unchanged(
        fixture,
        schema_version,
        "SqlTestUser",
        sql,
    )
}

fn assert_ddl_rejects_with_entity_index_visibility_unchanged(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    entity: &str,
    sql: &str,
) -> Error {
    let before = expect_show_indexes(
        query_sql(fixture, &format!("SHOW INDEXES FROM {entity}"))
            .expect("SHOW INDEXES FROM should read accepted indexes before rejected DDL"),
    );
    let err = schema_version
        .reject(fixture, sql)
        .expect_err("invalid DDL should reject");

    assert_ddl_rejection_error(
        &err,
        "invalid DDL should stay at the schema DDL admission boundary",
    );
    let after = expect_show_indexes(
        query_sql(fixture, &format!("SHOW INDEXES FROM {entity}"))
            .expect("SHOW INDEXES FROM should still read accepted indexes after rejected DDL"),
    );
    assert_eq!(
        after, before,
        "rejected DDL must leave accepted index visibility unchanged",
    );

    err
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
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
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DDL publication"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published accepted index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_expression_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_lower_name_idx ON SqlTestUser (LOWER(name))",
        )
        .expect(
            "supported expression CREATE INDEX DDL should publish through the canister endpoint",
        );

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
        panic!("supported expression CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes =
        expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
            "SHOW INDEXES FROM should read accepted indexes after expression DDL publication",
        ));
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_lower_name_idx (expr:v1:LOWER(name)) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published expression index: {indexes:?}",
    );

    let no_op = schema_version
        .no_op(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_lower_name_idx ON SqlTestUser (LOWER(name))",
        )
        .expect(
            "matching expression CREATE INDEX IF NOT EXISTS should no-op at the canister endpoint",
        );
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = no_op
    else {
        panic!("matching expression CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_unique_expression_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
        &fixture,
        "CREATE UNIQUE INDEX sql_test_user_lower_name_unique_idx ON SqlTestUser (LOWER(name))",
    )
    .expect(
        "supported unique expression CREATE INDEX DDL should publish through the canister endpoint",
    );

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
        panic!("supported unique expression CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_unique_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
        "SHOW INDEXES FROM should read accepted indexes after unique expression DDL publication",
    ));
    assert!(
        indexes.iter().any(|index| index
            == "UNIQUE INDEX sql_test_user_lower_name_unique_idx (expr:v1:LOWER(name)) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published unique expression index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_filtered_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_filtered_rank_idx ON SqlTestUser (rank) WHERE age > 30",
        )
        .expect("supported filtered CREATE INDEX DDL should publish through the canister endpoint");

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
        panic!("supported filtered CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_filtered_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 2);

    let indexes =
        expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
            "SHOW INDEXES FROM should read accepted indexes after filtered DDL publication",
        ));
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_filtered_rank_idx (rank) WHERE age > 30 [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published filtered index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_multi_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_age_idx ON SqlTestUser (rank, age)",
        )
        .expect(
            "supported multi-field CREATE INDEX DDL should publish through the canister endpoint",
        );

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
        panic!("supported multi-field CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_age_idx");
    assert_eq!(field_path, vec!["rank,age".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_rank_age_idx (rank, age) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published composite index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_treats_asc_index_order_as_default_order() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_age_asc_idx ON SqlTestUser (rank ASC, age ASC)",
        )
        .expect("CREATE INDEX with explicit ASC should publish through the canister endpoint");

    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported ASC CREATE INDEX should return a DDL payload");
    };
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_age_asc_idx");
    assert_eq!(field_path, vec!["rank,age".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after ASC DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_rank_age_asc_idx (rank, age) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose explicit ASC as the default index order: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_and_drops_supported_unique_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE UNIQUE INDEX sql_test_user_unique_rank_idx ON SqlTestUser (rank)",
        )
        .expect("supported CREATE UNIQUE INDEX DDL should publish through the canister endpoint");
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
        panic!("supported CREATE UNIQUE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_unique_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after unique DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "UNIQUE INDEX sql_test_user_unique_rank_idx (rank) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published unique index: {indexes:?}",
    );

    let ddl = schema_version
        .publish(
            &fixture,
            "DROP INDEX sql_test_user_unique_rank_idx ON SqlTestUser",
        )
        .expect("supported DROP INDEX should remove a DDL-published unique field-path index");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = ddl
    else {
        panic!("supported DROP INDEX should return a DDL payload");
    };
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_unique_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after unique DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_unique_rank_idx")),
        "SHOW INDEXES FROM should hide the dropped DDL unique index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_drops_supported_ddl_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before DROP INDEX");

    let ddl = schema_version
        .publish(&fixture, "DROP INDEX sql_test_user_rank_idx ON SqlTestUser")
        .expect("supported DROP INDEX DDL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = ddl
    else {
        panic!("supported DROP INDEX should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_rank_idx")),
        "SHOW INDEXES FROM should hide the dropped DDL index: {indexes:?}",
    );
}

#[test]
fn demo_rpg_ddl_endpoint_rejects_targetless_drop_index() {
    let fixture = install_demo_rpg_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX character_renown_idx ON Character (renown)",
        )
        .expect("setup CREATE INDEX should publish before targetless DROP INDEX");

    assert_ddl_rejects_with_entity_index_visibility_unchanged(
        &fixture,
        schema_version,
        "Character",
        "DROP INDEX character_renown_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_ambiguous_drop_index_shorthand() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before ambiguous DROP INDEX");

    assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX sql_test_user_rank_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_generated_index_drop_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let err = assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX idx_sql_test_user__name ON SqlTestUser",
    );
    assert_eq!(
        err.code(),
        ErrorCode::SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED,
        "generated index drop should preserve the compact DDL leaf code",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_create_index_if_not_exists_for_absent_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("absent CREATE INDEX IF NOT EXISTS should publish through the canister endpoint");
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
        panic!("absent CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after idempotent CREATE INDEX"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]"),
        "CREATE INDEX IF NOT EXISTS should expose the published accepted index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_noops_create_index_if_not_exists_for_existing_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before idempotent CREATE INDEX");

    let ddl = schema_version
        .no_op(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("matching CREATE INDEX IF NOT EXISTS should no-op through the canister endpoint");
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
        panic!("matching CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after no-op CREATE INDEX"),
    );
    let rank_index = "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]";
    let occurrences = indexes
        .iter()
        .filter(|index| index.as_str() == rank_index)
        .count();
    assert_eq!(
        occurrences, 1,
        "no-op CREATE INDEX IF NOT EXISTS should not duplicate accepted indexes: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_conflicting_create_index_if_not_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before conflicting idempotent CREATE INDEX");

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (age)",
        "INDEX sql_test_user_rank_idx (age)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_drops_existing_index_with_if_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before idempotent DROP INDEX");

    let ddl = schema_version
        .publish(
            &fixture,
            "DROP INDEX IF EXISTS sql_test_user_rank_idx ON SqlTestUser",
        )
        .expect("existing DROP INDEX IF EXISTS should publish through the canister endpoint");
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
        panic!("existing DROP INDEX IF EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after idempotent DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_rank_idx")),
        "DROP INDEX IF EXISTS should hide the dropped DDL index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_noops_drop_index_if_exists_for_missing_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let before = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes before no-op DROP INDEX"),
    );
    let ddl = schema_version
        .no_op(
            &fixture,
            "DROP INDEX IF EXISTS sql_test_user_missing_idx ON SqlTestUser",
        )
        .expect("missing DROP INDEX IF EXISTS should no-op through the canister endpoint");
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
        panic!("missing DROP INDEX IF EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_missing_idx");
    assert!(field_path.is_empty());
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let after = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after no-op DROP INDEX"),
    );
    assert_eq!(
        after, before,
        "no-op DROP INDEX IF EXISTS should leave accepted index visibility unchanged",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_generated_index_drop_with_if_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let err = assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX IF EXISTS idx_sql_test_user__name ON SqlTestUser",
    );
    assert_eq!(
        err.code(),
        ErrorCode::SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED,
        "generated index DROP IF EXISTS should preserve the compact DDL leaf code",
    );
}

#[test]
fn sql_canister_ddl_publication_updates_describe_explain_and_reads() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

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

    schema_version
        .publish(
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
                && index.origin() == "ddl"
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
    assert_projection_rendered(
        &rows,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
        "post-DDL indexed read should observe the accepted-after index without changing row semantics",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unknown_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_missing_idx ON SqlTestUser (missing)",
        "sql_test_user_missing_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_index_name_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before duplicate-name rejection");

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (age)",
        "INDEX sql_test_user_rank_idx (age)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_duplicate_name_idx ON SqlTestUser (name)",
        "sql_test_user_duplicate_name_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unsupported_create_index_shapes_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_rank_desc_idx ON SqlTestUser (rank DESC)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_alter_column_default() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN bonus nat64")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    let set_default = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 7",
        )
        .expect("ALTER COLUMN SET DEFAULT should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = set_default
    else {
        panic!("ALTER COLUMN SET DEFAULT should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "set_field_default");
    assert_eq!(target_index, "bonus");
    assert_eq!(field_path, vec!["bonus".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let describe_after_set = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after SET DEFAULT"),
    );
    assert!(
        describe_after_set.fields().iter().any(|field| {
            field.name() == "bonus"
                && field.kind().starts_with("nat64 default=slot_payload(")
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted default change: {describe_after_set:?}",
    );
    let set_default_no_op = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 7",
        )
        .expect("matching ALTER COLUMN SET DEFAULT should no-op through the canister endpoint");
    assert_ddl_no_op(set_default_no_op, "set_field_default", "bonus");

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN nickname text DEFAULT 'anonymous'",
        )
        .expect("setup nullable defaulted ADD COLUMN should publish through the canister endpoint");
    let drop_default = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP DEFAULT",
        )
        .expect("ALTER COLUMN DROP DEFAULT should publish for nullable accepted fields");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = drop_default
    else {
        panic!("ALTER COLUMN DROP DEFAULT should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_field_default");
    assert_eq!(target_index, "nickname");
    assert_eq!(field_path, vec!["nickname".to_string()]);
    assert_eq!(status, "published");

    let describe_after_drop = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after DROP DEFAULT"),
    );
    assert!(
        describe_after_drop.fields().iter().any(|field| {
            field.name() == "nickname"
                && field.kind() == "text(unbounded)"
                && field.nullable()
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted default removal: {describe_after_drop:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_alter_column_nullability() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN nickname text DEFAULT 'anonymous'",
        )
        .expect("setup nullable defaulted ADD COLUMN should publish through the canister endpoint");
    let set_not_null = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname SET NOT NULL",
        )
        .expect("ALTER COLUMN SET NOT NULL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = set_not_null
    else {
        panic!("ALTER COLUMN SET NOT NULL should return a DDL payload");
    };
    assert_eq!(mutation_kind, "set_field_not_null");
    assert_eq!(target_index, "nickname");
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 0);

    let describe_after_set = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after SET NOT NULL"),
    );
    assert!(
        describe_after_set.fields().iter().any(|field| {
            field.name() == "nickname"
                && !field.nullable()
                && field
                    .kind()
                    .starts_with("text(unbounded) default=slot_payload(")
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted nullability change: {describe_after_set:?}",
    );

    let drop_not_null = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP NOT NULL",
        )
        .expect("ALTER COLUMN DROP NOT NULL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = drop_not_null
    else {
        panic!("ALTER COLUMN DROP NOT NULL should return a DDL payload");
    };
    assert_eq!(mutation_kind, "drop_field_not_null");
    assert_eq!(target_index, "nickname");
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let drop_not_null_no_op = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP NOT NULL",
        )
        .expect("matching ALTER COLUMN DROP NOT NULL should no-op through the canister endpoint");
    assert_ddl_no_op(drop_not_null_no_op, "drop_field_not_null", "nickname");
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unsupported_alter_column_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN required_score nat64 NOT NULL DEFAULT 7",
        )
        .expect("setup required ADD COLUMN DEFAULT should publish before unsupported DROP DEFAULT");
    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN bonus nat64")
        .expect("setup nullable ADD COLUMN should publish before invalid SET DEFAULT");

    for (sql, expected_code) in [
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN rank SET DEFAULT 7",
            ErrorCode::SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 'seven'",
            ErrorCode::SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN rank DROP NOT NULL",
            ErrorCode::SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET NOT NULL",
            ErrorCode::SCHEMA_DDL_SET_NOT_NULL_VALIDATION_FAILED,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN required_score DROP DEFAULT",
            ErrorCode::SCHEMA_DDL_REQUIRED_DROP_DEFAULT_UNSUPPORTED,
        ),
    ] {
        let before = expect_describe(
            query_sql(&fixture, "DESCRIBE SqlTestUser")
                .expect("DESCRIBE should read accepted schema before rejected ALTER COLUMN"),
        );
        let err = schema_version
            .reject(&fixture, sql)
            .expect_err("ALTER COLUMN should reject before publication");
        assert_ddl_rejection_error(
            &err,
            "ALTER COLUMN should stay at the schema DDL admission boundary",
        );
        assert_eq!(
            err.code(),
            expected_code,
            "{sql} should preserve the compact DDL admission leaf code",
        );
        let after = expect_describe(
            query_sql(&fixture, "DESCRIBE SqlTestUser")
                .expect("DESCRIBE should read accepted schema after rejected ALTER COLUMN"),
        );
        assert_eq!(
            after, before,
            "rejected ALTER COLUMN must leave accepted schema visibility unchanged",
        );
    }
}

#[test]
fn sql_canister_ddl_endpoint_publishes_drop_column_for_ddl_owned_field() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let missing = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser DROP COLUMN IF EXISTS missing",
        )
        .expect("DROP COLUMN IF EXISTS should no-op for missing accepted fields");
    assert_ddl_no_op(missing, "drop_field", "missing");

    let generated_err = schema_version
        .reject(&fixture, "ALTER TABLE SqlTestUser DROP COLUMN rank")
        .expect_err("DROP COLUMN should reject generated accepted fields");
    assert_ddl_rejection_error(
        &generated_err,
        "generated DROP COLUMN rejection should stay at the schema DDL admission boundary",
    );

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN nickname text")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN handle text")
        .expect("setup second nullable ADD COLUMN should publish through the canister endpoint");
    let before = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema before DROP COLUMN"),
    );
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        target_store,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
    } = schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser DROP COLUMN nickname")
        .expect("DROP COLUMN should publish retained-slot field removal")
    else {
        panic!("DROP COLUMN should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_field");
    assert_eq!(target_index, "nickname");
    assert_eq!(target_store, "SqlTestUser");
    assert_eq!(field_path, vec!["nickname".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 0);
    let after = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after DROP COLUMN"),
    );
    assert!(
        before
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "setup should expose DDL-owned field before DROP COLUMN",
    );
    assert!(
        before.fields().iter().any(|field| field.name() == "handle"),
        "setup should expose later DDL-owned field before DROP COLUMN",
    );
    assert!(
        !after
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "published DROP COLUMN should remove the accepted field",
    );
    assert!(
        after.fields().iter().any(|field| field.name() == "handle"),
        "published non-trailing DROP COLUMN should preserve later active fields",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_rename_column_for_ddl_owned_field() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let same_name = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN rank TO rank",
        )
        .expect("same-name RENAME COLUMN should no-op through the canister endpoint");
    assert_ddl_no_op(same_name, "rename_field", "rank");

    let generated_err = schema_version
        .reject(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN rank TO score",
        )
        .expect_err("RENAME COLUMN should reject generated accepted fields");
    assert_ddl_rejection_error(
        &generated_err,
        "generated RENAME COLUMN rejection should stay at the schema DDL admission boundary",
    );

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN nickname text")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_nickname_idx ON SqlTestUser (nickname)",
        )
        .expect("setup field-path CREATE INDEX should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_lower_nickname_idx ON SqlTestUser (LOWER(nickname))",
        )
        .expect("setup expression CREATE INDEX should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_filtered_nickname_idx ON SqlTestUser (nickname) WHERE nickname IS NOT NULL",
        )
        .expect("setup filtered CREATE INDEX should publish through the canister endpoint");
    let before = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema before RENAME COLUMN"),
    );
    let rename = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN nickname TO handle",
        )
        .expect("RENAME COLUMN should publish DDL-owned accepted field metadata");
    assert_rename_column_ddl_report(rename);

    let after = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("DESCRIBE should read accepted schema after RENAME COLUMN"),
    );
    assert_rename_column_schema_visibility(&before, &after);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES should read accepted index metadata after RENAME COLUMN"),
    );
    assert_rename_column_index_visibility(&indexes);
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
    assert_projection_rendered(
        &scalar,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
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

    assert_projection_rendered(
        &post_aggregate,
        "SqlTestUser",
        &["avg_rounded", "count_plus_one", "spread"],
        &[&["32.67", "4", "19"]],
        1,
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
    assert_projection_rendered(
        &matched,
        "SqlTestUser",
        &["COUNT(*)"],
        &[&["3"]],
        1,
        "query(sql) should keep the implicit aggregate row when global HAVING matches",
    );

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded FROM SqlTestUser HAVING AVG(age) > 40",
        )
        .expect("global aggregate HAVING should still return projection payload when filtered"),
    );
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["avg_rounded"],
        &[],
        0,
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
    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["age - 1"],
        &[&["23"], &["30"]],
        2,
        "query(sql) should preserve scalar arithmetic projection payloads at the live canister boundary",
    );

    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(age / 3, 2) FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar ROUND SQL query should succeed"),
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["ROUND(age / 3, 2)"],
        &[&["8.00"], &["10.33"]],
        2,
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
    assert_projection_rendered(
        &precedence,
        "SqlTestUser",
        &["value"],
        &[&["26"], &["33"]],
        2,
        "query(sql) should preserve multiplication precedence inside chained scalar arithmetic at the live canister boundary",
    );

    let associativity = expect_projection(
        query_sql(
            &fixture,
            "SELECT age - 1 - 2 AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("chained scalar associativity SQL query should succeed"),
    );
    assert_projection_rendered(
        &associativity,
        "SqlTestUser",
        &["value"],
        &[&["21"], &["28"]],
        2,
        "query(sql) should preserve left-associative subtraction inside chained scalar arithmetic at the live canister boundary",
    );

    let parenthesized = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND((age + rank) / 2, 2) AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("parenthesized scalar ROUND SQL query should succeed"),
    );
    assert_projection_rendered(
        &parenthesized,
        "SqlTestUser",
        &["value"],
        &[&["24.50"], &["29.50"]],
        2,
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
    assert_projection_rendered(
        &result,
        "SqlTestUser",
        &["avg_shifted", "avg_balanced"],
        &[&["34.67", "32.67"]],
        1,
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
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["total"],
        &[&["49.00"], &["59.00"]],
        2,
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
    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["total"],
        &[&["49"], &["59"]],
        2,
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
    assert_projection_rendered(
        &small_width,
        "SqlTestNumericTypes",
        &[
            "label",
            "nat16_value + 1",
            "nat8_value + nat16_value",
            "int8_value - 1",
        ],
        &[&["alpha", "4", "17", "-2"], &["beta", "8", "23", "1"]],
        2,
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
    assert_projection_rendered(
        &wide_width,
        "SqlTestNumericTypes",
        &[
            "label",
            "int16_value + int32_value",
            "int64_value + nat64_value",
            "nat32_value + nat64_value",
        ],
        &[
            &["beta", "63", "18000", "9300"],
            &["alpha", "33", "500", "1120"],
        ],
        2,
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
    assert_projection_rendered(
        &decimal_float,
        "SqlTestNumericTypes",
        &[
            "label",
            "ROUND(decimal_value * 100, 2)",
            "TRUNC(decimal_value / 3, 2)",
            "float64_value / 2",
            "ROUND(float32_value + float64_value, 2)",
        ],
        &[
            &["beta", "25.00", "0.08", "0.125", "0.50"],
            &["alpha", "15.00", "0.05", "0.25", "1.25"],
        ],
        2,
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
    assert_projection_rendered(
        &global,
        "SqlTestNumericTypes",
        &[
            "COUNT(*)",
            "SUM(nat16_value)",
            "AVG(int32_value)",
            "MIN(int16_value)",
            "MAX(nat64_value)",
        ],
        &[&["2", "10", "46.5", "-2", "9000"]],
        1,
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

        assert_numeric_query_error(err, ErrorCode::QUERY_NUMERIC_OVERFLOW, sql);
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

        assert_numeric_query_error(err, ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE, sql);
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
    assert_projection_rendered(
        &ordered,
        "SqlTestUser",
        &["avg_rounded"],
        &[&["32.67"]],
        1,
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

    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["name", "next_age"],
        &[&["bob", "25"], &["alice", "32"]],
        2,
        "query(sql) should preserve arithmetic alias ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &field_to_field,
        "SqlTestUser",
        &["name", "total"],
        &[&["bob", "49"], &["alice", "59"]],
        2,
        "query(sql) should preserve field-to-field arithmetic alias ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["name", "rounded_age"],
        &[&["charlie", "14.33"], &["alice", "10.33"]],
        2,
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

    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["name", "age"],
        &[&["bob", "24"], &["alice", "31"]],
        2,
        "query(sql) should preserve direct arithmetic ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["name", "age"],
        &[&["charlie", "43"], &["alice", "31"]],
        2,
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
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["alice"]],
        1,
        "query(sql) should preserve field-to-field predicate filtering at the live canister boundary",
    );

    let mixed = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age > 18 AND age > rank ORDER BY age ASC LIMIT 10",
        )
        .expect("mixed literal and field-to-field predicate SQL query should succeed"),
    );
    assert_projection_rendered(
        &mixed,
        "SqlTestUser",
        &["name"],
        &[&["alice"]],
        1,
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
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["charlie"]],
        2,
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
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["charlie"]],
        2,
        "query(sql) should preserve bounded NOT LIKE prefix filtering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_show_tables_alias() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(&fixture, "SHOW TABLES").expect_err("SHOW TABLES should reject");

    assert_eq!(
        err.diagnostic_code(),
        DiagnosticCode::QueryUnsupportedSqlFeature,
        "SHOW TABLES should remain outside the current SQL catalog vocabulary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "SHOW TABLES should keep query-owned origin metadata",
    );
    assert_eq!(
        err.code(),
        ErrorCode::SQL_FEATURE_SHOW_UNSUPPORTED_COMMAND,
        "SHOW TABLES should preserve the numeric unsupported-feature leaf code",
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

    assert_query_sql_surface_mismatch_error(
        &err,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_DELETE,
        "wrong-lane SQL should keep query-owned origin metadata",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_update_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 22 WHERE name = 'alice'",
    )
    .expect_err("query(sql) must reject UPDATE statements");

    assert_query_sql_surface_mismatch_error(
        &err,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_UPDATE,
        "query endpoint UPDATE rejection should stay at the SQL surface boundary",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_row_mutation_sql_without_row_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for (label, sql) in [
        (
            "INSERT",
            "INSERT INTO SqlTestUser (name, age) VALUES ('zara', 50)",
        ),
        (
            "UPDATE",
            "UPDATE SqlTestUser SET age = 22 WHERE name = 'alice'",
        ),
        ("DELETE", "DELETE FROM SqlTestUser WHERE name = 'bob'"),
    ] {
        let before = expect_projection(
            query_sql(
                &fixture,
                "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
            )
            .expect("pre-rejection read should prove the row set exists"),
        );
        let err = ddl_sql(&fixture, sql).expect_err("DDL endpoint must reject row mutations");

        assert_eq!(
            err.diagnostic_code(),
            DiagnosticCode::SchemaDdlAdmission,
            "DDL endpoint {label} rejection should stay at the schema DDL admission boundary",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "DDL endpoint {label} rejection should keep query-owned origin metadata",
        );
        assert_eq!(
            err.code(),
            ErrorCode::SCHEMA_DDL_VALIDATION_FAILED,
            "DDL endpoint {label} rejection should preserve the NotDdl validation leaf code",
        );
        let after = expect_projection(
            query_sql(
                &fixture,
                "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
            )
            .expect("post-rejection read should still execute"),
        );
        assert_eq!(
            after, before,
            "rejected DDL endpoint {label} must not mutate rows",
        );
    }
}

#[test]
fn sql_canister_update_endpoint_admits_primary_key_update_only() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, age FROM SqlTestUser WHERE name = 'alice'",
        )
        .expect("pre-update read should find alice"),
    );
    let alice_id = first_projected_text(&alice);
    let result = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 32 WHERE id = '{alice_id}'").as_str(),
    )
    .expect("configured generated SQL update endpoint should admit primary-key UPDATE");

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "SqlTestUser".to_string(),
            row_count: 1,
        },
    );
    let after = expect_projection(
        query_sql(&fixture, "SELECT age FROM SqlTestUser WHERE name = 'alice'")
            .expect("post-update read should find alice"),
    );
    assert_eq!(after.rendered_rows(), string_rows(&[&["32"]]));
}

#[test]
fn sql_canister_update_endpoint_rejects_non_primary_key_update_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE name = 'alice'",
    )
    .expect_err("configured generated SQL update endpoint must reject non-PK UPDATE");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "generated SQL update endpoint should preserve policy rejection code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "generated SQL update endpoint policy rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected generated SQL update endpoint call must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_primary_key_update_with_extra_guard_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let err = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 32 WHERE id = '{alice_id}' AND age = 31").as_str(),
    )
    .expect_err("configured generated SQL update endpoint must reject guarded PK UPDATE");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "generated SQL update endpoint should reject extra guard predicates under the current primary-key policy",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "guarded primary-key UPDATE rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected guarded primary-key generated UPDATE must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_returns_primary_key_post_update_rows() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let returning = expect_projection(
        update_sql(
            &fixture,
            format!("UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING name, age")
                .as_str(),
        )
        .expect("primary-key generated SQL update endpoint should admit RETURNING"),
    );

    assert_projection_rendered(
        &returning,
        "SqlTestUser",
        &["name", "age"],
        &[&["alice", "34"]],
        1,
        "primary-key generated UPDATE RETURNING should return the post-update row image",
    );
}

#[test]
fn sql_canister_update_endpoint_returns_primary_key_post_update_star_rows() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let returning = expect_projection(
        update_sql(
            &fixture,
            format!("UPDATE SqlTestUser SET age = 35 WHERE id = '{alice_id}' RETURNING *").as_str(),
        )
        .expect("primary-key generated SQL update endpoint should admit RETURNING *"),
    );

    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(
        returning.columns,
        ["id", "name", "age", "rank", "created_at", "updated_at"],
        "primary-key generated UPDATE RETURNING * should preserve schema column order",
    );
    assert_eq!(returning.row_count, 1);
    let rows = returning.rendered_rows();
    let row = rows
        .first()
        .expect("primary-key generated UPDATE RETURNING * should return one row");
    assert_eq!(
        row.len(),
        returning.columns.len(),
        "primary-key generated UPDATE RETURNING * should return a complete row image",
    );
    assert_eq!(row[0], alice_id);
    assert_eq!(row[1], "alice");
    assert_eq!(
        row[2], "35",
        "primary-key generated UPDATE RETURNING * should expose the post-update value",
    );
    assert_eq!(
        row[3], "28",
        "primary-key generated UPDATE RETURNING * should preserve unchanged fields",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_oversized_returning_response_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alpha_id = sql_test_numeric_type_id_by_label(&fixture, "alpha");
    seed_oversized_sql_group_name(&fixture);
    let err = update_sql(
        &fixture,
        format!(
            "UPDATE SqlTestNumericTypes SET int32_value = 37 \
             WHERE id = '{alpha_id}' RETURNING group_name"
        )
        .as_str(),
    )
    .expect_err("primary-key generated UPDATE should reject oversized RETURNING response");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE,
        "primary-key generated UPDATE should enforce the default RETURNING response budget",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "oversized primary-key UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            format!("SELECT int32_value FROM SqlTestNumericTypes WHERE id = '{alpha_id}'").as_str(),
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after.rendered_rows(),
        string_rows(&[&["35"]]),
        "oversized primary-key UPDATE RETURNING should reject before mutation",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_computed_returning_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let err = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING LOWER(name)")
            .as_str(),
    )
    .expect_err("primary-key generated SQL update endpoint must reject computed RETURNING");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE,
        "computed primary-key UPDATE RETURNING should preserve the specific unsupported SQL feature code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "computed primary-key UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected primary-key UPDATE RETURNING must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_invalid_returning_fields_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");

    for (returning, expected_code) in [
        ("missing", ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD),
        ("name, name", ErrorCode::SQL_WRITE_DUPLICATE_RETURNING_FIELD),
    ] {
        let err = update_sql(
            &fixture,
            format!(
                "UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING {returning}"
            )
            .as_str(),
        )
        .expect_err(
            "primary-key generated SQL update endpoint must reject invalid RETURNING fields",
        );

        assert_eq!(
            err.code(),
            expected_code,
            "invalid primary-key UPDATE RETURNING field list should preserve its compact SQL write code",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "invalid primary-key UPDATE RETURNING rejection should stay query-owned",
        );
    }
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected invalid primary-key UPDATE RETURNING field lists must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_admits_explicit_limited_primary_key_order() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-update read should prove the row set exists"),
    );
    let result = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 2",
    )
    .expect("configured bounded SQL update endpoint should admit explicit bounded UPDATE");

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "SqlTestUser".to_string(),
            row_count: 2,
        },
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-update read should still execute"),
    );
    assert_ne!(
        after, before,
        "admitted bounded generated SQL update should mutate the limited target set",
    );
    assert_eq!(
        after
            .rendered_rows()
            .into_iter()
            .filter(|row| row.get(1).is_some_and(|age| age == "32"))
            .count(),
        2,
        "bounded generated SQL update should mutate exactly the admitted LIMIT window",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_unordered_limit_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 LIMIT 2",
    )
    .expect_err("configured bounded SQL update endpoint must reject implicit ordering");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should preserve policy rejection code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update endpoint policy rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected bounded generated SQL update endpoint call must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_limit_above_default_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 101",
    )
    .expect_err("configured bounded SQL update endpoint must reject excessive LIMIT");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should enforce the default row limit",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update limit rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update over the default row limit must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_non_primary_key_order_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY age ASC LIMIT 2",
    )
    .expect_err("configured bounded SQL update endpoint must reject non-PK ordering");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject non-primary-key ordering",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update non-primary-key ordering rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with non-primary-key ordering must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_desc_order_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id DESC LIMIT 2",
    )
    .expect_err("configured bounded SQL update endpoint must reject descending order");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject descending primary-key order",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update descending-order rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with descending order must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_offset_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 2 OFFSET 1",
    )
    .expect_err("configured bounded SQL update endpoint must reject OFFSET");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject OFFSET",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update OFFSET rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with OFFSET must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_returns_post_update_rows() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let target_names = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age >= 24 ORDER BY id ASC LIMIT 2",
        )
        .expect("pre-update target read should prove the bounded order"),
    );
    let returning = expect_projection(
        update_sql(
            &fixture,
            "UPDATE SqlTestUser SET age = 33 \
             WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING name, age",
        )
        .expect("bounded generated SQL update endpoint should admit bounded RETURNING"),
    );

    let expected_rows = target_names
        .rendered_rows()
        .into_iter()
        .map(|row| vec![row[0].clone(), "33".to_string()])
        .collect::<Vec<_>>();
    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(returning.columns, ["name", "age"]);
    assert_eq!(returning.rendered_rows(), expected_rows);
    assert_eq!(
        returning.row_count, 2,
        "bounded generated UPDATE RETURNING should return post-update rows in the frozen target order",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_returns_post_update_star_rows() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let targets = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, name, rank FROM SqlTestUser WHERE age >= 24 ORDER BY id ASC LIMIT 2",
        )
        .expect("pre-update target read should prove the bounded order"),
    );
    let returning = expect_projection(
        update_sql(
            &fixture,
            "UPDATE SqlTestUser SET age = 36 \
             WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING *",
        )
        .expect("bounded generated SQL update endpoint should admit bounded RETURNING *"),
    );

    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(
        returning.columns,
        ["id", "name", "age", "rank", "created_at", "updated_at"],
        "bounded generated UPDATE RETURNING * should preserve schema column order",
    );
    assert_eq!(returning.row_count, 2);
    assert_eq!(
        returning.rows.len(),
        targets.rows.len(),
        "bounded generated UPDATE RETURNING * should return the frozen target window",
    );
    let returning_rows = returning.rendered_rows();
    let target_rows = targets.rendered_rows();
    for (row, target) in returning_rows.iter().zip(target_rows.iter()) {
        assert_eq!(
            row.len(),
            returning.columns.len(),
            "bounded generated UPDATE RETURNING * should return complete row images",
        );
        assert_eq!(row[0], target[0]);
        assert_eq!(row[1], target[1]);
        assert_eq!(
            row[2], "36",
            "bounded generated UPDATE RETURNING * should expose post-update values",
        );
        assert_eq!(
            row[3], target[2],
            "bounded generated UPDATE RETURNING * should preserve unchanged fields",
        );
    }
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_oversized_returning_response_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    seed_oversized_sql_group_name(&fixture);
    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, int32_value FROM SqlTestNumericTypes ORDER BY id ASC",
        )
        .expect("pre-rejection read should still execute"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestNumericTypes SET int32_value = 37 \
         WHERE nat32_value >= 100 ORDER BY id ASC LIMIT 2 RETURNING group_name",
    )
    .expect_err("bounded generated UPDATE should reject oversized RETURNING response");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE,
        "bounded generated UPDATE should enforce the default RETURNING response budget",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "oversized bounded UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, int32_value FROM SqlTestNumericTypes ORDER BY id ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "oversized bounded UPDATE RETURNING should reject before mutation",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_computed_returning_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 33 \
         WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING LOWER(name)",
    )
    .expect_err("bounded generated SQL update endpoint must reject computed RETURNING");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE,
        "computed bounded UPDATE RETURNING should preserve the specific unsupported SQL feature code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "computed bounded UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected bounded UPDATE RETURNING must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_invalid_returning_fields_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );

    for (returning, expected_code) in [
        ("missing", ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD),
        ("name, name", ErrorCode::SQL_WRITE_DUPLICATE_RETURNING_FIELD),
    ] {
        let err = update_sql(
            &fixture,
            format!(
                "UPDATE SqlTestUser SET age = 33 \
                 WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING {returning}"
            )
            .as_str(),
        )
        .expect_err("bounded generated SQL update endpoint must reject invalid RETURNING fields");

        assert_eq!(
            err.code(),
            expected_code,
            "invalid bounded UPDATE RETURNING field list should preserve its compact SQL write code",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "invalid bounded UPDATE RETURNING rejection should stay query-owned",
        );
    }
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected invalid bounded UPDATE RETURNING field lists must not mutate rows",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_malformed_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(&fixture, "SELECT FROM SqlTestUser")
        .expect_err("query(sql) must reject malformed SQL before execution");

    assert_runtime_unsupported_query_error(
        &err,
        "malformed SQL should keep query-owned origin metadata",
    );
}
