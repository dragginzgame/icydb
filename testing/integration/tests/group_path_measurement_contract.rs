//! Frozen scalar record-path grouping actor and Slice 3 behavior contract.

use candid::Principal;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::sql::{SqlGroupedRowsOutput, SqlQueryResult},
    diagnostic::DiagnosticCode,
};
use icydb_testing_integration::{
    group_path_contract::{
        GROUP_PATH_AGGREGATE_INPUT_QUERY, GROUP_PATH_COMPLETE_INDEX_DDL,
        GROUP_PATH_DIRECT_INDEX_DDL, GROUP_PATH_DIRECT_QUERY, GROUP_PATH_EXPRESSION_QUERY,
        GROUP_PATH_FIXTURE_ROWS, GROUP_PATH_HAVING_QUERY, GROUP_PATH_MIXED_INDEX_DDL,
        GROUP_PATH_MIXED_QUERY, GROUP_PATH_NULLABLE_TERMINAL_QUERY, GROUP_PATH_OMISSION_INDEX_DDL,
        GROUP_PATH_OMISSION_PREFIX_INDEX_DDL, GROUP_PATH_OMISSION_PREFIX_NON_NULL_QUERY,
        GROUP_PATH_OMISSION_PREFIX_QUERY, GROUP_PATH_OPTIONAL_NON_NULL_QUERY,
        GROUP_PATH_OPTIONAL_QUERY, GROUP_PATH_PAGED_QUERY, GROUP_PATH_RECORD_TERMINAL_QUERY,
        GROUP_PATH_REQUIRED_COUNT_QUERY, GROUP_PATH_REQUIRED_QUERY,
        GROUP_PATH_SIBLING_PROJECTION_QUERY, GROUP_PATH_UNKNOWN_MEMBER_QUERY,
    },
    install_fixture_canister,
};

fn load_group_path_rows(fixture: &StandaloneCanisterFixture) {
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );
}

fn query_group_path_rows(fixture: &StandaloneCanisterFixture, sql: &str) -> SqlGroupedRowsOutput {
    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (sql.to_string(),))
        .expect("group-path query should decode");
    let SqlQueryResult::Grouped(grouped) = result.expect("group-path query should execute") else {
        panic!("group-path query should return grouped rows");
    };
    grouped
}

fn explain_group_path(fixture: &StandaloneCanisterFixture, sql: &str) -> String {
    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (format!("EXPLAIN JSON {sql}"),))
        .expect("group-path explain should decode");
    let SqlQueryResult::Explain { explain, .. } =
        result.expect("group-path explain should execute")
    else {
        panic!("group-path explain should return an explain payload");
    };
    explain
}

fn nat_u64(value: &candid::Nat) -> u64 {
    match value.0.to_u64_digits().as_slice() {
        [] => 0,
        [value] => *value,
        _ => panic!("group-path query statistic should fit u64"),
    }
}

fn assert_path_instruction_budget(label: &str, direct: u64, path: u64) {
    assert!(direct > 0, "{label} direct query must report instructions");
    let ceiling = direct.saturating_add(direct / 5);
    assert!(
        path <= ceiling,
        "{label} path query used {path} instructions; direct control used {direct} (ceiling {ceiling})",
    );
}

fn measure_cold_query_instructions(sql: &str) -> (u64, u64) {
    measure_query_instructions_with_index(sql, None)
}

fn measure_query_instructions_with_index(sql: &str, index_ddl: Option<&str>) -> (u64, u64) {
    const APPLICATION_SUBNET_NODES: u8 = 13;
    const QUERY_STATS_EPOCHS: u8 = 4;
    const ROUNDS_PER_EPOCH: u8 = 60;
    const REPORTED_EPOCHS: u64 = 3;

    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );
    if let Some(index_ddl) = index_ddl {
        let ddl: Result<SqlQueryResult, Error> = fixture
            .update_candid("ddl_group_path", (index_ddl.to_string(),))
            .expect("measured index DDL response should decode");
        assert!(
            matches!(ddl, Ok(SqlQueryResult::Ddl { .. })),
            "measured index DDL should publish: {ddl:?}",
        );
    }

    for epoch in 0..QUERY_STATS_EPOCHS {
        for node in 0..APPLICATION_SUBNET_NODES {
            let caller = Principal::from_slice(&[epoch.saturating_add(1), node.saturating_add(1)]);
            let result: Result<SqlQueryResult, Error> = fixture
                .query_candid_as(caller, "query_group_path", (sql.to_string(),))
                .expect("measured group-path query response should decode");
            let SqlQueryResult::Grouped(_) = result.expect("measured query should execute") else {
                panic!("measured query should return grouped rows");
            };
        }
        for _ in 0..ROUNDS_PER_EPOCH {
            fixture.pocket_ic().tick();
        }
    }

    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .expect("group-path audit canister status should be available");
    let calls = nat_u64(&status.query_stats.num_calls_total);
    let instructions = nat_u64(&status.query_stats.num_instructions_total);
    assert_eq!(
        calls,
        u64::from(APPLICATION_SUBNET_NODES).saturating_mul(REPORTED_EPOCHS),
    );

    (calls, instructions)
}

#[test]
fn direct_grouping_fixture_is_executable() {
    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );

    let direct: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (GROUP_PATH_DIRECT_QUERY.to_string(),))
        .expect("direct grouped control should decode");
    let SqlQueryResult::Grouped(direct) = direct.expect("direct grouped control should execute")
    else {
        panic!("direct grouped control should return grouped rows");
    };
    assert_eq!(direct.row_count, 127);
    assert_eq!(direct.rows.first(), Some(&vec!["0".into(), "17".into()]));
    assert_eq!(direct.rows.last(), Some(&vec!["126".into(), "16".into()]),);
}

#[test]
fn scalar_path_grouping_matches_direct_and_preserves_null_semantics() {
    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );

    let query = |sql: &str| {
        let result: Result<SqlQueryResult, Error> = fixture
            .query_candid("query_group_path", (sql.to_string(),))
            .expect("group-path query response should decode");
        let SqlQueryResult::Grouped(grouped) = result.unwrap_or_else(|error| {
            panic!(
                "group-path query `{sql}` should execute: error={error:?}, diagnostic={:?}",
                error.diagnostic(),
            )
        }) else {
            panic!("group-path query should return grouped rows");
        };
        grouped
    };

    let direct = query(GROUP_PATH_DIRECT_QUERY);
    let required = query(GROUP_PATH_REQUIRED_QUERY);
    assert_eq!(required.rows, direct.rows);

    let unbounded_count = query(GROUP_PATH_REQUIRED_COUNT_QUERY);
    assert_eq!(unbounded_count.row_count, direct.row_count);
    assert_eq!(unbounded_count.rows, direct.rows);

    let mixed = query(GROUP_PATH_MIXED_QUERY);
    assert_eq!(mixed.row_count, direct.row_count);
    for (mixed_row, direct_row) in mixed.rows.iter().zip(&direct.rows) {
        assert_eq!(mixed_row[0], mixed_row[1]);
        assert_eq!(mixed_row[0], direct_row[0]);
        assert_eq!(mixed_row[2], direct_row[1]);
    }

    let optional = query(GROUP_PATH_OPTIONAL_QUERY);
    assert_eq!(optional.row_count, 128);
    assert_eq!(
        optional
            .rows
            .iter()
            .map(|row| row[1]
                .parse::<u32>()
                .expect("group count should be numeric"))
            .sum::<u32>(),
        GROUP_PATH_FIXTURE_ROWS,
    );
    assert_eq!(
        optional
            .rows
            .iter()
            .find(|row| row[0] == "null")
            .map(|row| row[1].as_str()),
        Some("683"),
    );

    let having = query(GROUP_PATH_HAVING_QUERY);
    assert_eq!(having.row_count, 7);
    assert_eq!(having.rows.first().map(|row| row[0].as_str()), Some("120"));
    assert_eq!(having.rows.last().map(|row| row[0].as_str()), Some("126"));

    let expression = query(GROUP_PATH_EXPRESSION_QUERY);
    assert_eq!(expression.row_count, direct.row_count);
    for (expression_row, direct_row) in expression.rows.iter().zip(&direct.rows) {
        assert_eq!(
            expression_row[0]
                .parse::<i32>()
                .expect("group-key expression should be numeric"),
            direct_row[0]
                .parse::<i32>()
                .expect("direct group key should be numeric")
                + 1,
        );
        assert_eq!(expression_row[1], direct_row[1]);
    }

    let aggregate_input = query(GROUP_PATH_AGGREGATE_INPUT_QUERY);
    assert_eq!(aggregate_input.row_count, direct.row_count);
    for (aggregate_row, direct_row) in aggregate_input.rows.iter().zip(&direct.rows) {
        let rank = direct_row[0]
            .parse::<i32>()
            .expect("direct group key should be numeric");
        let count = direct_row[1]
            .parse::<i32>()
            .expect("direct group count should be numeric");
        assert_eq!(aggregate_row[0], direct_row[0]);
        assert_eq!(
            aggregate_row[1]
                .parse::<i32>()
                .expect("path aggregate should be numeric"),
            rank * count,
        );
    }

    let paged = query(GROUP_PATH_PAGED_QUERY);
    assert_eq!(paged.row_count, 17);
    assert!(
        paged.next_cursor.is_some(),
        "bounded path grouping should emit a continuation cursor",
    );
}

#[test]
fn scalar_path_grouping_stays_within_cold_instruction_budget() {
    let (direct_calls, direct_instructions) =
        measure_cold_query_instructions(GROUP_PATH_DIRECT_QUERY);
    let (path_calls, path_instructions) =
        measure_cold_query_instructions(GROUP_PATH_REQUIRED_QUERY);
    assert_eq!(path_calls, direct_calls);
    assert_path_instruction_budget("cold", direct_instructions, path_instructions);
    eprintln!(
        "group-path cold query instructions across {direct_calls} reported calls: direct={direct_instructions}, path={path_instructions}",
    );
}

#[test]
fn scalar_path_ordered_grouping_stays_within_instruction_budget() {
    let (direct_calls, direct_instructions) = measure_query_instructions_with_index(
        GROUP_PATH_DIRECT_QUERY,
        Some(GROUP_PATH_DIRECT_INDEX_DDL),
    );
    let (path_calls, path_instructions) = measure_query_instructions_with_index(
        GROUP_PATH_REQUIRED_QUERY,
        Some(GROUP_PATH_COMPLETE_INDEX_DDL),
    );
    assert_eq!(path_calls, direct_calls);
    let ceiling = direct_instructions.saturating_add(direct_instructions.saturating_mul(15) / 100);
    assert!(
        path_instructions <= ceiling,
        "ordered path query used {path_instructions} instructions; ordered direct control used {direct_instructions} (ceiling {ceiling})",
    );
    eprintln!(
        "group-path ordered query instructions across {direct_calls} reported calls: direct={direct_instructions}, path={path_instructions}",
    );
}

#[test]
fn complete_scalar_path_index_admits_ordered_grouping() {
    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );

    let ddl: Result<SqlQueryResult, Error> = fixture
        .update_candid(
            "ddl_group_path",
            (GROUP_PATH_COMPLETE_INDEX_DDL.to_string(),),
        )
        .expect("complete path-index DDL response should decode");
    let SqlQueryResult::Ddl {
        target_index,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl.unwrap_or_else(|error| {
        panic!(
            "complete path index should publish: error={error:?}, diagnostic={:?}",
            error.diagnostic(),
        )
    })
    else {
        panic!("complete path-index DDL should return a DDL payload");
    };
    assert_eq!(target_index, "group_path_profile_rank_idx");
    assert_eq!(rows_scanned, u64::from(GROUP_PATH_FIXTURE_ROWS));
    assert_eq!(index_keys_written, u64::from(GROUP_PATH_FIXTURE_ROWS));

    let explain_sql = format!("EXPLAIN JSON {GROUP_PATH_REQUIRED_QUERY}");
    let explain: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (explain_sql,))
        .expect("ordered path-group explain should decode");
    let SqlQueryResult::Explain { explain, .. } =
        explain.expect("ordered path-group explain should execute")
    else {
        panic!("ordered path-group explain should return an explain payload");
    };
    assert!(
        explain.contains("group_path_profile_rank_idx"),
        "ordered path-group explain should select the complete path index: {explain}",
    );
    assert!(
        explain.contains("ordered_group"),
        "ordered path-group explain should select ordered grouping: {explain}",
    );

    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (GROUP_PATH_PAGED_QUERY.to_string(),))
        .expect("ordered path-group result should decode");
    let SqlQueryResult::Grouped(grouped) = result.expect("ordered path grouping should execute")
    else {
        panic!("ordered path grouping should return grouped rows");
    };
    assert_eq!(grouped.row_count, 17);
    assert_eq!(grouped.rows.first(), Some(&vec!["0".into(), "17".into()]));
    assert_eq!(grouped.rows.last(), Some(&vec!["16".into(), "16".into()]));
    assert!(grouped.next_cursor.is_some());
}

#[test]
fn complete_mixed_direct_path_index_admits_one_ordered_prefix() {
    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );
    let ddl: Result<SqlQueryResult, Error> = fixture
        .update_candid("ddl_group_path", (GROUP_PATH_MIXED_INDEX_DDL.to_string(),))
        .expect("mixed path-index DDL response should decode");
    assert!(matches!(ddl, Ok(SqlQueryResult::Ddl { .. })));

    let explain: Result<SqlQueryResult, Error> = fixture
        .query_candid(
            "query_group_path",
            (format!("EXPLAIN JSON {GROUP_PATH_MIXED_QUERY}"),),
        )
        .expect("mixed ordered explain should decode");
    let SqlQueryResult::Explain { explain, .. } =
        explain.expect("mixed ordered explain should execute")
    else {
        panic!("mixed ordered explain should return an explain payload");
    };
    assert!(
        explain.contains("group_path_mixed_rank_idx") && explain.contains("ordered_group"),
        "mixed grouping must use one matching composite index prefix: {explain}",
    );

    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("query_group_path", (GROUP_PATH_MIXED_QUERY.to_string(),))
        .expect("mixed ordered result should decode");
    let SqlQueryResult::Grouped(grouped) = result.expect("mixed ordered grouping should execute")
    else {
        panic!("mixed ordered grouping should return grouped rows");
    };
    assert_eq!(grouped.row_count, 127);
    assert_eq!(
        grouped.rows.first(),
        Some(&vec!["0".into(), "0".into(), "17".into()])
    );
}

#[test]
fn omission_capable_path_index_requires_a_non_null_query_proof() {
    let fixture = install_fixture_canister("group_path_sql_query");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_group_path_fixture", (GROUP_PATH_FIXTURE_ROWS,))
        .expect("group-path fixture row count should decode");
    assert_eq!(
        loaded.expect("group-path fixture should load"),
        GROUP_PATH_FIXTURE_ROWS,
    );

    let unindexed: Result<SqlQueryResult, Error> = fixture
        .query_candid(
            "query_group_path",
            (GROUP_PATH_OPTIONAL_NON_NULL_QUERY.to_string(),),
        )
        .expect("unindexed non-null path grouping should decode");
    let SqlQueryResult::Grouped(unindexed) =
        unindexed.expect("unindexed non-null path grouping should execute")
    else {
        panic!("unindexed non-null path grouping should return grouped rows");
    };

    for ddl in [GROUP_PATH_COMPLETE_INDEX_DDL, GROUP_PATH_OMISSION_INDEX_DDL] {
        let result: Result<SqlQueryResult, Error> = fixture
            .update_candid("ddl_group_path", (ddl.to_string(),))
            .expect("path-index DDL response should decode");
        assert!(
            matches!(result, Ok(SqlQueryResult::Ddl { .. })),
            "path-index DDL should publish: {result:?}",
        );
    }

    let explain = |sql: &str| {
        let result: Result<SqlQueryResult, Error> = fixture
            .query_candid("query_group_path", (format!("EXPLAIN JSON {sql}"),))
            .expect("path-group explain should decode");
        let SqlQueryResult::Explain { explain, .. } =
            result.expect("path-group explain should execute")
        else {
            panic!("path-group explain should return an explain payload");
        };
        explain
    };

    let complete_explain = explain(GROUP_PATH_NULLABLE_TERMINAL_QUERY);
    assert!(
        complete_explain.contains("hash_group")
            && !complete_explain.contains("group_path_optional_rank_idx"),
        "an omission-capable index must not serve complete grouping: {complete_explain}",
    );

    let excluding_explain = explain(GROUP_PATH_OPTIONAL_NON_NULL_QUERY);
    assert!(
        excluding_explain.contains("ordered_group")
            && excluding_explain.contains("group_path_optional_rank_idx"),
        "a non-null range proof should admit the omission-capable index: {excluding_explain}",
    );

    let complete: Result<SqlQueryResult, Error> = fixture
        .query_candid(
            "query_group_path",
            (GROUP_PATH_NULLABLE_TERMINAL_QUERY.to_string(),),
        )
        .expect("complete optional path grouping should decode");
    let SqlQueryResult::Grouped(complete) =
        complete.expect("complete optional path grouping should execute")
    else {
        panic!("complete optional path grouping should return grouped rows");
    };
    assert_eq!(complete.row_count, 127);

    let excluding: Result<SqlQueryResult, Error> = fixture
        .query_candid(
            "query_group_path",
            (GROUP_PATH_OPTIONAL_NON_NULL_QUERY.to_string(),),
        )
        .expect("non-null optional path grouping should decode");
    let SqlQueryResult::Grouped(excluding) =
        excluding.expect("non-null optional path grouping should execute")
    else {
        panic!("non-null optional path grouping should return grouped rows");
    };
    assert_eq!(excluding.row_count, 127);
    assert_eq!(excluding.rows, unindexed.rows);
    assert_eq!(
        excluding
            .rows
            .iter()
            .map(|row| row[1]
                .parse::<u32>()
                .expect("group count should be numeric"))
            .sum::<u32>(),
        GROUP_PATH_FIXTURE_ROWS,
    );
    assert!(excluding.rows.iter().all(|row| row[0] != "null"));
}

#[test]
fn predicate_selected_composite_path_index_requires_complete_nullable_suffix() {
    let fixture = install_fixture_canister("group_path_sql_query");
    load_group_path_rows(&fixture);
    let unindexed = query_group_path_rows(&fixture, GROUP_PATH_OMISSION_PREFIX_QUERY);
    assert_eq!(unindexed.row_count, 2);
    assert!(unindexed.rows.contains(&vec!["null".into(), "6".into()]));
    assert!(unindexed.rows.contains(&vec!["0".into(), "11".into()]));

    let ddl: Result<SqlQueryResult, Error> = fixture
        .update_candid(
            "ddl_group_path",
            (GROUP_PATH_OMISSION_PREFIX_INDEX_DDL.to_string(),),
        )
        .expect("omission-prefix index DDL response should decode");
    assert!(
        matches!(ddl, Ok(SqlQueryResult::Ddl { .. })),
        "omission-prefix index DDL should publish: {ddl:?}",
    );

    let incomplete_explain = explain_group_path(&fixture, GROUP_PATH_OMISSION_PREFIX_QUERY);
    assert!(
        incomplete_explain.contains("hash_group")
            && !incomplete_explain.contains("group_path_optional_prefix_idx"),
        "an incomplete nullable suffix must reject predicate-selected prefix access: {incomplete_explain}",
    );
    assert_eq!(
        query_group_path_rows(&fixture, GROUP_PATH_OMISSION_PREFIX_QUERY).rows,
        unindexed.rows,
    );

    let complete_explain = explain_group_path(&fixture, GROUP_PATH_OMISSION_PREFIX_NON_NULL_QUERY);
    assert!(
        complete_explain.contains("ordered_group")
            && complete_explain.contains("group_path_optional_prefix_idx"),
        "a suffix non-null proof should admit predicate-selected prefix access: {complete_explain}",
    );
    assert_eq!(
        query_group_path_rows(&fixture, GROUP_PATH_OMISSION_PREFIX_NON_NULL_QUERY).rows,
        vec![vec![String::from("0"), String::from("11")]],
    );

    let delete: Result<SqlQueryResult, Error> = fixture
        .update_candid(
            "mutate_group_path",
            ("DELETE FROM GroupPathAuditRow WHERE id = 0".to_string(),),
        )
        .expect("indexed null-root delete should decode");
    assert!(
        matches!(delete, Ok(SqlQueryResult::Count { row_count: 1, .. })),
        "accepted-index maintenance should omit the null-root path: {delete:?}",
    );
}

#[test]
fn path_grouping_rejects_non_scalar_unknown_and_undeclared_paths() {
    let fixture = install_fixture_canister("group_path_sql_query");

    for (sql, expected_code) in [
        (
            GROUP_PATH_SIBLING_PROJECTION_QUERY,
            DiagnosticCode::QueryUnsupportedSqlFeature,
        ),
        (
            GROUP_PATH_RECORD_TERMINAL_QUERY,
            DiagnosticCode::QueryUnsupportedSqlFeature,
        ),
        (GROUP_PATH_UNKNOWN_MEMBER_QUERY, DiagnosticCode::QueryPlan),
    ] {
        let result: Result<SqlQueryResult, Error> = fixture
            .query_candid("query_group_path", (sql.to_string(),))
            .expect("path grouped rejection should decode");
        assert_eq!(
            result
                .expect_err("invalid path grouping must reject")
                .diagnostic_code(),
            expected_code,
        );
    }
}
