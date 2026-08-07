//! Focused 0.222 streaming-execution fixture and baseline contract.

use candid::{CandidType, Deserialize};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
};
use icydb_testing_integration::{
    install_fixture_canister,
    streaming_execution_contract::{
        STREAMING_EXECUTION_CONTRACT_VERSION, STREAMING_EXECUTION_FIXTURE_ROWS,
        STREAMING_EXECUTION_FIXTURE_SEED, STREAMING_EXECUTION_FIXTURES,
        STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES, StreamingFixtureContinuation,
    },
};

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct StreamingExecutionFixtureFacts {
    profile_version: u32,
    seed: u64,
    fixture_rows: u32,
    lane_a_zero_rows: u32,
    lane_b_zero_rows: u32,
    sparse_overlap_rows: u32,
    empty_overlap_rows: u32,
    group_count: u32,
    wide_payload_bytes: Vec<u32>,
}

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct StreamingQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[test]
fn streaming_fixture_realizes_the_frozen_distribution() {
    let fixture = install_fixture_canister("sql_perf");
    let facts: Result<StreamingExecutionFixtureFacts, Error> = fixture
        .update_candid("load_streaming_execution_fixture", ())
        .expect("streaming fixture facts should decode");
    let facts = facts.expect("streaming fixture should load");

    assert_eq!(facts.profile_version, STREAMING_EXECUTION_CONTRACT_VERSION);
    assert_eq!(facts.seed, STREAMING_EXECUTION_FIXTURE_SEED);
    assert_eq!(facts.fixture_rows, STREAMING_EXECUTION_FIXTURE_ROWS);
    assert_eq!(facts.lane_a_zero_rows, 21);
    assert_eq!(facts.lane_b_zero_rows, 20);
    assert_eq!(facts.sparse_overlap_rows, 1);
    assert_eq!(facts.empty_overlap_rows, 0);
    assert_eq!(facts.group_count, 17);
    assert_eq!(
        facts.wide_payload_bytes,
        STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES
    );
}

#[test]
fn one_shot_baselines_keep_exact_queries_rows_and_evidence() {
    let fixture = install_fixture_canister("sql_perf");
    let facts: Result<StreamingExecutionFixtureFacts, Error> = fixture
        .update_candid("load_streaming_execution_fixture", ())
        .expect("streaming fixture facts should decode");
    facts.expect("streaming fixture should load");

    for declaration in STREAMING_EXECUTION_FIXTURES.iter().filter(|declaration| {
        declaration.continuation == StreamingFixtureContinuation::OneShot
            && declaration.id != "hard_budget_typed_headroom"
    }) {
        let measured: Result<StreamingQueryPerfResult, Error> = fixture
            .query_candid(
                "query_streaming_execution_with_perf",
                (declaration.sql.to_string(),),
            )
            .unwrap_or_else(|error| panic!("{} should decode: {error}", declaration.id));
        let measured =
            measured.unwrap_or_else(|error| panic!("{} should execute: {error}", declaration.id));
        let rows = result_rows(&measured.result);

        assert_eq!(rows, declaration.expected_rows, "{}", declaration.id);
        assert!(measured.attribution.total_local_instructions > 0);
        assert_patch_4_intersection_evidence(declaration.id, &measured.attribution);
        println!(
            "icydb_0222_baseline id={} instructions={} index_entries={} store_gets={} rows={} peak_retained_candidates={}",
            declaration.id,
            measured.attribution.total_local_instructions,
            measured.attribution.index_store_entry_reads,
            measured.attribution.store_get_calls,
            rows,
            measured
                .attribution
                .kernel_row
                .as_ref()
                .map_or(0, |kernel| kernel.peak_retained_candidates),
        );
    }
}

fn assert_patch_4_intersection_evidence(
    fixture_id: &str,
    attribution: &SqlQueryExecutionAttribution,
) {
    let expected = match fixture_id {
        "intersection_empty" => Some((41, 0)),
        "intersection_2_sparse" => Some((41, 1)),
        "intersection_3_sparse_desc" => Some((161, 1)),
        "intersection_dense" => Some((2_048, 2_048)),
        "compound_prefix_control" => Some((1, 0)),
        _ => None,
    };
    let Some((index_entries, store_gets)) = expected else {
        return;
    };

    assert_eq!(
        attribution.index_store_entry_reads, index_entries,
        "{fixture_id} index-entry evidence"
    );
    assert_eq!(
        attribution.store_get_calls, store_gets,
        "{fixture_id} row-fetch evidence"
    );
}

#[test]
fn sparse_intersection_and_compound_control_publish_distinct_planner_choices() {
    let fixture = install_fixture_canister("sql_perf");
    let facts: Result<StreamingExecutionFixtureFacts, Error> = fixture
        .update_candid("load_streaming_execution_fixture", ())
        .expect("streaming fixture facts should decode");
    facts.expect("streaming fixture should load");

    let sparse = query_explain_json(
        &fixture,
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
    );
    assert!(sparse.contains("\"kind\":\"Intersection\""), "{sparse}");
    assert!(
        sparse.contains("\"reason\":\"planner_exact_index_intersection\""),
        "{sparse}"
    );
    assert!(
        sparse.contains("\"has_residual_predicate\":true"),
        "{sparse}"
    );
    assert!(
        sparse.contains("\"residual_predicate_count\":2"),
        "{sparse}"
    );
    let sparse_execution = query_explain_statement(
        &fixture,
        "EXPLAIN EXECUTION SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
    );
    assert!(
        sparse_execution.contains("residual_filter_predicate="),
        "{sparse_execution}"
    );

    let compound = query_explain_json(
        &fixture,
        "SELECT id FROM PerfAuditStreamingCompoundRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
    );
    assert!(compound.contains("\"kind\":\"IndexPrefix\""), "{compound}");
    assert!(
        !compound.contains("\"reason\":\"planner_exact_index_intersection\""),
        "{compound}"
    );
}

fn query_explain_json(fixture: &StandaloneCanisterFixture, sql: &str) -> String {
    query_explain_statement(fixture, format!("EXPLAIN JSON {sql}").as_str())
}

fn query_explain_statement(fixture: &StandaloneCanisterFixture, sql: &str) -> String {
    let measured: Result<StreamingQueryPerfResult, Error> = fixture
        .query_candid("query_streaming_execution_with_perf", (sql.to_string(),))
        .expect("EXPLAIN JSON result should decode");
    let measured = measured.expect("EXPLAIN JSON should execute");
    match measured.result {
        SqlQueryResult::Explain { explain, .. } => explain,
        other => panic!("expected EXPLAIN result, received {other:?}"),
    }
}

const fn result_rows(result: &SqlQueryResult) -> u32 {
    match result {
        SqlQueryResult::Count { row_count, .. } => *row_count,
        SqlQueryResult::Projection(rows) => rows.row_count,
        SqlQueryResult::Grouped(rows) => rows.row_count,
        SqlQueryResult::Explain { .. }
        | SqlQueryResult::Describe(_)
        | SqlQueryResult::ShowIndexes { .. }
        | SqlQueryResult::ShowConstraints { .. }
        | SqlQueryResult::ShowColumns { .. }
        | SqlQueryResult::ShowEntities { .. }
        | SqlQueryResult::ShowStores { .. }
        | SqlQueryResult::ShowMemory { .. }
        | SqlQueryResult::Ddl { .. } => 0,
    }
}
