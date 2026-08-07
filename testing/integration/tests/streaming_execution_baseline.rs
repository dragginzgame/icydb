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
        assert_patch_6_scalar_evidence(declaration.id, &measured.attribution);
        println!(
            "icydb_0222_baseline id={} instructions={} index_entries={} store_gets={} rows={} peak_retained_candidates={} peak_retained_backing_bytes={}",
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
            measured
                .attribution
                .kernel_row
                .as_ref()
                .map_or(0, |kernel| kernel.peak_retained_backing_bytes),
        );
        if declaration.id == "topn_wide_payload" {
            println!(
                "icydb_0222_patch6_topn_phase {:?}",
                measured.attribution.kernel_row,
            );
        }
    }

    let early_selective = query_with_perf(
        &fixture,
        "SELECT id FROM PerfAuditStreamingRow WHERE label = 'early-wide' ORDER BY id ASC LIMIT 1",
    );
    assert_eq!(result_rows(&early_selective.result), 1);
    assert_eq!(early_selective.attribution.store_get_calls, 1);
    assert_eq!(
        early_selective
            .attribution
            .kernel_row
            .as_ref()
            .map_or(0, |kernel| kernel.peak_retained_candidates),
        1,
    );
    println!(
        "icydb_0222_patch6 id=limit1_early_selective instructions={} store_gets={} rows=1 peak_retained_candidates=1",
        early_selective.attribution.total_local_instructions,
        early_selective.attribution.store_get_calls,
    );

    let no_match = query_with_perf(
        &fixture,
        "SELECT id FROM PerfAuditStreamingRow WHERE label = 'absent' ORDER BY id ASC LIMIT 1",
    );
    assert_eq!(result_rows(&no_match.result), 0);
    assert_eq!(no_match.attribution.store_get_calls, 2_048);

    let zero_window = query_with_perf(
        &fixture,
        "SELECT id FROM PerfAuditStreamingRow WHERE label = 'late-match' ORDER BY sort_key ASC, id ASC LIMIT 0",
    );
    assert_eq!(result_rows(&zero_window.result), 0);
    assert_eq!(zero_window.attribution.store_get_calls, 0);
}

#[test]
fn distinct_routes_publish_adjacent_and_global_ownership_evidence() {
    let fixture = install_fixture_canister("sql_perf");
    let facts: Result<StreamingExecutionFixtureFacts, Error> = fixture
        .update_candid("load_streaming_execution_fixture", ())
        .expect("streaming fixture facts should decode");
    facts.expect("streaming fixture should load");

    let adjacent = query_with_perf(
        &fixture,
        "SELECT DISTINCT label FROM PerfAuditStreamingRow ORDER BY label ASC LIMIT 10",
    );
    assert_eq!(result_rows(&adjacent.result), 3);
    let adjacent_work = adjacent
        .attribution
        .distinct_projection
        .expect("ordered adjacent DISTINCT should publish attribution");
    assert_eq!(adjacent_work.adjacent_path_hits, 1);
    assert_eq!(adjacent_work.global_path_hits, 0);
    assert_eq!(adjacent_work.candidate_rows, 2_048);
    assert_eq!(adjacent_work.unique_rows, 3);
    assert_eq!(adjacent_work.peak_retained_entries, 1);

    let global = query_with_perf(
        &fixture,
        "SELECT DISTINCT label, group_key FROM PerfAuditStreamingRow ORDER BY group_key ASC LIMIT 10",
    );
    assert_eq!(result_rows(&global.result), 10);
    let global_work = global
        .attribution
        .distinct_projection
        .expect("non-leading DISTINCT tuple should publish attribution");
    assert_eq!(global_work.adjacent_path_hits, 0);
    assert_eq!(global_work.global_path_hits, 1);
    assert_eq!(global_work.candidate_rows, 2_048);
    assert_eq!(global_work.unique_rows, 19);
    assert_eq!(global_work.peak_retained_entries, 19);

    println!(
        "icydb_0222_patch7 adjacent_instructions={} adjacent_peak_entries={} adjacent_peak_bytes={} global_instructions={} global_peak_entries={} global_peak_bytes={}",
        adjacent.attribution.total_local_instructions,
        adjacent_work.peak_retained_entries,
        adjacent_work.peak_retained_backing_bytes,
        global.attribution.total_local_instructions,
        global_work.peak_retained_entries,
        global_work.peak_retained_backing_bytes,
    );
}

fn query_with_perf(fixture: &StandaloneCanisterFixture, sql: &str) -> StreamingQueryPerfResult {
    let measured: Result<StreamingQueryPerfResult, Error> = fixture
        .query_candid("query_streaming_execution_with_perf", (sql.to_string(),))
        .expect("streaming perf query should decode");

    measured.expect("streaming perf query should execute")
}

fn assert_patch_6_scalar_evidence(fixture_id: &str, attribution: &SqlQueryExecutionAttribution) {
    let expected = match fixture_id {
        "limit1_late_selective" => Some((2_048, 1)),
        "topn_wide_payload" => Some((2_048, 10)),
        "full_sort_control" => Some((2_048, 2_048)),
        _ => None,
    };
    let Some((store_gets, retained_candidates)) = expected else {
        return;
    };

    assert_eq!(attribution.store_get_calls, store_gets, "{fixture_id}");
    assert_eq!(
        attribution
            .kernel_row
            .as_ref()
            .map_or(0, |kernel| kernel.peak_retained_candidates),
        retained_candidates,
        "{fixture_id}",
    );
    if fixture_id == "topn_wide_payload" {
        let retained_backing_bytes = attribution
            .kernel_row
            .as_ref()
            .map_or(0, |kernel| kernel.peak_retained_backing_bytes);
        assert!(
            retained_backing_bytes < u64::from(STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[2]),
            "top-N must not retain even the smallest unrelated wide payload: {retained_backing_bytes}",
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
