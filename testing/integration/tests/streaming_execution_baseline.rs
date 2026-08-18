//! Focused streaming-execution fixture and baseline contract.

use candid::{CandidType, Deserialize};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{
        ExhaustiveQueryPageOutput, LiveQueryPageOutput, ReadSetRevisionError, ReadSetRevisionProof,
        SqlQueryExecutionAttribution, sql::SqlQueryResult,
    },
    diagnostic::DiagnosticCode,
    value::OutputValue,
};
use icydb_testing_integration::{
    install_fixture_canister,
    streaming_execution_contract::{
        STREAMING_EXECUTION_CONTINUATION_ROWS, STREAMING_EXECUTION_CONTRACT_VERSION,
        STREAMING_EXECUTION_FIXTURE_ROWS, STREAMING_EXECUTION_FIXTURE_SEED,
        STREAMING_EXECUTION_FIXTURES, STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES,
        StreamingFixtureContinuation,
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

const PREFIX_FAMILY_MAX_FANOUT_INSTRUCTION_CEILING: u64 = 5_150_250;
// Total attribution includes accepted-schema observation. Keep the exact
// 41-entry/one-fetch structural gate below, while allowing for the wider
// 0.228 maximum-fanout audit schema in the shared canister.
const INTERSECTION_2_SPARSE_INSTRUCTION_CEILING: u64 = 2_100_000;
const TOPN_WIDE_PAYLOAD_INSTRUCTION_CEILING: u64 = 65_592_124;

#[derive(CandidType, Debug, Deserialize)]
enum StreamingExhaustivePageError {
    Database(Error),
    Revision(ReadSetRevisionError),
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
fn live_and_exhaustive_pages_traverse_the_frozen_ten_thousand_row_fixture() {
    let fixture = install_fixture_canister("sql_perf");
    let loaded: Result<u32, Error> = fixture
        .update_candid("load_streaming_execution_continuation_fixture", ())
        .expect("continuation fixture row count should decode");
    assert_eq!(
        loaded.expect("continuation fixture should load"),
        STREAMING_EXECUTION_CONTINUATION_ROWS,
    );

    let live_ids = traverse_live_continuation_fixture(&fixture);
    let exhaustive_ids = traverse_exhaustive_continuation_fixture(&fixture);
    let expected_ids = (1..=i64::from(STREAMING_EXECUTION_CONTINUATION_ROWS)).collect::<Vec<_>>();

    assert_eq!(live_ids, expected_ids);
    assert_eq!(exhaustive_ids, expected_ids);
}

fn traverse_live_continuation_fixture(fixture: &StandaloneCanisterFixture) -> Vec<i64> {
    let mut continuation = None;
    let mut ids = Vec::new();
    let mut pages = 0_u32;
    let mut entries_visited = 0_u64;

    loop {
        let page: Result<LiveQueryPageOutput, Error> = fixture
            .query_candid(
                "query_streaming_execution_live_page",
                (continuation.clone(),),
            )
            .expect("live continuation page should decode");
        let page = page.expect("live continuation page should execute");
        pages = pages.saturating_add(1);
        entries_visited = entries_visited.saturating_add(page.work.entries_visited);
        append_dense_page_ids(&mut ids, &page.rows);
        assert_eq!(page.row_count as usize, page.rows.len());
        assert_eq!(page.work.result_rows, page.row_count);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
        assert!(
            page.row_count > 0,
            "dense live traversal must make progress"
        );
        assert!(pages < 100, "live traversal must terminate");
    }

    assert_eq!(pages, 10, "10,001 rows at 1,024 rows per page");
    assert_eq!(
        entries_visited,
        u64::from(STREAMING_EXECUTION_CONTINUATION_ROWS) + u64::from(pages - 1),
        "each nonterminal page may reread only its one unconsumed lookahead",
    );
    ids
}

fn traverse_exhaustive_continuation_fixture(fixture: &StandaloneCanisterFixture) -> Vec<i64> {
    let mut continuation = None;
    let mut proof: Option<ReadSetRevisionProof> = None;
    let mut ids = Vec::new();
    let mut pages = 0_u32;
    let mut entries_visited = 0_u64;

    loop {
        let page: Result<ExhaustiveQueryPageOutput, StreamingExhaustivePageError> = fixture
            .query_candid(
                "query_streaming_execution_exhaustive_page",
                (continuation.clone(), proof.clone()),
            )
            .expect("exhaustive continuation page should decode");
        let page = match page {
            Ok(page) => page,
            Err(StreamingExhaustivePageError::Database(error)) => {
                panic!("exhaustive continuation database read should execute: {error:?}")
            }
            Err(StreamingExhaustivePageError::Revision(error)) => {
                panic!("exhaustive continuation proof should remain valid: {error:?}")
            }
        };
        pages = pages.saturating_add(1);
        entries_visited = entries_visited.saturating_add(page.work.entries_visited);
        append_dense_page_ids(&mut ids, &page.rows);
        assert_eq!(page.row_count as usize, page.rows.len());
        assert_eq!(page.work.result_rows, page.row_count);
        proof = Some(page.proof.clone());
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
        assert!(
            page.row_count > 0,
            "dense exhaustive traversal must make progress"
        );
        assert!(pages < 100, "exhaustive traversal must terminate");
    }

    assert_eq!(pages, 10, "10,001 rows at 1,024 rows per page");
    assert_eq!(
        entries_visited,
        u64::from(STREAMING_EXECUTION_CONTINUATION_ROWS) + u64::from(pages - 1),
        "each nonterminal page may reread only its one unconsumed lookahead",
    );
    ids
}

fn append_dense_page_ids(ids: &mut Vec<i64>, rows: &[Vec<OutputValue>]) {
    for row in rows {
        let [OutputValue::Int64(id)] = row.as_slice() else {
            panic!("continuation fixture must return one Int64 id column");
        };
        let expected = i64::try_from(ids.len()).expect("fixture row count fits i64") + 1;
        assert_eq!(
            *id, expected,
            "continuation traversal skipped or repeated a row"
        );
        ids.push(*id);
    }
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
        assert_frozen_instruction_gate(declaration.id, &measured.attribution);
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

#[test]
fn grouped_routes_publish_closed_ordered_and_complete_hash_state_evidence() {
    let fixture = install_fixture_canister("sql_perf");
    let facts: Result<StreamingExecutionFixtureFacts, Error> = fixture
        .update_candid("load_streaming_execution_fixture", ())
        .expect("streaming fixture facts should decode");
    facts.expect("streaming fixture should load");

    let ordered_explain = query_explain_json(
        &fixture,
        "SELECT group_key, COUNT(*) FROM PerfAuditStreamingRow GROUP BY group_key ORDER BY group_key ASC LIMIT 10",
    );
    assert!(
        ordered_explain.contains("\"type\":\"IndexRange\"")
            && ordered_explain.contains("\"name\":\"idx_perf_audit_streaming_row__group_key_id\""),
        "the frozen ordered-group query must use the accepted group-key index: {ordered_explain}",
    );

    let ordered = query_with_perf(
        &fixture,
        "SELECT group_key, COUNT(*) FROM PerfAuditStreamingRow GROUP BY group_key ORDER BY group_key ASC LIMIT 10",
    );
    let SqlQueryResult::Grouped(ordered_rows) = &ordered.result else {
        panic!("ordered grouped fixture should return grouped rows");
    };
    assert_eq!(ordered_rows.row_count, 10);
    assert_eq!(
        ordered_rows.rows,
        vec![
            vec!["0".to_string(), "121".to_string()],
            vec!["1".to_string(), "121".to_string()],
            vec!["2".to_string(), "121".to_string()],
            vec!["3".to_string(), "121".to_string()],
            vec!["4".to_string(), "121".to_string()],
            vec!["5".to_string(), "121".to_string()],
            vec!["6".to_string(), "121".to_string()],
            vec!["7".to_string(), "121".to_string()],
            vec!["8".to_string(), "120".to_string()],
            vec!["9".to_string(), "120".to_string()],
        ],
        "ordered execution must emit only complete closed groups",
    );
    assert!(ordered_rows.next_cursor.is_some());
    let ordered_work = ordered
        .attribution
        .grouped
        .expect("ordered grouping should publish attribution");
    assert_eq!(ordered_work.groups_observed, ordered_work.groups_finalized);
    assert_eq!(ordered_work.peak_live_groups, 1);
    assert_eq!(ordered_work.peak_live_aggregate_states, 1);
    assert!(ordered_work.early_scan_stop);
    assert!(ordered_work.peak_estimated_state_bytes > 0);

    let hash_declaration = STREAMING_EXECUTION_FIXTURES
        .iter()
        .find(|declaration| declaration.id == "group_hash_noncontiguous")
        .expect("frozen hash-group declaration should exist");
    let hash_explain = query_explain_json(&fixture, hash_declaration.sql);
    assert!(
        hash_explain.contains("max_groups: 10000")
            && hash_explain.contains("max_group_bytes: 16777216"),
        "no-LIMIT hash grouping must carry finite planner-owned hard limits: {hash_explain}",
    );
    let hash = query_with_perf(&fixture, hash_declaration.sql);
    let SqlQueryResult::Grouped(hash_rows) = &hash.result else {
        panic!("hash grouped fixture should return grouped rows");
    };
    assert_eq!(hash_rows.row_count, 3);
    assert_eq!(
        hash_rows.rows,
        vec![
            vec!["early-wide".to_string(), "1".to_string()],
            vec!["late-match".to_string(), "1".to_string()],
            vec!["ordinary".to_string(), "2046".to_string()],
        ],
        "hash execution must combine noncontiguous rows before emitting groups",
    );
    assert!(hash_rows.next_cursor.is_none());
    let hash_work = hash
        .attribution
        .grouped
        .expect("hash grouping should publish attribution");
    assert_eq!(hash_work.rows_scanned, 2_048);
    assert_eq!(hash_work.groups_observed, 3);
    assert_eq!(hash_work.groups_finalized, 3);
    assert_eq!(hash_work.peak_live_groups, 3);
    assert_eq!(hash_work.peak_live_aggregate_states, 3);
    assert!(!hash_work.early_scan_stop);
    assert!(
        hash_work.peak_estimated_state_bytes > ordered_work.peak_estimated_state_bytes,
        "complete hash grouping should retain its three groups while ordered grouping retains one",
    );

    assert_unbounded_grouped_top_k_rejected(&fixture);

    println!(
        "icydb_0222_patch8 ordered_instructions={} ordered_rows_scanned={} ordered_peak_groups={} ordered_peak_bytes={} hash_instructions={} hash_rows_scanned={} hash_peak_groups={} hash_peak_bytes={}",
        ordered.attribution.total_local_instructions,
        ordered_work.rows_scanned,
        ordered_work.peak_live_groups,
        ordered_work.peak_estimated_state_bytes,
        hash.attribution.total_local_instructions,
        hash_work.rows_scanned,
        hash_work.peak_live_groups,
        hash_work.peak_estimated_state_bytes,
    );
}

fn assert_unbounded_grouped_top_k_rejected(fixture: &StandaloneCanisterFixture) {
    let rejected: Result<StreamingQueryPerfResult, Error> = fixture
        .query_candid(
            "query_streaming_execution_with_perf",
            ("SELECT label, COUNT(*) AS row_count FROM PerfAuditStreamingRow GROUP BY label ORDER BY row_count DESC, label ASC".to_string(),),
        )
        .expect("unbounded grouped Top-K rejection should decode");
    assert_eq!(
        rejected
            .expect_err("aggregate-driven grouped ordering must still require LIMIT")
            .diagnostic_code(),
        DiagnosticCode::QueryPlan,
    );
}

fn query_with_perf(fixture: &StandaloneCanisterFixture, sql: &str) -> StreamingQueryPerfResult {
    let measured: Result<StreamingQueryPerfResult, Error> = fixture
        .query_candid("query_streaming_execution_with_perf", (sql.to_string(),))
        .expect("streaming perf query should decode");

    measured.unwrap_or_else(|error| panic!("streaming perf query should execute: {sql}: {error:?}"))
}

fn assert_patch_6_scalar_evidence(fixture_id: &str, attribution: &SqlQueryExecutionAttribution) {
    let expected = match fixture_id {
        "limit1_late_selective" => Some((2_048, 1)),
        "topn_wide_payload" => Some((0, 10)),
        "full_sort_control" => Some((0, 2_048)),
        _ => None,
    };
    let Some((store_gets, retained_candidates)) = expected else {
        return;
    };

    assert_eq!(
        attribution.store_get_calls, store_gets,
        "{fixture_id} stored-row read evidence",
    );
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

fn assert_frozen_instruction_gate(fixture_id: &str, attribution: &SqlQueryExecutionAttribution) {
    let ceiling = match fixture_id {
        "prefix_family_max_fanout" => Some(PREFIX_FAMILY_MAX_FANOUT_INSTRUCTION_CEILING),
        "intersection_2_sparse" => Some(INTERSECTION_2_SPARSE_INSTRUCTION_CEILING),
        "topn_wide_payload" => Some(TOPN_WIDE_PAYLOAD_INSTRUCTION_CEILING),
        _ => None,
    };
    let Some(ceiling) = ceiling else {
        return;
    };

    assert!(
        attribution.total_local_instructions <= ceiling,
        "{fixture_id} exceeded its frozen instruction ceiling: {} > {ceiling}: {attribution:?}",
        attribution.total_local_instructions,
    );
    if fixture_id == "prefix_family_max_fanout" {
        assert!(
            attribution.index_store_entry_reads < 68,
            "prefix merge must improve on the frozen 68-entry baseline",
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
        sparse.contains("\"access_bound_predicate_count\":2"),
        "{sparse}"
    );
    assert!(
        sparse.contains("\"has_residual_predicate\":false"),
        "{sparse}"
    );
    assert!(
        sparse.contains("\"residual_predicate_count\":0"),
        "{sparse}"
    );
    let sparse_execution = query_explain_statement(
        &fixture,
        "EXPLAIN EXECUTION SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
    );
    assert!(
        sparse_execution.contains("residual_filter_predicate=And"),
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
        | SqlQueryResult::ShowColumns(_)
        | SqlQueryResult::ShowRelations(_)
        | SqlQueryResult::ShowEntities { .. }
        | SqlQueryResult::ShowStores { .. }
        | SqlQueryResult::ShowMemory { .. }
        | SqlQueryResult::Ddl { .. } => 0,
    }
}
