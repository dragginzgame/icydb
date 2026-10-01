//! Frozen current-path evidence, reusable for a matched production candidate.

use super::{SqlQueryPerfResult, preparation_measurement_wasm, settle_measurement_rounds};
use candid::CandidType;
use icydb::{
    Error,
    db::{ScalarPageWork, sql::SqlQueryResult},
    value::OutputValue,
};
use icydb_testing_integration::install_prebuilt_fixture_canister;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(CandidType, Debug, Deserialize)]
struct IntersectionPageSample {
    ids: Vec<i32>,
    continuation: Option<String>,
    work: ScalarPageWork,
    instructions: u64,
}

// Independent answer table: do not derive expected rows by executing the
// fixture loader's membership function or consulting the chosen index.
fn expected_ids(
    case: u8,
    children: u8,
    descending: bool,
    residual: bool,
    limit: Option<u32>,
) -> Vec<i32> {
    let mut ids: Vec<_> = match (case, children) {
        (0, 2) => (7..128).step_by(8).collect(),
        (0, 3) => (15..128).step_by(16).collect(),
        (1, _) => Vec::new(),
        (2 | 4, 2) => (112..128).collect(),
        (2, 3) | (5, _) => (120..128).collect(),
        (3 | 10 | 13, _) => (0..128).collect(),
        (4, 3) | (6, _) => (116..128).collect(),
        (7 | 15, _) => (0..16).collect(),
        (8, _) => (0..80).collect(),
        (9, _) => (0..112).collect(),
        (11, _) => (0..144).collect(),
        (12 | 14, _) => (0..160).collect(),
        (16, _) => (0..20).collect(),
        (17, _) => (0..512).collect(),
        (18, _) => (32..160).collect(),
        (19, _) => (0..160).filter(|id| id % 5 < 4).collect(),
        _ => panic!("unknown frozen workload"),
    };
    if residual {
        ids.retain(|id| id % 2 == 1);
    }
    if descending {
        ids.reverse();
    }
    if let Some(limit) = limit {
        ids.truncate(limit as usize);
    }
    ids
}

fn sample_page(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    children: u8,
    descending: bool,
    residual: bool,
    wide: bool,
    limit: Option<u32>,
    continuation: Option<String>,
) -> (IntersectionPageSample, u128) {
    settle_measurement_rounds(fixture);
    let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let sample: Result<IntersectionPageSample, Error> = fixture
        .update_candid(
            "measure_seek_intersection_page",
            (children, descending, residual, wide, limit, continuation),
        )
        .expect("page sample should decode");
    let cycles = before
        .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
        .expect("query update should consume cycles");
    (sample.expect("live page should execute"), cycles)
}

fn assert_explain(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    case: u8,
    children: u8,
    descending: bool,
) {
    let direction = if descending { "DESC" } else { "ASC" };
    let third = if children == 3 {
        " AND group_key = 0"
    } else {
        ""
    };
    let explain: Result<SqlQueryResult, Error> = fixture.query_candid("query_user", (
        format!("EXPLAIN EXECUTION SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0{third} ORDER BY id {direction}"),
    )).expect("EXPLAIN should decode");
    let SqlQueryResult::Explain { explain, .. } = explain.expect("EXPLAIN should execute") else {
        panic!("expected execution descriptor");
    };
    assert!(explain.contains("Intersection"), "{explain}");
    println!("seek_plan case={case} children={children} descending={descending} {explain:?}");
}

fn assert_resume_suffixes(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    children: u8,
    descending: bool,
    wide: bool,
    limit: Option<u32>,
    tokens: Vec<(String, usize)>,
    expected: &[i32],
) {
    for (token, offset) in tokens {
        let mut cursor = Some(token);
        let mut suffix = Vec::new();
        for _ in 0..32 {
            let (sample, _) =
                sample_page(fixture, children, descending, false, wide, limit, cursor);
            suffix.extend(sample.ids);
            cursor = sample.continuation;
            if cursor.is_none() {
                break;
            }
        }
        assert!(cursor.is_none());
        assert_eq!(suffix, expected[offset..]);
    }
}

fn measure_sql(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    case: u8,
    children: u8,
    descending: bool,
) {
    let direction = if descending { "DESC" } else { "ASC" };
    let third = if children == 3 {
        " AND group_key = 0"
    } else {
        ""
    };
    let sql = format!(
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0{third} ORDER BY id {direction}"
    );
    for repeat in 0..2 {
        settle_measurement_rounds(fixture);
        let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
        let sample: Result<SqlQueryPerfResult, Error> = fixture
            .update_candid("warm_user_query_with_perf", (sql.clone(),))
            .expect("SQL sample should decode");
        let cycles = before
            .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
            .unwrap();
        let sample = sample.expect("SQL sample should execute");
        let SqlQueryResult::Projection(rows) = sample.result else {
            panic!("expected rows");
        };
        let expected: Vec<_> = expected_ids(case, children, descending, false, None)
            .into_iter()
            .map(|id| vec![OutputValue::int64(i64::from(id))])
            .collect();
        assert_eq!(rows.rows, expected);
        println!(
            "seek_sql_sample case={case} children={children} descending={descending} repeat={repeat} rows={} instructions={} cycles={cycles}",
            rows.row_count, sample.instructions
        );
    }
}

/// Deliberately manual: records IC costs from one frozen post-linked artifact.
/// Every continuation comes from the engine; every suffix is replayed exactly.
#[test]
#[ignore = "manual wasm-release sparse intersection qualification"]
fn current_path_wasm_cost_matrix() {
    run_wasm_cost_cases(&[0, 1, 2, 3, 4, 5, 6]);
}

/// Bounded matched controls for the polling retirement decision.
#[test]
#[ignore = "manual wasm-release polling retirement controls"]
fn polling_wasm_cost_controls() {
    run_wasm_cost_cases(&[0, 5]);
}

/// Manual crossover evidence. Fixed controls avoid multiplying every density,
/// width, placement and population into an unrelated benchmark subsystem.
#[test]
#[ignore = "manual wasm-release dense selection crossover qualification"]
fn dense_selection_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    println!(
        "seek_wasm sha256={:x} raw_bytes={}",
        Sha256::digest(&module),
        module.len()
    );
    for case in 7..=19_u8 {
        let fixture = install_prebuilt_fixture_canister("sql_perf", module.clone());
        let rows: u16 = match case {
            15 | 16 => 20,
            17 => 640,
            _ => 160,
        };
        for start in (0..rows).step_by(4) {
            let loaded: Result<u32, Error> = fixture
                .update_candid("load_seek_intersection_fixture", (case, start))
                .expect("crossover loader should decode");
            assert_eq!(loaded.expect("bounded batch should load"), 4);
            // Keep setup journal debt bounded in large and wide controls.
            if [15, 16].contains(&case) || start % 64 == 60 {
                settle_measurement_rounds(&fixture);
            }
        }
        settle_measurement_rounds(&fixture);
        let child_counts: &[u8] = if case == 12 { &[2, 3] } else { &[2] };
        for &children in child_counts {
            for descending in [false, true] {
                assert_explain(&fixture, case, children, descending);
                for limit in [None, Some(1), Some(5)] {
                    measure_dynamic_pages(
                        &fixture,
                        case,
                        children,
                        descending,
                        (13..=16).contains(&case),
                        limit,
                    );
                }
            }
        }
    }
}

// One page/suffix protocol serves both manual matrices. Cost intervals exclude
// independent suffix replay and setup, and repeat the identical page input.
fn measure_dynamic_pages(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    case: u8,
    children: u8,
    descending: bool,
    wide: bool,
    limit: Option<u32>,
) {
    let expected = expected_ids(case, children, descending, false, limit);
    let mut continuation = None;
    let mut actual = Vec::new();
    let mut tokens = Vec::new();
    let mut pages = 0;
    for page in 0..32 {
        let input = continuation.clone();
        let (sample, cycles) = sample_page(
            fixture,
            children,
            descending,
            false,
            wide,
            limit,
            input.clone(),
        );
        let (repeat, repeat_cycles) =
            sample_page(fixture, children, descending, false, wide, limit, input);
        assert_eq!(repeat.ids, sample.ids);
        assert_eq!(repeat.continuation, sample.continuation);
        assert_eq!(repeat.work, sample.work);
        println!(
            "seek_sample case={case} children={children} descending={descending} limit={} page={page} rows={} entries={} instructions={} cycles={cycles} repeat_instructions={} repeat_cycles={repeat_cycles}",
            limit.unwrap_or(0),
            sample.ids.len(),
            sample.work.entries_visited,
            sample.instructions,
            repeat.instructions
        );
        assert_eq!(sample.work.result_rows as usize, sample.ids.len());
        assert!(sample.instructions > 0 && cycles > 0);
        actual.extend(sample.ids);
        pages += 1;
        continuation = sample.continuation;
        if let Some(token) = &continuation {
            tokens.push((token.clone(), actual.len()));
        } else {
            break;
        }
    }
    assert!(continuation.is_none(), "page traversal must terminate");
    assert_eq!(actual, expected);
    if [4, 6, 15, 16].contains(&case) && limit.is_none() {
        assert!(pages > 1, "wide rows must exercise real resume");
    }
    assert_resume_suffixes(
        fixture, children, descending, wide, limit, tokens, &expected,
    );
}

fn run_wasm_cost_cases(cases: &[u8]) {
    let module = preparation_measurement_wasm();
    println!(
        "seek_wasm sha256={:x} raw_bytes={}",
        Sha256::digest(&module),
        module.len()
    );
    for &case in cases {
        let fixture = install_prebuilt_fixture_canister("sql_perf", module.clone());
        for start in (0..160_u16).step_by(4) {
            let loaded: Result<u32, Error> = fixture
                .update_candid("load_seek_intersection_fixture", (case, start))
                .expect("fixture loader should decode");
            assert_eq!(loaded.expect("fixture batch should load"), 4);
            if [4, 6].contains(&case) {
                settle_measurement_rounds(&fixture);
            }
        }
        settle_measurement_rounds(&fixture);
        for children in [2_u8, 3] {
            for descending in [false, true] {
                assert_explain(&fixture, case, children, descending);
                if ![4, 6].contains(&case) {
                    measure_sql(&fixture, case, children, descending);
                }
                measure_dynamic_pages(
                    &fixture,
                    case,
                    children,
                    descending,
                    [4, 6].contains(&case),
                    None,
                );
                // Small total limits are not page-size controls. Residual
                // filtering remains owned by the ordinary live query pipeline.
                for (residual, limit) in [(true, None), (false, Some(1)), (true, Some(5))] {
                    if [4, 6].contains(&case) {
                        continue;
                    }
                    let (sample, _) =
                        sample_page(&fixture, children, descending, residual, false, limit, None);
                    assert_eq!(
                        sample.ids,
                        expected_ids(case, children, descending, residual, limit)
                    );
                    assert!(sample.continuation.is_none());
                }
            }
        }
    }
}
