//! Matched secondary-resume costs from a supplied canonical audit artifact.

use super::{preparation_measurement_wasm, settle_measurement_rounds};
use ic_host_artifacts::artifact::Sha256Digest;
use icydb::{
    Error,
    db::{ScalarPageWork, sql::SqlQueryResult},
};
use icydb_testing_audit_sql_perf_fixtures::contracts::{ScaleFixtureFacts, ScalePayloadProfile};
use icydb_testing_integration::{WireCallExt as _, install_prebuilt_fixture_canister};

#[derive(Debug, serde::Deserialize)]
struct PageSample {
    ids: Vec<i32>,
    continuation: Option<String>,
    work: ScalarPageWork,
    instructions: u64,
}

/// Setup and suffix replay are outside the measured update envelopes. The
/// actor's counter covers the engine request, not reply encoding or IC overhead.
#[test]
#[ignore = "manual wasm-release secondary-resume instruction and cycle measurement"]
fn secondary_resume_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    println!(
        "secondary_wasm sha256={} raw_bytes={}",
        Sha256Digest::compute(&module),
        module.len()
    );
    for rows in [256_u32, 2_048] {
        let fixture = install_prebuilt_fixture_canister("sql_perf", module.clone());
        let loaded: Result<ScaleFixtureFacts, Error> = fixture
            .pocket_ic()
            .update_wire(fixture.canister_id(), "load_user_scale_fixture", (rows,))
            .expect("loader should decode");
        let loaded = loaded.expect("scale fixture should load");
        assert_eq!(loaded.profile_version, 1);
        assert_eq!(loaded.surface, "user");
        assert_eq!(loaded.fixture_rows, rows);
        assert_eq!(loaded.zero_match_rows, 0);
        assert_eq!(loaded.one_match_rows, 1);
        assert_eq!(loaded.quarter_match_rows, rows / 4);
        assert_eq!(loaded.all_match_rows, rows);
        assert!(matches!(
            loaded.payload_profile,
            ScalePayloadProfile::NotApplicable
        ));
        let explain: Result<SqlQueryResult, Error> = fixture
            .pocket_ic()
            .query_wire(
                fixture.canister_id(),
                "query_user",
                ("EXPLAIN EXECUTION SELECT id FROM PerfAuditUser WHERE name >= 'scale-group-001' AND name <= 'scale-group-100' ORDER BY name, id LIMIT 100".to_string(),),
            )
            .expect("execution explanation should decode");
        let SqlQueryResult::Explain { explain, .. } =
            explain.expect("execution explanation should execute")
        else {
            panic!("expected an execution explanation");
        };
        assert!(explain.contains("IndexRangeScan"), "{explain}");
        assert!(explain.contains("SecondaryOrderPushdown"), "{explain}");
        println!("secondary_plan rows={rows} {explain:?}");
        for descending in [false, true] {
            for residual in [false, true] {
                for row_backed in [false, true] {
                    let mut expected: Vec<_> = (1..=i32::try_from(rows).unwrap())
                        .filter(|id| !residual || *id <= i32::try_from(rows / 4).unwrap())
                        .collect();
                    // Names repeat every 100 IDs. Sort independently from the
                    // index encoder and compare every emitted/replayed page.
                    expected.sort_by_key(|id| ((id - 1) % 100, *id));
                    if descending {
                        expected.reverse();
                    }
                    let mut cursor = None;
                    let mut actual = Vec::new();
                    let mut instructions = 0_u64;
                    let mut cycles = 0_u128;
                    let mut pages = 0;
                    loop {
                        assert!(pages < 64, "fixture traversal must remain bounded");
                        let input = cursor.clone();
                        let (sample, cost) =
                            sample_page(&fixture, descending, residual, row_backed, input.clone());
                        assert!(sample.ids.len() <= 100);
                        instructions += sample.instructions;
                        cycles += cost;
                        println!(
                            "secondary_page rows={rows} descending={descending} residual={residual} row_backed={row_backed} page={pages} output={} entries={} instructions={} cycles={cost}",
                            sample.ids.len(),
                            sample.work.entries_visited,
                            sample.instructions
                        );
                        // Replaying each authenticated page input must preserve
                        // its answer, next token and physical-work receipt.
                        let (replay, _) =
                            sample_page(&fixture, descending, residual, row_backed, input);
                        assert_eq!(replay.ids, sample.ids);
                        assert_eq!(replay.continuation, sample.continuation);
                        assert_eq!(replay.work, sample.work);
                        actual.extend(sample.ids);
                        cursor = sample.continuation;
                        pages += 1;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    assert_eq!(actual, expected);
                    println!(
                        "secondary_total rows={rows} descending={descending} residual={residual} row_backed={row_backed} pages={pages} instructions={instructions} cycles={cycles}"
                    );
                }
            }
        }
    }
}

fn sample_page(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    descending: bool,
    residual: bool,
    row_backed: bool,
    cursor: Option<String>,
) -> (PageSample, u128) {
    settle_measurement_rounds(fixture);
    let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let sample: Result<PageSample, Error> = fixture
        .pocket_ic()
        .update_wire(
            fixture.canister_id(),
            "measure_secondary_resume_page",
            (descending, residual, row_backed, cursor),
        )
        .expect("page sample should decode");
    let cycles = before
        .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
        .expect("update should consume cycles");
    (sample.expect("public page should execute"), cycles)
}
