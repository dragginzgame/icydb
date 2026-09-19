//! End-to-end batch evidence over the existing retained SQL fixture actor.

use crate::{expect_projection, query_sql};
use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, types::Ulid};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
    CanisterWasmProfile, build_canister_with_options, install_prebuilt_fixture_canister,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(CandidType, Debug, Deserialize)]
struct BatchWorkloadSample {
    result: Result<(), Error>,
    rejected_at: Option<u32>,
    read_instructions: u64,
    validation_instructions: u64,
    write_instructions: u64,
    total_instructions: u64,
}

// Expected data is defined independently from the actor's implementation and
// checked through a different, maintained read surface after every scenario.
fn assert_rows(fixture: &StandaloneCanisterFixture, count: u32, committed: u32, reject_last: bool) {
    let output = expect_projection(
        query_sql(
            fixture,
            "SELECT id, display_name FROM SqlTestEnrollmentUser ORDER BY id LIMIT 256",
        )
        .expect("independent readback"),
    );
    let mut expected = (0..count)
        .map(|position| {
            vec![
                Ulid::from_bytes((u128::from(position) + 1_000).to_be_bytes()).to_string(),
                if reject_last && position == count - 1 {
                    "blocked".to_string()
                } else {
                    format!("eligible-{position}")
                },
            ]
        })
        .collect::<Vec<_>>();
    expected.extend((0..committed).map(|position| {
        vec![
            Ulid::from_bytes((u128::from(position) + 2_000).to_be_bytes()).to_string(),
            format!("processed:eligible-{position}"),
        ]
    }));
    assert_eq!(output.rendered_rows(), expected);
}

fn qualify_scenario(module: &[u8], count: u32, chunk: u32, individual: bool, reject_last: bool) {
    let fixture = install_prebuilt_fixture_canister("sql", module.to_vec());
    let seeded: Result<(), Error> = fixture
        .update_candid("seed_batch_workload", (count, reject_last))
        .expect("seed response");
    seeded.expect("seed succeeds outside measurement");
    assert_rows(&fixture, count, 0, reject_last);
    let mut committed = 0;
    let mut cycle_total = 0_u128;
    let mut instruction_total = 0_u64;
    let mut calls = 0;
    for start in (0..count).step_by(usize::try_from(chunk).unwrap()) {
        let length = chunk.min(count - start);
        // Drain setup/previous-message work outside the explicit message cost.
        // This deliberately excludes asynchronous journal convergence costs.
        for _ in 0..64 {
            fixture.pocket_ic().tick();
        }
        let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
        let sample: BatchWorkloadSample = fixture
            .update_candid("measure_batch_workload", (start, length, individual))
            .expect("sample response");
        let cycles = before
            .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
            .expect("update consumes cycles");
        sample
            .result
            .as_ref()
            .expect("bounded application workload succeeds");
        let rejected = reject_last && start + length == count;
        assert_eq!(sample.rejected_at, rejected.then_some(count - 1));
        assert!(sample.read_instructions > 0);
        assert!(sample.validation_instructions > 0);
        assert_eq!(sample.write_instructions == 0, rejected);
        let stages =
            sample.read_instructions + sample.validation_instructions + sample.write_instructions;
        assert!(sample.total_instructions >= stages);
        if !rejected {
            committed += length;
        }
        cycle_total += cycles;
        instruction_total += sample.total_instructions;
        calls += 1;
        println!(
            "batch_message count={count} chunk={chunk} individual={individual} reject_last={reject_last} start={start} read={} validate={} write={} total={} cycles={cycles}",
            sample.read_instructions,
            sample.validation_instructions,
            sample.write_instructions,
            sample.total_instructions
        );
        // Check every committed prefix, not only the endpoint's self-report.
        assert_rows(&fixture, count, committed, reject_last);
    }
    assert_eq!(
        committed,
        if reject_last {
            count - chunk.min(count)
        } else {
            count
        }
    );
    println!(
        "batch_total count={count} chunk={chunk} individual={individual} reject_last={reject_last} calls={calls} committed={committed} instructions={instruction_total} cycles={cycle_total}"
    );
}

#[test]
fn application_batch_workload_preserves_results_and_reports_costs() {
    let artifact = build_canister_with_options(
        "sql",
        CanisterBuildOptions {
            profile: CanisterWasmProfile::WasmRelease,
            sql_mode: CanisterSqlMode::Enabled,
            candid_export: CanisterCandidExportMode::Enabled,
            build_profile: CanisterBuildProfile::LocalTest,
        },
    )
    .expect("retained actor build");
    let module = std::fs::read(&artifact).expect("read while retention is alive");
    println!(
        "batch_wasm path={} raw_bytes={} sha256={:x}",
        artifact.as_ref().display(),
        module.len(),
        Sha256::digest(&module)
    );
    for count in [1, 16, 64, 128] {
        for individual in [false, true] {
            qualify_scenario(&module, count, count, individual, false);
        }
    }
    // Chunking is application-owned across messages, not an atomic replacement:
    // a rejection in the last chunk retains the seven earlier committed chunks.
    qualify_scenario(&module, 128, 128, false, true);
    qualify_scenario(&module, 128, 16, false, false);
    qualify_scenario(&module, 128, 16, false, true);
}
