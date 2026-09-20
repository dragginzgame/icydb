//! Collection costs and independent typed results over one retained actor.

use candid::CandidType;
use icydb::{
    Error,
    value::{OutputValue, PublicValue},
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
    CanisterWasmProfile, build_canister_with_options, install_prebuilt_fixture_canister,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(CandidType, Deserialize)]
struct CollectionWorkloadSample {
    result: Result<Vec<Vec<OutputValue>>, Error>,
    query_instructions: u64,
}

#[test]
fn collection_workload_preserves_results_and_reports_costs() {
    let artifact = build_canister_with_options(
        "sql",
        CanisterBuildOptions {
            profile: CanisterWasmProfile::WasmRelease,
            sql_mode: CanisterSqlMode::Enabled,
            candid_export: CanisterCandidExportMode::Enabled,
            build_profile: CanisterBuildProfile::LocalTest,
        },
    )
    .expect("retained collection actor build");
    let module = std::fs::read(&artifact).expect("read while artifact retention is alive");
    println!(
        "collection_wasm path={} raw_bytes={} sha256={:x}",
        artifact.as_ref().display(),
        module.len(),
        Sha256::digest(&module)
    );

    for length in [16_u32, 256, 1_024] {
        let fixture = install_prebuilt_fixture_canister("sql", module.clone());
        let seeded: Result<(), Error> = fixture
            .update_candid("seed_collection_workload", (length,))
            .expect("seed response");
        seeded.expect("seed succeeds outside the measured messages");
        for (scenario, name) in [
            "scalar",
            "early",
            "late",
            "absent",
            "nonempty",
            "empty",
            "null",
            "project_list",
            "early_project_list",
        ]
        .into_iter()
        .enumerate()
        {
            for observation in 0..2 {
                // Only already queued work is drained; no plan-driving or time
                // advancement is used to manufacture a cheap measured request.
                for _ in 0..64 {
                    fixture.pocket_ic().tick();
                }
                let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
                let sample: CollectionWorkloadSample = fixture
                    .update_candid(
                        "measure_collection_workload",
                        (length, u8::try_from(scenario).unwrap()),
                    )
                    .expect("measurement response");
                let cycles = before
                    .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
                    .expect("update consumes cycles");
                let rows = sample
                    .result
                    .expect("bounded structural query succeeds")
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(OutputValue::into_public)
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                let expected = match name {
                    "absent" | "null" => vec![],
                    "empty" => vec![vec![PublicValue::Nat64(2)]],
                    "project_list" | "early_project_list" => vec![vec![
                        PublicValue::Nat64(1),
                        PublicValue::List((0..u64::from(length)).map(PublicValue::Nat64).collect()),
                    ]],
                    _ => vec![vec![PublicValue::Nat64(1)]],
                };
                assert_eq!(rows, expected, "length={length} scenario={name}");
                assert!(sample.query_instructions > 0);
                println!(
                    "collection_message length={length} scenario={name} observation={observation} rows={} query_instructions={} cycles={cycles}",
                    rows.len(),
                    sample.query_instructions
                );
            }
        }
    }
}
