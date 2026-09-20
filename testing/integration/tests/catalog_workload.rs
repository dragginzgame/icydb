//! Matched full-row catalogue measurement; not a Toko Miner deployment.

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::Error;
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
    CanisterWasmProfile, build_canister_with_options, install_prebuilt_fixture_canister,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct CatalogWorkloadRow {
    id: u64,
    key: String,
    name: String,
    description: String,
    placement: Option<CatalogWorkloadPlacement>,
    capacity: u64,
}

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct CatalogWorkloadPlacement {
    shape: Vec<u8>,
    points: Vec<(u64, u64)>,
    asset: String,
    enabled: bool,
}

#[derive(CandidType, Deserialize)]
struct CatalogWorkloadSample {
    result: Result<Vec<CatalogWorkloadRow>, Error>,
    total_instructions: u64,
    binding_instructions: u64,
    page_instructions: u64,
    adapter_instructions: u64,
    pages: u32,
}

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct CatalogLabel {
    id: u64,
    key: String,
    name: String,
}

#[derive(CandidType, Deserialize)]
struct CatalogLabelSample {
    result: Result<Vec<CatalogLabel>, Error>,
    instructions: u64,
    pages: u32,
}

#[test]
fn full_catalogue_rows_preserve_values_and_report_costs() {
    let artifact = build_canister_with_options(
        "sql",
        CanisterBuildOptions {
            profile: CanisterWasmProfile::WasmRelease,
            sql_mode: CanisterSqlMode::Enabled,
            candid_export: CanisterCandidExportMode::Enabled,
            build_profile: CanisterBuildProfile::LocalTest,
        },
    )
    .expect("retained catalogue actor build");
    let module = std::fs::read(&artifact).expect("read retained module");
    println!(
        "catalog_wasm path={} raw_bytes={} sha256={:x}",
        artifact.as_ref().display(),
        module.len(),
        Sha256::digest(&module)
    );
    for count in [16_u32, 128] {
        let fixture = install_prebuilt_fixture_canister("sql", module.clone());
        for start in (0..count).step_by(16) {
            let seeded: Result<(), Error> = fixture
                .update_candid("seed_catalog_workload", (start, 16_u32))
                .expect("seed reply");
            seeded.expect("seed outside measurement");
            // Seed convergence is outside query measurement; exercise the
            // ordinary lifecycle instead of exhausting pending journal work.
            fixture.pocket_ic().advance_time(Duration::from_secs(1));
            for _ in 0..64 {
                fixture.pocket_ic().tick();
            }
        }
        let expected: Vec<_> = (0..u64::from(count))
            .map(|id| CatalogWorkloadRow {
                id,
                key: format!("item-{id:04}"),
                name: format!("Item {id}"),
                description: "catalogue description ".repeat(8),
                capacity: id + 1,
                placement: (id % 3 != 0).then(|| CatalogWorkloadPlacement {
                    shape: vec![u8::try_from(id).unwrap(); 768],
                    points: (0..4).map(|x| (x, id + x)).collect(),
                    asset: format!("asset-{id}"),
                    enabled: id % 2 == 0,
                }),
            })
            .collect();
        for staged in [false, true] {
            for observation in 0..2 {
                for _ in 0..64 {
                    fixture.pocket_ic().tick();
                }
                let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
                let sample: CatalogWorkloadSample = fixture
                    .update_candid("measure_catalog_workload", (staged,))
                    .expect("measurement reply");
                let cycles = before
                    .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
                    .expect("update debits cycles");
                assert_eq!(sample.result.expect("typed query succeeds"), expected);
                assert!(sample.total_instructions > 0);
                assert_eq!(sample.pages, if count > 100 { 2 } else { 1 });
                if staged {
                    assert!(
                        sample.binding_instructions > 0
                            && sample.page_instructions > 0
                            && sample.adapter_instructions > 0
                    );
                }
                println!(
                    "catalog_message count={count} staged={staged} observation={observation} instructions={} binding={} page={} adapter={} pages={} cycles={cycles}",
                    sample.total_instructions,
                    sample.binding_instructions,
                    sample.page_instructions,
                    sample.adapter_instructions,
                    sample.pages
                );
            }
        }
        check_catalog_labels(&fixture, count);
    }
}

fn check_catalog_labels(fixture: &StandaloneCanisterFixture, count: u32) {
    let expected_labels: Vec<_> = (0..u64::from(count))
        .map(|id| CatalogLabel {
            id,
            key: format!("item-{id:04}"),
            name: format!("Item {id}"),
        })
        .collect();
    // Bracket selected calls with full reads on the same data and actor.
    // These are ordered observations, not isolated cold/warm cache claims.
    for (observation, selected) in [false, true, true, false].into_iter().enumerate() {
        for _ in 0..64 {
            fixture.pocket_ic().tick();
        }
        let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
        let sample: CatalogLabelSample = fixture
            .update_candid("measure_catalog_labels", (selected,))
            .expect("label measurement reply");
        let cycles = before
            .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
            .expect("label update debits cycles");
        assert_eq!(
            sample.result.expect("label query succeeds"),
            expected_labels
        );
        assert!(sample.instructions > 0);
        assert_eq!(sample.pages, if count > 100 { 2 } else { 1 });
        println!(
            "catalog_labels count={count} selected={selected} observation={observation} instructions={} pages={} cycles={cycles}",
            sample.instructions, sample.pages
        );
    }
}
