//! Shared-source model generation must survive real install and upgrade.

use ic_testkit::{
    pic::{InstallSpec, StandaloneCanisterFixture},
    pocket_ic::PocketIc,
};
use icydb_testing_integration::deliver_fixture_startup_watchdog;

#[test]
#[ignore = "requires ICYDB_MODEL_FACADE_WASM built from icydb-testing-model-facade-only"]
fn single_package_model_survives_install_write_and_upgrade() {
    let wasm = std::fs::read(std::env::var("ICYDB_MODEL_FACADE_WASM").unwrap()).unwrap();
    let fixture = StandaloneCanisterFixture::install(
        PocketIc::new(),
        InstallSpec::new(
            wasm.clone(),
            candid::encode_args(()).unwrap(),
            10_000_000_000_000,
        )
        .label("model_facade"),
    );
    let before_startup = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    deliver_fixture_startup_watchdog(&fixture);
    let startup_cycles = before_startup - fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .unwrap();
    println!(
        "startup_watchdog_cycles={startup_cycles} stable_bytes={}",
        status.memory_metrics.stable_memory_size
    );
    let rank: Result<Option<u64>, String> = fixture.query_candid("profile_rank", ()).unwrap();
    assert_eq!(rank.unwrap(), None);
    for sample in 0..3 {
        let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
        let instructions: Result<u64, String> = fixture.update_candid("open_database", ()).unwrap();
        let cycles = before - fixture.pocket_ic().cycle_balance(fixture.canister_id());
        println!(
            "warm_open_sample={sample} instructions={} cycles={cycles}",
            instructions.unwrap()
        );
    }
    let before_write = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let write: Result<(), String> = fixture.update_candid("insert_profile", (19_u64,)).unwrap();
    write.unwrap();
    println!(
        "typed_write_cycles={}",
        before_write - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let rank: Result<Option<u64>, String> = fixture.query_candid("profile_rank", ()).unwrap();
    assert_eq!(rank.unwrap(), Some(19));
    let before_upgrade = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            wasm,
            candid::encode_args(()).unwrap(),
            None,
        )
        .unwrap();
    println!(
        "upgrade_cycles={}",
        before_upgrade - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let before_recovery = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    deliver_fixture_startup_watchdog(&fixture);
    println!(
        "recovery_watchdog_cycles={}",
        before_recovery - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let rank: Result<Option<u64>, String> = fixture.query_candid("profile_rank", ()).unwrap();
    assert_eq!(rank.unwrap(), Some(19));
}

#[test]
#[ignore = "requires ICYDB_MODEL_FACADE_WASM built from icydb-testing-model-facade-only"]
fn single_package_install_cycle_cost() {
    let wasm = std::fs::read(std::env::var("ICYDB_MODEL_FACADE_WASM").unwrap()).unwrap();
    let pic = PocketIc::new();
    let canister = pic.create_canister();
    pic.add_cycles(canister, 10_000_000_000_000);
    // PocketIC also supplies an initial balance: do not infer it from funding.
    let before = pic.cycle_balance(canister);
    pic.install_canister(canister, wasm, candid::encode_args(()).unwrap(), None);
    println!("install_cycles={}", before - pic.cycle_balance(canister));
}
