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
    let before_write = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let write: Result<(), String> = fixture.update_candid("insert_profile", (19_u64,)).unwrap();
    write.unwrap();
    println!(
        "typed_write_cycles={}",
        before_write - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let rank: Result<Option<u64>, String> = fixture.query_candid("profile_rank", ()).unwrap();
    assert_eq!(rank.unwrap(), Some(19));
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            wasm,
            candid::encode_args(()).unwrap(),
            None,
        )
        .unwrap();
    deliver_fixture_startup_watchdog(&fixture);
    let rank: Result<Option<u64>, String> = fixture.query_candid("profile_rank", ()).unwrap();
    assert_eq!(rank.unwrap(), Some(19));
}
