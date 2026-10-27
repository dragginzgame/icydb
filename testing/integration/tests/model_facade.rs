//! Shared-source model generation must survive real install and upgrade.

use ic_testkit::pic::{InstallSpec, StandaloneCanisterFixture};
use icydb_testing_build::{CanisterWasmProfile, build_fixture_variant_wasm_with_profile};
use icydb_testing_integration::WireCallExt as _;
use icydb_testing_integration::deliver_fixture_startup_watchdog;

// The ordinary retained build owner binds the actor to current source/features
// and applies the canonical post-link pipeline before either scenario installs it.
fn model_facade_wasm() -> Vec<u8> {
    build_fixture_variant_wasm_with_profile(
        "icydb-testing-model-facade-only",
        "",
        "model-facade",
        CanisterWasmProfile::WasmRelease,
    )
    .expect("standalone facade actor should build")
}

fn init_arguments() -> Vec<u8> {
    canic_icydb::wire::encode_init_arguments(
        (),
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .expect("unit init arguments should encode")
}

#[test]
#[ignore = "manual standalone facade install/write/upgrade qualification"]
fn single_package_model_survives_install_write_and_upgrade() {
    let wasm = model_facade_wasm();
    let fixture = StandaloneCanisterFixture::try_install(
        icydb_testing_integration::start_fixture_pocket_ic(),
        InstallSpec {
            wasm: wasm.clone(),
            init_bytes: init_arguments(),
            cycles: 10_000_000_000_000,
            install_sender: None,
            label: Some("model_facade".to_string()),
        },
    )
    .unwrap_or_else(|error| panic!("install model-facade fixture: {error}"));
    let before_startup = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    deliver_fixture_startup_watchdog(fixture.pocket_ic());
    let startup_cycles = before_startup - fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .unwrap();
    println!(
        "startup_watchdog_cycles={startup_cycles} stable_bytes={}",
        status.memory_metrics.stable_memory_size
    );
    let rank: Result<Option<u64>, String> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "profile_rank", ())
        .unwrap();
    assert_eq!(rank.unwrap(), None);
    for sample in 0..3 {
        let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
        let instructions: Result<u64, String> = fixture
            .pocket_ic()
            .update_wire(fixture.canister_id(), "open_database", ())
            .unwrap();
        let cycles = before - fixture.pocket_ic().cycle_balance(fixture.canister_id());
        println!(
            "warm_open_sample={sample} instructions={} cycles={cycles}",
            instructions.unwrap()
        );
    }
    let before_write = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let write: Result<(), String> = fixture
        .pocket_ic()
        .update_wire(fixture.canister_id(), "insert_profile", (19_u64,))
        .unwrap();
    write.unwrap();
    println!(
        "typed_write_cycles={}",
        before_write - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let rank: Result<Option<u64>, String> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "profile_rank", ())
        .unwrap();
    assert_eq!(rank.unwrap(), Some(19));
    let before_upgrade = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            wasm,
            canic_icydb::wire::encode_post_upgrade_arguments(
                (),
                icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
            )
            .expect("unit post-upgrade arguments should encode"),
            None,
        )
        .unwrap();
    println!(
        "upgrade_cycles={}",
        before_upgrade - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let before_recovery = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    deliver_fixture_startup_watchdog(fixture.pocket_ic());
    println!(
        "recovery_watchdog_cycles={}",
        before_recovery - fixture.pocket_ic().cycle_balance(fixture.canister_id())
    );
    let rank: Result<Option<u64>, String> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "profile_rank", ())
        .unwrap();
    assert_eq!(rank.unwrap(), Some(19));
}

#[test]
#[ignore = "manual standalone facade installation cycle measurement"]
fn single_package_install_cycle_cost() {
    let wasm = model_facade_wasm();
    let instance = icydb_testing_integration::start_fixture_pocket_ic();
    let pic = instance.instance();
    let canister = pic.create_canister();
    pic.add_cycles(canister, 10_000_000_000_000);
    // PocketIC also supplies an initial balance: do not infer it from funding.
    let before = pic.cycle_balance(canister);
    pic.install_canister(canister, wasm, init_arguments(), None);
    println!("install_cycles={}", before - pic.cycle_balance(canister));
}
