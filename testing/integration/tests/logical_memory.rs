//! Generated logical-memory upgrades through the production allocator and driver.
//! Build the two test actors explicitly; no core test-only memory lookup is linked.

use ic_testkit::{
    pic::{InstallSpec, StandaloneCanisterFixture},
    pocket_ic::PocketIc,
};
use icydb::{
    Error, ErrorCode, ErrorOrigin,
    db::{DatabaseStartupState, StartupFailure},
};

const FULL_WASM: &str = "ICYDB_LOGICAL_MEMORY_FULL_WASM";
const OMITTED_WASM: &str = "ICYDB_LOGICAL_MEMORY_OMITTED_WASM";

fn artifact(variable: &str) -> Vec<u8> {
    std::fs::read(std::env::var(variable).expect("build and provide both logical-memory actors"))
        .unwrap()
}

fn install() -> StandaloneCanisterFixture {
    let fixture = StandaloneCanisterFixture::install(
        PocketIc::new(),
        InstallSpec::new(
            artifact(FULL_WASM),
            candid::encode_args(()).unwrap(),
            10_000_000_000_000,
        )
        .label("logical_memory"),
    );
    settle(&fixture);
    fixture
}

fn step(fixture: &StandaloneCanisterFixture) -> Result<bool, Error> {
    fixture.update_candid("step", ()).unwrap()
}

fn settle(fixture: &StandaloneCanisterFixture) {
    for _ in 0..32 {
        if step(fixture).unwrap() {
            let state: Result<DatabaseStartupState, StartupFailure> =
                fixture.query_candid("state", ()).unwrap();
            assert_eq!(state.unwrap(), DatabaseStartupState::Ready);
            return;
        }
    }
    panic!("small fixture must reach quiescence within its bounded driver calls");
}

fn upgrade(fixture: &StandaloneCanisterFixture, variable: &str) {
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            artifact(variable),
            candid::encode_args(()).unwrap(),
            None,
        )
        .unwrap();
}

fn control(fixture: &StandaloneCanisterFixture) -> Vec<u8> {
    fixture.query_candid("control_frame", ()).unwrap()
}

fn assert_kept_row(fixture: &StandaloneCanisterFixture) {
    let present: Result<bool, String> = fixture.query_candid("keep_row_exists", ()).unwrap();
    assert!(present.unwrap());
}

fn assert_rejected_without_control_publication(fixture: &StandaloneCanisterFixture, before: &[u8]) {
    for _ in 0..2 {
        let error = step(fixture).expect_err("unsafe registry change must reject");
        assert_eq!(error.code(), ErrorCode::RUNTIME_UNSUPPORTED);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        assert_eq!(control(fixture), before);
    }
}

#[test]
#[ignore = "requires both ICYDB_LOGICAL_MEMORY_{FULL,OMITTED}_WASM artifacts"]
fn empty_omitted_store_retires_without_relocating_survivors() {
    let fixture = install();
    let _: Vec<u8> = fixture.update_candid("insert_keep", ()).unwrap();
    settle(&fixture);
    let before: Vec<(String, u8)> = fixture.query_candid("allocations", ()).unwrap();
    upgrade(&fixture, OMITTED_WASM);
    let after: Vec<(String, u8)> = fixture.query_candid("allocations", ()).unwrap();
    assert_eq!(before.len(), 11);
    assert_eq!(after.len(), 8);
    for allocation in &after {
        assert!(before.contains(allocation));
    }
    for role in ["data", "index", "schema", "journal"] {
        let key = format!("icydb.logical_fixture.store.retiring.{role}.v1");
        assert_eq!(
            after.iter().any(|(current, _)| *current == key),
            role == "journal"
        );
    }
    settle(&fixture);
    assert_kept_row(&fixture);

    // Retired database identities cannot return, even though the allocator
    // still retains their original slots. This is the current lifecycle rule.
    let retired = control(&fixture);
    upgrade(&fixture, FULL_WASM);
    assert_rejected_without_control_publication(&fixture, &retired);
}

#[test]
#[ignore = "requires both ICYDB_LOGICAL_MEMORY_{FULL,OMITTED}_WASM artifacts"]
fn omitted_store_with_journal_debt_rejects_and_original_actor_recovers() {
    let fixture = install();
    fixture
        .update_candid::<(), _>("insert_retiring", ())
        .unwrap();
    let before = control(&fixture);
    upgrade(&fixture, OMITTED_WASM);
    assert_rejected_without_control_publication(&fixture, &before);
    upgrade(&fixture, FULL_WASM);
    settle(&fixture);
    let present: Result<bool, String> = fixture.query_candid("retiring_row_exists", ()).unwrap();
    assert!(present.unwrap());
}

#[test]
#[ignore = "requires both ICYDB_LOGICAL_MEMORY_{FULL,OMITTED}_WASM artifacts"]
fn valid_pending_marker_blocks_registry_change_after_journal_folding() {
    let fixture = install();
    let pending: Vec<u8> = fixture.update_candid("insert_keep", ()).unwrap();
    settle(&fixture);
    fixture
        .update_candid::<(), _>("restore_control_frame", (pending.clone(),))
        .unwrap();

    // Positive control: the captured production marker is valid and recovers
    // with unchanged stores. A malformed-marker rejection would not prove the
    // registry barrier and is not accepted by this test.
    upgrade(&fixture, FULL_WASM);
    settle(&fixture);
    assert_kept_row(&fixture);

    fixture
        .update_candid::<(), _>("restore_control_frame", (pending.clone(),))
        .unwrap();
    upgrade(&fixture, OMITTED_WASM);
    assert_rejected_without_control_publication(&fixture, &pending);
    upgrade(&fixture, FULL_WASM);
    settle(&fixture);
    assert_kept_row(&fixture);
}
