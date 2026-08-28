//! Dormant 0.229 convergence-candidate IC instruction evidence.

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::install_fixture_canister;
use serde::Deserialize;

const ACCEPTED_THREE_INDEX_RECOVERY_INSTRUCTIONS: u64 = 22_067_141_613;
const CANDIDATE_CALLBACK_LIMIT: u64 = 30_000_000_000;
const CANDIDATE_CALLBACK_HEAP_LIMIT_BYTES: u64 = 526_532_992;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ConvergenceCandidatePerfResult {
    effects: u32,
    stores: u32,
    selected_store: u8,
    remaining_effects: u32,
    checksum: u64,
    local_instructions: u64,
}

fn canister_memory_bytes(fixture: &StandaloneCanisterFixture) -> (u64, u64) {
    fn nat_u64(value: &candid::Nat) -> u64 {
        match value.0.to_u64_digits().as_slice() {
            [] => 0,
            [value] => *value,
            _ => panic!("canister memory bytes should fit u64"),
        }
    }

    let status = fixture
        .pocket_ic()
        .canister_status(fixture.canister_id(), None)
        .expect("audit canister status should be available");

    (
        nat_u64(&status.memory_metrics.wasm_memory_size),
        nat_u64(&status.memory_metrics.stable_memory_size),
    )
}

#[test]
fn maximum_dormant_retirement_and_store_scan_fit_the_reserved_callback_headroom() {
    let fixture = install_fixture_canister("sql_perf");
    let memory_before = canister_memory_bytes(&fixture);
    let measured: ConvergenceCandidatePerfResult = fixture
        .update_candid("measure_dormant_convergence_candidate", ())
        .expect("dormant convergence evidence should decode");
    let memory_after = canister_memory_bytes(&fixture);

    assert_eq!(measured.effects, 65_536);
    assert_eq!(measured.stores, 16);
    assert_eq!(measured.selected_store, 115);
    assert_eq!(measured.remaining_effects, 0);
    assert!(measured.checksum > 0);
    assert!(measured.local_instructions > 0);
    assert!(
        ACCEPTED_THREE_INDEX_RECOVERY_INSTRUCTIONS
            .checked_add(measured.local_instructions)
            .is_some_and(|combined| combined < CANDIDATE_CALLBACK_LIMIT),
        "dormant selector/publication/retirement evidence must fit the 30B candidate callback ceiling: baseline={} overhead={}",
        ACCEPTED_THREE_INDEX_RECOVERY_INSTRUCTIONS,
        measured.local_instructions,
    );
    assert!(memory_after.0 >= memory_before.0);
    assert_eq!(memory_after.1, memory_before.1);
    assert!(
        memory_after.0 <= CANDIDATE_CALLBACK_HEAP_LIMIT_BYTES,
        "dormant publication/retirement evidence must fit the candidate callback heap ceiling: observed={} ceiling={}",
        memory_after.0,
        CANDIDATE_CALLBACK_HEAP_LIMIT_BYTES,
    );

    println!(
        "0.229 dormant convergence candidate: effects={} stores={} overhead_instructions={} combined_conservative_instructions={} callback_limit={} wasm_memory_before={} wasm_memory_after={} stable_memory_before={} stable_memory_after={} callback_heap_limit={}",
        measured.effects,
        measured.stores,
        measured.local_instructions,
        ACCEPTED_THREE_INDEX_RECOVERY_INSTRUCTIONS + measured.local_instructions,
        CANDIDATE_CALLBACK_LIMIT,
        memory_before.0,
        memory_after.0,
        memory_before.1,
        memory_after.1,
        CANDIDATE_CALLBACK_HEAP_LIMIT_BYTES,
    );
}
