use super::*;

#[test]
fn frozen_nested_relation_contract_is_complete() {
    validate_nested_relation_contract().expect("frozen 0.253 contract should validate");
    for actor in RELATION_PROVING_ACTORS {
        assert!(
            icydb_testing_build::wasm_measurement::wasm_measurement_subjects()
                .any(|name| name == actor.name)
        );
        assert!(
            icydb_testing_build::canister_artifact::MAINTAINED_CANISTER_POLICIES
                .iter()
                .any(|policy| policy.canister == actor.name)
        );
    }
    assert_eq!(MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS, 3_276);
    assert_eq!(MAX_RELATION_BATCH_REVERSE_DELTAS, 5_460);
    assert_eq!(MAX_RELATION_BATCH_TRAVERSAL_WORK, 349_440);
    assert_eq!(REQUIRED_INSTRUCTION_HEADROOM, 10_000_000_000);
}
