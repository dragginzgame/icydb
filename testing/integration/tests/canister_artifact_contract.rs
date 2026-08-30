use std::{collections::BTreeSet, path::PathBuf, sync::Mutex};

use icydb_testing_integration::{
    CanisterBuildProfile, build_maintained_canister_contract_profiles_assuming_sources_immutable,
    canister_artifact::{
        CanisterMethod, ExpectedCanisterMethod, MAINTAINED_CANISTER_POLICIES,
        inspect_canister_artifacts,
    },
};

#[test]
#[ignore = "builds and inspects 34 independent canister artifacts; run `make test-canister-artifact-contract`"]
fn production_and_local_source_declarations_match_the_frozen_endpoint_policy() {
    let source_write_exclusion = Mutex::new(());
    let source_guard = source_write_exclusion
        .lock()
        .expect("lock immutable maintained-canister sources");
    let profile_artifacts =
        build_maintained_canister_contract_profiles_assuming_sources_immutable(&source_guard)
            .unwrap_or_else(|error| panic!("maintained canisters should build: {error}"));
    drop(source_guard);

    std::thread::scope(|scope| {
        for (build_profile, artifacts) in profile_artifacts {
            scope.spawn(move || verify_profile(build_profile, artifacts));
        }
    });
}

fn verify_profile(build_profile: CanisterBuildProfile, artifacts: Vec<(&'static str, PathBuf)>) {
    for (canister, wasm) in artifacts {
        let policy = MAINTAINED_CANISTER_POLICIES
            .iter()
            .find(|policy| policy.canister == canister)
            .expect("built canister should have a maintained policy");
        let expected = match build_profile {
            CanisterBuildProfile::LocalTest => policy.local_test_icydb_methods,
            CanisterBuildProfile::Production => policy.production_icydb_methods,
        };
        let manifest = inspect_canister_artifacts(&wasm).unwrap_or_else(|error| {
            panic!("{canister} artifacts should agree for {build_profile:?}: {error}")
        });

        if canister == "sql" {
            assert_eq!(
                manifest.candid.matches("U256 : nat;").count(),
                1,
                "the shared recursive public value contract should expose U256 once and only as Candid nat for {build_profile:?}",
            );
            assert!(
                !manifest.candid.contains("U256 : blob;")
                    && !manifest.candid.contains("U256 : record"),
                "generated clients must not receive a blob or limb-record U256 carrier for {build_profile:?}",
            );
        }

        assert_eq!(
            manifest.icydb_methods(),
            owned_methods(expected),
            "unexpected IcyDB surface for {canister} ({build_profile:?})",
        );
    }
}

fn owned_methods(expected: &[ExpectedCanisterMethod]) -> BTreeSet<CanisterMethod> {
    expected
        .iter()
        .map(|(name, mode)| CanisterMethod {
            name: (*name).to_string(),
            mode: *mode,
        })
        .collect()
}
