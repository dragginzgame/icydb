use std::collections::BTreeSet;

use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode,
    build_maintained_canisters_with_options,
    canister_artifact::{
        CanisterMethod, ExpectedCanisterMethod, MAINTAINED_CANISTER_POLICIES,
        inspect_canister_artifacts,
    },
};

#[test]
#[ignore = "builds and inspects 20 canister artifacts; run `make test-canister-artifact-contract`"]
fn production_and_local_source_declarations_match_the_frozen_endpoint_policy() {
    std::thread::scope(|scope| {
        scope.spawn(|| verify_profile(CanisterBuildProfile::LocalTest));
        scope.spawn(|| verify_profile(CanisterBuildProfile::Production));
    });
}

fn verify_profile(build_profile: CanisterBuildProfile) {
    let artifacts = build_maintained_canisters_with_options(CanisterBuildOptions {
        candid_export: CanisterCandidExportMode::Enabled,
        build_profile,
        ..CanisterBuildOptions::default()
    })
    .unwrap_or_else(|error| panic!("maintained canisters should build: {error}"));

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
