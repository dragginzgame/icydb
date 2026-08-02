use std::collections::BTreeSet;

use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode,
    build_canister_with_options,
    canister_artifact::{
        CanisterMethod, ExpectedCanisterMethod, MAINTAINED_CANISTER_POLICIES,
        inspect_canister_artifacts,
    },
};

#[test]
#[ignore = "builds and inspects 18 canister artifacts; run `make test-canister-artifact-contract`"]
fn production_and_local_source_declarations_match_the_frozen_endpoint_policy() {
    for policy in MAINTAINED_CANISTER_POLICIES {
        for (build_profile, expected) in [
            (
                CanisterBuildProfile::LocalTest,
                policy.local_test_icydb_methods,
            ),
            (
                CanisterBuildProfile::Production,
                policy.production_icydb_methods,
            ),
        ] {
            let wasm = build_canister_with_options(
                policy.canister,
                CanisterBuildOptions {
                    candid_export: CanisterCandidExportMode::Enabled,
                    build_profile,
                    ..CanisterBuildOptions::default()
                },
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{} should build for {build_profile:?}: {error}",
                    policy.canister
                )
            });
            let manifest = inspect_canister_artifacts(&wasm).unwrap_or_else(|error| {
                panic!(
                    "{} artifacts should agree for {build_profile:?}: {error}",
                    policy.canister
                )
            });

            assert_eq!(
                manifest.icydb_methods(),
                owned_methods(expected),
                "unexpected IcyDB surface for {} ({build_profile:?})",
                policy.canister
            );
        }
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
