use std::collections::BTreeSet;

use icydb_testing_build::{
    BuiltCanisterArtifacts, CanisterBuildOptions, CanisterBuildProfile,
    build_canister_with_options, build_maintained_canister_contract_profiles,
    canister_artifact::{
        CanisterMethod, ExpectedCanisterMethod, MAINTAINED_CANISTER_POLICIES, MethodMode,
        inspect_canister_artifacts,
    },
};

#[test]
fn production_sql_actors_exclude_fixture_exports() {
    for canister in ["sql", "sql_bounded", "sql_perf"] {
        let policy = MAINTAINED_CANISTER_POLICIES
            .iter()
            .find(|policy| policy.canister == canister)
            .unwrap();
        for build_profile in [
            CanisterBuildProfile::Production,
            CanisterBuildProfile::LocalTest,
        ] {
            let artifact = build_canister_with_options(
                canister,
                CanisterBuildOptions {
                    build_profile,
                    ..CanisterBuildOptions::default()
                },
            )
            .unwrap();
            let manifest = inspect_canister_artifacts(artifact.as_ref()).unwrap();
            if build_profile == CanisterBuildProfile::Production {
                // Inspection already requires the Wire roster to match every
                // application Wasm export, excluding only admitted timer hooks.
                assert_eq!(
                    manifest.wire_methods,
                    owned_methods(policy.production_icydb_methods),
                    "unexpected production application export for {canister}"
                );
            } else {
                let required = if canister == "sql_perf" {
                    &["query_user"][..]
                } else {
                    &[
                        "seed_oversized_sql_group_name",
                        "measure_identity_closeout_perf",
                        "measure_application_behavior_perf",
                        "seed_convergence_metrics_backlog",
                    ][..]
                };
                for name in required {
                    assert!(
                        manifest
                            .wire_methods
                            .iter()
                            .any(|method| method.name == *name),
                        "local {canister} lost fixture method {name}"
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "builds and inspects both maintained canister profiles; run `make test-canister-artifact-contract`"]
fn production_and_local_source_declarations_match_the_frozen_endpoint_policy() {
    let profile_artifacts = build_maintained_canister_contract_profiles()
        .unwrap_or_else(|error| panic!("maintained canisters should build: {error}"));

    std::thread::scope(|scope| {
        for (build_profile, artifacts) in profile_artifacts {
            scope.spawn(move || verify_profile(build_profile, artifacts));
        }
    });
}

fn verify_profile(
    build_profile: CanisterBuildProfile,
    artifacts: Vec<(&'static str, BuiltCanisterArtifacts)>,
) {
    for (canister, wasm) in artifacts {
        let policy = MAINTAINED_CANISTER_POLICIES
            .iter()
            .find(|policy| policy.canister == canister)
            .expect("built canister should have a maintained policy");
        let expected = match build_profile {
            CanisterBuildProfile::LocalTest => policy.local_test_icydb_methods,
            CanisterBuildProfile::Production => policy.production_icydb_methods,
        };
        let manifest = inspect_canister_artifacts(wasm.as_ref()).unwrap_or_else(|error| {
            panic!("{canister} artifacts should agree for {build_profile:?}: {error}")
        });

        assert_eq!(
            manifest.icydb_methods(),
            owned_methods(expected),
            "unexpected IcyDB surface for {canister} ({build_profile:?})",
        );
        if build_profile == CanisterBuildProfile::Production {
            let mut expected_updates = owned_methods(policy.production_icydb_methods);
            expected_updates.retain(|method| method.mode == MethodMode::Update);
            expected_updates.extend(policy.production_fixture_updates.iter().map(|name| {
                CanisterMethod {
                    name: (*name).to_string(),
                    mode: MethodMode::Update,
                }
            }));
            let actual_updates = manifest
                .wire_methods
                .into_iter()
                .filter(|method| method.mode == MethodMode::Update)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual_updates, expected_updates,
                "unexpected production update surface for {canister}"
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
