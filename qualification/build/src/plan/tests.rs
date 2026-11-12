use super::plan_maintained_canister_builds;
use crate::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterWasmProfile,
    resolve_fixture_canister_build_configuration, workspace_root,
};

#[test]
fn maintained_actor_requests_keep_profile_features_and_report_identity() {
    let root = workspace_root();
    for profile in [
        CanisterWasmProfile::Debug,
        CanisterWasmProfile::Release,
        CanisterWasmProfile::WasmRelease,
        CanisterWasmProfile::WasmAttribution,
    ] {
        for build_profile in [
            CanisterBuildProfile::LocalTest,
            CanisterBuildProfile::Production,
        ] {
            let options = CanisterBuildOptions {
                profile,
                build_profile,
                ..CanisterBuildOptions::default()
            };
            let plan = plan_maintained_canister_builds(&root, options).unwrap();
            let specs = plan.cargo_specs(&root);
            for (entry, request) in plan.entries.iter().zip(&specs) {
                let resolved =
                    resolve_fixture_canister_build_configuration(entry.policy.canister, options)
                        .unwrap();
                let command = request.spec().cargo_build_command().unwrap();
                let arguments = command
                    .get_args()
                    .map(|arg| arg.to_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(request.spec().packages(), [entry.policy.package]);
                assert_eq!(request.label(), entry.context);
                let selected_profile = arguments
                    .iter()
                    .find_map(|argument| argument.strip_prefix("--profile="));
                assert_eq!(
                    arguments.contains(&"--release"),
                    profile == CanisterWasmProfile::Release
                );
                assert_eq!(
                    selected_profile,
                    match profile {
                        CanisterWasmProfile::Debug | CanisterWasmProfile::Release => None,
                        CanisterWasmProfile::WasmRelease => Some("wasm-release"),
                        CanisterWasmProfile::WasmAttribution => Some("wasm-attribution"),
                    }
                );
                assert_eq!(
                    arguments.contains(&"--no-default-features"),
                    resolved.no_default_features()
                );
                let features = arguments
                    .iter()
                    .find_map(|argument| argument.strip_prefix("--features="));
                assert_eq!(
                    features,
                    (!resolved.features().is_empty())
                        .then(|| resolved.features().join(","))
                        .as_deref()
                );
            }
            assert_eq!(specs.len(), plan.entries.len());
        }
    }
}
