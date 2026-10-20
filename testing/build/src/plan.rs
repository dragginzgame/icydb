//! One entry owns each maintained actor's configuration and report context.

use crate::{
    CanisterBuildOptions, ConfiguredCanisterBuild,
    canister_artifact::{MAINTAINED_CANISTER_POLICIES, MaintainedCanisterPolicy},
    canister_build_cache::{CargoWasmBatchEntry, cargo_wasm_batch_specs},
    configure_canister_build, target_dir,
};
use ic_build::artifacts::LabeledWasmBuildSpec;
use std::path::Path;

pub(super) struct PlannedCanisterBuild {
    pub(super) policy: &'static MaintainedCanisterPolicy,
    pub(super) configured: ConfiguredCanisterBuild,
    pub(super) context: String,
}

pub(super) struct MaintainedCanisterBuildPlan {
    pub(super) options: CanisterBuildOptions,
    pub(super) entries: Vec<PlannedCanisterBuild>,
}

impl MaintainedCanisterBuildPlan {
    // Derive the request list in the same order as the retained entries. Only
    // upstream report indices need admission; local configuration/context cannot
    // drift between separately stored vectors.
    pub(super) fn cargo_specs(&self, root: &Path) -> Vec<LabeledWasmBuildSpec> {
        let entries = self
            .entries
            .iter()
            .map(|entry| CargoWasmBatchEntry {
                context: &entry.context,
                package: entry.policy.package,
                selection: &entry.configured.selection,
                encoded_rustflags: entry.configured.encoded_rustflags.as_deref(),
            })
            .collect::<Vec<_>>();
        cargo_wasm_batch_specs(
            root,
            &target_dir(root).join(self.options.build_profile.target_dir_name()),
            &entries,
        )
    }
}

pub(super) fn plan_maintained_canister_builds(
    root: &Path,
    options: CanisterBuildOptions,
) -> Result<MaintainedCanisterBuildPlan, String> {
    let target = target_dir(root).join(options.build_profile.target_dir_name());
    let entries = MAINTAINED_CANISTER_POLICIES
        .iter()
        .map(|policy| {
            Ok(PlannedCanisterBuild {
                policy,
                configured: configure_canister_build(root, &target, policy.package, options)?,
                context: format!(
                    "{} canister build ({}, {:?})",
                    policy.canister,
                    options.profile.as_str(),
                    options.build_profile,
                ),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MaintainedCanisterBuildPlan { options, entries })
}

#[cfg(test)]
mod tests {
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
                    let resolved = resolve_fixture_canister_build_configuration(
                        entry.policy.canister,
                        options,
                    )
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
}
