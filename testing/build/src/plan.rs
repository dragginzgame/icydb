//! One entry owns each maintained actor's configuration and report context.

use crate::{
    ADAPTER_WORKSPACE_DIR, CanisterBuildOptions, ConfiguredCanisterBuild,
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
            &root.join(ADAPTER_WORKSPACE_DIR),
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
mod tests;
