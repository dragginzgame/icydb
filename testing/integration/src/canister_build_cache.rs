use std::{
    collections::BTreeSet,
    env,
    ffi::{OsStr, OsString},
    path::Path,
    time::Duration,
};

use ic_testkit::artifacts::{
    ArtifactCachePreparation, ArtifactCachePrunePolicy, ArtifactCacheRecord, ArtifactCacheSpec,
    SharedIncrementalTargetMaintenanceOutcome, SharedIncrementalTargetPrunePolicy,
    WasmBuildOutcome, WasmBuildSpec, build_wasm_canisters_cached,
    maintain_shared_incremental_target_at_most_every, prepare_artifact_cache,
};

use crate::wasm_optimizer::{
    POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, optimize_deployable_wasm, pinned_wasm_optimizer,
};

const CACHE_TRACE_ENV: &str = "ICYDB_CANISTER_CACHE_TRACE";
const POST_LINK_CACHE_NAMESPACE: &str = "icydb-canister-wasm";
const POST_LINK_CACHE_RECIPE: &str = "icydb/post-link/v1";
const CACHE_MAX_AGE: Duration = Duration::from_hours(336);
const CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const CACHE_MAINTENANCE_INTERVAL: Duration = Duration::from_hours(24);
const SHARED_INCREMENTAL_TARGET_MAX_BYTES: u64 = 6 * 1024 * 1024 * 1024;
const ADDITIONAL_CARGO_INPUTS: [&str; 1] = ["schema"];
const EXTRA_BUILD_ENVIRONMENT: [&str; 9] = [
    "AR",
    "CARGO_HOME",
    "CC",
    "CFLAGS",
    "CXX",
    "CXXFLAGS",
    "HOST_CC",
    "HOST_CFLAGS",
    "SOURCE_DATE_EPOCH",
];

pub(crate) struct CargoWasmCacheRequest<'a> {
    pub(crate) workspace_root: &'a Path,
    pub(crate) target_dir: &'a Path,
    pub(crate) packages: &'a [&'a str],
    pub(crate) profile_target_dir: &'a str,
    pub(crate) arguments: &'a [OsString],
    pub(crate) effective_rustflags: Option<&'a str>,
}

pub(crate) struct PostLinkCacheRequest<'a> {
    pub(crate) workspace_root: &'a Path,
    pub(crate) cache_root: &'a Path,
    pub(crate) coordination_scope: &'a str,
    pub(crate) compiler_emitted: &'a Path,
    pub(crate) final_deployable: &'a Path,
}

pub(crate) fn build_cached_cargo_wasm(
    request: &CargoWasmCacheRequest<'_>,
) -> Result<WasmBuildOutcome, String> {
    let mut spec = WasmBuildSpec::new(
        request.workspace_root,
        request.target_dir,
        request.packages,
        request.profile_target_dir,
    )
    .with_cargo_profile_args_os(request.arguments.iter().cloned())
    .with_inherited_env_os(inherited_build_environment())
    .with_additional_inputs(&ADDITIONAL_CARGO_INPUTS)
    .with_shared_incremental_target(request.target_dir)
    .with_prune_policy_at_most_every(artifact_cache_prune_policy(), CACHE_MAINTENANCE_INTERVAL);
    if let Some(rustflags) = request.effective_rustflags {
        spec = spec.with_extra_env_os([(OsString::from("RUSTFLAGS"), OsString::from(rustflags))]);
    }

    let maintenance = maintain_shared_incremental_target_at_most_every(
        &spec,
        shared_incremental_target_prune_policy(),
        CACHE_MAINTENANCE_INTERVAL,
    )
    .map_err(|error| format!("shared Cargo target maintenance failed: {error}"))?;
    trace_shared_target_maintenance(&maintenance);

    build_wasm_canisters_cached(&spec)
        .map_err(|error| format!("cached Cargo Wasm build failed: {error}"))
}

pub(crate) fn cache_post_link_wasm(
    request: &PostLinkCacheRequest<'_>,
) -> Result<ArtifactCacheRecord, String> {
    let optimizer = pinned_wasm_optimizer()?;
    let spec = ArtifactCacheSpec::new(
        request.cache_root,
        POST_LINK_CACHE_NAMESPACE,
        POST_LINK_CACHE_RECIPE,
    )
    .with_coordination_scope(request.coordination_scope)
    .with_input("compiler-emitted", request.compiler_emitted)
    .with_input(
        "post-link-pipeline",
        &request
            .workspace_root
            .join("testing/integration/src/wasm_optimizer.rs"),
    )
    .with_tool("wasm-opt", &optimizer)
    .with_arguments(&WASM_OPT_FLAGS)
    .with_identity_bytes(
        "post-link-pipeline-identity",
        POST_LINK_PIPELINE_IDENTITY.as_bytes(),
    )
    .with_output("final-deployable", request.final_deployable)
    .with_prune_policy_at_most_every(artifact_cache_prune_policy(), CACHE_MAINTENANCE_INTERVAL);

    match prepare_artifact_cache(&spec)
        .map_err(|error| format!("post-link artifact cache failed: {error}"))?
    {
        ArtifactCachePreparation::Reused(record) => Ok(record),
        ArtifactCachePreparation::Build(transaction) => {
            let output = transaction
                .output_path("final-deployable")
                .map_err(|error| format!("post-link artifact cache failed: {error}"))?;
            optimize_deployable_wasm(request.compiler_emitted, &output)?;
            transaction
                .commit()
                .map(|outcome| outcome.record().clone())
                .map_err(|error| format!("post-link artifact cache failed: {error}"))
        }
    }
}

pub(crate) fn trace_wasm_build(context: &str, outcome: &WasmBuildOutcome) {
    if env::var_os(CACHE_TRACE_ENV).is_some() {
        eprintln!("{context}: cargo_wasm_cache={outcome}");
    }
}

pub(crate) fn trace_post_link(context: &str, record: &ArtifactCacheRecord) {
    if env::var_os(CACHE_TRACE_ENV).is_some() {
        eprintln!(
            "{context}: post_link_cache key={} {}",
            record.key(),
            record.timings(),
        );
    }
}

fn trace_shared_target_maintenance(outcome: &SharedIncrementalTargetMaintenanceOutcome) {
    if env::var_os(CACHE_TRACE_ENV).is_some() {
        eprintln!("shared_cargo_target_maintenance={outcome}");
    }
}

const fn artifact_cache_prune_policy() -> ArtifactCachePrunePolicy {
    ArtifactCachePrunePolicy::new()
        .with_max_age(CACHE_MAX_AGE)
        .with_max_size_bytes(CACHE_MAX_BYTES)
}

const fn shared_incremental_target_prune_policy() -> SharedIncrementalTargetPrunePolicy {
    SharedIncrementalTargetPrunePolicy::new()
        .with_max_age(CACHE_MAX_AGE)
        .with_max_size_bytes(SHARED_INCREMENTAL_TARGET_MAX_BYTES)
}

fn inherited_build_environment() -> BTreeSet<OsString> {
    let mut names = EXTRA_BUILD_ENVIRONMENT
        .into_iter()
        .map(OsString::from)
        .collect::<BTreeSet<_>>();
    names.extend(
        env::vars_os().filter_map(|(name, _)| relevant_prefixed_environment(&name).then_some(name)),
    );
    names
}

fn relevant_prefixed_environment(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name.starts_with("CARGO_PROFILE_")
        || (name.starts_with("CARGO_TARGET_") && name != "CARGO_TARGET_DIR")
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::{
        ADDITIONAL_CARGO_INPUTS, CACHE_MAX_AGE, CACHE_MAX_BYTES, EXTRA_BUILD_ENVIRONMENT,
        SHARED_INCREMENTAL_TARGET_MAX_BYTES, artifact_cache_prune_policy,
        relevant_prefixed_environment, shared_incremental_target_prune_policy,
    };

    #[test]
    fn cargo_cache_inputs_and_environment_are_narrow_and_target_safe() {
        assert_eq!(ADDITIONAL_CARGO_INPUTS, ["schema"]);
        assert!(EXTRA_BUILD_ENVIRONMENT.is_sorted());
        assert!(relevant_prefixed_environment(OsStr::new(
            "CARGO_PROFILE_WASM_RELEASE_OPT_LEVEL"
        )));
        assert!(relevant_prefixed_environment(OsStr::new(
            "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS"
        )));
        assert!(!relevant_prefixed_environment(OsStr::new(
            "CARGO_TARGET_DIR"
        )));
    }

    #[test]
    fn cargo_cache_retention_bounds_exact_and_incremental_state() {
        let artifact_policy = artifact_cache_prune_policy();
        assert_eq!(artifact_policy.max_age(), Some(CACHE_MAX_AGE));
        assert_eq!(artifact_policy.max_size_bytes(), Some(CACHE_MAX_BYTES));

        let shared_target_policy = shared_incremental_target_prune_policy();
        assert_eq!(shared_target_policy.max_age(), Some(CACHE_MAX_AGE));
        assert_eq!(
            shared_target_policy.max_size_bytes(),
            Some(SHARED_INCREMENTAL_TARGET_MAX_BYTES)
        );
    }
}
