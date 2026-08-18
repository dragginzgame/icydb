use std::{
    collections::BTreeSet,
    env,
    ffi::{OsStr, OsString},
    io::{self, Write as _},
    path::Path,
    time::Duration,
};

use ic_testkit::artifacts::{
    ArtifactCacheBatchOutcomeEntry, ArtifactCacheOutcome, ArtifactCachePreparation,
    ArtifactCachePrunePolicy, ArtifactCacheSpec, LabeledArtifactCacheSpec,
    SharedIncrementalTargetMaintenanceConfig, SharedIncrementalTargetMaintenanceFailureMode,
    SharedIncrementalTargetPrunePolicy, WasmBuildBatchConfig, WasmBuildBatchProgressEvent,
    WasmBuildOutcome, WasmBuildProgressConfig, WasmBuildProgressEvent, WasmBuildSpec,
    build_artifact_caches_batch, build_wasm_canisters_cached_batch_with_config_and_progress,
    build_wasm_canisters_cached_with_progress, prepare_artifact_cache,
};

use crate::wasm_optimizer::{
    POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, optimize_deployable_wasm_with_optimizer,
    pinned_wasm_optimizer,
};

const CACHE_TRACE_ENV: &str = "ICYDB_CANISTER_CACHE_TRACE";
const POST_LINK_CACHE_NAMESPACE: &str = "icydb-canister-wasm";
const POST_LINK_CACHE_RECIPE: &str = "icydb/post-link/v1";
const CACHE_MAX_AGE: Duration = Duration::from_hours(336);
const CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const CACHE_MAINTENANCE_INTERVAL: Duration = Duration::from_hours(24);
const BUILD_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SHARED_INCREMENTAL_TARGET_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;
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
    pub(crate) context: &'a str,
    pub(crate) workspace_root: &'a Path,
    pub(crate) target_dir: &'a Path,
    pub(crate) packages: &'a [&'a str],
    pub(crate) profile_target_dir: &'a str,
    pub(crate) arguments: &'a [OsString],
    pub(crate) effective_rustflags: Option<&'a str>,
}

pub(crate) struct CargoWasmBatchEntry<'a> {
    pub(crate) context: &'a str,
    pub(crate) package: &'a str,
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

pub(crate) struct PostLinkBatchEntry<'a> {
    pub(crate) context: &'a str,
    pub(crate) compiler_emitted: &'a Path,
    pub(crate) final_deployable: &'a Path,
}

pub(crate) struct CanisterCacheBatchReport {
    pub(crate) successful_indexes: Vec<usize>,
    pub(crate) failures: Vec<String>,
}

pub(crate) fn build_cached_cargo_wasm(
    request: &CargoWasmCacheRequest<'_>,
) -> Result<WasmBuildOutcome, String> {
    let spec = cargo_wasm_spec(
        request.workspace_root,
        request.target_dir,
        request.packages,
        request.profile_target_dir,
        request.arguments,
        request.effective_rustflags,
        true,
    );

    let outcome =
        build_wasm_canisters_cached_with_progress(&spec, wasm_build_progress_config(), |event| {
            report_wasm_build_progress(request.context, event);
        })
        .map_err(|error| format!("cached Cargo Wasm build failed: {error}"))?;
    report_shared_target_maintenance_failure(request.context, &outcome);
    Ok(outcome)
}

pub(crate) fn build_cached_cargo_wasm_batch(
    workspace_root: &Path,
    target_dir: &Path,
    profile_target_dir: &str,
    entries: &[CargoWasmBatchEntry<'_>],
) -> CanisterCacheBatchReport {
    let specs = entries
        .iter()
        .map(|entry| {
            cargo_wasm_spec(
                workspace_root,
                target_dir,
                &[entry.package],
                profile_target_dir,
                entry.arguments,
                entry.effective_rustflags,
                false,
            )
        })
        .collect::<Vec<_>>();
    let batch_config = WasmBuildBatchConfig::new()
        .with_shared_incremental_target_maintenance(shared_incremental_target_maintenance_config());
    let report = build_wasm_canisters_cached_batch_with_config_and_progress(
        &specs,
        batch_config,
        wasm_build_progress_config(),
        |event| report_wasm_build_batch_progress(entries, event),
    );

    for (index, outcome) in report.outcomes() {
        let context = entries
            .get(index)
            .map_or("maintained canister batch", |entry| entry.context);
        trace_wasm_build(context, outcome);
        report_shared_target_maintenance_failure(context, outcome);
    }
    eprintln!("maintained canister cargo_wasm_batch={report}");

    let successful_indexes = report.outcomes().map(|(index, _)| index).collect();
    let failures = report
        .failures()
        .map(|failure| {
            let index = failure.index();
            let context = entries
                .get(index)
                .map_or("maintained canister batch", |entry| entry.context);
            format!(
                "Cargo [{index}] {context} after {:?}: {}",
                failure.entry_elapsed(),
                failure.error(),
            )
        })
        .collect::<Vec<_>>();
    CanisterCacheBatchReport {
        successful_indexes,
        failures,
    }
}

fn cargo_wasm_spec(
    workspace_root: &Path,
    target_dir: &Path,
    packages: &[&str],
    profile_target_dir: &str,
    arguments: &[OsString],
    effective_rustflags: Option<&str>,
    integrated_shared_target_maintenance: bool,
) -> WasmBuildSpec {
    let mut spec = WasmBuildSpec::new(workspace_root, target_dir, packages, profile_target_dir)
        .with_cargo_profile_args(arguments)
        .with_inherited_env(inherited_build_environment())
        .with_shared_incremental_target(target_dir)
        .with_prune_policy_at_most_every(artifact_cache_prune_policy(), CACHE_MAINTENANCE_INTERVAL);
    if integrated_shared_target_maintenance {
        spec = spec.with_shared_incremental_target_maintenance(
            shared_incremental_target_maintenance_config(),
        );
    }
    if let Some(rustflags) = effective_rustflags {
        spec = spec.with_extra_env([(OsString::from("RUSTFLAGS"), OsString::from(rustflags))]);
    }
    spec
}

pub(crate) fn cache_post_link_wasm(
    request: &PostLinkCacheRequest<'_>,
) -> Result<ArtifactCacheOutcome, String> {
    let optimizer = pinned_wasm_optimizer()?;
    let spec = post_link_cache_spec(request, &optimizer);

    match prepare_artifact_cache(&spec)
        .map_err(|error| format!("post-link artifact cache failed: {error}"))?
    {
        ArtifactCachePreparation::Reused(record) => Ok(ArtifactCacheOutcome::Reused(record)),
        ArtifactCachePreparation::Build(transaction) => {
            let output = transaction
                .output_path("final-deployable")
                .map_err(|error| format!("post-link artifact cache failed: {error}"))?;
            optimize_deployable_wasm_with_optimizer(request.compiler_emitted, &output, &optimizer)?;
            transaction
                .commit()
                .map_err(|error| format!("post-link artifact cache failed: {error}"))
        }
    }
}

pub(crate) fn cache_post_link_wasm_batch(
    workspace_root: &Path,
    cache_root: &Path,
    entries: &[PostLinkBatchEntry<'_>],
) -> Result<CanisterCacheBatchReport, String> {
    let optimizer = pinned_wasm_optimizer()?;
    let requests = entries
        .iter()
        .map(|entry| PostLinkCacheRequest {
            workspace_root,
            cache_root,
            coordination_scope: entry.context,
            compiler_emitted: entry.compiler_emitted,
            final_deployable: entry.final_deployable,
        })
        .collect::<Vec<_>>();
    let specs = requests
        .iter()
        .map(|request| {
            LabeledArtifactCacheSpec::new(
                request.coordination_scope,
                post_link_cache_spec(request, &optimizer),
            )
        })
        .collect::<Vec<_>>();
    let report = build_artifact_caches_batch(&specs, |label, transaction| {
        let entry = entries
            .iter()
            .find(|entry| entry.context == label)
            .ok_or_else(|| format!("post-link batch returned unknown entry label {label:?}"))?;
        let output = transaction
            .output_path("final-deployable")
            .map_err(|error| format!("post-link artifact cache failed: {error}"))?;
        optimize_deployable_wasm_with_optimizer(entry.compiler_emitted, &output, &optimizer)
    })
    .map_err(|error| format!("post-link artifact batch contract failed: {error}"))?;

    for outcome in report.outcomes() {
        trace_post_link(outcome.label(), outcome.outcome());
    }
    eprintln!("maintained canister post_link_batch={report}");

    let successful_indexes = report
        .outcomes()
        .map(ArtifactCacheBatchOutcomeEntry::index)
        .collect();
    let failures = report
        .failures()
        .map(|failure| {
            let index = failure.index();
            format!(
                "post-link [{index}] {} failed during {} after {:?}: {}",
                failure.label(),
                failure.failure().phase(),
                failure.entry_elapsed(),
                failure.failure(),
            )
        })
        .collect::<Vec<_>>();
    Ok(CanisterCacheBatchReport {
        successful_indexes,
        failures,
    })
}

fn post_link_cache_spec(request: &PostLinkCacheRequest<'_>, optimizer: &Path) -> ArtifactCacheSpec {
    ArtifactCacheSpec::new(
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
    .with_tool("wasm-opt", optimizer)
    .with_arguments(WASM_OPT_FLAGS)
    .with_identity_bytes(
        "post-link-pipeline-identity",
        POST_LINK_PIPELINE_IDENTITY.as_bytes(),
    )
    .with_output("final-deployable", request.final_deployable)
    .with_prune_policy_at_most_every(artifact_cache_prune_policy(), CACHE_MAINTENANCE_INTERVAL)
}

pub(crate) fn trace_wasm_build(context: &str, outcome: &WasmBuildOutcome) {
    if env::var_os(CACHE_TRACE_ENV).is_some() {
        eprintln!(
            "{context}: cargo_wasm_cache={outcome} exact_cache_path={}",
            outcome.record().exact_cache_path().display(),
        );
    }
}

pub(crate) fn trace_post_link(context: &str, outcome: &ArtifactCacheOutcome) {
    if env::var_os(CACHE_TRACE_ENV).is_some() {
        eprintln!("{context}: post_link_cache={outcome}");
    }
}

fn report_wasm_build_progress(context: &str, event: WasmBuildProgressEvent) {
    let WasmBuildProgressEvent::Heartbeat { phase, elapsed } = event else {
        return;
    };
    let _ = writeln!(
        io::stderr().lock(),
        "{context}: ic_testkit_progress phase={phase} elapsed={elapsed:?}",
    );
}

fn report_wasm_build_batch_progress(
    entries: &[CargoWasmBatchEntry<'_>],
    event: WasmBuildBatchProgressEvent,
) {
    let WasmBuildBatchProgressEvent::BuildProgress { index, event } = event else {
        return;
    };
    let context = entries
        .get(index)
        .map_or("maintained canister batch", |entry| entry.context);
    report_wasm_build_progress(context, event);
}

fn report_shared_target_maintenance_failure(context: &str, outcome: &WasmBuildOutcome) {
    let Some(message) = outcome
        .record()
        .shared_incremental_maintenance()
        .and_then(|maintenance| maintenance.failure_message())
    else {
        return;
    };
    let _ = writeln!(
        io::stderr().lock(),
        "{context}: ic_testkit_maintenance_warning error={message}",
    );
}

fn wasm_build_progress_config() -> WasmBuildProgressConfig {
    WasmBuildProgressConfig::new()
        .with_heartbeat_interval(BUILD_PROGRESS_HEARTBEAT_INTERVAL)
        .with_cargo_output(false)
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

const fn shared_incremental_target_maintenance_config() -> SharedIncrementalTargetMaintenanceConfig
{
    SharedIncrementalTargetMaintenanceConfig::new(
        shared_incremental_target_prune_policy(),
        CACHE_MAINTENANCE_INTERVAL,
    )
    .with_failure_mode(SharedIncrementalTargetMaintenanceFailureMode::BestEffort)
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
    use std::{
        env,
        ffi::OsStr,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        BUILD_PROGRESS_HEARTBEAT_INTERVAL, CACHE_MAINTENANCE_INTERVAL, CACHE_MAX_AGE,
        CACHE_MAX_BYTES, EXTRA_BUILD_ENVIRONMENT, PostLinkBatchEntry,
        SHARED_INCREMENTAL_TARGET_MAX_BYTES, SharedIncrementalTargetMaintenanceFailureMode,
        artifact_cache_prune_policy, cache_post_link_wasm_batch, relevant_prefixed_environment,
        shared_incremental_target_maintenance_config, wasm_build_progress_config,
    };

    #[test]
    fn cargo_cache_environment_is_narrow_and_target_safe() {
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

        let maintenance = shared_incremental_target_maintenance_config();
        let shared_target_policy = maintenance.policy();
        assert_eq!(shared_target_policy.max_age(), Some(CACHE_MAX_AGE));
        assert_eq!(
            shared_target_policy.max_size_bytes(),
            Some(SHARED_INCREMENTAL_TARGET_MAX_BYTES)
        );
        assert_eq!(maintenance.minimum_interval(), CACHE_MAINTENANCE_INTERVAL);
        assert_eq!(
            maintenance.failure_mode(),
            SharedIncrementalTargetMaintenanceFailureMode::BestEffort
        );
    }

    #[test]
    fn cargo_cache_progress_reports_long_quiet_phases_without_raw_output() {
        let config = wasm_build_progress_config();
        assert_eq!(
            config.heartbeat_interval(),
            Some(BUILD_PROGRESS_HEARTBEAT_INTERVAL)
        );
        assert!(!config.emits_cargo_output());
    }

    #[test]
    fn post_link_batch_reports_failure_and_finishes_later_artifact() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow the Unix epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!(
            "icydb-post-link-batch-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create post-link batch fixture");
        let invalid_input = root.join("invalid.wasm");
        let valid_input = root.join("valid.wasm");
        let invalid_output = root.join("invalid.final.wasm");
        let valid_output = root.join("valid.final.wasm");
        fs::write(&invalid_input, b"not wasm").expect("write invalid Wasm fixture");
        fs::write(&valid_input, b"\0asm\x01\0\0\0").expect("write valid Wasm fixture");
        let entries = [
            PostLinkBatchEntry {
                context: "invalid post-link fixture",
                compiler_emitted: &invalid_input,
                final_deployable: &invalid_output,
            },
            PostLinkBatchEntry {
                context: "valid post-link fixture",
                compiler_emitted: &valid_input,
                final_deployable: &valid_output,
            },
        ];
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("integration crate should live below the workspace root")
            .to_path_buf();

        let report =
            cache_post_link_wasm_batch(&workspace_root, &root.join("artifact-cache"), &entries)
                .expect("post-link batch setup should succeed");

        assert_eq!(report.successful_indexes, [1]);
        assert_eq!(report.failures.len(), 1);
        assert!(report.failures[0].contains("invalid post-link fixture"));
        assert!(report.failures[0].contains("failed during callback"));
        assert!(report.failures[0].contains(" after "));
        assert!(report.failures[0].contains("timings=("));
        assert!(!invalid_output.exists());
        assert!(valid_output.is_file());
        fs::remove_dir_all(root).expect("remove post-link batch fixture");
    }
}
