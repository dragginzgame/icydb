//! Module: build::canister_build_cache
//! Responsibility: build-artifact caching.
//! Does not own: feature policy or artifact semantics.
//! Boundary: caches resolved build recipes.

use crate::wasm_optimizer::{
    POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, optimize_deployable_wasm_with_optimizer,
    pinned_wasm_optimizer,
};
use std::{
    collections::BTreeMap,
    env,
    ffi::{OsStr, OsString},
    fmt::Write as _,
    io::{self, Write as _},
    path::Path,
    time::Duration,
};

use ic_build::artifacts::{
    ArtifactCacheBatchFailure, ArtifactCacheBatchReport, ArtifactCacheOutcome, ArtifactCacheSpec,
    CacheRetentionLimits, LabeledArtifactCacheSpec, LabeledWasmBuildSpec,
    SharedIncrementalTargetMaintenanceConfig, SharedIncrementalTargetMaintenanceFailureMode,
    WasmBuildBatchConfig, WasmBuildBatchContractError, WasmBuildBatchProgressEvent,
    WasmBuildBatchReport, WasmBuildOutcome, WasmBuildProgressConfig, WasmBuildProgressEvent,
    WasmBuildSpec, build_artifact_caches_batch, build_wasm_canisters_cached_batch,
    build_wasm_canisters_cached_with_progress,
};
use ic_build::cargo::{environment::ConfigurationLimits, selection::CargoSelection};
use ic_host_process::child::{CleanupPolicy, OwnedChild};

// Build 0.67 requires caller-owned admission for discovered Cargo configuration.
// One fixed budget covers single/batch fixtures and Cargo-home/ancestor includes;
// Build owns traversal/refusal. This adds no user setting or alternate cache route.
const CARGO_CONFIGURATION_LIMITS: ConfigurationLimits = ConfigurationLimits {
    file_bytes: 1024 * 1024,
    total_bytes: 16 * 1024 * 1024,
    files: 128,
    depth: 32,
};

const CACHE_TRACE_ENV: &str = "ICYDB_CANISTER_CACHE_TRACE";
const POST_LINK_CACHE_NAMESPACE: &str = "icydb-canister-wasm";
const POST_LINK_CACHE_RECIPE: &str = "icydb/post-link/v1";
const CACHE_MAX_AGE: Duration = Duration::from_hours(336);
const CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const CACHE_MAINTENANCE_INTERVAL: Duration = Duration::from_hours(24);
const BUILD_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SHARED_INCREMENTAL_TARGET_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;

pub(crate) struct CargoWasmCacheRequest<'a> {
    pub(crate) context: &'a str,
    pub(crate) workspace_root: &'a Path,
    pub(crate) target_dir: &'a Path,
    pub(crate) packages: &'a [&'a str],
    pub(crate) selection: &'a CargoSelection,
    pub(crate) encoded_rustflags: Option<&'a str>,
}

pub(crate) struct CargoWasmBatchEntry<'a> {
    pub(crate) context: &'a str,
    pub(crate) package: &'a str,
    pub(crate) selection: &'a CargoSelection,
    pub(crate) encoded_rustflags: Option<&'a str>,
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

pub(crate) fn build_cached_cargo_wasm(
    request: &CargoWasmCacheRequest<'_>,
) -> Result<WasmBuildOutcome, String> {
    let spec = cargo_wasm_spec(
        request.workspace_root,
        request.target_dir,
        request.packages,
        request.selection,
        request.encoded_rustflags,
        true,
    );

    let outcome = build_wasm_canisters_cached_with_progress(
        &spec,
        wasm_build_progress_config(),
        |event| report_wasm_build_progress(request.context, event),
        // Observed Cargo builds own their group through observer unwind.
        |command| OwnedChild::spawn(command, CleanupPolicy::KillAndWait),
    )
    .map_err(|error| format!("cached Cargo Wasm build failed: {error}"))?;
    report_shared_target_maintenance_failure(request.context, &outcome);
    Ok(outcome)
}

pub(crate) fn cargo_wasm_batch_specs(
    workspace_root: &Path,
    target_dir: &Path,
    entries: &[CargoWasmBatchEntry<'_>],
) -> Vec<LabeledWasmBuildSpec> {
    entries
        .iter()
        .map(|entry| {
            LabeledWasmBuildSpec::new(
                entry.context,
                cargo_wasm_spec(
                    workspace_root,
                    target_dir,
                    &[entry.package],
                    entry.selection,
                    entry.encoded_rustflags,
                    false,
                ),
            )
        })
        .collect()
}

pub(crate) fn build_cached_cargo_wasm_batch(
    specs: &[LabeledWasmBuildSpec],
) -> Result<WasmBuildBatchReport, WasmBuildBatchContractError> {
    let batch_config = WasmBuildBatchConfig::new()
        .with_shared_incremental_target_maintenance(shared_incremental_target_maintenance_config());
    let mut observer = report_wasm_build_batch_progress;
    let report = build_wasm_canisters_cached_batch(
        specs,
        batch_config,
        Some((wasm_build_progress_config(), &mut observer)),
        |command| OwnedChild::spawn(command, CleanupPolicy::KillAndWait),
    )?;
    for entry in report.outcomes() {
        if let Some(outcome) = entry.outcome() {
            trace_wasm_build(entry.label(), outcome);
            report_shared_target_maintenance_failure(entry.label(), outcome);
        }
    }
    eprintln!("maintained canister cargo_wasm_batch={report}");
    Ok(report)
}

pub(crate) fn cargo_wasm_spec(
    workspace_root: &Path,
    target_dir: &Path,
    packages: &[&str],
    selection: &CargoSelection,
    encoded_rustflags: Option<&str>,
    integrated_shared_target_maintenance: bool,
) -> WasmBuildSpec {
    // Build clears child inheritance. Select the caller's environment once so
    // probes, compilation and the retained fingerprint observe the same inputs.
    let mut environment: BTreeMap<_, _> = env::vars_os().collect();
    // The specification owns the destination, including when a parent Cargo
    // process supplied its own target directory to this build tool.
    environment.remove(OsStr::new("CARGO_TARGET_DIR"));
    if let Some(rustflags) = encoded_rustflags {
        environment.insert(
            OsString::from("CARGO_ENCODED_RUSTFLAGS"),
            OsString::from(rustflags),
        );
    }
    let mut spec = WasmBuildSpec::new(
        workspace_root,
        target_dir,
        packages,
        environment,
        CARGO_CONFIGURATION_LIMITS,
    )
    .with_cargo_selection(selection.clone())
    .with_shared_incremental_target(target_dir)
    .with_prune_policy_at_most_every(artifact_cache_prune_policy(), CACHE_MAINTENANCE_INTERVAL);
    if integrated_shared_target_maintenance {
        spec = spec.with_shared_incremental_target_maintenance(
            shared_incremental_target_maintenance_config(),
        );
    }
    spec
}

pub(crate) fn cache_post_link_wasm(
    request: &PostLinkCacheRequest<'_>,
) -> Result<ArtifactCacheOutcome, String> {
    let report = cache_post_link_wasm_entries(
        request.workspace_root,
        request.cache_root,
        &[PostLinkBatchEntry {
            context: request.coordination_scope,
            compiler_emitted: request.compiler_emitted,
            final_deployable: request.final_deployable,
        }],
    )?;
    let entry = report
        .into_entries()
        .pop()
        .ok_or_else(|| "post-link artifact batch omitted its single entry".to_owned())?;
    entry.into_parts().2.map_err(|failure| match failure {
        ArtifactCacheBatchFailure::Preparation { source, .. }
        | ArtifactCacheBatchFailure::Commit { source, .. } => {
            format!("post-link artifact cache failed: {source}")
        }
        ArtifactCacheBatchFailure::Callback {
            source,
            cleanup_error,
            ..
        } => {
            let mut message = *source;
            if let Some(cleanup) = cleanup_error {
                let _ = write!(message, "\npost-link artifact cleanup: {cleanup}");
            }
            message
        }
    })
}

pub(crate) fn cache_post_link_wasm_batch(
    workspace_root: &Path,
    cache_root: &Path,
    entries: &[PostLinkBatchEntry<'_>],
) -> Result<ArtifactCacheBatchReport<String>, String> {
    let report = cache_post_link_wasm_entries(workspace_root, cache_root, entries)?;

    for entry in report.outcomes() {
        if let Some(outcome) = entry.outcome() {
            trace_post_link(entry.label(), outcome);
        }
    }
    eprintln!("maintained canister post_link_batch={report}");

    Ok(report)
}

// One transaction engine owns cache reuse, producer abort and commit for both lanes.
fn cache_post_link_wasm_entries(
    workspace_root: &Path,
    cache_root: &Path,
    entries: &[PostLinkBatchEntry<'_>],
) -> Result<ArtifactCacheBatchReport<String>, String> {
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
                post_link_cache_spec(request, optimizer.path()),
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

    Ok(report)
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
            .join("testing/build/src/wasm_optimizer.rs"),
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
        "{context}: ic_build_progress phase={phase} elapsed={elapsed:?}",
    );
}

fn report_wasm_build_batch_progress(event: WasmBuildBatchProgressEvent) {
    let WasmBuildBatchProgressEvent::BuildProgress { label, event, .. } = event else {
        return;
    };
    report_wasm_build_progress(&label, event);
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
        "{context}: ic_build_maintenance_warning error={message}",
    );
}

fn wasm_build_progress_config() -> WasmBuildProgressConfig {
    WasmBuildProgressConfig::new().with_heartbeat_interval(BUILD_PROGRESS_HEARTBEAT_INTERVAL)
}

const fn artifact_cache_prune_policy() -> CacheRetentionLimits {
    CacheRetentionLimits::new()
        .with_max_age(CACHE_MAX_AGE)
        .with_max_size_bytes(CACHE_MAX_BYTES)
}

const fn shared_incremental_target_prune_policy() -> CacheRetentionLimits {
    CacheRetentionLimits::new()
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
