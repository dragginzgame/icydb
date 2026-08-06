use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use ic_testkit::artifacts::{
    ArtifactCachePreparation, ArtifactCachePrunePolicy, ArtifactCacheRecord, ArtifactCacheSpec,
    prepare_artifact_cache,
};

use crate::wasm_optimizer::{POST_LINK_PIPELINE_IDENTITY, pinned_wasm_optimizer};

const CACHE_NAMESPACE: &str = "icydb-canister-wasm";
const CACHE_RECIPE_ID: &str = "icydb/cargo-incremental+post-link/v1";
const CACHE_TRACE_ENV: &str = "ICYDB_CANISTER_CACHE_TRACE";
const CACHE_MAX_AGE: Duration = Duration::from_hours(336);
const CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const BUILD_INPUTS: [(&str, &str); 14] = [
    ("workspace-manifest", "Cargo.toml"),
    ("workspace-lock", "Cargo.lock"),
    ("rust-toolchain", "rust-toolchain.toml"),
    ("canisters", "canisters"),
    ("schema", "schema"),
    ("wasm-helpers", "testing/wasm-helpers"),
    ("icydb", "crates/icydb"),
    ("icydb-core", "crates/icydb-core"),
    ("icydb-diagnostic-code", "crates/icydb-diagnostic-code"),
    ("icydb-model", "crates/icydb-model"),
    ("icydb-model-macros", "crates/icydb-model-macros"),
    ("icydb-schema", "crates/icydb-schema"),
    ("canister-build-pipeline", "testing/integration/src/lib.rs"),
    (
        "canister-build-cache",
        "testing/integration/src/canister_build_cache.rs",
    ),
];
const EXACT_BUILD_ENVIRONMENT: [&str; 17] = [
    "AR",
    "CARGO_BUILD_RUSTC",
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_HOME",
    "CC",
    "CFLAGS",
    "CXX",
    "CXXFLAGS",
    "HOST_CC",
    "HOST_CFLAGS",
    "ICYDB_WASM_OPT_BIN",
    "RUSTC",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "RUSTFLAGS",
    "RUSTUP_TOOLCHAIN",
    "SOURCE_DATE_EPOCH",
];

pub(crate) struct CanisterBuildCacheRequest<'a> {
    pub(crate) workspace_root: &'a Path,
    pub(crate) cache_root: &'a Path,
    pub(crate) coordination_scope: &'a str,
    pub(crate) arguments: &'a [String],
    pub(crate) effective_rustflags: Option<&'a str>,
    pub(crate) compiler_emitted: &'a Path,
    pub(crate) final_deployable: Option<&'a Path>,
}

pub(crate) fn prepare_canister_build_cache(
    request: &CanisterBuildCacheRequest<'_>,
) -> Result<ArtifactCachePreparation, String> {
    let mut spec = ArtifactCacheSpec::new(request.cache_root, CACHE_NAMESPACE, CACHE_RECIPE_ID)
        .with_coordination_scope(request.coordination_scope)
        .with_arguments(
            &request
                .arguments
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )
        .with_identity_bytes(
            "build-environment-v1",
            &build_environment_identity(request.effective_rustflags),
        )
        .with_identity_bytes(
            "cargo-version",
            &command_identity(
                request.workspace_root,
                OsStr::new("cargo"),
                &["--version", "--verbose"],
            )?,
        )
        .with_identity_bytes(
            "rustc-version",
            &command_identity(
                request.workspace_root,
                env::var_os("RUSTC")
                    .as_deref()
                    .unwrap_or_else(|| OsStr::new("rustc")),
                &["-vV"],
            )?,
        )
        .with_output("compiler-emitted", request.compiler_emitted)
        .with_prune_policy(
            ArtifactCachePrunePolicy::new()
                .with_max_age(CACHE_MAX_AGE)
                .with_max_size_bytes(CACHE_MAX_BYTES),
        );

    for (label, relative_path) in BUILD_INPUTS {
        spec = spec.with_input(label, &request.workspace_root.join(relative_path));
    }
    for (label, path) in cargo_configuration_inputs(request.workspace_root) {
        spec = spec.with_input(&label, &path);
    }

    if let Some(final_deployable) = request.final_deployable {
        let optimizer = pinned_wasm_optimizer()?;
        spec = spec
            .with_input(
                "post-link-pipeline",
                &request
                    .workspace_root
                    .join("testing/integration/src/wasm_optimizer.rs"),
            )
            .with_tool("wasm-opt", &optimizer)
            .with_identity_bytes(
                "post-link-pipeline-identity",
                POST_LINK_PIPELINE_IDENTITY.as_bytes(),
            )
            .with_output("final-deployable", final_deployable);
    }

    prepare_artifact_cache(&spec)
        .map_err(|error| format!("canister artifact cache failed: {error}"))
}

pub(crate) fn trace_cache_record(context: &str, disposition: &str, record: &ArtifactCacheRecord) {
    if env::var_os(CACHE_TRACE_ENV).is_none() {
        return;
    }

    let timings = record.timings();
    eprintln!(
        "{context}: artifact_cache={disposition} input_capture_ms={:.2} caller_build_ms={} materialization_ms={:.2} total_ms={:.2}",
        duration_millis(timings.input_capture()),
        timings.caller_build().map_or_else(
            || "none".to_owned(),
            |duration| format!("{:.2}", duration_millis(duration))
        ),
        duration_millis(timings.materialization()),
        duration_millis(timings.total()),
    );
}

fn cargo_configuration_inputs(workspace_root: &Path) -> Vec<(String, PathBuf)> {
    let mut paths = BTreeSet::new();
    for ancestor in workspace_root.ancestors() {
        for file_name in ["config.toml", "config"] {
            let path = ancestor.join(".cargo").join(file_name);
            if path.is_file() {
                paths.insert(path);
            }
        }
    }
    if let Some(cargo_home) = env::var_os("CARGO_HOME") {
        for file_name in ["config.toml", "config"] {
            let path = PathBuf::from(&cargo_home).join(file_name);
            if path.is_file() {
                paths.insert(path);
            }
        }
    }

    paths
        .into_iter()
        .enumerate()
        .map(|(index, path)| (format!("cargo-config/{index}"), path))
        .collect()
}

fn build_environment_identity(effective_rustflags: Option<&str>) -> Vec<u8> {
    let mut environment = env::vars_os()
        .filter(|(name, _)| relevant_build_environment(name))
        .map(|(name, value)| (name, Some(value)))
        .collect::<BTreeMap<_, _>>();
    for name in EXACT_BUILD_ENVIRONMENT {
        environment.entry(OsString::from(name)).or_insert(None);
    }
    if let Some(rustflags) = effective_rustflags {
        environment.insert(OsString::from("RUSTFLAGS"), Some(OsString::from(rustflags)));
    }

    let mut identity = Vec::new();
    for (name, value) in environment {
        append_identity_field(&mut identity, name.as_encoded_bytes());
        match value {
            Some(value) => {
                identity.push(1);
                append_identity_field(&mut identity, value.as_encoded_bytes());
            }
            None => identity.push(0),
        }
    }
    identity
}

fn relevant_build_environment(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    EXACT_BUILD_ENVIRONMENT.contains(&name)
        || name.starts_with("CARGO_PROFILE_")
        || name.starts_with("CARGO_TARGET_")
}

fn append_identity_field(identity: &mut Vec<u8>, value: &[u8]) {
    identity.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    identity.extend_from_slice(value);
}

fn command_identity(
    workspace_root: &Path,
    program: &OsStr,
    arguments: &[&str],
) -> Result<Vec<u8>, String> {
    let output = Command::new(program)
        .current_dir(workspace_root)
        .args(arguments)
        .output()
        .map_err(|error| {
            format!(
                "failed to read build tool identity from {}: {error}",
                program.to_string_lossy()
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "build tool identity command {} failed with status {}\nstdout:\n{}\nstderr:\n{}",
            program.to_string_lossy(),
            output.status,
            String::from_utf8_lossy(&output.stdout).trim_end(),
            String::from_utf8_lossy(&output.stderr).trim_end(),
        ));
    }

    let mut identity = output.stdout;
    identity.extend_from_slice(&output.stderr);
    Ok(identity)
}

fn duration_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::BUILD_INPUTS;

    #[test]
    fn declared_canister_build_inputs_are_unique_existing_and_target_free() {
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("integration crate should be nested under the workspace")
            .to_path_buf();
        let mut labels = BTreeSet::new();

        for (label, relative_path) in BUILD_INPUTS {
            assert!(labels.insert(label), "cache input labels must be unique");
            assert!(
                workspace_root.join(relative_path).exists(),
                "declared cache input should exist: {relative_path}"
            );
            assert!(
                !relative_path.starts_with("target"),
                "cache inputs must not recursively include build outputs"
            );
        }
    }
}
