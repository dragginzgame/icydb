//! Consumer deployment exclusions, bound to the retained Cargo and final Wasm.
//! IC Build owns source custody; Shared Tooling owns graph traversal and aliases.

#[cfg(test)]
mod tests;

use crate::canister_build_cache::{CargoWasmCacheRequest, cargo_wasm_spec};
use ic_build::artifacts::{WasmBuildRecord, resolve_cargo_build_inputs};
use ic_host_artifacts::artifact::hash_reader;
use ic_host_process::tool::{
    CompletedExecution, OutputLimit, OutputLimits, capture_group_command, format_failure_evidence,
};
use serde_json::{Value, json};
use std::{fs::File, path::Path, process::Command, time::Duration};

// These are test runners, build drivers and native oracles, not actor DTOs.
// Schema fixtures, wasm-helpers, codec-query and Canic's host/schema build
// dependencies are intentional actor inputs and must remain permitted.
const EXCLUDED_PACKAGES: &[&str] = &[
    "ic-build",
    "ic-testkit",
    "pocket-ic",
    "proptest",
    "rusqlite",
    "trybuild",
    "icydb-testing-build",
    "icydb-testing-integration",
    "icydb-testing-sql-generator",
    "icydb-testing-sqlite-reference",
];

// Bound process diagnostics/graph retention without adding a caller mode.
const GRAPH_OUTPUT_LIMITS: OutputLimits = OutputLimits {
    stdout: OutputLimit::Terminate(8 * 1024 * 1024),
    stderr: OutputLimit::Terminate(1024 * 1024),
    timeout: Some(Duration::from_secs(600)),
};

/// Reinspect every acquisition, including cache hits. No saved observation
/// authorizes a different source, lockfile, target, feature set or artifact.
pub(crate) fn admit(
    repository: &Path,
    request: &CargoWasmCacheRequest<'_>,
    record: &WasmBuildRecord,
    deployable: &Path,
) -> Result<Value, String> {
    let spec = cargo_wasm_spec(
        request.workspace_root,
        request.target_dir,
        request.packages,
        request.selection,
        request.encoded_rustflags,
    );
    let inputs = resolve_cargo_build_inputs(&spec).map_err(|error| error.to_string())?;
    if inputs.fingerprint() != record.fingerprint() {
        return Err("dependency admission does not match retained Cargo inputs".to_owned());
    }
    let facts = inputs.facts();
    let mut command = Command::new("perl");
    command
        .arg(repository.join("scripts/ci/check-cargo-graph.pl"))
        .args(["--consumer"])
        .arg(request.workspace_root)
        .args([
            "--target",
            facts.target.as_str(),
            "--json",
            "--explain",
            "--observations",
        ])
        .env_clear()
        .envs(spec.environment());
    for package in request.packages {
        command.args(["--package", package]);
    }
    for package in EXCLUDED_PACKAGES {
        command.args(["--forbid", package]);
    }
    if !facts.selection.features.default_features {
        command.arg("--no-default-features");
    }
    if facts.selection.features.all_features {
        command.arg("--all-features");
    } else if !facts.selection.features.features.is_empty() {
        command.args([
            "--features",
            &facts
                .selection
                .features
                .features
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(","),
        ]);
    }
    let output = capture_group_command(&mut command, GRAPH_OUTPUT_LIMITS)
        .and_then(CompletedExecution::require_success)
        .map_err(|error| format_failure_evidence("canister dependency admission", &error))?;
    let graph: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode dependency admission: {error}"))?;
    // The graph subprocess must not drift away from the source/lock/config
    // snapshot whose successful compiler output we retained.
    if !inputs
        .is_current(&spec)
        .map_err(|error| error.to_string())?
        || !inputs
            .is_content_current()
            .map_err(|error| error.to_string())?
    {
        return Err("Cargo inputs changed during dependency admission".to_owned());
    }
    let [compiler] = record.artifacts() else {
        return Err("dependency admission requires one retained compiler artifact".to_owned());
    };
    let identity = |path: &Path| {
        let source = File::open(path).map_err(|error| error.to_string())?;
        hash_reader(source, 128 * 1024 * 1024).map_err(|error| error.to_string())
    };
    let compiler = identity(compiler)?;
    let deployable = identity(deployable)?;
    let lock = identity(&request.workspace_root.join("Cargo.lock"))?;
    Ok(json!({
        "cargo_fingerprint": record.fingerprint().to_string(),
        "source_digest": record.input_digest().to_string(),
        "lock_sha256": lock.sha256.to_string(),
        "target": facts.target,
        "profile": facts.selection.profile,
        "default_features": facts.selection.features.default_features,
        "all_features": facts.selection.features.all_features,
        "features": facts.selection.features.features,
        "excluded_packages": EXCLUDED_PACKAGES,
        "compiler_sha256": compiler.sha256.to_string(),
        "deployable_sha256": deployable.sha256.to_string(),
        "deployable_bytes": deployable.bytes,
        "graph": graph,
    }))
}
