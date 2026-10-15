//! Module: ICP process helpers.
//! Responsibility: run icp-cli checks, preserve failures and inspect structured installation facts.
//! Does not own: high-level canister workflows, project discovery, or generated endpoint calls.
//! Boundary: exposes process primitives to sibling ICP modules.

use std::process::{Command, Output, Stdio};

/// Run one icp-cli command as a client call. This never starts or stops a local network.
pub(super) fn run_external_command(mut command: Command, label: &str) -> Result<(), String> {
    let status = command
        .stdin(Stdio::null())
        .status()
        .map_err(|err| format!("{label}: {err}"))?;
    if status.success() {
        return Ok(());
    }

    Err(format!("{label} failed with {status}"))
}

/// Return whether icp-cli reports an installed canister in the selected environment.
pub(super) fn canister_is_installed(environment: &str, canister: &str) -> Result<bool, String> {
    let output = canister_status_output(environment, canister, CanisterStatusOutput::Json)?;
    let status = serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .map_err(|err| format!("parse icp canister status --json: {err}"))?;
    // A failed probe never proves absence. ICP owns the successful status
    // contract: module_hash is null for a created canister without installed Wasm.
    match status.get("module_hash") {
        Some(serde_json::Value::Null) => Ok(false),
        Some(serde_json::Value::String(_)) => Ok(true),
        _ => Err("icp canister status --json omitted a valid module_hash".to_string()),
    }
}

/// Resolve an icp-cli canister id without treating absent local ids as fatal.
pub(super) fn canister_id(environment: &str, canister: &str) -> Result<Option<String>, String> {
    let output = canister_status_output(environment, canister, CanisterStatusOutput::IdOnly)?;
    let id = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_string();

    Ok((!id.is_empty()).then_some(id))
}

pub(super) enum CanisterStatusOutput {
    Capture,
    Json,
    IdOnly,
}

fn canister_status_output(
    environment: &str,
    canister: &str,
    output: CanisterStatusOutput,
) -> Result<Output, String> {
    let output = canister_status_command(environment, canister, output)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| format!("failed to run icp canister status: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "icp canister status for '{canister}' in environment '{environment}' failed with {}: {}",
            output.status,
            output_stderr(&output.stderr),
        ));
    }
    Ok(output)
}

pub(super) fn canister_status_command(
    environment: &str,
    canister: &str,
    output: CanisterStatusOutput,
) -> Command {
    let mut command = Command::new("icp");
    command.arg("canister").arg("status").arg(canister);
    match output {
        CanisterStatusOutput::Capture => {
            command.stdout(Stdio::piped());
        }
        CanisterStatusOutput::Json => {
            command.arg("--json").stdout(Stdio::piped());
        }
        CanisterStatusOutput::IdOnly => {
            command.arg("--id-only").stdout(Stdio::piped());
        }
    }

    command.arg("--environment").arg(environment);

    command
}

pub(super) fn output_stderr(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr).trim().to_string()
}
