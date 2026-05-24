//! Module: ICP process helpers.
//! Responsibility: run icp-cli process checks and normalize local-network reachability errors.
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
    let output = canister_status_output(environment, canister, CanisterStatusOutput::Discard)?;
    if output.status.success() {
        return Ok(true);
    }

    let stderr = output_stderr(output.stderr.as_slice());
    if is_unreachable_network_error(stderr.as_str()) {
        return Err(stderr);
    }

    Ok(false)
}

/// Resolve an icp-cli canister id without treating absent local ids as fatal.
pub(super) fn canister_id(environment: &str, canister: &str) -> Result<Option<String>, String> {
    let output = canister_status_output(environment, canister, CanisterStatusOutput::IdOnly)?;
    if !output.status.success() {
        let stderr = output_stderr(output.stderr.as_slice());
        if is_unreachable_network_error(stderr.as_str()) {
            return Err(stderr);
        }

        return Ok(None);
    }

    let id = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_string();

    Ok((!id.is_empty()).then_some(id))
}

enum CanisterStatusOutput {
    Discard,
    IdOnly,
}

fn canister_status_output(
    environment: &str,
    canister: &str,
    output: CanisterStatusOutput,
) -> Result<Output, String> {
    let mut command = Command::new("icp");
    command.arg("canister").arg("status").arg(canister);
    match output {
        CanisterStatusOutput::Discard => {
            command.stdout(Stdio::null());
        }
        CanisterStatusOutput::IdOnly => {
            command.arg("--id-only").stdout(Stdio::piped());
        }
    }

    command
        .arg("--environment")
        .arg(environment)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())
}

fn output_stderr(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr).trim().to_string()
}

fn is_unreachable_network_error(message: &str) -> bool {
    unreachable_network_hint(message).is_some()
}

/// Recognize common icp-cli connection failures and return explicit lifecycle guidance.
pub(super) fn unreachable_network_hint(message: &str) -> Option<&'static str> {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("connection refused")
        || lowered.contains("failed to connect")
        || lowered.contains("replica")
        || lowered.contains("local network")
        || lowered.contains("pocketic")
        || lowered.contains("network is not running")
        || lowered.contains("unable to access network")
    {
        return Some(
            "local ICP network is not reachable. Start the configured local ICP network outside this CLI, then retry.",
        );
    }

    None
}
