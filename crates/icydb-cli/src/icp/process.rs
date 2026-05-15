use std::process::{Command, Stdio};

/// Run one icp-cli command as a client call. This never starts or stops a local network.
pub(crate) fn run_external_command(mut command: Command, label: &str) -> Result<(), String> {
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
pub(crate) fn canister_is_installed(environment: &str, canister: &str) -> Result<bool, String> {
    let output = Command::new("icp")
        .arg("canister")
        .arg("status")
        .arg(canister)
        .arg("--environment")
        .arg(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;
    if output.status.success() {
        return Ok(true);
    }

    let stderr = String::from_utf8_lossy(output.stderr.as_slice())
        .trim()
        .to_string();
    if unreachable_network_hint(stderr.as_str()).is_some() {
        return Err(stderr);
    }

    Ok(false)
}

/// Resolve an icp-cli canister id without treating absent local ids as fatal.
pub(crate) fn canister_id(environment: &str, canister: &str) -> Result<Option<String>, String> {
    let output = Command::new("icp")
        .arg("canister")
        .arg("status")
        .arg(canister)
        .arg("--id-only")
        .arg("--environment")
        .arg(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(output.stderr.as_slice())
            .trim()
            .to_string();
        if unreachable_network_hint(stderr.as_str()).is_some() {
            return Err(stderr);
        }

        return Ok(None);
    }

    let id = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_string();

    Ok((!id.is_empty()).then_some(id))
}

/// Recognize common icp-cli connection failures and return explicit lifecycle guidance.
pub(crate) fn unreachable_network_hint(message: &str) -> Option<&'static str> {
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
