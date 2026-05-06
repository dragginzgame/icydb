use std::process::{Command, Stdio};

/// Run one dfx command as a client call. This never starts or stops dfx.
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

/// Return whether dfx reports an installed canister at the current local target.
pub(crate) fn canister_is_installed(canister: &str) -> Result<bool, String> {
    let output = Command::new("dfx")
        .arg("canister")
        .arg("status")
        .arg(canister)
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
    if unreachable_daemon_hint(stderr.as_str()).is_some() {
        return Err(stderr);
    }

    Ok(false)
}

/// Resolve a dfx canister id without treating absent local ids as fatal.
pub(crate) fn canister_id(canister: &str) -> Result<Option<String>, String> {
    let output = Command::new("dfx")
        .arg("canister")
        .arg("id")
        .arg(canister)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(output.stderr.as_slice())
            .trim()
            .to_string();
        if unreachable_daemon_hint(stderr.as_str()).is_some() {
            return Err(stderr);
        }

        return Ok(None);
    }

    let id = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_string();

    Ok((!id.is_empty()).then_some(id))
}

/// Call a no-argument fixture method on an already selected dfx canister.
pub(crate) fn call_unit_method(canister: &str, method: &str) -> Result<(), String> {
    let mut command = Command::new("dfx");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg("()");

    run_external_command(command, "dfx canister call")
}

/// Recognize common dfx connection failures and return explicit lifecycle guidance.
pub(crate) fn unreachable_daemon_hint(message: &str) -> Option<&'static str> {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("connection refused")
        || lowered.contains("failed to connect")
        || lowered.contains("replica")
        || lowered.contains("local network")
        || lowered.contains("pocketic")
    {
        return Some(
            "dfx local daemon is not reachable. Start `dfx start` in another terminal, then retry.",
        );
    }

    None
}
