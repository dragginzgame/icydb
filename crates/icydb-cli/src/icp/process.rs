//! Module: ICP process helpers.
//! Responsibility: run icp-cli checks, preserve failures and inspect structured installation facts.
//! Does not own: high-level canister workflows, project discovery, or generated endpoint calls.
//! Boundary: exposes process primitives to sibling ICP modules.

use std::{
    io,
    process::{Command, Output, Stdio},
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ic_host_process::{
    child::CleanupPolicy,
    signal::SignalActions,
    tool::{
        CompletedExecution, ExecutionFailure, OutputLimit, OutputLimits, ToolError,
        capture_command, capture_group_command_with_cancellation,
    },
};
use icydb_schema::Principal;

const PROBE_STDOUT_BYTES: usize = 16 * 1024 * 1024;
const PROBE_STDERR_BYTES: usize = 64 * 1024;

/// Capture read-only ICP observations with finite output and elapsed-time budgets.
pub(super) fn probe_output(command: &mut Command) -> io::Result<Output> {
    captured_output(capture_command(
        command,
        OutputLimits {
            stdout: OutputLimit::Terminate(PROBE_STDOUT_BYTES),
            stderr: OutputLimit::Terminate(PROBE_STDERR_BYTES),
            timeout: Some(Duration::from_secs(30)),
        },
    ))
}

/// Approved mutation policy: bounded capture, five-minute deadline, owned group.
/// This controls local resources; cleanup is never proof of remote rollback.
pub(super) const UPDATE_OUTPUT_LIMITS: OutputLimits = OutputLimits {
    stdout: OutputLimit::Terminate(PROBE_STDOUT_BYTES),
    stderr: OutputLimit::Terminate(PROBE_STDERR_BYTES),
    timeout: Some(Duration::from_mins(5)),
};

// Signal dispositions are process-global; only one synchronous update capture
// may own this scope. The mutex protects no data, so poisoning needs no repair.
static UPDATE_SIGNALS: Mutex<()> = Mutex::new(());
static UPDATE_INTERRUPTED: AtomicBool = AtomicBool::new(false);

extern "C" fn update_interrupted(_: libc::c_int) {
    UPDATE_INTERRUPTED.store(true, Ordering::Relaxed);
}

#[expect(
    unsafe_code,
    reason = "Host owns native actions; the CLI owns the exclusive async-signal-safe cancellation policy"
)]
pub(super) fn update_output(command: &mut Command) -> io::Result<Output> {
    let _exclusive = UPDATE_SIGNALS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    UPDATE_INTERRUPTED.store(false, Ordering::Relaxed);
    // SAFETY: the synchronous CLI owns these dispositions through guard drop,
    // under exclusive registration custody. The static handler only stores a
    // lock-free atomic flag. Drop restores actions before releasing the mutex.
    let _signals = unsafe {
        SignalActions::install(
            &[libc::SIGINT, libc::SIGTERM, libc::SIGHUP],
            update_interrupted,
        )
    }
    .map_err(io::Error::other)?;
    captured_output(capture_group_command_with_cancellation(
        command,
        UPDATE_OUTPUT_LIMITS,
        CleanupPolicy::KillAndWait,
        None,
        || UPDATE_INTERRUPTED.load(Ordering::Relaxed),
    ))
}

// Adapt the shared owner's evidence without replacing its typed failure/cause.
// Both call lanes retain original bytes and status until their presentation seam.
pub(super) fn captured_output(result: Result<CompletedExecution, ToolError>) -> io::Result<Output> {
    result
        .map(|output| Output {
            status: output.status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
        .map_err(|error| {
            let kind = match error.execution_error().map(|error| &error.failure) {
                Some(ExecutionFailure::Io { source, .. }) => source.kind(),
                _ => io::ErrorKind::Other,
            };
            io::Error::new(kind, error)
        })
}

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
    let id = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("parse icp canister id UTF-8: {error}"))?
        .trim();
    if id.is_empty() {
        return Ok(None);
    }

    // A successful status command is not proof that its machine output names a
    // valid principal. Validate before admitting calls or copying the identifier.
    Principal::from_text(id)
        .map_err(|_| "icp canister status --id-only returned an invalid principal".to_string())?;
    Ok(Some(id.to_owned()))
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
    let output = probe_output(&mut canister_status_command(environment, canister, output))
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
    ic_host_text::escape::terminal_text(String::from_utf8_lossy(stderr).trim()).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_stderr_is_escaped_for_presentation() {
        let raw = b"bad\x1b[2J\r\n\tmessage";
        let rendered = output_stderr(raw);
        assert!(!rendered.chars().any(char::is_control));
        assert!(rendered.contains("message"));
        assert_eq!(raw[3], 27);
    }

    #[cfg(unix)]
    #[test]
    fn probe_capture_preserves_nonzero_status_and_original_bytes() {
        let mut command = Command::new("sh");
        command.args(["-c", "printf '\\377'; printf failure >&2; exit 7"]);
        let output = probe_output(&mut command).unwrap();
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout, [255]);
        assert_eq!(output.stderr, b"failure");
    }

    #[cfg(unix)]
    #[test]
    fn probe_capture_refuses_excess_machine_output() {
        let mut command = Command::new("sh");
        command.args(["-c", "dd if=/dev/zero bs=1048576 count=17 2>/dev/null"]);
        let error = probe_output(&mut command).unwrap_err();
        let cause = error
            .get_ref()
            .unwrap()
            .downcast_ref::<ic_host_process::tool::ToolError>()
            .unwrap();
        assert!(matches!(
            cause.execution_error().unwrap().failure,
            ExecutionFailure::OutputLimit { .. }
        ));
    }
}
