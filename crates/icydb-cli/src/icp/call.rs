//! Module: ICP canister call construction.
//! Responsibility: build icp-cli canister call commands and decode hex responses.
//! Does not own: command execution, endpoint selection, or Wire decoding.
//! Boundary: exposes reusable call builders and response decoding to CLI command surfaces.

use std::{
    fmt::Write as _,
    io::{self, Write as _},
    path::Path,
    process::{Command, Output, Stdio},
};

use ic_host_fs::scratch::TemporaryFile;
use ic_host_tools::response::{ResponseError, ResponseFormat, ResponseLimits, decode};

use ic_host_process::tool::{ExecutionFailure, ExecutionOperation, ToolError};

use crate::icp::process::{output_stderr, probe_output, update_output};

pub(super) fn icp_query_command(
    environment: &str,
    canister: &str,
    method: &str,
    argument_file: &Path,
) -> Command {
    icp_call_command(
        environment,
        canister,
        method,
        argument_file,
        IcpCallKind::Query,
    )
}

pub(super) fn icp_update_command(
    environment: &str,
    canister: &str,
    method: &str,
    argument_file: &Path,
) -> Command {
    icp_call_command(
        environment,
        canister,
        method,
        argument_file,
        IcpCallKind::Update,
    )
}

#[derive(Clone, Copy)]
pub(crate) enum IcpCallKind {
    Query,
    Update,
}

impl IcpCallKind {
    /// Classify lost replies at the actual call lane, before application errors
    /// are projected. A decoded endpoint error is an observed reply, not a loss.
    pub(crate) fn reply_error(self, detail: String) -> String {
        match self {
            Self::Query => detail,
            Self::Update => format!(
                "update outcome unknown: {detail}; inspect canister state before repeating; no automatic retry was attempted"
            ),
        }
    }

    fn capture_error(self, error: io::Error) -> String {
        let not_started = capture_failed_before_dispatch(&error);
        let mut detail = format!("failed to run icp canister call: {error}");
        if matches!(self, Self::Update)
            && let Some(evidence) = error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<ToolError>())
                .and_then(ToolError::evidence)
        {
            if let Some(status) = evidence.status {
                let _ = write!(detail, " (process exited with {status})");
            }
            if !evidence.stderr.is_empty() {
                detail.push_str(": ");
                detail.push_str(&output_stderr(&evidence.stderr));
            }
        }
        if not_started {
            detail
        } else {
            self.reply_error(detail)
        }
    }
}

// Only typed pre-spawn evidence proves this call did not dispatch. Missing
// evidence or capture failure after spawn must remain conservative.
fn capture_failed_before_dispatch(error: &io::Error) -> bool {
    let Some(cause) = error.get_ref() else {
        return false;
    };
    if cause
        .downcast_ref::<ic_host_process::signal::SignalActionError>()
        .is_some()
    {
        return true;
    }
    cause
        .downcast_ref::<ToolError>()
        .is_some_and(|error| match error {
            ToolError::InvalidInvocation(_) => true,
            ToolError::Execution(error) => matches!(
                error.failure,
                ExecutionFailure::Io {
                    operation: ExecutionOperation::Spawn,
                    ..
                }
            ),
            _ => false,
        })
}

fn icp_call_command(
    environment: &str,
    canister: &str,
    method: &str,
    argument_file: &Path,
    kind: IcpCallKind,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg("--args-file")
        .arg(argument_file)
        .arg("--args-format")
        .arg("bin");
    match kind {
        IcpCallKind::Query => {
            command.arg("--query");
        }
        IcpCallKind::Update => {}
    }
    command
        .arg("--output")
        .arg("hex")
        .arg("--environment")
        .arg(environment);

    command
}

pub(super) fn hex_response_bytes(output: &str) -> Result<Vec<u8>, String> {
    // IcyDB owns presentation labels and Unicode whitespace normalization.
    // The shared owner receives one compact hex body, without format inference.
    let candidate = output
        .rsplit_once("response (hex):")
        .map_or(output, |(_, value)| value)
        .trim();
    let hex = candidate.split_whitespace().collect::<String>();
    if hex.is_empty() {
        return Err("icp canister call returned an empty hex response".to_string());
    }
    if hex.len() % 2 != 0 {
        return Err("icp canister call returned odd-length hex response".to_string());
    }

    decode(
        hex.as_bytes(),
        ResponseFormat::Hex,
        ResponseLimits {
            input_bytes: hex.len(),
            decoded_bytes: hex.len() / 2,
        },
    )
    .map_err(|error| match error {
        ResponseError::InvalidHex { offset } => hex.as_bytes().get(offset).map_or_else(
            || "icp canister call returned invalid hex response".to_string(),
            // Preserve the byte identity without emitting external terminal
            // controls in a decoding diagnostic.
            |byte| format!("icp canister call returned non-hex byte 0x{byte:02x}"),
        ),
        other => format!("icp canister call response decoding failed: {other}"),
    })
}

pub(super) fn call_query_hex(
    environment: &str,
    canister: &str,
    method: &str,
    wire_arg: &[u8],
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call_hex(
        wire_arg,
        |path| icp_query_command(environment, canister, method, path),
        error_message,
        IcpCallKind::Query,
        probe_output,
    )
}

pub(super) fn call_update_hex(
    environment: &str,
    canister: &str,
    method: &str,
    wire_arg: &[u8],
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call_hex(
        wire_arg,
        |path| icp_update_command(environment, canister, method, path),
        error_message,
        IcpCallKind::Update,
        update_output,
    )
}

pub(super) fn call_hex(
    wire_arg: &[u8],
    build_command: impl FnOnce(&Path) -> Command,
    error_message: impl FnOnce(&str) -> String,
    kind: IcpCallKind,
    capture: impl FnOnce(&mut Command) -> io::Result<Output>,
) -> Result<Vec<u8>, String> {
    // The private file stays alive through child cleanup. Explicit close reports
    // removal failures; Drop retains unwind cleanup. Paths avoid argv/NUL limits.
    // Host owns exclusive allocation and scoped file custody. Complete the
    // write before starting a child, preserving both write and cleanup failures.
    let mut arguments = TemporaryFile::new_in(&std::env::temp_dir(), "icydb-icp-args")
        .map_err(|source| format!("create ICP argument file: {source}"))?;
    if let Err(source) = arguments.as_file_mut().write_all(wire_arg) {
        let path = arguments.path().to_owned();
        return Err(match arguments.close() {
            Ok(()) => format!("write ICP argument file: {source}"),
            Err(cleanup) => format!(
                "write ICP argument file: {source}; remove '{}': {cleanup}",
                path.display()
            ),
        });
    }
    let argument_path = arguments.path().to_owned();
    let result = (|| {
        let output = capture(
            build_command(&argument_path)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
        .map_err(|error| kind.capture_error(error))?;

        let response = (|| {
            if !output.status.success() {
                let stderr = output_stderr(output.stderr.as_slice());
                let detail = error_message(stderr.as_str());
                return Err(match kind {
                    IcpCallKind::Query => detail,
                    IcpCallKind::Update => {
                        format!("{detail} (process exited with {})", output.status)
                    }
                });
            }
            let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;
            hex_response_bytes(stdout.as_str())
        })();
        response.map_err(|error| kind.reply_error(error))
    })();
    match arguments.close() {
        Ok(()) => result,
        Err(cleanup) => {
            let detail = format!(
                "remove ICP argument file '{}': {cleanup}",
                argument_path.display()
            );
            let detail = output_stderr(detail.as_bytes());
            match result {
                Ok(_) => Err(kind.reply_error(detail)),
                Err(error) => Err(format!("{error}; {detail}")),
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt as _,
        panic::{AssertUnwindSafe, catch_unwind},
        path::PathBuf,
        process::Command,
    };

    use super::{IcpCallKind, call_hex, probe_output};

    #[test]
    fn private_argument_file_lives_through_child_and_is_removed_on_success() {
        let bytes = [0, 255, 1, 0, 128];
        let mut argument_path = PathBuf::new();
        let result = call_hex(
            &bytes,
            |path| {
                argument_path = path.to_owned();
                assert_eq!(fs::read(path).unwrap(), bytes);
                assert_eq!(
                    fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
                let mut command = Command::new("sh");
                command.args(["-c", "test -f \"$1\" && printf 00", "argument-file"]);
                command.arg(path);
                command
            },
            str::to_owned,
            IcpCallKind::Query,
            probe_output,
        );
        assert_eq!(result.unwrap(), [0]);
        assert!(!argument_path.exists());
    }

    #[test]
    fn argument_file_is_removed_after_child_and_response_failures() {
        for recipe in [
            Some("exit 7"),
            Some("printf invalid_hex"),
            Some("printf '\\377'"),
            None,
        ] {
            let mut argument_path = PathBuf::new();
            let result = call_hex(
                b"request",
                |path| {
                    argument_path = path.to_owned();
                    assert_eq!(fs::read(path).unwrap(), b"request");
                    if let Some(recipe) = recipe {
                        let mut command = Command::new("sh");
                        command.args(["-c", recipe]);
                        command
                    } else {
                        Command::new(path.with_extension("missing-executable"))
                    }
                },
                str::to_owned,
                IcpCallKind::Query,
                probe_output,
            );
            assert!(result.is_err());
            assert!(!argument_path.exists());
        }
    }

    #[test]
    fn argument_file_is_removed_during_unwind() {
        let mut argument_path = PathBuf::new();
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = call_hex(
                b"request",
                |path| {
                    argument_path = path.to_owned();
                    panic!("command construction failed")
                },
                str::to_owned,
                IcpCallKind::Query,
                probe_output,
            );
        }));
        assert!(panic.is_err());
        assert!(!argument_path.exists());
    }
}

#[cfg(all(test, unix))]
mod mutation_tests {
    use super::{IcpCallKind, call_hex, capture_failed_before_dispatch};
    use crate::icp::process::{UPDATE_OUTPUT_LIMITS, captured_output, update_output};
    use ic_host_fs::scratch::TemporaryFile;
    use ic_host_process::{
        child::CleanupPolicy,
        tool::{
            ExecutionFailure, OutputLimits, OutputStream, ToolError,
            capture_group_command_with_cancellation,
        },
    };
    use std::{fs, process::Command, time::Duration};

    fn marker() -> TemporaryFile {
        TemporaryFile::new_in(&std::env::temp_dir(), "icydb-update-test").unwrap()
    }

    #[test]
    fn mutation_limits_preserve_cause_and_argument_custody() {
        for (stream, recipe) in [
            (
                OutputStream::Stdout,
                "command -p dd if=/dev/zero bs=1048576 count=17 2>/dev/null",
            ),
            (
                OutputStream::Stderr,
                "(command -p dd if=/dev/zero bs=1024 count=65 2>/dev/null) >&2",
            ),
        ] {
            let observed = marker();
            let mut argument_path = std::path::PathBuf::new();
            let result = call_hex(
                b"private request",
                |path| {
                    argument_path = path.to_owned();
                    let mut command = Command::new("/bin/sh");
                    command
                        .args([
                            "-c",
                            &format!("command -p cat \"$1\" > \"$2\"; {recipe}"),
                            "fixture",
                        ])
                        .arg(path)
                        .arg(observed.path());
                    command
                },
                str::to_owned,
                IcpCallKind::Update,
                |command| {
                    let error = update_output(command).unwrap_err();
                    assert!(!capture_failed_before_dispatch(&error));
                    let cause = error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<ToolError>()
                        .unwrap();
                    let execution = cause.execution_error().unwrap();
                    assert!(
                        matches!(execution.failure, ExecutionFailure::OutputLimit { stream: observed } if observed == stream)
                    );
                    assert!(execution.evidence.status.is_some());
                    assert!(execution.cleanup.is_none());
                    assert!(
                        execution.evidence.stdout.len()
                            <= UPDATE_OUTPUT_LIMITS.stdout.retained_bytes()
                    );
                    assert!(
                        execution.evidence.stderr.len()
                            <= UPDATE_OUTPUT_LIMITS.stderr.retained_bytes()
                    );
                    Err(error)
                },
            );
            assert!(result.is_err());
            assert_eq!(fs::read(observed.path()).unwrap(), b"private request");
            assert!(!argument_path.exists());
        }
    }

    #[test]
    fn mutation_deadline_and_group_exit_preserve_argument_custody() {
        for timeout in [false, true] {
            let observed = marker();
            let mut argument_path = std::path::PathBuf::new();
            let result = call_hex(
                b"private request",
                |path| {
                    argument_path = path.to_owned();
                    let mut command = Command::new("/bin/sh");
                    let ending = if timeout { "wait" } else { "printf 00; exit 0" };
                    command
                        .args([
                            "-c",
                            &format!(
                                "command -p sleep 30 & command -p cat \"$1\" > \"$2\"; {ending}"
                            ),
                            "fixture",
                        ])
                        .arg(path)
                        .arg(observed.path());
                    command
                },
                str::to_owned,
                IcpCallKind::Update,
                |command| {
                    // Exercise the same published capture/adapter with a short
                    // fixture deadline, retaining production stream ceilings.
                    let output = captured_output(capture_group_command_with_cancellation(
                        command,
                        OutputLimits {
                            timeout: Some(Duration::from_secs(5)),
                            ..UPDATE_OUTPUT_LIMITS
                        },
                        CleanupPolicy::KillAndWait,
                        None,
                        || false,
                    ));
                    if timeout {
                        let error = output.as_ref().unwrap_err();
                        assert!(!capture_failed_before_dispatch(error));
                        let cause = error
                            .get_ref()
                            .unwrap()
                            .downcast_ref::<ToolError>()
                            .unwrap();
                        let execution = cause.execution_error().unwrap();
                        assert!(matches!(execution.failure, ExecutionFailure::TimedOut));
                        assert!(execution.evidence.status.is_some());
                        assert!(execution.cleanup.is_none());
                    }
                    output
                },
            );
            if timeout {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), [0]);
            }
            assert_eq!(fs::read(observed.path()).unwrap(), b"private request");
            assert!(!argument_path.exists());
        }
    }

    #[test]
    fn mutation_argument_cleanup_refusal_never_reports_success() {
        use std::cell::{Cell, RefCell};

        for process_failure in [false, true] {
            let path = RefCell::new(std::path::PathBuf::new());
            let original_failure_observed = Cell::new(false);
            let result = call_hex(
                b"private request",
                |argument| {
                    *path.borrow_mut() = argument.to_owned();
                    let mut command = Command::new("/bin/sh");
                    command.args([
                        "-c",
                        if process_failure {
                            "exit 7"
                        } else {
                            "printf 00"
                        },
                    ]);
                    command
                },
                |_| {
                    original_failure_observed.set(true);
                    "original failure".to_owned()
                },
                IcpCallKind::Update,
                |command| {
                    let output = update_output(command);
                    // Simulate pathname replacement only after child cleanup.
                    // The original unlinked private file remains guard-owned.
                    fs::remove_file(&*path.borrow()).unwrap();
                    fs::create_dir(&*path.borrow()).unwrap();
                    output
                },
            );
            assert!(result.is_err());
            assert_eq!(original_failure_observed.get(), process_failure);
            fs::remove_dir(path.into_inner()).unwrap();
        }
    }

    #[test]
    fn mutation_pre_dispatch_refusals_keep_typed_evidence() {
        let missing = marker();
        let mut command = Command::new(missing.path().with_extension("absent"));
        let error = update_output(&mut command).unwrap_err();
        assert!(capture_failed_before_dispatch(&error));
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        let error = captured_output(capture_group_command_with_cancellation(
            &mut command,
            OutputLimits {
                timeout: Some(Duration::ZERO),
                ..UPDATE_OUTPUT_LIMITS
            },
            CleanupPolicy::KillAndWait,
            None,
            || false,
        ))
        .unwrap_err();
        assert!(capture_failed_before_dispatch(&error));
        let cause = error
            .get_ref()
            .unwrap()
            .downcast_ref::<ToolError>()
            .unwrap();
        assert!(matches!(cause, ToolError::InvalidInvocation(_)));
    }
}
