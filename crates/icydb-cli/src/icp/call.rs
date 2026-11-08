//! Module: ICP canister call construction.
//! Responsibility: build icp-cli canister call commands and decode hex responses.
//! Does not own: command execution, endpoint selection, or Wire decoding.
//! Boundary: exposes reusable call builders and response decoding to CLI command surfaces.

use std::{
    ffi::OsStr,
    fmt::Write as _,
    io::{self, Write as _},
    path::Path,
    process::{Command, Output, Stdio},
};

use ic_host_fs::scratch::TemporaryFile;
use ic_host_text::escape::terminal_text;
use ic_host_tools::{
    icp::{CallArguments, CallMode, call_command},
    response::{ResponseError, ResponseFormat, ResponseLimits, decode},
};

use ic_host_process::tool::{ExecutionFailure, ExecutionOperation, ToolError};

use crate::icp::process::{capture_error_detail, output_stderr, probe_output, update_output};

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
        let mut detail = format!(
            "failed to run icp canister call: {}",
            capture_error_detail(&error)
        );
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
    let mode = match kind {
        IcpCallKind::Query => CallMode::Query,
        IcpCallKind::Update => CallMode::Update,
    };
    let mut command = call_command(
        Command::new("icp"),
        OsStr::new(canister),
        OsStr::new(method),
        CallArguments::BinaryFile(argument_file),
        mode,
    );
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

    decode(
        hex.as_bytes(),
        ResponseFormat::Hex,
        ResponseLimits {
            input_bytes: hex.len(),
            decoded_bytes: hex.len() / 2,
        },
    )
    .map_err(|error| match error {
        ResponseError::EmptyHex => "icp canister call returned an empty hex response".to_string(),
        ResponseError::OddHexLength => {
            "icp canister call returned odd-length hex response".to_string()
        }
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
    let mut arguments =
        TemporaryFile::new_in(&std::env::temp_dir(), "icydb-icp-args").map_err(|source| {
            format!(
                "create ICP argument file: {}",
                terminal_text(&source.to_string())
            )
        })?;
    if let Err(source) = arguments.as_file_mut().write_all(wire_arg) {
        let path = arguments.path().to_owned();
        return Err(match arguments.close() {
            Ok(()) => format!(
                "write ICP argument file: {}",
                terminal_text(&source.to_string())
            ),
            Err(cleanup) => format!(
                "write ICP argument file: {}; remove '{}': {}",
                terminal_text(&source.to_string()),
                terminal_text(&path.display().to_string()),
                terminal_text(&cleanup.to_string()),
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
mod tests;

#[cfg(all(test, unix))]
mod mutation_tests;
