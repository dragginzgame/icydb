//! Module: ICP canister call construction.
//! Responsibility: build icp-cli canister call commands and decode hex responses.
//! Does not own: command execution, endpoint selection, or Candid decoding.
//! Boundary: exposes reusable call builders and response decoding to CLI command surfaces.

use std::process::{Command, Stdio};

use ic_host_tools::response::{ResponseError, ResponseFormat, ResponseLimits, decode};

use crate::icp::process::output_stderr;

pub(super) fn icp_query_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Command {
    icp_call_command(
        environment,
        canister,
        method,
        candid_arg,
        IcpCallKind::Query,
    )
}

pub(super) fn icp_update_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Command {
    icp_call_command(
        environment,
        canister,
        method,
        candid_arg,
        IcpCallKind::Update,
    )
}

enum IcpCallKind {
    Query,
    Update,
}

fn icp_call_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
    kind: IcpCallKind,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg(candid_arg);
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
            |byte| {
                format!(
                    "icp canister call returned non-hex byte '{}'",
                    char::from(*byte)
                )
            },
        ),
        other => format!("icp canister call response decoding failed: {other}"),
    })
}

pub(super) fn call_query_hex(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call_hex(
        icp_query_command(environment, canister, method, candid_arg),
        error_message,
    )
}

pub(super) fn call_update_hex(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call_hex(
        icp_update_command(environment, canister, method, candid_arg),
        error_message,
    )
}

fn call_hex(
    mut command: Command,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = output_stderr(output.stderr.as_slice());
        return Err(error_message(stderr.as_str()));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}
