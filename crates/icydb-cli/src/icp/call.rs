//! Module: ICP canister call construction.
//! Responsibility: build icp-cli canister call commands and decode hex responses.
//! Does not own: command execution, endpoint selection, or Candid decoding.
//! Boundary: exposes reusable call builders and response decoding to CLI command surfaces.

use std::process::{Command, Stdio};

use super::process::output_stderr;

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

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }

    Ok(bytes)
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

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => Err(format!(
            "icp canister call returned non-hex byte '{}'",
            char::from(other)
        )),
    }
}
