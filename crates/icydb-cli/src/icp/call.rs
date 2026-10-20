//! Module: ICP canister call construction.
//! Responsibility: build icp-cli canister call commands and decode hex responses.
//! Does not own: command execution, endpoint selection, or Wire decoding.
//! Boundary: exposes reusable call builders and response decoding to CLI command surfaces.

use std::{
    io::Write as _,
    path::Path,
    process::{Command, Stdio},
};

use ic_host_fs::scratch::TemporaryFile;
use ic_host_tools::response::{ResponseError, ResponseFormat, ResponseLimits, decode};

use crate::icp::process::output_stderr;

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

enum IcpCallKind {
    Query,
    Update,
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
    wire_arg: &[u8],
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    call_hex(
        wire_arg,
        |path| icp_query_command(environment, canister, method, path),
        error_message,
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
    )
}

pub(super) fn call_hex(
    wire_arg: &[u8],
    build_command: impl FnOnce(&Path) -> Command,
    error_message: impl FnOnce(&str) -> String,
) -> Result<Vec<u8>, String> {
    // The private file stays alive through child completion and is removed on
    // every return. Passing a path avoids OS argv limits and binary NUL bytes.
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
    let output = build_command(arguments.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| format!("failed to run icp canister call: {err}"))?;

    if !output.status.success() {
        let stderr = output_stderr(output.stderr.as_slice());
        return Err(error_message(stderr.as_str()));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
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

    use super::call_hex;

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
            );
        }));
        assert!(panic.is_err());
        assert!(!argument_path.exists());
    }
}
