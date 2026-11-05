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
