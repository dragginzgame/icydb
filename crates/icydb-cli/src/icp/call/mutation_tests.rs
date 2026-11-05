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
                    execution.evidence.stdout.len() <= UPDATE_OUTPUT_LIMITS.stdout.retained_bytes()
                );
                assert!(
                    execution.evidence.stderr.len() <= UPDATE_OUTPUT_LIMITS.stderr.retained_bytes()
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
                        &format!("command -p sleep 30 & command -p cat \"$1\" > \"$2\"; {ending}"),
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
