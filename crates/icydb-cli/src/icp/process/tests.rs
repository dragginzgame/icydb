use super::*;

#[test]
fn capture_diagnostics_include_the_original_io_cause_without_changing_its_type() {
    use ic_host_process::tool::{ExecutionError, ExecutionEvidence, ExecutionOperation};

    // This opaque payload tests presentation, not OS-message classification.
    let diagnostic = "opaque-π\x1b[2J\r\n\tmarker";
    let error = captured_output(Err(ToolError::Execution(Box::new(ExecutionError {
        failure: ExecutionFailure::Io {
            operation: ExecutionOperation::Spawn,
            source: io::Error::new(io::ErrorKind::PermissionDenied, diagnostic),
        },
        evidence: ExecutionEvidence {
            status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        },
        cleanup: None,
    }))))
    .unwrap_err();
    let rendered = capture_error_detail(&error);
    assert!(rendered.contains("opaque-π"));
    assert!(rendered.contains("marker"));
    assert!(!rendered.chars().any(char::is_control));
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let cause = error
        .get_ref()
        .unwrap()
        .downcast_ref::<ToolError>()
        .unwrap();
    assert!(matches!(
        &cause.execution_error().unwrap().failure,
        ExecutionFailure::Io { operation: ExecutionOperation::Spawn, source }
            if source.kind() == io::ErrorKind::PermissionDenied
    ));
}

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
