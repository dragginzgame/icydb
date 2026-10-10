//! Canonical post-link optimizer for deployable fixture-canister Wasm.

use std::{
    env,
    ffi::OsString,
    fmt::Write as _,
    fs,
    io::Read as _,
    path::{Path, PathBuf},
    time::Duration,
};

use ic_host_fs::durable::{NamedWriteError, write_named_with};
use ic_host_process::tool::{
    AdmittedTool, ExecutionContext, ExecutionEvidence, OutputLimit, OutputLimits, ToolError,
    ToolSpec, resolve_executable,
};

/// Environment variable that may point at the pinned `wasm-opt` executable.
pub const WASM_OPT_BIN_ENV: &str = "ICYDB_WASM_OPT_BIN";
/// Exact Binaryen CLI version accepted by the deployable-Wasm pipeline.
pub const WASM_OPT_VERSION: &str = "wasm-opt version 133 (version_133)";
/// Stable identity of the only deployable post-link pipeline.
pub const POST_LINK_PIPELINE_IDENTITY: &str =
    "binaryen-133-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1";
/// Exact ordered optimizer arguments after the compiler-emitted input path.
pub const WASM_OPT_FLAGS: [&str; 5] = [
    "-Oz",
    "--enable-bulk-memory",
    "--enable-sign-ext",
    "--enable-nontrapping-float-to-int",
    "--one-caller-inline-max-function-size=0",
];
/// Exact effective feature set reported for canonical Binaryen 133 output.
///
/// Binaryen reports the explicit proposal flags together with features it
/// detects in the input module. `bulk-memory-opt` covers the emitted
/// `memory.copy`/`memory.fill` operations; `mutable-globals` covers the
/// module's mutable global.
pub const WASM_OPT_OUTPUT_FEATURES: [&str; 5] = [
    "--enable-bulk-memory",
    "--enable-bulk-memory-opt",
    "--enable-mutable-globals",
    "--enable-nontrapping-float-to-int",
    "--enable-sign-ext",
];

// Binaryen diagnostics are captured, never the Wasm payload. Fixed resource
// ceilings prevent unbounded output or a stuck child; this is not a performance gate.
const OPTIMIZER_OUTPUT_LIMITS: OutputLimits = OutputLimits {
    stdout: OutputLimit::Terminate(1024 * 1024),
    stderr: OutputLimit::Terminate(1024 * 1024),
    timeout: Some(Duration::from_secs(600)),
};

/// Resolve the admitted executable digest for the native host.
///
/// Runtime verification and reports consume the raw executable admission table;
/// the shared installer separately owns archive selection and verification.
pub fn wasm_opt_sha256() -> Result<&'static str, String> {
    let platform = match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") => "linux_x86_64",
        ("macos", "x86_64") => "darwin_x86_64",
        ("macos", "aarch64") => "darwin_arm64",
        (os, arch) => return Err(format!("unsupported Binaryen platform: {os} {arch}")),
    };
    let pins = include_str!("../../../scripts/ci/wasm-optimizer-checksums.tsv");
    pins.lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            if fields.next()? != platform {
                return None;
            }
            fields.next()
        })
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        .ok_or_else(|| format!("missing or invalid Binaryen digest for {platform}"))
}

/// Resolve and validate the exact optimizer used by the deployable-Wasm pipeline.
pub fn pinned_wasm_optimizer() -> Result<AdmittedTool, String> {
    let requested = env::var_os(WASM_OPT_BIN_ENV).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tools/ic/bin/wasm-opt"),
        PathBuf::from,
    );
    let current_dir = env::current_dir()
        .map_err(|error| format!("failed to resolve optimizer working directory: {error}"))?;
    admit_wasm_optimizer(&requested, &current_dir)
}

/// Admit a caller-selected optimizer under the sole deployable pipeline's pins.
///
/// Relative paths and bare names resolve from `current_dir` and the inherited
/// PATH. The returned handle carries the checked version/digest into later runs.
pub fn admit_wasm_optimizer(requested: &Path, current_dir: &Path) -> Result<AdmittedTool, String> {
    // The consumer supplies PATH only for an explicit bare-name override.
    let search_directories = env::var_os("PATH")
        .map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    let executable =
        resolve_executable(requested, current_dir, &search_directories).map_err(|error| {
            format!("failed to resolve wasm optimizer: {error}; run make install-ic-tools")
        })?;
    // Preserve the caller's inherited execution environment explicitly. The
    // admitted handle carries pin authority into every batch transform.
    let environment = env::vars_os().collect::<Vec<_>>();
    AdmittedTool::admit(
        &ToolSpec {
            executable: &executable,
            sha256: wasm_opt_sha256()?
                .parse()
                .map_err(|error| format!("invalid pinned optimizer digest: {error}"))?,
            executable_bytes: u64::MAX,
            version_arguments: &[OsString::from("--version")],
            version_identity: WASM_OPT_VERSION,
        },
        &ExecutionContext {
            current_dir,
            environment: &environment,
        },
        OPTIMIZER_OUTPUT_LIMITS,
    )
    .map_err(|error| format_tool_failure("pinned wasm optimizer admission", &error))
}

/// Run an admitted optimizer with the pipeline's inherited environment and bounds.
///
/// The shared handle checks executable identity before execution. Transform and
/// report callers supply their arguments and working directory; failure evidence
/// retains the operation context, captured streams and cleanup errors.
pub fn run_wasm_optimizer(
    optimizer: &AdmittedTool,
    arguments: &[OsString],
    current_dir: &Path,
    operation: &str,
) -> Result<ExecutionEvidence, String> {
    let environment = env::vars_os().collect::<Vec<_>>();
    optimizer
        .run(
            arguments,
            &ExecutionContext {
                current_dir,
                environment: &environment,
            },
            OPTIMIZER_OUTPUT_LIMITS,
        )
        .map_err(|error| format_tool_failure(operation, &error))
}

/// Transform compiler-emitted Wasm into the sole final deployable artifact.
pub fn optimize_deployable_wasm(input: &Path, output: &Path) -> Result<(), String> {
    let optimizer = pinned_wasm_optimizer()?;
    optimize_deployable_wasm_with_optimizer(input, output, &optimizer)
}

/// Run the canonical transform with an optimizer already validated by the batch owner.
pub(crate) fn optimize_deployable_wasm_with_optimizer(
    input: &Path,
    output: &Path,
    optimizer: &AdmittedTool,
) -> Result<(), String> {
    if !input.is_file() {
        return Err(format!(
            "compiler-emitted wasm is missing: {}",
            input.display()
        ));
    }
    let current_dir = env::current_dir()
        .map_err(|error| format!("failed to resolve optimizer working directory: {error}"))?;
    write_named_with(output, |stage| {
        let arguments = std::iter::once(input.as_os_str().to_owned())
            .chain(WASM_OPT_FLAGS.map(OsString::from))
            .chain([OsString::from("-o"), stage.as_os_str().to_owned()])
            .collect::<Vec<_>>();
        run_wasm_optimizer(
            optimizer,
            &arguments,
            &current_dir,
            "canonical wasm optimization",
        )?;
        // Staging is precreated by the shared owner. Check bounded Wasm framing
        // before admitting publication; existence alone proves no producer work.
        let mut header = [0; 8];
        fs::File::open(stage)
            .and_then(|mut file| file.read_exact(&mut header))
            .map_err(|error| format!("read optimized Wasm header: {error}"))?;
        if header != *b"\0asm\x01\0\0\0" {
            return Err("canonical wasm optimizer produced an invalid Wasm header".to_string());
        }
        Ok(())
    })
    .map_err(|error| match error {
        NamedWriteError::Producer {
            source,
            cleanup_error,
        } => {
            let mut message = source;
            if let Some(cleanup) = cleanup_error {
                let _ = write!(message, "\nstaging cleanup: {cleanup}");
            }
            message
        }
        NamedWriteError::BeforePublication {
            source,
            cleanup_error,
        } => {
            let mut message = format!(
                "failed before publishing deployable wasm {}: {source}",
                output.display()
            );
            if let Some(cleanup) = cleanup_error {
                let _ = write!(message, "\nstaging cleanup: {cleanup}");
            }
            message
        }
        NamedWriteError::AfterPublication { source } => format!(
            "deployable wasm {} is published but directory sync failed: {source}",
            output.display()
        ),
    })
}

/// Format bounded process output and cleanup evidence with IcyDB's context.
///
/// Shared error display omits captured bytes; callers use this projection when
/// their diagnostic contract needs the original status, streams and cleanup errors.
#[must_use]
pub fn format_tool_failure(context: &str, error: &ToolError) -> String {
    let mut message = format!("{context}: {error}");
    if let Some(evidence) = error.evidence() {
        let _ = write!(
            message,
            "\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            evidence.status,
            String::from_utf8_lossy(&evidence.stdout).trim_end(),
            String::from_utf8_lossy(&evidence.stderr).trim_end(),
        );
    }
    if let Some(cleanup) = error
        .execution_error()
        .and_then(|failure| failure.cleanup.as_ref())
    {
        let _ = write!(message, "\ncleanup: {cleanup}");
    }
    message
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use ic_host_process::{
        child::CleanupError,
        tool::{ExecutionError, ExecutionEvidence, ExecutionFailure, ToolError},
    };
    use std::io;

    use super::{
        POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, WASM_OPT_OUTPUT_FEATURES, WASM_OPT_VERSION,
        format_tool_failure, optimize_deployable_wasm_with_optimizer, pinned_wasm_optimizer,
        wasm_opt_sha256,
    };

    #[test]
    fn failure_projection_retains_each_cleanup_error_and_original_evidence() {
        for field in 0..4 {
            let mut failure = ExecutionError {
                failure: ExecutionFailure::Cancelled,
                evidence: ExecutionEvidence {
                    status: None,
                    stdout: b"retained stdout\n".to_vec(),
                    stderr: b"retained stderr\n".to_vec(),
                    ..ExecutionEvidence::default()
                },
                cleanup: None,
            };
            let mut cleanup = CleanupError {
                status: None,
                term_error: None,
                group_error: None,
                kill_error: None,
                wait_error: None,
            };
            let cause = io::Error::from(io::ErrorKind::PermissionDenied);
            match field {
                0 => cleanup.term_error = Some(cause),
                1 => cleanup.group_error = Some(cause),
                2 => cleanup.kill_error = Some(cause),
                _ => cleanup.wait_error = Some(cause),
            }
            failure.cleanup = Some(Box::new(cleanup));
            // Check the diagnostic projection, deriving error display from its
            // typed owner rather than freezing platform-dependent error prose.
            let expected = format!(
                "fixture: {failure}\nstatus: {:?}\nstdout:\nretained stdout\nstderr:\nretained stderr\ncleanup: {}",
                failure.evidence.status,
                failure.cleanup.as_ref().unwrap(),
            );
            assert_eq!(
                format_tool_failure("fixture", &ToolError::Execution(Box::new(failure))),
                expected,
            );
        }
    }

    #[test]
    fn post_link_optimizer_contract_is_exact_and_available() {
        assert_eq!(WASM_OPT_VERSION, "wasm-opt version 133 (version_133)");
        assert_eq!(wasm_opt_sha256().unwrap().len(), 64);
        assert_eq!(
            WASM_OPT_FLAGS,
            [
                "-Oz",
                "--enable-bulk-memory",
                "--enable-sign-ext",
                "--enable-nontrapping-float-to-int",
                "--one-caller-inline-max-function-size=0",
            ]
        );
        assert_eq!(
            WASM_OPT_OUTPUT_FEATURES,
            [
                "--enable-bulk-memory",
                "--enable-bulk-memory-opt",
                "--enable-mutable-globals",
                "--enable-nontrapping-float-to-int",
                "--enable-sign-ext",
            ]
        );
        assert_eq!(
            POST_LINK_PIPELINE_IDENTITY,
            "binaryen-133-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1"
        );
        let optimizer = pinned_wasm_optimizer().expect("pinned optimizer should admit");
        assert_eq!(
            optimizer.identity().sha256.to_string(),
            wasm_opt_sha256().unwrap()
        );
        assert_eq!(optimizer.version_identity(), WASM_OPT_VERSION);
    }

    #[test]
    fn admitted_optimizer_preserves_outputs_on_failure_and_handles_spaced_paths() {
        let root =
            std::env::temp_dir().join(format!("icydb optimizer host {}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let input = root.join("compiler input.wasm");
        let output = root.join("final output.wasm");
        let optimizer = pinned_wasm_optimizer().unwrap();
        let wasm = b"\0asm\x01\0\0\0";
        std::fs::write(&input, wasm).unwrap();
        optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), wasm);
        std::fs::write(&input, b"invalid wasm").unwrap();
        assert!(optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), wasm);
        assert_eq!(std::fs::read(&input).unwrap(), b"invalid wasm");
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn successful_producer_must_write_a_wasm_header_before_publication() {
        use ic_host_process::tool::{AdmittedTool, ExecutionContext, VersionSpec};
        use std::{ffi::OsString, os::unix::fs::PermissionsExt};

        let root =
            std::env::temp_dir().join(format!("icydb empty optimizer {}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let input = root.join("input.wasm");
        let output = root.join("output.wasm");
        let previous = b"\0asm\x01\0\0\0";
        std::fs::write(&input, previous).unwrap();
        std::fs::write(&output, previous).unwrap();
        for (case, producer) in [
            ":",
            "printf 'short' > \"$output\"",
            "printf 'not-wasm' > \"$output\"",
        ]
        .into_iter()
        .enumerate()
        {
            // Keep each executed inode immutable across producer cases.
            let executable = root.join(format!("producer-{case}"));
            std::fs::write(&executable, format!(
                "#!/bin/sh\nif [ \"$1\" = --version ]; then echo fixture; exit 0; fi\nwhile [ \"$1\" != -o ]; do shift; done\noutput=$2\n[ -f \"$output\" ] && [ ! -s \"$output\" ] || exit 9\n{producer}\n"
            )).unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
            let executable = std::fs::canonicalize(executable).unwrap();
            let current_dir = std::env::current_dir().unwrap();
            let optimizer = AdmittedTool::admit_version(
                &VersionSpec {
                    executable: &executable,
                    executable_bytes: u64::MAX,
                    version_arguments: &[OsString::from("--version")],
                    version_identity: "fixture",
                },
                &ExecutionContext {
                    current_dir: &current_dir,
                    environment: &[],
                },
                super::OPTIMIZER_OUTPUT_LIMITS,
            )
            .unwrap();
            assert!(optimize_deployable_wasm_with_optimizer(&input, &output, &optimizer).is_err());
            assert_eq!(std::fs::read(&output).unwrap(), previous);
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 3);
            std::fs::remove_file(&executable).unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
