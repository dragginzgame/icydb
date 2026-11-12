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

use ic_host_fs::durable::{NamedWriteError, PublicationMode, WriteOptions, write_named_with};
use ic_host_process::tool::{
    AdmittedTool, CompletedExecution, ExecutionContext, OutputLimit, OutputLimits, VersionSpec,
    format_failure_evidence, resolve_executable,
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
    let pins = include_str!("../../../../scripts/ci/wasm-optimizer-checksums.tsv");
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
        &VersionSpec {
            executable: &executable,
            executable_bytes: u64::MAX,
            version_arguments: &[OsString::from("--version")],
        },
        wasm_opt_sha256()?
            .parse()
            .map_err(|error| format!("invalid pinned optimizer digest: {error}"))?,
        WASM_OPT_VERSION,
        &ExecutionContext {
            current_dir,
            environment: &environment,
        },
        OPTIMIZER_OUTPUT_LIMITS,
    )
    .map_err(|error| format_failure_evidence("pinned wasm optimizer admission", &error))
}

/// Run an admitted optimizer with the pipeline's inherited environment and bounds.
///
/// The shared handle checks executable identity before execution. Transform and
/// report callers supply their arguments and working directory; failure evidence
/// retains the operation context, captured streams and cleanup errors. Only a
/// successful exit permits callers to parse output or publish an artifact.
pub fn run_wasm_optimizer(
    optimizer: &AdmittedTool,
    arguments: &[OsString],
    current_dir: &Path,
    operation: &str,
) -> Result<CompletedExecution, String> {
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
        .and_then(CompletedExecution::require_success)
        .map_err(|error| format_failure_evidence(operation, &error))
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
    write_named_with(
        output,
        WriteOptions {
            mode: PublicationMode::Replace,
            permissions: 0o666,
        },
        |stage| {
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
        },
    )
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
