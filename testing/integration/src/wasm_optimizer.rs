//! Canonical post-link optimizer for deployable fixture-canister Wasm.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

/// Environment variable that may point at the pinned `wasm-opt` executable.
pub const WASM_OPT_BIN_ENV: &str = "ICYDB_WASM_OPT_BIN";
/// Exact Binaryen CLI version accepted by the deployable-Wasm pipeline.
pub const WASM_OPT_VERSION: &str = "wasm-opt version 132 (version_132)";
/// Stable identity of the only deployable post-link pipeline.
pub const POST_LINK_PIPELINE_IDENTITY: &str =
    "binaryen-132-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1";
/// Exact ordered optimizer arguments after the compiler-emitted input path.
pub const WASM_OPT_FLAGS: [&str; 5] = [
    "-Oz",
    "--enable-bulk-memory",
    "--enable-sign-ext",
    "--enable-nontrapping-float-to-int",
    "--one-caller-inline-max-function-size=0",
];
/// Exact effective feature set reported for canonical Binaryen 132 output.
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

static TEMPORARY_OUTPUT_ORDINAL: AtomicU64 = AtomicU64::new(0);
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Resolve the admitted executable digest for the native host.
///
/// Installation, runtime verification and reports consume the same pin table.
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
            fields.nth(2)
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
pub fn pinned_wasm_optimizer() -> Result<PathBuf, String> {
    let requested =
        env::var_os(WASM_OPT_BIN_ENV).map_or_else(|| PathBuf::from("wasm-opt"), PathBuf::from);
    let executable = resolve_executable(&requested)?;
    // Verify the admitted bytes before executing the candidate.
    let observed_sha256 = sha256_hex(&executable)?;
    let expected_sha256 = wasm_opt_sha256()?;
    if observed_sha256 != expected_sha256 {
        return Err(format!(
            "unsupported wasm optimizer binary {} with SHA-256 {observed_sha256}, expected {expected_sha256}",
            executable.display()
        ));
    }

    let version = Command::new(&executable)
        .arg("--version")
        .output()
        .map_err(|error| {
            format!(
                "failed to execute pinned wasm optimizer {}: {error}",
                executable.display()
            )
        })?;
    if !version.status.success() {
        return Err(format_process_failure(
            "pinned wasm optimizer version check",
            &version,
        ));
    }
    let observed_version = String::from_utf8_lossy(&version.stdout).trim().to_owned();
    if observed_version != WASM_OPT_VERSION {
        return Err(format!(
            "unsupported wasm optimizer version '{observed_version}', expected '{WASM_OPT_VERSION}'"
        ));
    }

    Ok(executable)
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
    optimizer: &Path,
) -> Result<(), String> {
    if !input.is_file() {
        return Err(format!(
            "compiler-emitted wasm is missing: {}",
            input.display()
        ));
    }
    let output_parent = output.parent().ok_or_else(|| {
        format!(
            "final deployable wasm path has no parent: {}",
            output.display()
        )
    })?;
    fs::create_dir_all(output_parent).map_err(|error| {
        format!(
            "failed to create final deployable wasm directory {}: {error}",
            output_parent.display()
        )
    })?;

    let temporary = temporary_output_path(output);
    let mut command = Command::new(optimizer);
    command.arg(input);
    command.args(WASM_OPT_FLAGS);
    command.arg("-o").arg(&temporary);
    let result = command.output().map_err(|error| {
        format!(
            "failed to run canonical wasm optimizer for {}: {error}",
            input.display()
        )
    })?;
    if !result.status.success() {
        let _ = fs::remove_file(&temporary);
        return Err(format_process_failure(
            "canonical wasm optimization",
            &result,
        ));
    }
    if !temporary.is_file() {
        return Err(format!(
            "canonical wasm optimizer produced no output at {}",
            temporary.display()
        ));
    }

    fs::rename(&temporary, output).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        format!(
            "failed to publish final deployable wasm {}: {error}",
            output.display()
        )
    })
}

fn resolve_executable(requested: &Path) -> Result<PathBuf, String> {
    if requested.is_absolute() || requested.components().count() > 1 {
        return requested.canonicalize().map_err(|error| {
            format!(
                "failed to resolve wasm optimizer {}: {error}",
                requested.display()
            )
        });
    }

    let path = env::var_os("PATH").ok_or_else(|| {
        format!("PATH is unset and {WASM_OPT_BIN_ENV} does not name an absolute wasm optimizer")
    })?;
    env::split_paths(&path)
        .map(|directory| directory.join(requested))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
        .ok_or_else(|| {
            format!(
                "missing pinned wasm optimizer '{}'; run `bash scripts/ci/install-wasm-optimizer.sh` or set {WASM_OPT_BIN_ENV}",
                requested.display()
            )
        })
}

fn temporary_output_path(output: &Path) -> PathBuf {
    let ordinal = TEMPORARY_OUTPUT_ORDINAL.fetch_add(1, Ordering::Relaxed);
    let file_name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("canister.wasm");
    output.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        ordinal
    ))
}

fn sha256_hex(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read {} for SHA-256: {error}", path.display()))?;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn format_process_failure(context: &str, output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!(
        "{context} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout.trim_end(),
        stderr.trim_end()
    )
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, WASM_OPT_OUTPUT_FEATURES, WASM_OPT_VERSION,
        pinned_wasm_optimizer, wasm_opt_sha256,
    };

    #[test]
    fn post_link_optimizer_contract_is_exact_and_available() {
        assert_eq!(WASM_OPT_VERSION, "wasm-opt version 132 (version_132)");
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
            "binaryen-132-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1"
        );
        assert!(pinned_wasm_optimizer().is_ok());
    }
}
