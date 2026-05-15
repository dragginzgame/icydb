//! Shared canic-testkit-backed PocketIC integration harness helpers.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const DEMO_RPG_CANISTER_NAME: &str = "demo_rpg";
const DEMO_RPG_CANISTER_PACKAGE: &str = "canister_demo_rpg";
const TEST_SQL_CANISTER_NAME: &str = "sql";
const TEST_SQL_CANISTER_PACKAGE: &str = "canister_test_sql";
const MINIMAL_CANISTER_NAME: &str = "minimal";
const MINIMAL_CANISTER_PACKAGE: &str = "canister_audit_minimal";
const ONE_SIMPLE_CANISTER_NAME: &str = "one_simple";
const ONE_SIMPLE_CANISTER_PACKAGE: &str = "canister_audit_one_simple";
const SQL_PERF_CANISTER_NAME: &str = "sql_perf";
const SQL_PERF_CANISTER_PACKAGE: &str = "canister_audit_sql_perf";
const ONE_COMPLEX_CANISTER_NAME: &str = "one_complex";
const ONE_COMPLEX_CANISTER_PACKAGE: &str = "canister_audit_one_complex";
const TEN_SIMPLE_CANISTER_NAME: &str = "ten_simple";
const TEN_SIMPLE_CANISTER_PACKAGE: &str = "canister_audit_ten_simple";
const TEN_COMPLEX_CANISTER_NAME: &str = "ten_complex";
const TEN_COMPLEX_CANISTER_PACKAGE: &str = "canister_audit_ten_complex";
const WASM_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";

/// Cargo wasm profile used when building fixture canisters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterWasmProfile {
    /// Cargo's default debug profile.
    Debug,
    /// Cargo's standard release profile.
    Release,
    /// Workspace-defined wasm release profile.
    WasmRelease,
}

impl CanisterWasmProfile {
    /// Parse a user-facing profile name.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "debug" => Ok(Self::Debug),
            "release" => Ok(Self::Release),
            "wasm-release" => Ok(Self::WasmRelease),
            other => Err(format!(
                "invalid canister wasm profile '{other}', expected 'debug', 'release', or 'wasm-release'"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
            Self::WasmRelease => "wasm-release",
        }
    }
}

/// SQL feature mode for fixture canister builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterSqlMode {
    /// Build with the default SQL feature set.
    Enabled,
    /// Build without default SQL features.
    Disabled,
}

impl CanisterSqlMode {
    /// Parse a user-facing SQL mode.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "on" | "sql-on" | "enabled" => Ok(Self::Enabled),
            "off" | "sql-off" | "disabled" => Ok(Self::Disabled),
            other => Err(format!(
                "invalid canister SQL mode '{other}', expected 'on'/'sql-on' or 'off'/'sql-off'"
            )),
        }
    }

    const fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Explicit build options for fixture canisters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanisterBuildOptions {
    /// Cargo profile to use for the wasm build.
    pub profile: CanisterWasmProfile,
    /// Whether default SQL features stay enabled.
    pub sql_mode: CanisterSqlMode,
}

impl Default for CanisterBuildOptions {
    fn default() -> Self {
        Self {
            profile: CanisterWasmProfile::Debug,
            sql_mode: CanisterSqlMode::Enabled,
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live under testing/pocket-ic")
        .to_path_buf()
}

fn target_dir(workspace_root: &Path) -> PathBuf {
    env::var_os("CARGO_TARGET_DIR").map_or_else(|| workspace_root.join("target"), PathBuf::from)
}

fn canister_wasm_path(workspace_root: &Path, profile: &str, package_name: &str) -> PathBuf {
    target_dir(workspace_root)
        .join(WASM_TARGET_TRIPLE)
        .join(profile)
        .join(format!("{package_name}.wasm"))
}

fn package_for_canister_name(canister_name: &str) -> Result<&'static str, String> {
    match canister_name {
        DEMO_RPG_CANISTER_NAME => Ok(DEMO_RPG_CANISTER_PACKAGE),
        TEST_SQL_CANISTER_NAME => Ok(TEST_SQL_CANISTER_PACKAGE),
        MINIMAL_CANISTER_NAME => Ok(MINIMAL_CANISTER_PACKAGE),
        ONE_SIMPLE_CANISTER_NAME => Ok(ONE_SIMPLE_CANISTER_PACKAGE),
        SQL_PERF_CANISTER_NAME => Ok(SQL_PERF_CANISTER_PACKAGE),
        ONE_COMPLEX_CANISTER_NAME => Ok(ONE_COMPLEX_CANISTER_PACKAGE),
        TEN_SIMPLE_CANISTER_NAME => Ok(TEN_SIMPLE_CANISTER_PACKAGE),
        TEN_COMPLEX_CANISTER_NAME => Ok(TEN_COMPLEX_CANISTER_PACKAGE),
        _ => Err(format!(
            "unsupported canister '{canister_name}', expected '{DEMO_RPG_CANISTER_NAME}', '{TEST_SQL_CANISTER_NAME}', '{MINIMAL_CANISTER_NAME}', '{ONE_SIMPLE_CANISTER_NAME}', '{SQL_PERF_CANISTER_NAME}', '{ONE_COMPLEX_CANISTER_NAME}', '{TEN_SIMPLE_CANISTER_NAME}', or '{TEN_COMPLEX_CANISTER_NAME}'"
        )),
    }
}

fn run_checked(mut command: Command, context: &str) -> Result<(), String> {
    let output = command
        .output()
        .map_err(|err| format!("{context}: failed to spawn process: {err}"))?;
    if !output.status.success() {
        return Err(format_failed_process_output(context, &output));
    }

    Ok(())
}

// Format a failed child-process result with captured output. Successful
// Pocket-IC canister builds stay quiet, while cargo/rustc diagnostics are still
// visible when a nested build actually fails.
fn format_failed_process_output(context: &str, output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut message = format!("{context}: process exited with status {}", output.status);

    if !stdout.trim().is_empty() {
        message.push_str("\nstdout:\n");
        message.push_str(stdout.trim_end());
    }
    if !stderr.trim().is_empty() {
        message.push_str("\nstderr:\n");
        message.push_str(stderr.trim_end());
    }

    message
}

// Shorten retained source/build paths in release wasm artifacts without
// changing semantics. These remaps only affect diagnostic path payloads that
// would otherwise inflate the module data section.
fn wasm_release_path_trim_flags(root: &Path) -> Vec<String> {
    let mut flags = vec![format!("--remap-path-prefix={}=/w", root.display())];

    let cargo_home =
        env::var_os("CARGO_HOME").map_or_else(|| root.join(".cache/cargo/icydb"), PathBuf::from);
    let registry_src = cargo_home.join("registry").join("src");
    if let Ok(entries) = fs::read_dir(&registry_src) {
        for entry in entries.flatten() {
            let registry_root = entry.path();
            if registry_root.is_dir() {
                flags.push(format!(
                    "--remap-path-prefix={}=/c",
                    registry_root.display()
                ));
            }
        }
    }

    if let Ok(output) = Command::new("rustc").args(["--print", "sysroot"]).output()
        && output.status.success()
    {
        let sysroot = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !sysroot.is_empty() {
            let rust_library = PathBuf::from(sysroot)
                .join("lib")
                .join("rustlib")
                .join("src")
                .join("rust")
                .join("library");
            if rust_library.is_dir() {
                flags.push(format!("--remap-path-prefix={}=/r", rust_library.display()));
            }
        }
    }

    flags
}

// Preserve caller-provided rustflags and append any canister-specific flags to
// the same environment variable Cargo already understands.
fn append_rustflags(command: &mut Command, extra_flags: &[String]) {
    if extra_flags.is_empty() {
        return;
    }

    let mut combined = env::var("RUSTFLAGS").unwrap_or_default();
    for flag in extra_flags {
        if !combined.is_empty() {
            combined.push(' ');
        }
        combined.push_str(flag);
    }

    command.env("RUSTFLAGS", combined);
}

fn build_canister_package(
    package_name: &str,
    options: CanisterBuildOptions,
    context_label: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root();
    let mut cargo = Command::new("cargo");
    let profile = options.profile.as_str();

    // Phase 1: configure the wasm cargo build request.
    cargo.current_dir(&root).args([
        "build",
        "--target",
        WASM_TARGET_TRIPLE,
        "--package",
        package_name,
    ]);
    if !options.sql_mode.enabled() {
        cargo.arg("--no-default-features");
    }
    if profile == "release" {
        cargo.arg("--release");
    } else if profile != "debug" {
        cargo.args(["--profile", profile]);
    }
    if profile == "wasm-release" {
        append_rustflags(&mut cargo, &wasm_release_path_trim_flags(&root));
    }

    // Phase 2: run the build and fail loudly if cargo does not succeed.
    run_checked(cargo, context_label)?;

    // Phase 3: resolve the built wasm from the configured target directory.
    let wasm_path = canister_wasm_path(&root, profile, package_name);
    if !wasm_path.is_file() {
        return Err(format!(
            "{context_label}: build succeeded but wasm was not found at {}",
            wasm_path.display()
        ));
    }

    Ok(wasm_path)
}

///
/// build_canister
///
/// Build one supported SQL canister WASM with default debug options and return
/// the built wasm path.
pub fn build_canister(canister_name: &str) -> Result<PathBuf, String> {
    build_canister_with_options(canister_name, CanisterBuildOptions::default())
}

/// Build one supported SQL canister WASM with explicit options and return the
/// built wasm path.
pub fn build_canister_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Result<PathBuf, String> {
    let package_name = package_for_canister_name(canister_name)?;
    build_canister_package(
        package_name,
        options,
        &format!(
            "{canister_name} canister build ({})",
            options.profile.as_str()
        ),
    )
}

///
/// stage_canister_for_icp
///
/// Build one supported canister and stage `.wasm` + `.did` artifacts into
/// `.icp/local/canisters/<canister_name>/`.
///

pub fn stage_canister_for_icp(canister_name: &str) -> Result<(PathBuf, Option<PathBuf>), String> {
    stage_canister_for_icp_with_options(canister_name, CanisterBuildOptions::default())
}

/// Build one supported canister with explicit options and stage `.wasm` +
/// `.did` artifacts into `.icp/local/canisters/<canister_name>/`.
pub fn stage_canister_for_icp_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Result<(PathBuf, Option<PathBuf>), String> {
    let root = workspace_root();
    let package_name = package_for_canister_name(canister_name)?;
    let built_wasm_path = build_canister_package(
        package_name,
        options,
        &format!(
            "canister build for ICP staging ({canister_name}, {})",
            options.profile.as_str()
        ),
    )?;

    let icp_canister_dir = root.join(".icp/local/canisters").join(canister_name);
    fs::create_dir_all(&icp_canister_dir).map_err(|err| {
        format!(
            "failed to create ICP canister output directory {}: {err}",
            icp_canister_dir.display()
        )
    })?;

    let staged_wasm_path = icp_canister_dir.join(format!("{canister_name}.wasm"));
    fs::copy(&built_wasm_path, &staged_wasm_path).map_err(|err| {
        format!(
            "failed to copy built wasm from {} to {}: {err}",
            built_wasm_path.display(),
            staged_wasm_path.display()
        )
    })?;

    let staged_did_path = icp_canister_dir.join(format!("{canister_name}.did"));

    let candid_output = Command::new("candid-extractor")
        .arg(&staged_wasm_path)
        .output()
        .map_err(|err| {
            format!(
                "failed to invoke candid-extractor on {}: {err}",
                staged_wasm_path.display()
            )
        })?;
    // Release wasm-size builds now intentionally allow canisters to omit the
    // `export_candid!()` entrypoint. In that case, keep staging the wasm and
    // report DID export as unavailable instead of failing the whole size pass.
    if !candid_output.status.success() {
        let stderr = String::from_utf8_lossy(&candid_output.stderr);
        if stderr.contains("get_candid_pointer") {
            // Remove any previously staged DID so release size reports do not
            // accidentally reuse stale export output from an earlier debug build.
            if staged_did_path.exists() {
                fs::remove_file(&staged_did_path).map_err(|err| {
                    format!(
                        "failed to remove stale staged did {}: {err}",
                        staged_did_path.display()
                    )
                })?;
            }

            return Ok((staged_wasm_path, None));
        }

        return Err(format!(
            "candid-extractor failed for {}: {stderr}",
            staged_wasm_path.display()
        ));
    }

    fs::write(&staged_did_path, &candid_output.stdout).map_err(|err| {
        format!(
            "failed to write candid output to {}: {err}",
            staged_did_path.display()
        )
    })?;

    Ok((staged_wasm_path, Some(staged_did_path)))
}
