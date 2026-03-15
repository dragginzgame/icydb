//! Shared Pocket-IC test harness helpers.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const SQL_TEST_CANISTER_NAME: &str = "sql_test";
const SQL_TEST_CANISTER_PACKAGE: &str = "canister_sql_test";
const MINIMAL_CANISTER_NAME: &str = "minimal";
const MINIMAL_CANISTER_PACKAGE: &str = "canister_minimal";
const WASM_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
const CANISTER_WASM_PROFILE_ENV: &str = "ICYDB_CANISTER_WASM_PROFILE";
const SQL_TEST_WASM_PROFILE_ENV: &str = "SQL_TEST_WASM_PROFILE";

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
        SQL_TEST_CANISTER_NAME => Ok(SQL_TEST_CANISTER_PACKAGE),
        MINIMAL_CANISTER_NAME => Ok(MINIMAL_CANISTER_PACKAGE),
        _ => Err(format!(
            "unsupported canister '{canister_name}', expected '{SQL_TEST_CANISTER_NAME}' or '{MINIMAL_CANISTER_NAME}'"
        )),
    }
}

fn run_checked(mut command: Command, context: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|err| format!("{context}: failed to spawn process: {err}"))?;
    if !status.success() {
        return Err(format!("{context}: process exited with status {status}"));
    }

    Ok(())
}

fn should_default_to_wasm_release_profile() -> bool {
    matches!(
        env::var("DFX_NETWORK").ok().as_deref(),
        Some("mainnet" | "staging" | "ic")
    )
}

fn selected_canister_wasm_profile() -> Result<&'static str, String> {
    let explicit_profile =
        env::var_os(CANISTER_WASM_PROFILE_ENV).or_else(|| env::var_os(SQL_TEST_WASM_PROFILE_ENV));
    if let Some(explicit_profile) = explicit_profile {
        let normalized = explicit_profile.to_string_lossy().to_ascii_lowercase();
        return match normalized.as_str() {
            "debug" => Ok("debug"),
            "release" => Ok("release"),
            "wasm-release" => Ok("wasm-release"),
            other => Err(format!(
                "invalid {CANISTER_WASM_PROFILE_ENV}/{SQL_TEST_WASM_PROFILE_ENV} value '{other}', expected 'debug', 'release', or 'wasm-release'"
            )),
        };
    }

    if should_default_to_wasm_release_profile() {
        Ok("wasm-release")
    } else {
        Ok("debug")
    }
}

fn build_canister_package(
    package_name: &str,
    profile: &str,
    context_label: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root();
    let mut cargo = Command::new("cargo");
    cargo.current_dir(&root).args([
        "build",
        "--target",
        WASM_TARGET_TRIPLE,
        "--package",
        package_name,
    ]);
    if profile == "release" {
        cargo.arg("--release");
    } else if profile != "debug" {
        cargo.args(["--profile", profile]);
    }
    run_checked(cargo, context_label)?;

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
/// build_sql_test_canister
///
/// Build the SQL test canister WASM and return the built wasm path.
///
/// Build profile selection:
/// - `wasm-release` when `DFX_NETWORK` is `mainnet`, `staging`, or `ic`
/// - `debug` otherwise
/// - overridden by `SQL_TEST_WASM_PROFILE=debug|release|wasm-release`
///

pub fn build_sql_test_canister() -> Result<PathBuf, String> {
    let profile = selected_canister_wasm_profile()?;
    build_canister_package(
        SQL_TEST_CANISTER_PACKAGE,
        profile,
        &format!("sql test canister build ({profile})"),
    )
}

///
/// build_minimal_canister
///
/// Build the minimal SQL canister WASM and return the built wasm path.
///

pub fn build_minimal_canister() -> Result<PathBuf, String> {
    let profile = selected_canister_wasm_profile()?;
    build_canister_package(
        MINIMAL_CANISTER_PACKAGE,
        profile,
        &format!("minimal canister build ({profile})"),
    )
}

///
/// stage_sql_test_canister_for_dfx
///
/// Build one supported canister and stage `.wasm` + `.did` artifacts into
/// `.dfx/local/canisters/<canister_name>/`.
///

pub fn stage_canister_for_dfx(canister_name: &str) -> Result<(PathBuf, PathBuf), String> {
    let root = workspace_root();
    let package_name = package_for_canister_name(canister_name)?;
    let profile = selected_canister_wasm_profile()?;
    let built_wasm_path = build_canister_package(
        package_name,
        profile,
        &format!("canister build for dfx staging ({canister_name}, {profile})"),
    )?;

    let dfx_canister_dir = root.join(".dfx/local/canisters").join(canister_name);
    fs::create_dir_all(&dfx_canister_dir).map_err(|err| {
        format!(
            "failed to create dfx canister output directory {}: {err}",
            dfx_canister_dir.display()
        )
    })?;

    let staged_wasm_path = dfx_canister_dir.join(format!("{canister_name}.wasm"));
    fs::copy(&built_wasm_path, &staged_wasm_path).map_err(|err| {
        format!(
            "failed to copy built wasm from {} to {}: {err}",
            built_wasm_path.display(),
            staged_wasm_path.display()
        )
    })?;

    let candid_output = Command::new("candid-extractor")
        .arg(&staged_wasm_path)
        .output()
        .map_err(|err| {
            format!(
                "failed to invoke candid-extractor on {}: {err}",
                staged_wasm_path.display()
            )
        })?;
    if !candid_output.status.success() {
        let stderr = String::from_utf8_lossy(&candid_output.stderr);
        return Err(format!(
            "candid-extractor failed for {}: {stderr}",
            staged_wasm_path.display()
        ));
    }

    let staged_did_path = dfx_canister_dir.join(format!("{canister_name}.did"));
    fs::write(&staged_did_path, &candid_output.stdout).map_err(|err| {
        format!(
            "failed to write candid output to {}: {err}",
            staged_did_path.display()
        )
    })?;

    Ok((staged_wasm_path, staged_did_path))
}

///
/// stage_sql_test_canister_for_dfx
///
/// Backward-compatible alias for staging supported test canisters.
///

pub fn stage_sql_test_canister_for_dfx(canister_name: &str) -> Result<(PathBuf, PathBuf), String> {
    stage_canister_for_dfx(canister_name)
}
