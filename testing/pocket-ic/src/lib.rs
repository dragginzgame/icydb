//! Shared Pocket-IC test harness helpers.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const SQL_TEST_CANISTER_PACKAGE: &str = "canister_sql_test";
const WASM_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
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

fn sql_test_wasm_path(workspace_root: &Path, profile: &str) -> PathBuf {
    target_dir(workspace_root)
        .join(WASM_TARGET_TRIPLE)
        .join(profile)
        .join(format!("{SQL_TEST_CANISTER_PACKAGE}.wasm"))
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

fn should_default_to_release_profile() -> bool {
    matches!(
        env::var("DFX_NETWORK").ok().as_deref(),
        Some("mainnet" | "staging" | "ic")
    )
}

fn selected_sql_test_wasm_profile() -> Result<&'static str, String> {
    if let Some(explicit_profile) = env::var_os(SQL_TEST_WASM_PROFILE_ENV) {
        let normalized = explicit_profile.to_string_lossy().to_ascii_lowercase();
        return match normalized.as_str() {
            "debug" => Ok("debug"),
            "release" => Ok("release"),
            other => Err(format!(
                "invalid {SQL_TEST_WASM_PROFILE_ENV} value '{other}', expected 'debug' or 'release'"
            )),
        };
    }

    if should_default_to_release_profile() {
        Ok("release")
    } else {
        Ok("debug")
    }
}

///
/// build_sql_test_canister
///
/// Build the SQL test canister WASM and return the built wasm path.
///
/// Build profile selection:
/// - `release` when `DFX_NETWORK` is `mainnet`, `staging`, or `ic`
/// - `debug` otherwise
/// - overridden by `SQL_TEST_WASM_PROFILE=debug|release`
///

pub fn build_sql_test_canister() -> Result<PathBuf, String> {
    let root = workspace_root();
    let profile = selected_sql_test_wasm_profile()?;
    let mut cargo = Command::new("cargo");
    cargo.current_dir(&root).args([
        "build",
        "--target",
        WASM_TARGET_TRIPLE,
        "--package",
        SQL_TEST_CANISTER_PACKAGE,
    ]);
    if profile == "release" {
        cargo.arg("--release");
    }
    run_checked(cargo, &format!("sql test canister build ({profile})"))?;

    let wasm_path = sql_test_wasm_path(&root, profile);
    if !wasm_path.is_file() {
        return Err(format!(
            "sql test canister build succeeded but wasm was not found at {}",
            wasm_path.display()
        ));
    }

    Ok(wasm_path)
}

///
/// stage_sql_test_canister_for_dfx
///
/// Build the SQL test canister and stage `.wasm` + `.did` artifacts into
/// `.dfx/local/canisters/<canister_name>/`.
///

pub fn stage_sql_test_canister_for_dfx(canister_name: &str) -> Result<(PathBuf, PathBuf), String> {
    let root = workspace_root();
    let built_wasm_path = build_sql_test_canister()?;

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
