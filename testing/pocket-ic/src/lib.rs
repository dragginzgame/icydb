//! Shared canic-testkit-backed PocketIC integration harness helpers.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const DEMO_RPG_CANISTER_NAME: &str = "demo_rpg";
const DEMO_RPG_CANISTER_PACKAGE: &str = "canister_demo_rpg";
const SQL_PARITY_CANISTER_NAME: &str = "sql_parity";
const SQL_PARITY_CANISTER_PACKAGE: &str = "canister_test_sql_parity";
const MINIMAL_CANISTER_NAME: &str = "minimal";
const MINIMAL_CANISTER_PACKAGE: &str = "canister_audit_minimal";
const ONE_SIMPLE_CANISTER_NAME: &str = "one_simple";
const ONE_SIMPLE_CANISTER_PACKAGE: &str = "canister_audit_one_simple";
const ONE_COMPLEX_CANISTER_NAME: &str = "one_complex";
const ONE_COMPLEX_CANISTER_PACKAGE: &str = "canister_audit_one_complex";
const TEN_SIMPLE_CANISTER_NAME: &str = "ten_simple";
const TEN_SIMPLE_CANISTER_PACKAGE: &str = "canister_audit_ten_simple";
const TEN_COMPLEX_CANISTER_NAME: &str = "ten_complex";
const TEN_COMPLEX_CANISTER_PACKAGE: &str = "canister_audit_ten_complex";
const WASM_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
const CANISTER_WASM_PROFILE_ENV: &str = "ICYDB_CANISTER_WASM_PROFILE";
const DEMO_RPG_WASM_PROFILE_ENV: &str = "DEMO_RPG_WASM_PROFILE";
const CANISTER_SQL_MODE_ENV: &str = "ICYDB_CANISTER_SQL_MODE";
const CANISTER_PERF_ATTRIBUTION_ENV: &str = "ICYDB_CANISTER_PERF_ATTRIBUTION";

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
        SQL_PARITY_CANISTER_NAME => Ok(SQL_PARITY_CANISTER_PACKAGE),
        MINIMAL_CANISTER_NAME => Ok(MINIMAL_CANISTER_PACKAGE),
        ONE_SIMPLE_CANISTER_NAME => Ok(ONE_SIMPLE_CANISTER_PACKAGE),
        ONE_COMPLEX_CANISTER_NAME => Ok(ONE_COMPLEX_CANISTER_PACKAGE),
        TEN_SIMPLE_CANISTER_NAME => Ok(TEN_SIMPLE_CANISTER_PACKAGE),
        TEN_COMPLEX_CANISTER_NAME => Ok(TEN_COMPLEX_CANISTER_PACKAGE),
        _ => Err(format!(
            "unsupported canister '{canister_name}', expected '{DEMO_RPG_CANISTER_NAME}', '{SQL_PARITY_CANISTER_NAME}', '{MINIMAL_CANISTER_NAME}', '{ONE_SIMPLE_CANISTER_NAME}', '{ONE_COMPLEX_CANISTER_NAME}', '{TEN_SIMPLE_CANISTER_NAME}', or '{TEN_COMPLEX_CANISTER_NAME}'"
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
        env::var_os(CANISTER_WASM_PROFILE_ENV).or_else(|| env::var_os(DEMO_RPG_WASM_PROFILE_ENV));
    if let Some(explicit_profile) = explicit_profile {
        let normalized = explicit_profile.to_string_lossy().to_ascii_lowercase();
        return match normalized.as_str() {
            "debug" => Ok("debug"),
            "release" => Ok("release"),
            "wasm-release" => Ok("wasm-release"),
            other => Err(format!(
                "invalid {CANISTER_WASM_PROFILE_ENV}/{DEMO_RPG_WASM_PROFILE_ENV} value '{other}', expected 'debug', 'release', or 'wasm-release'"
            )),
        };
    }

    if should_default_to_wasm_release_profile() {
        Ok("wasm-release")
    } else {
        Ok("debug")
    }
}

fn selected_canister_sql_enabled() -> Result<bool, String> {
    let Some(explicit_mode) = env::var_os(CANISTER_SQL_MODE_ENV) else {
        return Ok(true);
    };

    let normalized = explicit_mode.to_string_lossy().to_ascii_lowercase();
    match normalized.as_str() {
        "on" | "sql-on" | "enabled" => Ok(true),
        "off" | "sql-off" | "disabled" => Ok(false),
        other => Err(format!(
            "invalid {CANISTER_SQL_MODE_ENV} value '{other}', expected 'on'/'sql-on' or 'off'/'sql-off'"
        )),
    }
}

fn selected_canister_perf_attribution_enabled() -> Result<bool, String> {
    let Some(explicit_mode) = env::var_os(CANISTER_PERF_ATTRIBUTION_ENV) else {
        return Ok(false);
    };

    let normalized = explicit_mode.to_string_lossy().to_ascii_lowercase();
    match normalized.as_str() {
        "on" | "enabled" | "true" => Ok(true),
        "off" | "disabled" | "false" => Ok(false),
        other => Err(format!(
            "invalid {CANISTER_PERF_ATTRIBUTION_ENV} value '{other}', expected 'on'/'enabled'/'true' or 'off'/'disabled'/'false'"
        )),
    }
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
    profile: &str,
    context_label: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root();
    let sql_enabled = selected_canister_sql_enabled()?;
    let perf_attribution_enabled = selected_canister_perf_attribution_enabled()?;
    let mut cargo = Command::new("cargo");

    // Phase 1: configure the wasm cargo build request.
    cargo.current_dir(&root).args([
        "build",
        "--target",
        WASM_TARGET_TRIPLE,
        "--package",
        package_name,
    ]);
    if !sql_enabled {
        cargo.arg("--no-default-features");
    }
    if perf_attribution_enabled && package_name == SQL_PARITY_CANISTER_PACKAGE {
        cargo.args(["--features", "perf-attribution"]);
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
/// Build one supported SQL canister WASM and return the built wasm path.
///
/// Build profile selection:
/// - `wasm-release` when `DFX_NETWORK` is `mainnet`, `staging`, or `ic`
/// - `debug` otherwise
/// - overridden by `DEMO_RPG_WASM_PROFILE=debug|release|wasm-release`
///
pub fn build_canister(canister_name: &str) -> Result<PathBuf, String> {
    let package_name = package_for_canister_name(canister_name)?;
    let profile = selected_canister_wasm_profile()?;
    build_canister_package(
        package_name,
        profile,
        &format!("{canister_name} canister build ({profile})"),
    )
}

///
/// stage_canister_for_dfx
///
/// Build one supported canister and stage `.wasm` + `.did` artifacts into
/// `.dfx/local/canisters/<canister_name>/`.
///

pub fn stage_canister_for_dfx(canister_name: &str) -> Result<(PathBuf, Option<PathBuf>), String> {
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

    let staged_did_path = dfx_canister_dir.join(format!("{canister_name}.did"));

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
