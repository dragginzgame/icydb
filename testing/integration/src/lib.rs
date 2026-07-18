//! Shared integration harness helpers.

pub mod sql_performance_contract;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::OnceLock,
};

use ic_testkit::artifacts::wasm_path;
use ic_testkit::pic::{
    InstallSpec, StandaloneCanisterFixture, install_prebuilt_canister_from_spec,
    try_ensure_pocket_ic_bin,
};
use icydb::Error;

const WASM_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
const FIXTURE_INSTALL_CYCLES: u128 = 100_000_000_000_000;

struct FixtureCanister {
    name: &'static str,
    package: &'static str,
    local_wasm_bytes: OnceLock<Vec<u8>>,
}

static FIXTURE_CANISTERS: [FixtureCanister; 10] = [
    FixtureCanister {
        name: "demo_rpg",
        package: "canister_demo_rpg",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "sql",
        package: "canister_test_sql",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "sql_bounded",
        package: "canister_test_sql_bounded",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "default_empty",
        package: "canister_audit_default_empty",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "default_empty_metrics",
        package: "canister_audit_default_empty_metrics",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "one_entity_fluent_rows",
        package: "canister_audit_one_entity_fluent_rows",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "one_entity_fluent_execute",
        package: "canister_audit_one_entity_fluent_execute",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "one_entity_sql_query",
        package: "canister_audit_one_entity_sql_query",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "sql_perf",
        package: "canister_audit_sql_perf",
        local_wasm_bytes: OnceLock::new(),
    },
    FixtureCanister {
        name: "ten_entity_fluent_rows",
        package: "canister_audit_ten_entity_fluent_rows",
        local_wasm_bytes: OnceLock::new(),
    },
];

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

    /// Return the Cargo profile label accepted by [`CanisterWasmProfile::parse`].
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
            Self::WasmRelease => "wasm-release",
        }
    }
}

/// Package feature mode for fixture canister builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterSqlMode {
    /// Build with the package default feature set.
    Enabled,
    /// Build without package default features.
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

/// Candid metadata export mode for fixture canister builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterCandidExportMode {
    /// Export Candid metadata for local builds, but omit it from wasm-release.
    Auto,
    /// Always include Candid metadata.
    Enabled,
    /// Always omit Candid metadata.
    Disabled,
}

impl CanisterCandidExportMode {
    /// Parse a user-facing Candid export mode.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "auto" => Ok(Self::Auto),
            "on" | "enabled" => Ok(Self::Enabled),
            "off" | "disabled" => Ok(Self::Disabled),
            other => Err(format!(
                "invalid canister Candid export mode '{other}', expected 'auto', 'on', or 'off'"
            )),
        }
    }

    const fn enabled_for_profile(self, profile: CanisterWasmProfile) -> bool {
        match self {
            Self::Auto => !matches!(profile, CanisterWasmProfile::WasmRelease),
            Self::Enabled => true,
            Self::Disabled => false,
        }
    }
}

/// Target-sensitive generated-surface policy for fixture canister builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterBuildTarget {
    /// Preserve the caller's `ICYDB_BUILD_TARGET`, if any.
    Inherit,
    /// Local ICP/PocketIC fixture build.
    Local,
    /// Mainnet-oriented fixture build.
    Ic,
}

impl CanisterBuildTarget {
    const fn env_value(self) -> Option<&'static str> {
        match self {
            Self::Inherit => None,
            Self::Local => Some("local"),
            Self::Ic => Some("ic"),
        }
    }
}

/// Explicit build options for fixture canisters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanisterBuildOptions {
    /// Cargo profile to use for the wasm build.
    pub profile: CanisterWasmProfile,
    /// Whether package default features stay enabled.
    pub sql_mode: CanisterSqlMode,
    /// Whether generated Candid metadata export stays in the canister wasm.
    pub candid_export: CanisterCandidExportMode,
    /// Build target used by target-sensitive generated surface policy.
    pub build_target: CanisterBuildTarget,
}

impl Default for CanisterBuildOptions {
    fn default() -> Self {
        Self {
            profile: CanisterWasmProfile::Debug,
            sql_mode: CanisterSqlMode::Enabled,
            candid_export: CanisterCandidExportMode::Auto,
            build_target: CanisterBuildTarget::Inherit,
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live under testing/integration")
        .to_path_buf()
}

fn target_dir(workspace_root: &Path) -> PathBuf {
    env::var_os("CARGO_TARGET_DIR").map_or_else(|| workspace_root.join("target"), PathBuf::from)
}

fn fixture_for_canister_name(canister_name: &str) -> Result<&'static FixtureCanister, String> {
    FIXTURE_CANISTERS
        .iter()
        .find(|fixture| fixture.name == canister_name)
        .ok_or_else(|| {
            let expected = FIXTURE_CANISTERS
                .iter()
                .map(|fixture| fixture.name)
                .collect::<Vec<_>>()
                .join("', '");

            format!("unsupported canister '{canister_name}', expected one of '{expected}'")
        })
}

fn package_for_canister_name(canister_name: &str) -> Result<&'static str, String> {
    fixture_for_canister_name(canister_name).map(|fixture| fixture.package)
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
// Fixture canister builds stay quiet, while cargo/rustc diagnostics are still
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
    if options.candid_export.enabled_for_profile(options.profile) {
        cargo.args(["--features", "candid-export"]);
    }
    if let Some(build_target) = options.build_target.env_value() {
        cargo.env("ICYDB_BUILD_TARGET", build_target);
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
    let built_wasm_path = wasm_path(&target_dir(&root), package_name, profile);
    if !built_wasm_path.is_file() {
        return Err(format!(
            "{context_label}: build succeeded but wasm was not found at {}",
            built_wasm_path.display()
        ));
    }

    Ok(built_wasm_path)
}

///
/// build_canister
///
/// Build one supported canister WASM with default debug options and return the
/// built wasm path.
pub fn build_canister(canister_name: &str) -> Result<PathBuf, String> {
    build_canister_with_options(canister_name, CanisterBuildOptions::default())
}

/// Build one supported fixture canister and return its raw WASM bytes.
///
/// This boundary lets repeated isolated tests build once and install the exact
/// same module into multiple fresh PocketIC instances.
///
/// # Panics
///
/// Panics if the canister name is unsupported, its build fails, or the built
/// WASM cannot be read.
#[must_use]
pub fn build_fixture_canister_wasm_bytes_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Vec<u8> {
    local_fixture_wasm_bytes_with_options(canister_name, options)
}

/// Install already-built fixture WASM into one fresh standalone PocketIC instance.
///
/// # Panics
///
/// Panics if the canister name is unsupported, empty init arguments cannot be
/// encoded, PocketIC cannot start, or installation fails.
#[must_use]
pub fn install_prebuilt_fixture_canister(
    canister_name: &str,
    wasm: Vec<u8>,
) -> StandaloneCanisterFixture {
    fixture_for_canister_name(canister_name)
        .unwrap_or_else(|error| panic!("fixture canister should be supported: {error}"));
    install_prebuilt_canister_from_spec(
        InstallSpec::new(
            wasm,
            candid::encode_args(()).expect("encode empty init args"),
            FIXTURE_INSTALL_CYCLES,
        )
        .label(canister_name),
    )
}

/// Build one supported canister and install it into a fresh standalone fixture
/// with empty init args.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// init args cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister(canister_name: &str) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(
        canister_name,
        local_canister_build_options(),
        None,
    )
}

/// Build one supported fixture canister and install it into a fresh standalone
/// fixture while printing install-stage progress to stderr.
///
/// This is intended for expensive ignored audits where a hung PocketIC startup
/// or install needs a precise stage marker in test logs.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// init args cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister_with_progress(
    canister_name: &str,
    progress_label: &str,
) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(
        canister_name,
        local_canister_build_options(),
        Some(progress_label),
    )
}

/// Build one supported fixture canister with explicit options and install it
/// into a fresh standalone fixture with empty init args.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// init args cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(canister_name, options, None)
}

/// Build one supported fixture canister with explicit options and install it
/// into a fresh standalone fixture while printing install-stage progress to
/// stderr.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// init args cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister_with_options_and_progress(
    canister_name: &str,
    options: CanisterBuildOptions,
    progress_label: &str,
) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(
        canister_name,
        options,
        Some(progress_label),
    )
}

fn install_fixture_canister_with_options_and_optional_progress(
    canister_name: &str,
    options: CanisterBuildOptions,
    progress_label: Option<&str>,
) -> StandaloneCanisterFixture {
    if let Some(label) = progress_label {
        eprintln!("{label}: resolving/building local {canister_name} wasm");
    }
    let wasm = local_fixture_wasm_bytes_with_options(canister_name, options);
    if let Some(label) = progress_label {
        eprintln!(
            "{label}: local {canister_name} wasm ready ({} bytes)",
            wasm.len(),
        );
        eprintln!("{label}: resolving PocketIC binary");
        let pocket_ic_bin = try_ensure_pocket_ic_bin()
            .unwrap_or_else(|err| panic!("{label}: failed to resolve PocketIC binary: {err}"));
        eprintln!("{label}: PocketIC binary {}", pocket_ic_bin.display());
    }
    if let Some(label) = progress_label {
        eprintln!("{label}: handing off to PocketIC install/startup");
    }

    let fixture = install_prebuilt_canister_from_spec(
        InstallSpec::new(
            wasm,
            candid::encode_args(()).expect("encode empty init args"),
            FIXTURE_INSTALL_CYCLES,
        )
        .label(canister_name),
    );
    if let Some(label) = progress_label {
        eprintln!("{label}: installed {canister_name} canister in PocketIC");
    }
    fixture
}

fn local_fixture_wasm_bytes(canister_name: &str) -> Vec<u8> {
    local_fixture_wasm_bytes_with_options(canister_name, local_canister_build_options())
}

fn local_fixture_wasm_bytes_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Vec<u8> {
    let fixture = fixture_for_canister_name(canister_name)
        .unwrap_or_else(|err| panic!("fixture canister should be supported: {err}"));

    if options == local_canister_build_options() {
        return fixture
            .local_wasm_bytes
            .get_or_init(|| build_local_fixture_wasm_bytes_with_options(fixture, options))
            .clone();
    }

    build_local_fixture_wasm_bytes_with_options(fixture, options)
}

fn build_local_fixture_wasm_bytes_with_options(
    fixture: &FixtureCanister,
    options: CanisterBuildOptions,
) -> Vec<u8> {
    let wasm_path = build_canister_package(
        fixture.package,
        options,
        &canister_build_label(fixture, options),
    )
    .unwrap_or_else(|err| panic!("{} canister should build: {err}", fixture.name));

    fs::read(&wasm_path).unwrap_or_else(|err| {
        panic!(
            "failed to read built {} canister wasm at {}: {err}",
            fixture.name,
            wasm_path.display()
        )
    })
}

fn local_canister_build_options() -> CanisterBuildOptions {
    CanisterBuildOptions {
        build_target: CanisterBuildTarget::Local,
        ..CanisterBuildOptions::default()
    }
}

fn canister_build_label(fixture: &FixtureCanister, options: CanisterBuildOptions) -> String {
    format!(
        "{} canister build ({})",
        fixture.name,
        options.profile.as_str(),
    )
}

/// Reset and reload the generated IcyDB fixture set on one installed canister.
///
/// # Panics
///
/// Panics if the reset or load calls fail to decode or return fixture errors.
pub fn reset_icydb_fixtures(fixture: &StandaloneCanisterFixture) {
    let reset: Result<(), Error> = fixture
        .update_call("icydb_fixtures_reset", ())
        .expect("icydb_fixtures_reset should decode");
    reset.expect("icydb_fixtures_reset should succeed");

    let load: Result<(), Error> = fixture
        .update_call("icydb_fixtures_load", ())
        .expect("icydb_fixtures_load should decode");
    load.expect("icydb_fixtures_load should succeed");
}

/// Build and upgrade one installed fixture canister with the current local WASM.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// upgrade args cannot be encoded, or PocketIC rejects the upgrade.
pub fn upgrade_fixture_canister(fixture: &StandaloneCanisterFixture, canister_name: &str) {
    let wasm = local_fixture_wasm_bytes(canister_name);
    let args = candid::encode_args(()).expect("encode empty upgrade args");

    fixture
        .pic()
        .upgrade_canister(fixture.canister_id(), wasm, args, None)
        .unwrap_or_else(|err| panic!("{canister_name} canister upgrade should succeed: {err}"));
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
