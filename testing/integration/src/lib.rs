//! Module: integration
//! Responsibility: shared canister build, installation, and PocketIC harness contracts.
//! Does not own: product runtime semantics or individual integration scenarios.
//! Boundary: exposes reusable test-only build and runtime adapters to integration targets.

mod canister_build_cache;

pub mod canister_artifact;
pub mod durable_mutation_job_contract;
pub mod group_path_contract;
pub mod nested_relation_contract;
pub mod wasm_measurement;
pub mod wasm_optimizer;

use crate::canister_build_cache::{
    CargoWasmBatchEntry, CargoWasmCacheRequest, PostLinkBatchEntry, PostLinkCacheRequest,
    build_cached_cargo_wasm, build_cached_cargo_wasm_batch,
    build_cached_cargo_wasm_batch_from_snapshot, cache_post_link_wasm, cache_post_link_wasm_batch,
    cargo_wasm_batch_specs, trace_post_link, trace_wasm_build,
};
use std::{
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::Duration,
};

use candid::CandidType;
use ic_testkit::{
    artifacts::{LabeledWasmBuildSpec, WasmBuildInputSnapshot, wasm_path},
    pic::{InstallSpec, PocketIcBuilderExt, PocketIcStartupConfig, StandaloneCanisterFixture},
    pocket_ic::{PocketIc, PocketIcBuilder},
};
use icydb::{Error, ErrorCode};
use serde::Deserialize;

const FIXTURE_INSTALL_CYCLES: u128 = 100_000_000_000_000;
const FIXTURE_STARTUP_MESSAGE_COMPLETION_TICKS: usize = 4;
const POCKET_IC_INSTANCE_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const POCKET_IC_SERVER_URL_ENV: &str = "ICYDB_POCKET_IC_SERVER_URL";
// A production watchdog callback may consume its full 30-billion-instruction
// allocation under deterministic time slicing. The maintained timer-
// exhaustion proof uses this same zero-time completion envelope.
const WATCHDOG_MESSAGE_COMPLETION_TICKS: usize = 24;

/// Maximum watchdog deliveries in the frozen normal convergence residual proof.
///
/// This is `B_0 + C_driver`, or `64 + 4`, for the maximum admitted backlog.
pub const MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES: usize = 68;

/// Canonical instruction and completion evidence returned by the SQL audit
/// canister's generated startup watchdog.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StartupWatchdogPerfSnapshot {
    /// Scheduler callbacks observed by the timer runtime.
    pub scheduler_samples: u64,
    /// Total instructions consumed by scheduler callbacks.
    pub scheduler_total_instructions: u64,
    /// Maximum instructions consumed by one scheduler callback.
    pub scheduler_maximum_instructions: Option<u64>,
    /// IcyDB work callbacks observed inside scheduler callbacks.
    pub work_samples: u64,
    /// Total instructions consumed by IcyDB work callbacks.
    pub work_total_instructions: u64,
    /// Instructions consumed by the latest IcyDB work callback.
    pub work_latest_instructions: Option<u64>,
    /// Maximum instructions consumed by one IcyDB work callback.
    pub work_maximum_instructions: Option<u64>,
    /// IcyDB work callbacks that started.
    pub work_started: u64,
    /// IcyDB work callbacks that completed.
    pub work_completed: u64,
    /// Successful IcyDB work callbacks.
    pub succeeded: u64,
    /// Retryable IcyDB work callback failures.
    pub retryable_failures: u64,
    /// Invariant-failing IcyDB work callbacks.
    pub invariant_failures: u64,
}

/// Deliver pending startup-watchdog messages in PocketIC without advancing time.
pub fn deliver_startup_watchdog_message(fixture: &StandaloneCanisterFixture) {
    // Normal progress schedules zero-delay successor messages. These bounded
    // ticks let PocketIC deliver them and finish deterministic time slicing
    // without admitting a cadence-backed retry.
    for _ in 0..WATCHDOG_MESSAGE_COMPLETION_TICKS {
        fixture.pocket_ic().tick();
    }
}

/// Decode the SQL audit canister's canonical startup-watchdog evidence.
///
/// # Panics
///
/// Panics when the canister response does not decode as the maintained
/// watchdog snapshot contract.
#[must_use]
pub fn startup_watchdog_perf_snapshot(
    fixture: &StandaloneCanisterFixture,
) -> StartupWatchdogPerfSnapshot {
    fixture
        .query_candid("startup_watchdog_perf_snapshot", ())
        .expect("startup watchdog performance snapshot should decode")
}

/// Report whether the SQL audit canister's generated startup watchdog is armed.
///
/// # Panics
///
/// Panics when the canister response does not decode as a boolean armed state.
#[must_use]
pub fn startup_watchdog_armed(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .query_candid("startup_watchdog_armed", ())
        .expect("startup watchdog scheduling state should decode")
}

/// Deliver bounded startup-watchdog messages until the SQL audit fixture
/// admits ordinary work.
///
/// # Panics
///
/// Panics when the ordinary-work probe cannot be decoded, returns a terminal
/// error, or remains recovery-blocked beyond the maintained delivery bound.
pub fn advance_startup_watchdog_until_ready(fixture: &StandaloneCanisterFixture) {
    for delivered in 0..=MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        let probe: Result<(), Error> = fixture
            .update_candid("initialize_startup_observation_fixture", ())
            .expect("ordinary startup probe should decode");
        match probe {
            Ok(()) => return,
            Err(error)
                if error.code()
                    == ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
            {
                if delivered == MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
                    break;
                }
                deliver_startup_watchdog_message(fixture);
            }
            Err(error) => panic!("startup driver returned terminal error: {error}"),
        }
    }
    panic!("startup driver should finish within its frozen residual delivery bound");
}

struct FixtureCanister {
    policy: &'static canister_artifact::MaintainedCanisterPolicy,
    local_wasm_bytes: &'static OnceLock<Vec<u8>>,
}

impl FixtureCanister {
    const fn name(&self) -> &'static str {
        self.policy.canister
    }

    const fn package(&self) -> &'static str {
        self.policy.package
    }
}

struct BuiltCanisterArtifacts {
    compiler_emitted: PathBuf,
    final_deployable: PathBuf,
}

struct ConfiguredCanisterBuild {
    arguments: Vec<OsString>,
    rustflags: Option<String>,
    compiler_emitted: PathBuf,
    final_deployable: PathBuf,
}

struct MaintainedCanisterBuildPlan {
    options: CanisterBuildOptions,
    configured: Vec<(
        &'static canister_artifact::MaintainedCanisterPolicy,
        ConfiguredCanisterBuild,
    )>,
    contexts: Vec<String>,
    specs: Vec<LabeledWasmBuildSpec>,
}

static FIXTURE_LOCAL_WASM_BYTES: [OnceLock<Vec<u8>>;
    canister_artifact::MAINTAINED_CANISTER_POLICIES.len()] =
    [const { OnceLock::new() }; canister_artifact::MAINTAINED_CANISTER_POLICIES.len()];

/// Cargo wasm profile used when building fixture canisters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterWasmProfile {
    /// Cargo's default debug profile.
    Debug,
    /// Cargo's standard release profile.
    Release,
    /// Workspace-defined wasm release profile.
    WasmRelease,
    /// Audit-only wasm profile retaining symbol attribution.
    WasmAttribution,
}

impl CanisterWasmProfile {
    /// Parse a user-facing profile name.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "debug" => Ok(Self::Debug),
            "release" => Ok(Self::Release),
            "wasm-release" => Ok(Self::WasmRelease),
            "wasm-attribution" => Ok(Self::WasmAttribution),
            other => Err(format!(
                "invalid canister wasm profile '{other}', expected 'debug', 'release', 'wasm-release', or 'wasm-attribution'"
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
            Self::WasmAttribution => "wasm-attribution",
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

    /// Return the canonical build-configuration label for this feature mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    /// Return the canonical report label for this feature mode.
    #[must_use]
    pub const fn report_variant(self) -> &'static str {
        match self {
            Self::Enabled => "sql-on",
            Self::Disabled => "sql-off",
        }
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
            Self::Auto => !matches!(
                profile,
                CanisterWasmProfile::WasmRelease | CanisterWasmProfile::WasmAttribution
            ),
            Self::Enabled => true,
            Self::Disabled => false,
        }
    }
}

/// Explicit maintained Cargo feature profile for fixture canister builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanisterBuildProfile {
    /// Local ICP/PocketIC build with the maintained test endpoint features.
    LocalTest,
    /// Production-shaped build with development and fixture features absent.
    Production,
}

impl CanisterBuildProfile {
    /// Parse one canonical build-profile label.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "local" | "local-test" => Ok(Self::LocalTest),
            "production" => Ok(Self::Production),
            other => Err(format!(
                "invalid canister build profile '{other}', expected 'local' or 'production'"
            )),
        }
    }

    const fn target_dir_name(self) -> &'static str {
        match self {
            Self::LocalTest => "canister-local",
            Self::Production => "canister-production",
        }
    }

    /// Return the canonical evidence label for this maintained profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalTest => "local_test",
            Self::Production => "production",
        }
    }
}

/// Final artifacts for both maintained canister profiles in contract order.
pub type MaintainedCanisterContractProfileArtifacts =
    Vec<(CanisterBuildProfile, Vec<(&'static str, PathBuf)>)>;

/// Explicit build options for fixture canisters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanisterBuildOptions {
    /// Cargo profile to use for the wasm build.
    pub profile: CanisterWasmProfile,
    /// Whether package default features stay enabled.
    pub sql_mode: CanisterSqlMode,
    /// Whether generated Candid metadata export stays in the canister wasm.
    pub candid_export: CanisterCandidExportMode,
    /// Exact maintained package feature profile.
    pub build_profile: CanisterBuildProfile,
}

impl Default for CanisterBuildOptions {
    fn default() -> Self {
        Self {
            profile: CanisterWasmProfile::Debug,
            sql_mode: CanisterSqlMode::Enabled,
            candid_export: CanisterCandidExportMode::Auto,
            build_profile: CanisterBuildProfile::LocalTest,
        }
    }
}

/// Effective maintained build configuration for one fixture canister.
///
/// This is the canonical projection of build options plus the fixture's
/// maintained feature policy. Build execution and reproducibility evidence
/// consume the same projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedCanisterBuildConfiguration {
    profile: CanisterWasmProfile,
    build_profile: CanisterBuildProfile,
    sql_mode: CanisterSqlMode,
    candid_export: bool,
    no_default_features: bool,
    path_trimming: bool,
    features: Vec<&'static str>,
}

impl ResolvedCanisterBuildConfiguration {
    /// Return the exact Cargo profile passed to the build.
    #[must_use]
    pub const fn profile(&self) -> CanisterWasmProfile {
        self.profile
    }

    /// Return the exact maintained feature profile.
    #[must_use]
    pub const fn build_profile(&self) -> CanisterBuildProfile {
        self.build_profile
    }

    /// Return the exact SQL feature mode.
    #[must_use]
    pub const fn sql_mode(&self) -> CanisterSqlMode {
        self.sql_mode
    }

    /// Return whether generated Candid metadata is embedded.
    #[must_use]
    pub const fn candid_export(&self) -> bool {
        self.candid_export
    }

    /// Return whether release path-remapping flags are applied.
    #[must_use]
    pub const fn path_trimming(&self) -> bool {
        self.path_trimming
    }

    /// Return the exact ordered Cargo feature set passed to the build.
    #[must_use]
    pub fn features(&self) -> &[&'static str] {
        &self.features
    }

    /// Return whether Cargo package defaults are disabled.
    #[must_use]
    pub const fn no_default_features(&self) -> bool {
        self.no_default_features
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

fn fixture_for_canister_name(canister_name: &str) -> Result<FixtureCanister, String> {
    canister_artifact::MAINTAINED_CANISTER_POLICIES
        .iter()
        .enumerate()
        .find(|(_, policy)| policy.canister == canister_name)
        .map(|(index, policy)| FixtureCanister {
            policy,
            local_wasm_bytes: &FIXTURE_LOCAL_WASM_BYTES[index],
        })
        .ok_or_else(|| {
            let expected = canister_artifact::MAINTAINED_CANISTER_POLICIES
                .iter()
                .map(|policy| policy.canister)
                .collect::<Vec<_>>()
                .join("', '");

            format!("unsupported canister '{canister_name}', expected one of '{expected}'")
        })
}

fn package_for_canister_name(canister_name: &str) -> Result<&'static str, String> {
    fixture_for_canister_name(canister_name).map(|fixture| fixture.package())
}

/// Resolve the exact maintained build configuration for one fixture canister.
///
/// # Errors
///
/// Returns an error when the canister name is unknown.
pub fn resolve_fixture_canister_build_configuration(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Result<ResolvedCanisterBuildConfiguration, String> {
    let fixture = fixture_for_canister_name(canister_name)?;
    Ok(resolve_canister_build_configuration(
        fixture.policy,
        options,
    ))
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
fn combined_rustflags(extra_flags: &[String]) -> Option<String> {
    if extra_flags.is_empty() {
        return None;
    }

    let mut combined = env::var("RUSTFLAGS").unwrap_or_default();
    for flag in extra_flags {
        if !combined.is_empty() {
            combined.push(' ');
        }
        combined.push_str(flag);
    }

    Some(combined)
}

fn build_canister_package_artifacts(
    package_name: &str,
    options: CanisterBuildOptions,
    context_label: &str,
) -> Result<BuiltCanisterArtifacts, String> {
    let root = workspace_root();
    let canister_target_dir = target_dir(&root).join(options.build_profile.target_dir_name());
    let configured = configure_canister_build(&root, &canister_target_dir, package_name, options)?;
    let packages = [package_name];
    let outcome = build_cached_cargo_wasm(&CargoWasmCacheRequest {
        context: context_label,
        workspace_root: &root,
        target_dir: &canister_target_dir,
        packages: &packages,
        profile_target_dir: options.profile.as_str(),
        arguments: &configured.arguments,
        effective_rustflags: configured.rustflags.as_deref(),
    })
    .map_err(|error| format!("{context_label}: {error}"))?;
    trace_wasm_build(context_label, &outcome);

    finish_canister_build(&root, configured, options, context_label)
}

fn finish_canister_build(
    root: &Path,
    configured: ConfiguredCanisterBuild,
    options: CanisterBuildOptions,
    context_label: &str,
) -> Result<BuiltCanisterArtifacts, String> {
    if !configured.compiler_emitted.is_file() {
        return Err(format!(
            "{context_label}: build succeeded but wasm was not found at {}",
            configured.compiler_emitted.display()
        ));
    }

    if matches!(options.profile, CanisterWasmProfile::WasmAttribution) {
        return Ok(BuiltCanisterArtifacts {
            compiler_emitted: configured.compiler_emitted.clone(),
            final_deployable: configured.compiler_emitted,
        });
    }

    let cache_root = target_dir(root).join("canister-artifact-cache");
    let outcome = cache_post_link_wasm(&PostLinkCacheRequest {
        workspace_root: root,
        cache_root: &cache_root,
        coordination_scope: context_label,
        compiler_emitted: &configured.compiler_emitted,
        final_deployable: &configured.final_deployable,
    })
    .map_err(|error| format!("{context_label}: {error}"))?;
    trace_post_link(context_label, &outcome);

    Ok(BuiltCanisterArtifacts {
        compiler_emitted: configured.compiler_emitted,
        final_deployable: configured.final_deployable,
    })
}

fn configure_canister_build(
    root: &Path,
    canister_target_dir: &Path,
    package_name: &str,
    options: CanisterBuildOptions,
) -> Result<ConfiguredCanisterBuild, String> {
    let policy = canister_artifact::MAINTAINED_CANISTER_POLICIES
        .iter()
        .find(|policy| policy.package == package_name)
        .ok_or_else(|| format!("no maintained feature policy for package '{package_name}'"))?;
    let resolved = resolve_canister_build_configuration(policy, options);
    let profile = resolved.profile().as_str();
    let compiler_emitted = wasm_path(canister_target_dir, package_name, profile);
    let final_deployable = if matches!(options.profile, CanisterWasmProfile::WasmAttribution) {
        compiler_emitted.clone()
    } else {
        canister_target_dir
            .join("icydb-final")
            .join(profile)
            .join(format!("{package_name}.wasm"))
    };

    let mut arguments = cargo_profile_arguments(resolved.profile(), resolved.no_default_features());
    if !resolved.features().is_empty() {
        arguments.extend([
            OsString::from("--features"),
            resolved.features().join(",").into(),
        ]);
    }
    let extra_rustflags = if resolved.path_trimming() {
        wasm_release_path_trim_flags(root)
    } else {
        Vec::new()
    };
    let rustflags = combined_rustflags(&extra_rustflags);

    Ok(ConfiguredCanisterBuild {
        arguments,
        rustflags,
        compiler_emitted,
        final_deployable,
    })
}

fn resolve_canister_build_configuration(
    policy: &canister_artifact::MaintainedCanisterPolicy,
    options: CanisterBuildOptions,
) -> ResolvedCanisterBuildConfiguration {
    let selected_features = match options.build_profile {
        CanisterBuildProfile::LocalTest => policy.local_test_features,
        CanisterBuildProfile::Production => policy.production_features,
    };
    let candid_enabled = options.candid_export.enabled_for_profile(options.profile);
    let features = selected_features
        .iter()
        .copied()
        .filter(|feature| match *feature {
            "candid-export" => candid_enabled,
            _ => options.sql_mode.enabled(),
        })
        .collect();

    ResolvedCanisterBuildConfiguration {
        profile: options.profile,
        build_profile: options.build_profile,
        sql_mode: options.sql_mode,
        candid_export: candid_enabled,
        no_default_features: true,
        path_trimming: matches!(
            options.profile,
            CanisterWasmProfile::WasmRelease | CanisterWasmProfile::WasmAttribution
        ),
        features,
    }
}

fn cargo_profile_arguments(
    profile: CanisterWasmProfile,
    no_default_features: bool,
) -> Vec<OsString> {
    let mut arguments = vec![OsString::from("--locked")];
    if no_default_features {
        arguments.push(OsString::from("--no-default-features"));
    }
    match profile {
        CanisterWasmProfile::Debug => {}
        CanisterWasmProfile::Release => arguments.push(OsString::from("--release")),
        CanisterWasmProfile::WasmRelease | CanisterWasmProfile::WasmAttribution => arguments
            .extend([
                OsString::from("--profile"),
                OsString::from(profile.as_str()),
            ]),
    }
    arguments
}

fn build_canister_package(
    package_name: &str,
    options: CanisterBuildOptions,
    context_label: &str,
) -> Result<PathBuf, String> {
    build_canister_package_artifacts(package_name, options, context_label)
        .map(|artifacts| artifacts.final_deployable)
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

/// Build one fixture canister and return compiler-emitted plus final deployable Wasm bytes.
///
/// This audit boundary exists to prove upgrades from the pre-optimization
/// compiler artifact to the canonical post-link artifact. Normal callers must
/// install [`build_fixture_canister_wasm_bytes_with_options`] instead.
///
/// # Panics
///
/// Panics if the canister name is unsupported, either build stage fails, or
/// either Wasm artifact cannot be read.
#[must_use]
pub fn build_fixture_canister_wasm_stages_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> (Vec<u8>, Vec<u8>) {
    let fixture = fixture_for_canister_name(canister_name)
        .unwrap_or_else(|error| panic!("fixture canister should be supported: {error}"));
    let artifacts = build_canister_package_artifacts(
        fixture.package(),
        options,
        &canister_build_label(&fixture, options),
    )
    .unwrap_or_else(|error| panic!("{} canister should build: {error}", fixture.name()));
    let compiler_emitted = fs::read(&artifacts.compiler_emitted).unwrap_or_else(|error| {
        panic!(
            "failed to read compiler-emitted {} canister wasm at {}: {error}",
            fixture.name(),
            artifacts.compiler_emitted.display()
        )
    });
    let final_deployable = fs::read(&artifacts.final_deployable).unwrap_or_else(|error| {
        panic!(
            "failed to read final deployable {} canister wasm at {}: {error}",
            fixture.name(),
            artifacts.final_deployable.display()
        )
    });
    (compiler_emitted, final_deployable)
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
    let fixture = install_prebuilt_fixture_canister_without_startup_delivery(canister_name, wasm);
    deliver_fixture_startup_watchdog(&fixture);
    fixture
}

/// Install already-built fixture WASM without delivering its startup watchdog.
///
/// This is reserved for lifecycle tests that must observe the generated
/// canister before its first startup callback. Ordinary integration
/// tests should use [`install_prebuilt_fixture_canister`].
///
/// # Panics
///
/// Panics if the canister name is unsupported, empty init arguments cannot be
/// encoded, PocketIC cannot start, or installation fails.
#[must_use]
pub fn install_prebuilt_fixture_canister_without_startup_delivery(
    canister_name: &str,
    wasm: Vec<u8>,
) -> StandaloneCanisterFixture {
    fixture_for_canister_name(canister_name)
        .unwrap_or_else(|error| panic!("fixture canister should be supported: {error}"));
    StandaloneCanisterFixture::install(
        start_fixture_pocket_ic(),
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
    let fixture = install_fixture_canister_without_startup_delivery(canister_name);
    deliver_fixture_startup_watchdog(&fixture);
    fixture
}

/// Build and install one fixture without delivering its startup watchdog.
///
/// This is reserved for lifecycle tests that must observe the generated
/// canister before its first startup callback. Ordinary integration
/// tests should use [`install_fixture_canister`].
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, empty
/// init args cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister_without_startup_delivery(
    canister_name: &str,
) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(
        canister_name,
        local_canister_build_options(),
        None,
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
        eprintln!("{label}: handing off to PocketIC install/startup");
    }

    let fixture = StandaloneCanisterFixture::install(
        start_fixture_pocket_ic(),
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

fn start_fixture_pocket_ic() -> PocketIc {
    let Some(server_url) = env::var(POCKET_IC_SERVER_URL_ENV)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return PocketIc::new();
    };

    PocketIcBuilder::new()
        .with_application_subnet()
        .try_build(PocketIcStartupConfig::connect(
            server_url,
            POCKET_IC_INSTANCE_STARTUP_TIMEOUT,
        ))
        .unwrap_or_else(|error| panic!("start fixture PocketIC on governed server: {error}"))
}

/// Deliver a bounded set of generated startup-watchdog messages.
///
/// This helper is used after installation or upgrade when a test needs an
/// ordinary-work-ready canister but does not inspect the startup control
/// surface itself.
pub fn deliver_fixture_startup_watchdog(fixture: &StandaloneCanisterFixture) {
    // A generated schema application may need several bounded watchdog
    // messages, and PocketIC may need several deterministic-time slices to
    // finish each message. Keep ordinary fixture setup bounded while allowing
    // every maintained fresh-install schema to reach `Ready`.
    for _ in 0..8 {
        for _ in 0..FIXTURE_STARTUP_MESSAGE_COMPLETION_TICKS {
            fixture.pocket_ic().tick();
        }
    }
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
            .get_or_init(|| build_local_fixture_wasm_bytes_with_options(&fixture, options))
            .clone();
    }

    build_local_fixture_wasm_bytes_with_options(&fixture, options)
}

fn build_local_fixture_wasm_bytes_with_options(
    fixture: &FixtureCanister,
    options: CanisterBuildOptions,
) -> Vec<u8> {
    let wasm_path = build_canister_package(
        fixture.package(),
        options,
        &canister_build_label(fixture, options),
    )
    .unwrap_or_else(|err| panic!("{} canister should build: {err}", fixture.name()));

    fs::read(&wasm_path).unwrap_or_else(|err| {
        panic!(
            "failed to read built {} canister wasm at {}: {err}",
            fixture.name(),
            wasm_path.display()
        )
    })
}

fn local_canister_build_options() -> CanisterBuildOptions {
    CanisterBuildOptions::default()
}

fn canister_build_label(fixture: &FixtureCanister, options: CanisterBuildOptions) -> String {
    format!(
        "{} canister build ({})",
        fixture.name(),
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
        .update_candid("icydb_fixtures_reset", ())
        .expect("icydb_fixtures_reset should decode");
    reset.expect("icydb_fixtures_reset should succeed");

    let load: Result<(), Error> = fixture
        .update_candid("icydb_fixtures_load", ())
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
        .pocket_ic()
        .upgrade_canister(fixture.canister_id(), wasm, args, None)
        .unwrap_or_else(|err| panic!("{canister_name} canister upgrade should succeed: {err}"));
}

/// Build every maintained canister independently and return its final Wasm path.
///
/// This is intended for whole-fleet artifact contracts. The collect-all batch
/// retains every Cargo failure while sharing input resolution and the caller-owned
/// incremental target. Ordinary tests should continue to build only the fixture
/// they exercise.
///
/// # Errors
///
/// Returns one error containing every independent Cargo or post-link acquisition
/// failure. Configuration failures retain their maintained contextual error.
pub fn build_maintained_canisters_with_options(
    options: CanisterBuildOptions,
) -> Result<Vec<(&'static str, PathBuf)>, String> {
    let root = workspace_root();
    let plan = plan_maintained_canister_builds(&root, options)?;
    let cargo_report = build_cached_cargo_wasm_batch(&plan.specs);

    finish_maintained_canister_build_plan(&root, plan, cargo_report)
}

/// Build both maintained whole-fleet contract profiles from one immutable input snapshot.
///
/// This is the narrow artifact-contract path for concurrent LocalTest and
/// Production readers. Ordinary callers should continue using
/// [`build_maintained_canisters_with_options`].
///
/// The supplied guard must exclude mutation of every source, Cargo/rustc
/// executable, manifest, Cargo configuration file, declared build input, and
/// relevant environment value until this function returns. The guard's
/// provenance cannot be verified; passing an unrelated token violates the
/// snapshot contract.
///
/// # Errors
///
/// Returns an error if snapshot preparation, either Cargo or post-link batch,
/// or a scoped profile reader fails.
pub fn build_maintained_canister_contract_profiles_assuming_sources_immutable<Guard: ?Sized>(
    source_write_guard: &Guard,
) -> Result<MaintainedCanisterContractProfileArtifacts, String> {
    let root = workspace_root();
    let plans = [
        CanisterBuildProfile::LocalTest,
        CanisterBuildProfile::Production,
    ]
    .into_iter()
    .map(|build_profile| {
        plan_maintained_canister_builds(
            &root,
            CanisterBuildOptions {
                candid_export: CanisterCandidExportMode::Enabled,
                build_profile,
                ..CanisterBuildOptions::default()
            },
        )
    })
    .collect::<Result<Vec<_>, _>>()?;
    let prepared_specs = plans
        .iter()
        .flat_map(|plan| plan.specs.iter().map(|spec| spec.spec().clone()))
        .collect::<Vec<_>>();
    let snapshot = WasmBuildInputSnapshot::prepare_assuming_sources_immutable(
        source_write_guard,
        &prepared_specs,
    )
    .map_err(|error| format!("maintained canister input snapshot failed: {error}"))?;

    let builds = std::thread::scope(|scope| {
        let handles = plans
            .into_iter()
            .map(|plan| {
                let build_profile = plan.options.build_profile;
                let snapshot = &snapshot;
                let root = &root;
                let handle = scope.spawn(move || {
                    let cargo_report =
                        build_cached_cargo_wasm_batch_from_snapshot(snapshot, &plan.specs);
                    finish_maintained_canister_build_plan(root, plan, cargo_report)
                });
                (build_profile, handle)
            })
            .collect::<Vec<_>>();
        let mut profile_artifacts = Vec::with_capacity(handles.len());
        let mut failures = Vec::new();
        for (build_profile, handle) in handles {
            match handle.join() {
                Ok(Ok(artifacts)) => profile_artifacts.push((build_profile, artifacts)),
                Ok(Err(error)) => failures.push(format!("{build_profile:?}: {error}")),
                Err(_) => failures.push(format!(
                    "maintained {build_profile:?} canister reader panicked"
                )),
            }
        }
        if failures.is_empty() {
            Ok(profile_artifacts)
        } else {
            Err(format!(
                "maintained canister profile builds failed:\n  - {}",
                failures.join("\n  - ")
            ))
        }
    });
    let metrics = snapshot.metrics();
    eprintln!(
        "maintained canister input_snapshot=specifications={} input_resolution_runs={} input_resolution_reuses={} reader_reuses={} invalidated={} timings={:?}",
        metrics.specifications(),
        metrics.input_resolution_runs(),
        metrics.input_resolution_reuses(),
        metrics.reader_reuses(),
        metrics.is_invalidated(),
        metrics.input_resolution_timings(),
    );
    builds
}

fn plan_maintained_canister_builds(
    root: &Path,
    options: CanisterBuildOptions,
) -> Result<MaintainedCanisterBuildPlan, String> {
    let canister_target_dir = target_dir(root).join(options.build_profile.target_dir_name());
    let configured = canister_artifact::MAINTAINED_CANISTER_POLICIES
        .iter()
        .map(|policy| {
            configure_canister_build(root, &canister_target_dir, policy.package, options)
                .map(|configured| (policy, configured))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let contexts = configured
        .iter()
        .map(|(policy, _)| {
            format!(
                "{} canister build ({}, {:?})",
                policy.canister,
                options.profile.as_str(),
                options.build_profile,
            )
        })
        .collect::<Vec<_>>();
    let batch_entries = configured
        .iter()
        .zip(&contexts)
        .map(|((policy, configured), context)| CargoWasmBatchEntry {
            context,
            package: policy.package,
            arguments: &configured.arguments,
            effective_rustflags: configured.rustflags.as_deref(),
        })
        .collect::<Vec<_>>();
    let specs = cargo_wasm_batch_specs(
        root,
        &canister_target_dir,
        options.profile.as_str(),
        &batch_entries,
    );
    Ok(MaintainedCanisterBuildPlan {
        options,
        configured,
        contexts,
        specs,
    })
}

fn finish_maintained_canister_build_plan(
    root: &Path,
    plan: MaintainedCanisterBuildPlan,
    cargo_report: canister_build_cache::CanisterCacheBatchReport,
) -> Result<Vec<(&'static str, PathBuf)>, String> {
    finish_maintained_canister_builds(
        root,
        plan.configured,
        &plan.contexts,
        cargo_report,
        plan.options,
    )
}

fn finish_maintained_canister_builds(
    root: &Path,
    configured: Vec<(
        &'static canister_artifact::MaintainedCanisterPolicy,
        ConfiguredCanisterBuild,
    )>,
    contexts: &[String],
    cargo_report: canister_build_cache::CanisterCacheBatchReport,
    options: CanisterBuildOptions,
) -> Result<Vec<(&'static str, PathBuf)>, String> {
    let mut failures = cargo_report.failures;
    let mut post_link_indexes = Vec::with_capacity(cargo_report.successful_indexes.len());
    for index in cargo_report.successful_indexes {
        let Some((policy, configured)) = configured.get(index) else {
            failures.push(format!(
                "Cargo returned unknown successful entry index {index}"
            ));
            continue;
        };
        if configured.compiler_emitted.is_file() {
            post_link_indexes.push(index);
        } else {
            failures.push(format!(
                "Cargo [{index}] {}: build succeeded but wasm was not found at {}",
                policy.canister,
                configured.compiler_emitted.display()
            ));
        }
    }

    if !post_link_indexes.is_empty()
        && !matches!(options.profile, CanisterWasmProfile::WasmAttribution)
    {
        let cache_root = target_dir(root).join("canister-artifact-cache");
        let entries = post_link_indexes
            .iter()
            .filter_map(|index| {
                let (_, configured) = configured.get(*index)?;
                let context = contexts.get(*index)?;
                Some(PostLinkBatchEntry {
                    context,
                    compiler_emitted: &configured.compiler_emitted,
                    final_deployable: &configured.final_deployable,
                })
            })
            .collect::<Vec<_>>();
        if entries.len() == post_link_indexes.len() {
            let post_link_report = cache_post_link_wasm_batch(root, &cache_root, &entries)?;
            failures.extend(post_link_report.failures);
        } else {
            failures.push("post-link batch context mapping was incomplete".to_owned());
        }
    }

    if !failures.is_empty() {
        return Err(format!(
            "maintained canister build failed:\n  - {}",
            failures.join("\n  - ")
        ));
    }

    Ok(configured
        .into_iter()
        .map(|(policy, configured)| {
            let artifact = if matches!(options.profile, CanisterWasmProfile::WasmAttribution) {
                configured.compiler_emitted
            } else {
                configured.final_deployable
            };
            (policy.canister, artifact)
        })
        .collect())
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
    let artifacts = build_canister_package_artifacts(
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
    fs::copy(&artifacts.final_deployable, &staged_wasm_path).map_err(|err| {
        format!(
            "failed to copy built wasm from {} to {}: {err}",
            artifacts.final_deployable.display(),
            staged_wasm_path.display()
        )
    })?;
    let staged_compiler_wasm_path = icp_canister_dir.join(format!("{canister_name}.compiler.wasm"));
    fs::copy(&artifacts.compiler_emitted, &staged_compiler_wasm_path).map_err(|err| {
        format!(
            "failed to copy compiler-emitted wasm from {} to {}: {err}",
            artifacts.compiler_emitted.display(),
            staged_compiler_wasm_path.display()
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
