//! Module: integration
//! Responsibility: shared canister build, installation, and PocketIC harness contracts.
//! Does not own: product runtime semantics or individual integration scenarios.
//! Boundary: exposes reusable test-only build and runtime adapters to integration targets.

#[cfg(test)]
mod build_flags_tests;
mod canister_build_cache;
#[cfg(test)]
mod pocketic_startup_tests;

pub mod canister_artifact;
pub mod durable_mutation_job_contract;
pub mod group_path_contract;
pub mod nested_relation_contract;
pub mod wasm_measurement;
pub mod wasm_optimizer;

use crate::canister_artifact::read_wasm_artifact;
use crate::canister_build_cache::{
    CargoWasmBatchEntry, CargoWasmCacheRequest, PostLinkBatchEntry, PostLinkCacheRequest,
    build_cached_cargo_wasm, build_cached_cargo_wasm_batch, cache_post_link_wasm,
    cache_post_link_wasm_batch, cargo_wasm_batch_specs, trace_post_link, trace_wasm_build,
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
    artifacts::{ArtifactCacheRecord, LabeledWasmBuildSpec, WasmBuildRecord},
    pic::{InstallSpec, PocketIcBuilderExt, PocketIcStartupConfig, StandaloneCanisterFixture},
    pocket_ic::{PocketIc, PocketIcBuilder},
};
use icydb::{Error, ErrorCode};
use serde::Deserialize;

const FIXTURE_INSTALL_CYCLES: u128 = 100_000_000_000_000;
const FIXTURE_STARTUP_MESSAGE_COMPLETION_TICKS: usize = 4;
const POCKET_IC_INSTANCE_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
// A production watchdog callback may consume its full 30-billion-instruction
// allocation under deterministic time slicing. The maintained timer-
// exhaustion proof uses this same zero-time completion envelope.
const WATCHDOG_MESSAGE_COMPLETION_TICKS: usize = 24;

/// Maximum watchdog deliveries in the frozen normal convergence residual proof.
///
/// This is `B_0 + C_driver`, or `64 + 6`, for the maximum admitted backlog:
/// the original four driver deliveries plus separate replay and verification.
/// Ordinary online convergence still needs at most one fold per batch.
pub const MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES: usize = 70;

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
    /// Callbacks that completed normally while startup dependencies were pending.
    pub no_work: u64,
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
                // A new boot first waits for raw_rand; completed replies do
                // not make the cadence-backed retry immediately due.
                fixture.pocket_ic().advance_time(Duration::from_secs(1));
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

/// Exact compiler and final Wasm artifacts retained through reading or staging.
///
/// Keep this owner alive while using its borrowed path. Copying the path alone
/// does not retain the cache entry. `AsRef<Path>` selects the final artifact.
pub struct BuiltCanisterArtifacts {
    compiler_emitted: PathBuf,
    final_deployable: PathBuf,
    _cargo: WasmBuildRecord,
    post_link: Option<ArtifactCacheRecord>,
}

impl BuiltCanisterArtifacts {
    fn from_cargo(record: WasmBuildRecord) -> Result<Self, String> {
        let [path] = record.artifacts() else {
            return Err("canister build must retain exactly one compiler artifact".to_owned());
        };
        Ok(Self {
            compiler_emitted: path.clone(),
            final_deployable: path.clone(),
            _cargo: record,
            post_link: None,
        })
    }

    fn retain_post_link(&mut self, record: ArtifactCacheRecord) -> Result<(), String> {
        let [artifact] = record.artifacts() else {
            return Err("canister post-link must retain exactly one final artifact".to_owned());
        };
        if artifact.name() != "final-deployable" {
            return Err("canister post-link returned an unexpected artifact name".to_owned());
        }
        self.final_deployable = artifact.path().to_path_buf();
        self.post_link = Some(record);
        Ok(())
    }
}

impl AsRef<Path> for BuiltCanisterArtifacts {
    fn as_ref(&self) -> &Path {
        &self.final_deployable
    }
}

struct ConfiguredCanisterBuild {
    arguments: Vec<OsString>,
    encoded_rustflags: Option<String>,
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
pub type MaintainedCanisterContractProfileArtifacts = Vec<(
    CanisterBuildProfile,
    Vec<(&'static str, BuiltCanisterArtifacts)>,
)>;

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

// Cargo selects encoded flags before ordinary flags, even when explicitly empty.
// Append remaps to that effective input and encode argument boundaries once so
// spaces in paths survive. With no additions, leave Cargo's inheritance alone.
fn combined_encoded_rustflags(extra_flags: &[String]) -> Option<String> {
    if extra_flags.is_empty() {
        return None;
    }

    let mut combined = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_else(|_| {
        env::var("RUSTFLAGS")
            .unwrap_or_default()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("\x1f")
    });
    for flag in extra_flags {
        if !combined.is_empty() {
            combined.push('\x1f');
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
    build_configured_canister_artifacts(
        &root,
        &canister_target_dir,
        package_name,
        options,
        context_label,
        configured,
    )
}

fn build_configured_canister_artifacts(
    root: &Path,
    canister_target_dir: &Path,
    package_name: &str,
    options: CanisterBuildOptions,
    context_label: &str,
    configured: ConfiguredCanisterBuild,
) -> Result<BuiltCanisterArtifacts, String> {
    let packages = [package_name];
    let outcome = build_cached_cargo_wasm(&CargoWasmCacheRequest {
        context: context_label,
        workspace_root: root,
        target_dir: canister_target_dir,
        packages: &packages,
        profile_target_dir: options.profile.as_str(),
        arguments: &configured.arguments,
        encoded_rustflags: configured.encoded_rustflags.as_deref(),
    })
    .map_err(|error| format!("{context_label}: {error}"))?;
    trace_wasm_build(context_label, &outcome);

    finish_canister_build(
        root,
        configured,
        options,
        context_label,
        outcome.record().clone(),
    )
}

/// Build the full and omitted-store logical-memory test actors, in that order.
///
/// Both use the ordinary retained Cargo/post-link pipeline. Read each result
/// while its owner is alive, before another variant can replace shared outputs.
/// These actors expose test-only controls and must not host application data.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_logical_memory_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm(
            "canister_test_logical_memory",
            "test-admin-api",
            "logical-memory-full",
        )?,
        build_fixture_variant_wasm(
            "canister_test_logical_memory",
            "test-admin-api,omit-retiring-store",
            "logical-memory-omitted",
        )?,
    ))
}

/// Build the maintained source and successor schema-migration actors.
///
/// Both source shapes use the current library and storage format. Artifact
/// retention lasts through each read, before the next variant is built.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_schema_migration_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,schema-migration-api",
            "schema-migration-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,schema-migration-v2",
            "schema-migration-successor",
        )?,
    ))
}

/// Build the populated entity-rename source and successor using the SQL actor.
/// Both actors use current formats and the same durable namespace/store keys.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_rename_fixture_wasms() -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename",
            "entity-rename-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename-successor",
            "entity-rename-successor",
        )?,
    ))
}

/// Build populated source, additive, metadata and physical creation actors.
/// Every actor uses current formats and the same durable namespace/store keys.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_creation_fixture_wasms() -> Result<[Vec<u8>; 4], String> {
    Ok([
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-rename",
            "entity-creation-source",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation",
            "entity-creation-additive",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation,entity-rename-successor",
            "entity-creation-metadata",
        )?,
        build_fixture_variant_wasm(
            "canister_test_sql",
            "test-admin-api,local-sql-query,entity-creation-physical",
            "entity-creation-physical",
        )?,
    ])
}

/// Build matched source, unchanged-schema export control and additive actors.
///
/// The control changes only the existing Candid export feature; all artifacts
/// retain the same storage namespace and use the selected canonical profile.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_entity_creation_lifecycle_fixture_wasms(
    profile: CanisterWasmProfile,
) -> Result<[Vec<u8>; 3], String> {
    let build = |features, label| {
        build_fixture_variant_wasm_with_profile("canister_test_sql", features, label, profile)
    };
    Ok([
        build(
            "test-admin-api,local-sql-query,entity-rename",
            "creation-lifecycle-source",
        )?,
        build(
            "test-admin-api,local-sql-query,entity-rename,candid-export",
            "creation-lifecycle-control",
        )?,
        build(
            "test-admin-api,local-sql-query,entity-creation,candid-export",
            "creation-lifecycle-additive",
        )?,
    ])
}

// Read while the retained Cargo/post-link owner is alive. Variant-specific
// features must not borrow a mutable artifact path from another build.
fn build_fixture_variant_wasm(
    package: &str,
    features: &str,
    label: &str,
) -> Result<Vec<u8>, String> {
    build_fixture_variant_wasm_with_profile(
        package,
        features,
        label,
        CanisterBuildOptions::default().profile,
    )
}

fn build_fixture_variant_wasm_with_profile(
    package: &str,
    features: &str,
    label: &str,
    profile: CanisterWasmProfile,
) -> Result<Vec<u8>, String> {
    let root = workspace_root();
    let options = CanisterBuildOptions {
        profile,
        ..CanisterBuildOptions::default()
    };
    let target = target_dir(&root).join(options.build_profile.target_dir_name());
    let mut arguments = cargo_profile_arguments(options.profile, true);
    arguments.extend([OsString::from("--features"), features.into()]);
    let artifacts = build_configured_canister_artifacts(
        &root,
        &target,
        package,
        options,
        label,
        ConfiguredCanisterBuild {
            arguments,
            encoded_rustflags: combined_encoded_rustflags(&[]),
            final_deployable: target
                .join("icydb-final")
                .join(profile.as_str())
                .join(format!("{label}.wasm")),
        },
    )?;
    read_wasm_artifact(artifacts.as_ref())
        .map_err(|error| format!("read retained {label}: {error}"))
}

fn finish_canister_build(
    root: &Path,
    configured: ConfiguredCanisterBuild,
    options: CanisterBuildOptions,
    context_label: &str,
    cargo: WasmBuildRecord,
) -> Result<BuiltCanisterArtifacts, String> {
    let mut artifacts = BuiltCanisterArtifacts::from_cargo(cargo)?;
    if matches!(options.profile, CanisterWasmProfile::WasmAttribution) {
        return Ok(artifacts);
    }

    let cache_root = target_dir(root).join("canister-artifact-cache");
    let outcome = cache_post_link_wasm(&PostLinkCacheRequest {
        workspace_root: root,
        cache_root: &cache_root,
        coordination_scope: context_label,
        compiler_emitted: &artifacts.compiler_emitted,
        final_deployable: &configured.final_deployable,
    })
    .map_err(|error| format!("{context_label}: {error}"))?;
    trace_post_link(context_label, &outcome);

    artifacts.retain_post_link(outcome.record().clone())?;
    Ok(artifacts)
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
    let final_deployable = canister_target_dir
        .join("icydb-final")
        .join(profile)
        .join(format!("{package_name}.wasm"));

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
    let encoded_rustflags = combined_encoded_rustflags(&extra_rustflags);

    Ok(ConfiguredCanisterBuild {
        arguments,
        encoded_rustflags,
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

///
/// build_canister
///
/// Build one supported canister WASM with default debug options and return the
/// retained artifacts.
pub fn build_canister(canister_name: &str) -> Result<BuiltCanisterArtifacts, String> {
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
    let compiler_emitted =
        read_wasm_artifact(&artifacts.compiler_emitted).unwrap_or_else(|error| {
            panic!(
                "failed to read compiler-emitted {} canister wasm at {}: {error}",
                fixture.name(),
                artifacts.compiler_emitted.display()
            )
        });
    let final_deployable =
        read_wasm_artifact(&artifacts.final_deployable).unwrap_or_else(|error| {
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

/// Create an application-subnet instance using Testkit's explicit environment contract.
///
/// Connect to `IC_TESTKIT_POCKET_IC_URL` when selected, otherwise use the
/// prepared `POCKET_IC_BIN`. No executable discovery or download occurs.
///
/// # Panics
/// Panics if the selected configuration is invalid or instance startup fails.
#[must_use]
pub fn start_fixture_pocket_ic() -> PocketIc {
    let config = PocketIcStartupConfig::from_env(POCKET_IC_INSTANCE_STARTUP_TIMEOUT)
        .unwrap_or_else(|error| panic!("configure fixture PocketIC: {error}"));
    PocketIcBuilder::new()
        .with_application_subnet()
        .try_build(config)
        .unwrap_or_else(|error| panic!("start fixture PocketIC on governed server: {error}"))
}

/// Deliver a bounded set of generated startup-watchdog messages.
///
/// This helper is used after installation or upgrade when a test needs an
/// ordinary-work-ready canister but does not inspect the startup control
/// surface itself.
pub fn deliver_fixture_startup_watchdog(fixture: &StandaloneCanisterFixture) {
    // Drain asynchronous entropy replies before admitting the next one-second
    // watchdog retry. Ticks alone advance only deterministic execution slices,
    // so they cannot finish startup after its first entropy-pending result.
    // Keep setup bounded for both install and upgrade.
    for _ in 0..8 {
        fixture.pocket_ic().advance_time(Duration::from_secs(1));
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
    let artifacts = build_canister_package_artifacts(
        fixture.package(),
        options,
        &canister_build_label(fixture, options),
    )
    .unwrap_or_else(|err| panic!("{} canister should build: {err}", fixture.name()));

    read_wasm_artifact(artifacts.as_ref()).unwrap_or_else(|err| {
        panic!(
            "failed to read built {} canister wasm at {}: {err}",
            fixture.name(),
            artifacts.as_ref().display()
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

/// Build every maintained canister independently and return retained artifacts.
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
) -> Result<Vec<(&'static str, BuiltCanisterArtifacts)>, String> {
    let root = workspace_root();
    let plan = plan_maintained_canister_builds(&root, options)?;
    let cargo_report = build_cached_cargo_wasm_batch(&plan.specs);

    finish_maintained_canister_build_plan(&root, plan, cargo_report)
}

/// Build both maintained whole-fleet contract profiles with checked input resolution.
///
/// This is the narrow artifact-contract path for concurrent LocalTest and
/// Production readers. Ordinary callers should continue using
/// [`build_maintained_canisters_with_options`].
///
/// Each profile uses the ordinary checked batch owner. A test-local token cannot
/// exclude external source edits, so this path makes no immutability assumption.
/// Changed inputs still fail publication rather than producing a stale artifact.
///
/// # Errors
///
/// Returns an error if planning, either Cargo or post-link batch, or a scoped
/// profile reader fails.
pub fn build_maintained_canister_contract_profiles()
-> Result<MaintainedCanisterContractProfileArtifacts, String> {
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
    std::thread::scope(|scope| {
        let handles = plans
            .into_iter()
            .map(|plan| {
                let build_profile = plan.options.build_profile;
                let root = &root;
                let handle = scope.spawn(move || {
                    let cargo_report = build_cached_cargo_wasm_batch(&plan.specs);
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
    })
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
            encoded_rustflags: configured.encoded_rustflags.as_deref(),
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
    cargo_report: canister_build_cache::CanisterCacheBatchReport<WasmBuildRecord>,
) -> Result<Vec<(&'static str, BuiltCanisterArtifacts)>, String> {
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
    cargo_report: canister_build_cache::CanisterCacheBatchReport<WasmBuildRecord>,
    options: CanisterBuildOptions,
) -> Result<Vec<(&'static str, BuiltCanisterArtifacts)>, String> {
    let mut failures = cargo_report.failures;
    let mut retained = Vec::with_capacity(cargo_report.successes.len());
    for (index, record) in cargo_report.successes {
        let Some((policy, configured)) = configured.get(index) else {
            failures.push(format!(
                "Cargo returned unknown successful entry index {index}"
            ));
            continue;
        };
        let Some(context) = contexts.get(index) else {
            failures.push("post-link batch context mapping was incomplete".to_owned());
            continue;
        };
        match BuiltCanisterArtifacts::from_cargo(record) {
            Ok(artifacts) => retained.push((policy.canister, configured, context, artifacts)),
            Err(error) => failures.push(format!("Cargo [{index}] {}: {error}", policy.canister)),
        }
    }

    if !retained.is_empty() && !matches!(options.profile, CanisterWasmProfile::WasmAttribution) {
        let cache_root = target_dir(root).join("canister-artifact-cache");
        let entries = retained
            .iter()
            .map(|(_, configured, context, artifacts)| PostLinkBatchEntry {
                context,
                compiler_emitted: &artifacts.compiler_emitted,
                final_deployable: &configured.final_deployable,
            })
            .collect::<Vec<_>>();
        let post_link_report = cache_post_link_wasm_batch(root, &cache_root, &entries)?;
        failures.extend(post_link_report.failures);
        for (index, record) in post_link_report.successes {
            let Some((_, _, _, artifacts)) = retained.get_mut(index) else {
                failures.push(format!(
                    "post-link returned unknown successful entry index {index}"
                ));
                continue;
            };
            if let Err(error) = artifacts.retain_post_link(record) {
                failures.push(error);
            }
        }
    }

    if !failures.is_empty() {
        return Err(format!(
            "maintained canister build failed:\n  - {}",
            failures.join("\n  - ")
        ));
    }

    Ok(retained
        .into_iter()
        .map(|(canister, _, _, artifacts)| (canister, artifacts))
        .collect())
}

/// Build one supported SQL canister WASM with explicit options and return the
/// retained artifacts. Keep the result alive while reading its final Wasm path.
pub fn build_canister_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Result<BuiltCanisterArtifacts, String> {
    let package_name = package_for_canister_name(canister_name)?;
    build_canister_package_artifacts(
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

    stage_canister_artifact_paths(
        &artifacts.compiler_emitted,
        &artifacts.final_deployable,
        &root.join(".icp/local/canisters").join(canister_name),
        canister_name,
        options.candid_export.enabled_for_profile(options.profile),
    )
}

// Each file publishes independently, as before; shared filesystem mechanics
// preserve an existing destination when the stream/extractor producer fails.
fn stage_canister_artifact_paths(
    compiler_wasm: &Path,
    deployable_wasm: &Path,
    directory: &Path,
    canister_name: &str,
    candid_enabled: bool,
) -> Result<(PathBuf, Option<PathBuf>), String> {
    let wasm = directory.join(format!("{canister_name}.wasm"));
    let compiler = directory.join(format!("{canister_name}.compiler.wasm"));
    publish_artifact_copy(deployable_wasm, &wasm)?;
    publish_artifact_copy(compiler_wasm, &compiler)?;
    let did = directory.join(format!("{canister_name}.did"));
    if !candid_enabled {
        // Build feature selection owns deliberate Candid omission. Never infer
        // optional export from an extractor's diagnostic prose.
        match fs::remove_file(&did) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("remove stale staged Candid: {error}")),
        }
        return Ok((wasm, None));
    }
    let candid = canister_artifact::extract_canister_candid(&wasm)?;
    ic_host_fs::durable::write_bytes(&did, candid.as_bytes())
        .map_err(|error| format!("publish staged Candid '{}': {error:?}", did.display()))?;
    Ok((wasm, Some(did)))
}

fn publish_artifact_copy(input: &Path, output: &Path) -> Result<(), String> {
    let mut source = fs::File::open(input)
        .map_err(|error| format!("open artifact '{}': {error}", input.display()))?;
    let metadata = source.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err(format!(
            "artifact is not a regular file: '{}'",
            input.display()
        ));
    }
    ic_host_fs::durable::write_with(
        output,
        ic_host_fs::durable::WriteOptions {
            mode: ic_host_fs::durable::PublicationMode::Replace,
            permissions: 0o666,
        },
        |sink| {
            sink.set_permissions(metadata.permissions())?;
            ic_host_artifacts::artifact::copy_reader(&mut source, sink, u64::MAX)
                .map_err(std::io::Error::from)
        },
    )
    // String diagnostics retain the publication phase and separate cleanup cause.
    .map_err(|error| format!("publish artifact '{}': {error:?}", output.display()))?;
    Ok(())
}
