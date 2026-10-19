//! Module: build
//! Responsibility: maintained canister builds, artifact policy and Wasm reports.
//! Does not own: PocketIC installation, simulator lifecycle or integration scenarios.
//! Boundary: reuses IC Build and Host owners within the root workspace.

mod canister_build_cache;
mod plan;

pub mod canister_artifact;
pub mod wasm_measurement;
pub mod wasm_optimizer;

use crate::canister_artifact::read_wasm_artifact;
use crate::canister_build_cache::{
    CargoWasmCacheRequest, PostLinkBatchEntry, PostLinkCacheRequest, build_cached_cargo_wasm,
    build_cached_cargo_wasm_batch, cache_post_link_wasm, cache_post_link_wasm_batch,
    trace_post_link, trace_wasm_build,
};
use crate::plan::{MaintainedCanisterBuildPlan, plan_maintained_canister_builds};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

use ic_build::{
    artifacts::{
        ArtifactCacheOutcome, ArtifactCacheRecord, WasmBuildBatchContractError,
        WasmBuildBatchReport, WasmBuildOutcome, WasmBuildRecord,
    },
    cargo::{
        flags::combined_encoded_rustflags,
        selection::{CargoFeatureSelection, CargoSelection},
    },
};

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
    cargo: WasmBuildRecord,
    post_link: Option<ArtifactCacheRecord>,
}

impl BuiltCanisterArtifacts {
    /// Return the retained Cargo record, including its observed build facts.
    #[must_use]
    pub const fn cargo(&self) -> &WasmBuildRecord {
        &self.cargo
    }

    /// Return the compiler artifact while this owner retains its cache entry.
    #[must_use]
    pub fn compiler_emitted(&self) -> &Path {
        &self.compiler_emitted
    }

    fn from_cargo(record: WasmBuildRecord) -> Result<Self, String> {
        let [path] = record.artifacts() else {
            return Err("canister build must retain exactly one compiler artifact".to_owned());
        };
        Ok(Self {
            compiler_emitted: path.clone(),
            final_deployable: path.clone(),
            cargo: record,
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
    selection: CargoSelection,
    encoded_rustflags: Option<String>,
    final_deployable: PathBuf,
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
    /// Exact maintained package feature profile.
    pub build_profile: CanisterBuildProfile,
}

impl Default for CanisterBuildOptions {
    fn default() -> Self {
        Self {
            profile: CanisterWasmProfile::Debug,
            sql_mode: CanisterSqlMode::Enabled,
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
        .expect("build crate should live under testing/build")
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
        selection: &configured.selection,
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

// Read while the retained Cargo/post-link owner is alive. Variant-specific
// features must not borrow a mutable artifact path from another build.
/// Build and read one caller-described fixture variant under the selected profile.
///
/// Package/features and role labels belong to the scenario. Cargo and post-link
/// records remain alive through the read, so later variants cannot replace the
/// referenced artifact before its bytes have been copied.
///
/// # Errors
/// Returns a build, post-link or retained-artifact read failure.
pub fn build_fixture_variant_wasm_with_profile(
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
    let mut selection = cargo_selection(options.profile, true);
    selection.features.features = features
        .split(',')
        .flat_map(str::split_whitespace)
        .map(str::to_owned)
        .collect();
    let artifacts = build_configured_canister_artifacts(
        &root,
        &target,
        package,
        options,
        label,
        ConfiguredCanisterBuild {
            selection,
            encoded_rustflags: None,
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

    let mut selection = cargo_selection(resolved.profile(), resolved.no_default_features());
    selection.features.features = resolved
        .features()
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect();
    let extra_rustflags = if resolved.path_trimming() {
        wasm_release_path_trim_flags(root)
    } else {
        Vec::new()
    };
    let inherited_encoded = env::var("CARGO_ENCODED_RUSTFLAGS").ok();
    let inherited_ordinary = env::var("RUSTFLAGS").ok();
    let encoded_rustflags = combined_encoded_rustflags(
        inherited_encoded.as_deref(),
        inherited_ordinary.as_deref(),
        &extra_rustflags,
    );

    Ok(ConfiguredCanisterBuild {
        selection,
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
    let features = selected_features
        .iter()
        .copied()
        .filter(|_| options.sql_mode.enabled())
        .collect();

    ResolvedCanisterBuildConfiguration {
        profile: options.profile,
        build_profile: options.build_profile,
        sql_mode: options.sql_mode,
        no_default_features: true,
        path_trimming: matches!(
            options.profile,
            CanisterWasmProfile::WasmRelease | CanisterWasmProfile::WasmAttribution
        ),
        features,
    }
}

// The upstream typed selection is shared by metadata, cache identity and Cargo.
fn cargo_selection(profile: CanisterWasmProfile, no_default_features: bool) -> CargoSelection {
    CargoSelection {
        profile: match profile {
            CanisterWasmProfile::Debug => None,
            _ => Some(profile.as_str().to_owned()),
        },
        features: CargoFeatureSelection {
            default_features: !no_default_features,
            ..Default::default()
        },
        locked: true,
        ..Default::default()
    }
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

/// Read the retained default-profile artifact for one supported fixture.
///
/// # Panics
/// Panics if the fixture is unsupported or its build or artifact read fails.
#[must_use]
pub fn local_fixture_wasm_bytes(canister_name: &str) -> Vec<u8> {
    local_fixture_wasm_bytes_with_options(canister_name, CanisterBuildOptions::default())
}

/// Read one fixture artifact, retaining the default-profile bytes for later installs.
///
/// # Panics
/// Panics if the fixture is unsupported or its build or artifact read fails.
#[must_use]
pub fn local_fixture_wasm_bytes_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Vec<u8> {
    let fixture = fixture_for_canister_name(canister_name)
        .unwrap_or_else(|err| panic!("fixture canister should be supported: {err}"));

    if options == CanisterBuildOptions::default() {
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

fn canister_build_label(fixture: &FixtureCanister, options: CanisterBuildOptions) -> String {
    format!(
        "{} canister build ({})",
        fixture.name(),
        options.profile.as_str(),
    )
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
    let cargo_report = build_cached_cargo_wasm_batch(&plan.cargo_specs(&root));

    finish_maintained_canister_builds(&root, plan, cargo_report)
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
                    let cargo_report = build_cached_cargo_wasm_batch(&plan.cargo_specs(root));
                    finish_maintained_canister_builds(root, plan, cargo_report)
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

fn finish_maintained_canister_builds(
    root: &Path,
    plan: MaintainedCanisterBuildPlan,
    cargo_report: Result<WasmBuildBatchReport, WasmBuildBatchContractError>,
) -> Result<Vec<(&'static str, BuiltCanisterArtifacts)>, String> {
    let options = plan.options;
    let cargo_report = cargo_report.map_err(|error| {
        format!("maintained canister build failed:\n  - Cargo Wasm batch contract failed: {error}")
    })?;
    let mut failures = cargo_report
        .failures()
        .filter_map(|entry| {
            entry.error().map(|error| {
                format!(
                    "Cargo [{}] {} failed after {:?}; details={:?}: {error}",
                    entry.index(),
                    entry.label(),
                    entry.entry_elapsed(),
                    entry.failure_details(),
                )
            })
        })
        .collect::<Vec<_>>();
    let mut retained = Vec::with_capacity(cargo_report.outcomes().count());
    // Upstream entries carry the identity and retained record through both stages.
    for entry in cargo_report.into_entries() {
        let (index, _, outcome, _, _) = entry.into_parts();
        let Ok(outcome) = outcome else {
            continue;
        };
        let record = match outcome {
            WasmBuildOutcome::Built(record) | WasmBuildOutcome::Reused(record) => record,
        };
        let Some(entry) = plan.entries.get(index) else {
            failures.push(format!(
                "Cargo returned unknown successful entry index {index}"
            ));
            continue;
        };
        let policy = entry.policy;
        let configured = &entry.configured;
        let context = &entry.context;
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
        failures.extend(post_link_report.failures().filter_map(|entry| {
            entry.failure().map(|failure| {
                format!(
                    "post-link [{}] {} failed after {:?}: {}",
                    entry.index(),
                    entry.label(),
                    entry.entry_elapsed(),
                    failure,
                )
            })
        }));
        for entry in post_link_report.into_entries() {
            let (index, _, outcome, _) = entry.into_parts();
            let Ok(outcome) = outcome else {
                continue;
            };
            let record = match outcome {
                ArtifactCacheOutcome::Built(record) | ArtifactCacheOutcome::Reused(record) => {
                    record
                }
            };
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
/// Build one supported canister and stage compiler and deployable Wasm artifacts into
/// `.icp/local/canisters/<canister_name>/`.
///

pub fn stage_canister_for_icp(canister_name: &str) -> Result<PathBuf, String> {
    stage_canister_for_icp_with_options(canister_name, CanisterBuildOptions::default())
}

/// Build one supported canister with explicit options and stage Wasm artifacts into `.icp/local/canisters/<canister_name>/`.
pub fn stage_canister_for_icp_with_options(
    canister_name: &str,
    options: CanisterBuildOptions,
) -> Result<PathBuf, String> {
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
    )
}

// Each retained Wasm publishes independently through the shared filesystem owner.
fn stage_canister_artifact_paths(
    compiler_wasm: &Path,
    deployable_wasm: &Path,
    directory: &Path,
    canister_name: &str,
) -> Result<PathBuf, String> {
    let wasm = directory.join(format!("{canister_name}.wasm"));
    let compiler = directory.join(format!("{canister_name}.compiler.wasm"));
    publish_artifact_copy(deployable_wasm, &wasm)?;
    publish_artifact_copy(compiler_wasm, &compiler)?;
    Ok(wasm)
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
