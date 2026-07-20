//! Module: sql_perf_environment
//! Responsibility: complete comparable SQL performance environment identity.
//! Does not own: scenario execution, baseline thresholds, or artifact sharding.
//! Boundary: captures canonical host/canister facts and rejects incomparable reports.

use crate::{
    sql_perf_phase::PERFORMANCE_PHASE_OWNERSHIP_VERSION,
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError},
};

use std::{
    error::Error,
    fmt::{self, Display, Write as _},
    fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

use icydb::db::EntitySchemaDescription;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FIXTURE_PROFILE_VERSION: u32 = 1;
const FIXTURE_GENERATOR_VERSION: u32 = 1;
const WASM_TARGET: &str = "wasm32-unknown-unknown";
const DIAGNOSTICS_ATTRIBUTION_SCHEMA_VERSION: u32 = 2;

///
/// PerfFixtureSurfaceIdentity
///
/// Exact main-fixture cardinality and deterministic distribution identities for one surface.
/// Owned by the checked-in fixture profile and included in its canonical hash.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfFixtureSurfaceIdentity {
    /// Stable matrix surface name.
    pub(crate) surface: String,

    /// Exact row count in the main P1/P2 fixture.
    pub(crate) main_fixture_rows: u32,

    /// Stable identity of the main fixture's value distribution.
    pub(crate) main_distribution: String,

    /// Stable identity of zero/one/quarter/all scale predicates for the surface.
    pub(crate) scale_distribution: String,

    /// Exact blob payload profile, or typed non-applicability code.
    pub(crate) payload_profile: String,
}

///
/// PerfSeedMaterial
///
/// Typed seed policy for deterministic performance fixture generation.
/// Owned by fixture identity; absence is explicit because the current profile is enumerated.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PerfSeedMaterial {
    /// The profile is fully checked in and uses no pseudo-random seed.
    NotApplicableCheckedInProfile,
}

///
/// PerfFixtureProfileIdentity
///
/// Canonical deterministic fixture facts shared by comparable performance runs.
/// Owned by the performance profile rather than inferred from query results.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfFixtureProfileIdentity {
    /// Hard-cut fixture profile version.
    pub(crate) version: u32,

    /// Canonical BLAKE3 identity of every field below.
    pub(crate) profile_hash: String,

    /// Fixture generator version.
    pub(crate) generator_version: u32,

    /// Typed seed policy.
    pub(crate) seed_material: PerfSeedMaterial,

    /// Exact scale scenario-set identity.
    pub(crate) scale_scenario_set_hash: String,

    /// Exact scale row-cardinality ladder.
    pub(crate) scale_row_cardinalities: Vec<u32>,

    /// Exact result-window ladder.
    pub(crate) result_window_sizes: Vec<u32>,

    /// Exact reviewed selectivity classes.
    pub(crate) selectivity_classes: Vec<String>,

    /// Stable ordered surface facts.
    pub(crate) surfaces: Vec<PerfFixtureSurfaceIdentity>,
}

///
/// PerfCanisterBuildIdentity
///
/// Comparable build configuration for the measured SQL audit canister.
/// Owned by the performance runner; raw WASM content belongs to subject identity.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfCanisterBuildIdentity {
    /// Cargo profile used for measurement.
    pub(crate) cargo_profile: String,

    /// Target-sensitive generation mode.
    pub(crate) build_target: String,

    /// Whether SQL package defaults remain enabled.
    pub(crate) sql_mode: String,

    /// Whether Candid metadata is embedded in the measured module.
    pub(crate) candid_export: bool,

    /// Whether workspace paths are removed from WASM diagnostics.
    pub(crate) path_trimming: bool,
}

///
/// PerfCacheModePolicy
///
/// Checked-in cache-state proof required by current performance artifacts.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PerfCacheModePolicy {
    /// P1 is cold; P2 uses isolated cold and typed-hit-proven warm samples.
    IsolatedColdAndTypedWarmV1,
}

///
/// PerfInstructionCounterPolicy
///
/// Checked-in source and interpretation of the production-cost signal.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PerfInstructionCounterPolicy {
    /// IC performance counter type 1, measured as canister-local deltas.
    IcPerformanceCounter1LocalDeltaV1,
}

///
/// PerfComparableEnvironmentIdentity
///
/// Every environment fact that must match before a baseline delta is meaningful.
/// Owned by baseline admission and compared field by field.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfComparableEnvironmentIdentity {
    /// Checked-in performance profile version.
    pub(crate) performance_profile_version: u32,

    /// Exact P1 scenario-set identity.
    pub(crate) p1_scenario_set_hash: String,

    /// Exact accepted runtime-schema identity from the installed canister.
    pub(crate) accepted_snapshot_hash: String,

    /// Complete deterministic fixture identity.
    pub(crate) fixture: PerfFixtureProfileIdentity,

    /// Exact comparable canister build configuration.
    pub(crate) canister_build: PerfCanisterBuildIdentity,

    /// Full `rustc -vV` identity.
    pub(crate) rust_toolchain: String,

    /// Rust compilation target for measured WASM.
    pub(crate) wasm_target: String,

    /// Exact sorted canister feature set.
    pub(crate) feature_set: Vec<String>,

    /// PocketIC binary-reported version.
    pub(crate) pocket_ic_version: String,

    /// SHA-256 of the exact PocketIC binary.
    pub(crate) pocket_ic_sha256: String,

    /// Diagnostics/attribution DTO schema version.
    pub(crate) diagnostics_attribution_schema_version: u32,

    /// Versioned phase-ownership schema.
    pub(crate) phase_ownership_version: u32,

    /// Checked-in cold/warm cache proof policy.
    pub(crate) cache_mode_policy: PerfCacheModePolicy,

    /// Checked-in instruction counter policy.
    pub(crate) instruction_counter_policy: PerfInstructionCounterPolicy,
}

///
/// PerfSubjectIdentity
///
/// Expected before/after subject facts recorded but deliberately excluded from comparability.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfSubjectIdentity {
    /// Source revision measured by the run.
    pub(crate) source_revision: String,

    /// Whether tracked or untracked source state differs from the revision.
    pub(crate) source_dirty: bool,

    /// SHA-256 of the workspace lockfile used to build the subject.
    pub(crate) lockfile_sha256: String,

    /// SHA-256 of the raw non-gzipped measured WASM.
    pub(crate) raw_wasm_sha256: String,

    /// Exact raw non-gzipped measured WASM byte count.
    pub(crate) raw_wasm_bytes: u64,
}

///
/// PerfEnvironmentIdentity
///
/// Complete comparable environment and measured subject identity for one artifact.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerfEnvironmentIdentity {
    /// Facts required to match across a baseline pair.
    pub(crate) comparable: PerfComparableEnvironmentIdentity,

    /// Before/after subject facts allowed to differ.
    pub(crate) subject: PerfSubjectIdentity,
}

///
/// PerfEnvironmentField
///
/// One independently compared environment field that can make reports incomparable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PerfEnvironmentField {
    /// Performance profile or P1 scenario identity.
    PerformanceProfile,
    /// Accepted runtime schema.
    AcceptedSnapshot,
    /// Fixture profile, cardinalities, or distribution.
    FixtureProfile,
    /// Canister build configuration.
    CanisterBuild,
    /// Rust toolchain.
    RustToolchain,
    /// WASM target.
    WasmTarget,
    /// Canister feature set.
    FeatureSet,
    /// PocketIC version or binary.
    PocketIc,
    /// Diagnostics/attribution schema.
    DiagnosticsAttribution,
    /// Phase ownership schema.
    PhaseOwnership,
    /// Cache mode policy.
    CacheModePolicy,
    /// Instruction counter policy.
    InstructionCounterPolicy,
}

/// Capture the complete current SQL performance environment identity.
///
/// # Errors
///
/// Returns a typed error for invalid profile/schema encoding, canonical hashing,
/// command execution, UTF-8, filesystem reads, or invalid captured identity.
pub(crate) fn capture_perf_environment(
    profile: PerformanceProfile,
    workspace_root: &Path,
    wasm_profile: &str,
    wasm_bytes: &[u8],
    accepted_descriptions: &[EntitySchemaDescription],
    pocket_ic_binary: &Path,
) -> Result<PerfEnvironmentIdentity, PerfEnvironmentError> {
    profile
        .validate()
        .map_err(PerfEnvironmentError::InvalidProfile)?;
    let accepted_bytes = candid::encode_one(accepted_descriptions.to_vec())
        .map_err(PerfEnvironmentError::AcceptedSnapshotEncoding)?;
    let accepted_snapshot_hash = accepted_snapshot_hash(&accepted_bytes)?;
    let lockfile = workspace_root.join("Cargo.lock");
    let lockfile_bytes = fs::read(&lockfile).map_err(|source| PerfEnvironmentError::Io {
        path: lockfile.clone(),
        operation: "read",
        source,
    })?;
    let pocket_ic_bytes =
        fs::read(pocket_ic_binary).map_err(|source| PerfEnvironmentError::Io {
            path: pocket_ic_binary.to_path_buf(),
            operation: "read",
            source,
        })?;
    let rust_toolchain = command_text(workspace_root, "rustc", &["-vV"])?;
    let pocket_ic_version = command_path_text(workspace_root, pocket_ic_binary, &["--version"])?;
    let source_revision = command_text(workspace_root, "git", &["rev-parse", "HEAD"])?;
    let source_status = command_text(
        workspace_root,
        "git",
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    )?;
    let identity = PerfEnvironmentIdentity {
        comparable: PerfComparableEnvironmentIdentity {
            performance_profile_version: profile.version(),
            p1_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
            accepted_snapshot_hash,
            fixture: current_fixture_profile(profile)?,
            canister_build: PerfCanisterBuildIdentity {
                cargo_profile: wasm_profile.to_string(),
                build_target: "local".to_string(),
                sql_mode: "enabled".to_string(),
                candid_export: false,
                path_trimming: true,
            },
            rust_toolchain,
            wasm_target: WASM_TARGET.to_string(),
            feature_set: ["diagnostics", "sql", "sql-explain"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            pocket_ic_version,
            pocket_ic_sha256: sha256_hex(&pocket_ic_bytes),
            diagnostics_attribution_schema_version: DIAGNOSTICS_ATTRIBUTION_SCHEMA_VERSION,
            phase_ownership_version: PERFORMANCE_PHASE_OWNERSHIP_VERSION,
            cache_mode_policy: PerfCacheModePolicy::IsolatedColdAndTypedWarmV1,
            instruction_counter_policy:
                PerfInstructionCounterPolicy::IcPerformanceCounter1LocalDeltaV1,
        },
        subject: PerfSubjectIdentity {
            source_revision,
            source_dirty: !source_status.is_empty(),
            lockfile_sha256: sha256_hex(&lockfile_bytes),
            raw_wasm_sha256: sha256_hex(wasm_bytes),
            raw_wasm_bytes: u64::try_from(wasm_bytes.len())
                .map_err(|_| PerfEnvironmentError::WasmSizeOverflow)?,
        },
    };
    validate_perf_environment(profile, &identity)?;

    Ok(identity)
}

/// Validate one captured or decoded environment identity against the current profile.
///
/// # Errors
///
/// Returns a typed error for missing facts, malformed hashes, unsorted features,
/// fixture-hash drift, or profile/build-policy drift.
pub(crate) fn validate_perf_environment(
    profile: PerformanceProfile,
    identity: &PerfEnvironmentIdentity,
) -> Result<(), PerfEnvironmentError> {
    profile
        .validate()
        .map_err(PerfEnvironmentError::InvalidProfile)?;
    let comparable = &identity.comparable;
    if comparable.performance_profile_version != profile.version()
        || comparable.p1_scenario_set_hash != profile.expected_scenario_set_hash()
        || comparable.fixture.scale_scenario_set_hash != profile.expected_scale_scenario_set_hash()
        || comparable.canister_build.cargo_profile != "wasm-release"
        || comparable.canister_build.build_target != "local"
        || comparable.canister_build.sql_mode != "enabled"
        || comparable.canister_build.candid_export
        || !comparable.canister_build.path_trimming
        || comparable.wasm_target != WASM_TARGET
        || comparable.diagnostics_attribution_schema_version
            != DIAGNOSTICS_ATTRIBUTION_SCHEMA_VERSION
        || comparable.phase_ownership_version != PERFORMANCE_PHASE_OWNERSHIP_VERSION
    {
        return Err(PerfEnvironmentError::InvalidIdentity(
            "fixed environment contract drifted",
        ));
    }
    let required_text = [
        comparable.accepted_snapshot_hash.as_str(),
        comparable.rust_toolchain.as_str(),
        comparable.pocket_ic_version.as_str(),
        comparable.pocket_ic_sha256.as_str(),
        identity.subject.source_revision.as_str(),
        identity.subject.lockfile_sha256.as_str(),
        identity.subject.raw_wasm_sha256.as_str(),
    ];
    if required_text.iter().any(|value| value.is_empty())
        || !is_sha256(&comparable.accepted_snapshot_hash)
        || !is_sha256(&comparable.pocket_ic_sha256)
        || !is_sha256(&identity.subject.lockfile_sha256)
        || !is_sha256(&identity.subject.raw_wasm_sha256)
        || identity.subject.raw_wasm_bytes == 0
    {
        return Err(PerfEnvironmentError::InvalidIdentity(
            "required environment identity field is empty or malformed",
        ));
    }
    let mut sorted_features = comparable.feature_set.clone();
    sorted_features.sort();
    sorted_features.dedup();
    if sorted_features != comparable.feature_set {
        return Err(PerfEnvironmentError::InvalidIdentity(
            "feature set must be sorted and duplicate-free",
        ));
    }
    validate_fixture_profile(profile, &comparable.fixture)?;

    Ok(())
}

/// Require two artifacts to have the same comparable environment.
///
/// # Errors
///
/// Returns the first typed differing field in stable contract order. Subject
/// revision, dependency lock, and raw WASM identity are intentionally not
/// compared because their performance effects belong to the measured change.
pub(crate) fn require_comparable_environment(
    baseline: &PerfEnvironmentIdentity,
    current: &PerfEnvironmentIdentity,
) -> Result<(), PerfEnvironmentMismatch> {
    let baseline = &baseline.comparable;
    let current = &current.comparable;
    let checks = [
        (
            baseline.performance_profile_version == current.performance_profile_version
                && baseline.p1_scenario_set_hash == current.p1_scenario_set_hash,
            PerfEnvironmentField::PerformanceProfile,
        ),
        (
            baseline.accepted_snapshot_hash == current.accepted_snapshot_hash,
            PerfEnvironmentField::AcceptedSnapshot,
        ),
        (
            baseline.fixture == current.fixture,
            PerfEnvironmentField::FixtureProfile,
        ),
        (
            baseline.canister_build == current.canister_build,
            PerfEnvironmentField::CanisterBuild,
        ),
        (
            baseline.rust_toolchain == current.rust_toolchain,
            PerfEnvironmentField::RustToolchain,
        ),
        (
            baseline.wasm_target == current.wasm_target,
            PerfEnvironmentField::WasmTarget,
        ),
        (
            baseline.feature_set == current.feature_set,
            PerfEnvironmentField::FeatureSet,
        ),
        (
            baseline.pocket_ic_version == current.pocket_ic_version
                && baseline.pocket_ic_sha256 == current.pocket_ic_sha256,
            PerfEnvironmentField::PocketIc,
        ),
        (
            baseline.diagnostics_attribution_schema_version
                == current.diagnostics_attribution_schema_version,
            PerfEnvironmentField::DiagnosticsAttribution,
        ),
        (
            baseline.phase_ownership_version == current.phase_ownership_version,
            PerfEnvironmentField::PhaseOwnership,
        ),
        (
            baseline.cache_mode_policy == current.cache_mode_policy,
            PerfEnvironmentField::CacheModePolicy,
        ),
        (
            baseline.instruction_counter_policy == current.instruction_counter_policy,
            PerfEnvironmentField::InstructionCounterPolicy,
        ),
    ];
    if let Some((_, field)) = checks.into_iter().find(|(matches, _)| !matches) {
        return Err(PerfEnvironmentMismatch { field });
    }

    Ok(())
}

/// Require one measured subject to come from an exact clean source revision.
///
/// Dirty artifacts remain useful for local method validation, but cannot satisfy
/// reviewed baseline discovery or a closeout regression verdict.
///
/// # Errors
///
/// Returns a typed subject-state error when tracked or untracked source differs
/// from the recorded revision.
pub(crate) const fn require_clean_perf_subject(
    identity: &PerfEnvironmentIdentity,
) -> Result<(), PerfSubjectStateError> {
    if identity.subject.source_dirty {
        return Err(PerfSubjectStateError::DirtySource);
    }

    Ok(())
}

fn current_fixture_profile(
    profile: PerformanceProfile,
) -> Result<PerfFixtureProfileIdentity, PerfEnvironmentError> {
    let mut fixture = PerfFixtureProfileIdentity {
        version: FIXTURE_PROFILE_VERSION,
        profile_hash: String::new(),
        generator_version: FIXTURE_GENERATOR_VERSION,
        seed_material: PerfSeedMaterial::NotApplicableCheckedInProfile,
        scale_scenario_set_hash: profile.expected_scale_scenario_set_hash().to_string(),
        scale_row_cardinalities: profile.scale_row_cardinalities().to_vec(),
        result_window_sizes: profile.result_window_sizes().to_vec(),
        selectivity_classes: ["zero", "one", "quarter", "all"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        surfaces: fixture_surfaces(),
    };
    fixture.profile_hash = fixture_profile_hash(&fixture)?;

    Ok(fixture)
}

fn fixture_surfaces() -> Vec<PerfFixtureSurfaceIdentity> {
    [
        (
            "account",
            6,
            "handcrafted-account-v1",
            "handle-a/id-1/gold-active/all",
            "not_applicable",
        ),
        (
            "blob",
            6,
            "handcrafted-blob-v1",
            "label-blob/id-1/bucket-10/all",
            "blob_cycle_v1",
        ),
        (
            "heap_user",
            512,
            "generated-user-mirror-v1",
            "name-A/id-1/age-24-40/all",
            "not_applicable",
        ),
        (
            "journaled_user",
            512,
            "generated-user-mirror-v1",
            "name-A/id-1/age-24-40/all",
            "not_applicable",
        ),
        (
            "token",
            260,
            "branch-pressure-token-v1",
            "missing/id-20001/target-collection/all",
            "not_applicable",
        ),
        (
            "user",
            6,
            "handcrafted-user-v1",
            "name-A/id-1/age-24-40/all",
            "not_applicable",
        ),
    ]
    .into_iter()
    .map(
        |(surface, main_fixture_rows, main_distribution, scale_distribution, payload_profile)| {
            PerfFixtureSurfaceIdentity {
                surface: surface.to_string(),
                main_fixture_rows,
                main_distribution: main_distribution.to_string(),
                scale_distribution: scale_distribution.to_string(),
                payload_profile: payload_profile.to_string(),
            }
        },
    )
    .collect()
}

fn validate_fixture_profile(
    profile: PerformanceProfile,
    fixture: &PerfFixtureProfileIdentity,
) -> Result<(), PerfEnvironmentError> {
    let expected = current_fixture_profile(profile)?;
    if fixture != &expected {
        return Err(PerfEnvironmentError::InvalidIdentity(
            "fixture profile differs from current canonical facts",
        ));
    }

    Ok(())
}

fn fixture_profile_hash(
    fixture: &PerfFixtureProfileIdentity,
) -> Result<String, PerfEnvironmentError> {
    let mut hasher = CanonicalIdentityHasher::new(b"icydb-sql-perf-fixture/v1");
    hasher.u32("version", fixture.version)?;
    hasher.u32("generator_version", fixture.generator_version)?;
    hasher.text("seed_material", "not_applicable_checked_in_profile")?;
    hasher.text("scale_scenario_set_hash", &fixture.scale_scenario_set_hash)?;
    for cardinality in &fixture.scale_row_cardinalities {
        hasher.u32("scale_row_cardinality", *cardinality)?;
    }
    for window in &fixture.result_window_sizes {
        hasher.u32("result_window", *window)?;
    }
    for selectivity in &fixture.selectivity_classes {
        hasher.text("selectivity", selectivity)?;
    }
    for surface in &fixture.surfaces {
        hasher.text("surface", &surface.surface)?;
        hasher.u32("main_fixture_rows", surface.main_fixture_rows)?;
        hasher.text("main_distribution", &surface.main_distribution)?;
        hasher.text("scale_distribution", &surface.scale_distribution)?;
        hasher.text("payload_profile", &surface.payload_profile)?;
    }

    Ok(hasher.finish())
}

fn accepted_snapshot_hash(encoded: &[u8]) -> Result<String, PerfEnvironmentError> {
    let mut hasher = CanonicalIdentityHasher::new(b"icydb-sql-perf-accepted-snapshot/v1");
    hasher.bytes("accepted_descriptions_candid", encoded)?;

    Ok(hasher.finish())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }

    encoded
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn command_text(
    current_dir: &Path,
    program: &'static str,
    arguments: &[&str],
) -> Result<String, PerfEnvironmentError> {
    command_path_text(current_dir, Path::new(program), arguments)
}

fn command_path_text(
    current_dir: &Path,
    program: &Path,
    arguments: &[&str],
) -> Result<String, PerfEnvironmentError> {
    let output = Command::new(program)
        .current_dir(current_dir)
        .args(arguments)
        .output()
        .map_err(|source| PerfEnvironmentError::CommandIo {
            program: program.to_path_buf(),
            source,
        })?;
    if !output.status.success() {
        return Err(PerfEnvironmentError::CommandStatus {
            program: program.to_path_buf(),
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    let stdout =
        String::from_utf8(output.stdout).map_err(|source| PerfEnvironmentError::CommandUtf8 {
            program: program.to_path_buf(),
            source,
        })?;

    Ok(stdout.trim().to_string())
}

struct CanonicalIdentityHasher {
    hasher: blake3::Hasher,
}

impl CanonicalIdentityHasher {
    fn new(domain: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        Self { hasher }
    }

    fn text(&mut self, label: &'static str, value: &str) -> Result<(), PerfEnvironmentError> {
        self.bytes(label, value.as_bytes())
    }

    fn u32(&mut self, label: &'static str, value: u32) -> Result<(), PerfEnvironmentError> {
        self.bytes(label, &value.to_be_bytes())
    }

    fn bytes(&mut self, label: &'static str, value: &[u8]) -> Result<(), PerfEnvironmentError> {
        let label_length = u32::try_from(label.len())
            .map_err(|_| PerfEnvironmentError::CanonicalFieldTooLong(label))?;
        let value_length = u64::try_from(value.len())
            .map_err(|_| PerfEnvironmentError::CanonicalFieldTooLong(label))?;
        self.hasher.update(&label_length.to_be_bytes());
        self.hasher.update(label.as_bytes());
        self.hasher.update(&value_length.to_be_bytes());
        self.hasher.update(value);

        Ok(())
    }

    fn finish(self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

/// Typed mismatch that prevents baseline comparison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PerfEnvironmentMismatch {
    /// First differing comparable field in stable contract order.
    pub(crate) field: PerfEnvironmentField,
}

impl Display for PerfEnvironmentMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "performance environments differ at {:?}",
            self.field,
        )
    }
}

impl Error for PerfEnvironmentMismatch {}

///
/// PerfSubjectStateError
///
/// Typed subject state that cannot participate in release performance evidence.
/// Owned by environment admission and preserved by selection and comparison errors.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PerfSubjectStateError {
    /// Tracked or untracked source differs from the recorded revision.
    DirtySource,
}

impl Display for PerfSubjectStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirtySource => {
                formatter.write_str("performance subject was measured from a dirty source worktree")
            }
        }
    }
}

impl Error for PerfSubjectStateError {}

/// Typed failure while capturing or validating environment identity.
#[derive(Debug)]
pub(crate) enum PerfEnvironmentError {
    /// Accepted schema descriptions could not be encoded canonically.
    AcceptedSnapshotEncoding(candid::Error),
    /// One canonical identity field exceeded its fixed length prefix.
    CanonicalFieldTooLong(&'static str),
    /// A child command could not start.
    CommandIo {
        /// Program path.
        program: PathBuf,
        /// Process spawn cause.
        source: io::Error,
    },
    /// A child command returned a failure status.
    CommandStatus {
        /// Program path.
        program: PathBuf,
        /// Failure status.
        status: ExitStatus,
        /// Trimmed standard error.
        stderr: String,
    },
    /// A child command returned non-UTF-8 output.
    CommandUtf8 {
        /// Program path.
        program: PathBuf,
        /// UTF-8 decoding cause.
        source: std::string::FromUtf8Error,
    },
    /// One captured or decoded identity violates the current contract.
    InvalidIdentity(&'static str),
    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),
    /// A required file could not be read.
    Io {
        /// Affected path.
        path: PathBuf,
        /// Human-readable operation.
        operation: &'static str,
        /// I/O cause.
        source: io::Error,
    },
    /// Raw WASM length cannot be represented in the artifact.
    WasmSizeOverflow,
}

impl Display for PerfEnvironmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AcceptedSnapshotEncoding(source) => {
                write!(
                    formatter,
                    "accepted schema descriptions could not be encoded: {source}"
                )
            }
            Self::CanonicalFieldTooLong(field) => {
                write!(
                    formatter,
                    "canonical environment field {field:?} is too long"
                )
            }
            Self::CommandIo { program, source } => {
                write!(
                    formatter,
                    "environment command {} could not start: {source}",
                    program.display()
                )
            }
            Self::CommandStatus {
                program,
                status,
                stderr,
            } => write!(
                formatter,
                "environment command {} failed with {status}: {stderr}",
                program.display(),
            ),
            Self::CommandUtf8 { program, source } => write!(
                formatter,
                "environment command {} returned invalid UTF-8: {source}",
                program.display(),
            ),
            Self::InvalidIdentity(detail) => {
                write!(
                    formatter,
                    "invalid performance environment identity: {detail}"
                )
            }
            Self::InvalidProfile(source) => {
                write!(formatter, "invalid performance profile: {source}")
            }
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "environment file {} could not be {operation}: {source}",
                path.display()
            ),
            Self::WasmSizeOverflow => formatter.write_str("raw WASM byte count overflowed u64"),
        }
    }
}

impl Error for PerfEnvironmentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AcceptedSnapshotEncoding(source) => Some(source),
            Self::CommandIo { source, .. } | Self::Io { source, .. } => Some(source),
            Self::CommandUtf8 { source, .. } => Some(source),
            Self::InvalidProfile(source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::sql_perf_profile::SQL_PERFORMANCE_PROFILE;

    use super::*;

    pub(crate) fn identity() -> PerfEnvironmentIdentity {
        PerfEnvironmentIdentity {
            comparable: PerfComparableEnvironmentIdentity {
                performance_profile_version: SQL_PERFORMANCE_PROFILE.version(),
                p1_scenario_set_hash: SQL_PERFORMANCE_PROFILE
                    .expected_scenario_set_hash()
                    .to_string(),
                accepted_snapshot_hash: "11".repeat(32),
                fixture: current_fixture_profile(SQL_PERFORMANCE_PROFILE)
                    .expect("fixture identity should build"),
                canister_build: PerfCanisterBuildIdentity {
                    cargo_profile: "wasm-release".to_string(),
                    build_target: "local".to_string(),
                    sql_mode: "enabled".to_string(),
                    candid_export: false,
                    path_trimming: true,
                },
                rust_toolchain: "rustc test".to_string(),
                wasm_target: WASM_TARGET.to_string(),
                feature_set: vec![
                    "diagnostics".to_string(),
                    "sql".to_string(),
                    "sql-explain".to_string(),
                ],
                pocket_ic_version: "pocket-ic-server test".to_string(),
                pocket_ic_sha256: "33".repeat(32),
                diagnostics_attribution_schema_version: DIAGNOSTICS_ATTRIBUTION_SCHEMA_VERSION,
                phase_ownership_version: PERFORMANCE_PHASE_OWNERSHIP_VERSION,
                cache_mode_policy: PerfCacheModePolicy::IsolatedColdAndTypedWarmV1,
                instruction_counter_policy:
                    PerfInstructionCounterPolicy::IcPerformanceCounter1LocalDeltaV1,
            },
            subject: PerfSubjectIdentity {
                source_revision: "44".repeat(20),
                source_dirty: false,
                lockfile_sha256: "22".repeat(32),
                raw_wasm_sha256: "55".repeat(32),
                raw_wasm_bytes: 1,
            },
        }
    }

    #[test]
    fn fixture_and_accepted_snapshot_hashes_have_golden_vectors() {
        let fixture = current_fixture_profile(SQL_PERFORMANCE_PROFILE)
            .expect("fixture identity should build");
        assert_eq!(
            fixture.profile_hash,
            "66ae745f871de9b7a8335e728f22182a36154b8fe4630d360ded8730105b20d5",
        );
        let accepted =
            accepted_snapshot_hash(b"accepted-schema-test").expect("accepted payload should hash");
        assert_eq!(
            accepted,
            "cfb265638e7b5b8fd3cdbe7b50e750ea074768928811074c2cc5b830e0b3745d",
        );
    }

    #[test]
    fn environment_comparison_ignores_subject_but_rejects_comparable_drift() {
        let baseline = identity();
        let mut current = baseline.clone();
        current.subject.source_revision = "66".repeat(20);
        current.subject.lockfile_sha256 = "99".repeat(32);
        current.subject.raw_wasm_sha256 = "77".repeat(32);
        assert_eq!(require_comparable_environment(&baseline, &current), Ok(()));

        current.comparable.accepted_snapshot_hash = "88".repeat(32);
        assert_eq!(
            require_comparable_environment(&baseline, &current),
            Err(PerfEnvironmentMismatch {
                field: PerfEnvironmentField::AcceptedSnapshot,
            }),
        );
    }

    #[test]
    fn environment_comparison_classifies_each_variable_comparable_field() {
        let baseline = identity();
        let assert_field = |current: PerfEnvironmentIdentity, field| {
            assert_eq!(
                require_comparable_environment(&baseline, &current),
                Err(PerfEnvironmentMismatch { field }),
            );
        };

        let mut current = baseline.clone();
        current.comparable.p1_scenario_set_hash = "00".repeat(32);
        assert_field(current, PerfEnvironmentField::PerformanceProfile);

        let mut current = baseline.clone();
        current.comparable.accepted_snapshot_hash = "00".repeat(32);
        assert_field(current, PerfEnvironmentField::AcceptedSnapshot);

        let mut current = baseline.clone();
        current.comparable.fixture.generator_version += 1;
        assert_field(current, PerfEnvironmentField::FixtureProfile);

        let mut current = baseline.clone();
        current.comparable.canister_build.path_trimming = false;
        assert_field(current, PerfEnvironmentField::CanisterBuild);

        let mut current = baseline.clone();
        current.comparable.rust_toolchain.push_str(" changed");
        assert_field(current, PerfEnvironmentField::RustToolchain);

        let mut current = baseline.clone();
        current.comparable.wasm_target.push_str("-changed");
        assert_field(current, PerfEnvironmentField::WasmTarget);

        let mut current = baseline.clone();
        current.comparable.feature_set.push("changed".to_string());
        assert_field(current, PerfEnvironmentField::FeatureSet);

        let mut current = baseline.clone();
        current.comparable.pocket_ic_version.push_str(" changed");
        assert_field(current, PerfEnvironmentField::PocketIc);

        let mut current = baseline.clone();
        current.comparable.diagnostics_attribution_schema_version += 1;
        assert_field(current, PerfEnvironmentField::DiagnosticsAttribution);

        let mut current = baseline.clone();
        current.comparable.phase_ownership_version += 1;
        assert_field(current, PerfEnvironmentField::PhaseOwnership);
    }

    #[test]
    fn single_variant_environment_policies_reject_unknown_serialized_values() {
        let mut encoded = serde_json::to_value(identity()).expect("test identity should encode");
        encoded["comparable"]["cache_mode_policy"] = serde_json::json!("legacy_cache_mode");
        assert!(serde_json::from_value::<PerfEnvironmentIdentity>(encoded).is_err());

        let mut encoded = serde_json::to_value(identity()).expect("test identity should encode");
        encoded["comparable"]["instruction_counter_policy"] = serde_json::json!("legacy_counter");
        assert!(serde_json::from_value::<PerfEnvironmentIdentity>(encoded).is_err());
    }

    #[test]
    fn environment_validation_rejects_unsorted_features_and_fixture_drift() {
        let mut environment = identity();
        validate_perf_environment(SQL_PERFORMANCE_PROFILE, &environment)
            .expect("current test identity should validate");
        environment.comparable.feature_set.swap(0, 1);
        assert!(matches!(
            validate_perf_environment(SQL_PERFORMANCE_PROFILE, &environment),
            Err(PerfEnvironmentError::InvalidIdentity(
                "feature set must be sorted and duplicate-free"
            ))
        ));

        let mut environment = identity();
        environment.comparable.fixture.surfaces[0].main_fixture_rows += 1;
        assert!(matches!(
            validate_perf_environment(SQL_PERFORMANCE_PROFILE, &environment),
            Err(PerfEnvironmentError::InvalidIdentity(
                "fixture profile differs from current canonical facts"
            ))
        ));

        let mut environment = identity();
        environment.subject.lockfile_sha256 = "invalid".to_string();
        assert!(matches!(
            validate_perf_environment(SQL_PERFORMANCE_PROFILE, &environment),
            Err(PerfEnvironmentError::InvalidIdentity(
                "required environment identity field is empty or malformed"
            ))
        ));
    }
}
