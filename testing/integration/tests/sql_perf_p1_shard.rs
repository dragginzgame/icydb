//! Module: sql_perf_p1_shard
//! Responsibility: independently executable P1 shard artifacts and exact merge authority.
//! Does not own: scenario construction, PocketIC execution, P2 selection, or final rendering.
//! Boundary: validates one deterministic shard at a time and merges exactly eight complete shards.

use crate::{
    MatrixFailure, MatrixSample, expected_phase_reconciliations,
    sql_perf_environment::{
        PerfEnvironmentError, PerfEnvironmentIdentity, validate_perf_environment,
    },
    sql_perf_measurement::{PerformanceMeasurementCoverage, current_measurement_coverage},
    sql_perf_phase::{PhaseOwnershipTable, current_phase_ownership},
    sql_perf_profile::PerformanceProfile,
    sql_perf_receipt::{
        P1ReceiptError, P1ShardReceipt, p1_shard_receipt, validate_p1_shard_receipts,
    },
};

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// One independently executable P1 shard and its complete typed outcomes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P1ShardReport {
    /// Checked-in performance profile version.
    performance_profile_version: u32,
    /// Full expected scenario-set identity shared by every shard.
    expected_scenario_set_hash: String,
    /// Required canister build profile.
    canister_wasm_profile: String,
    /// Versioned phase-ownership contract used by the samples.
    phase_ownership: PhaseOwnershipTable,
    /// Canonical measured and explicitly unmeasured resource dimensions.
    measurement_coverage: PerformanceMeasurementCoverage,
    /// Complete comparable environment and measured subject identity.
    environment: PerfEnvironmentIdentity,
    /// Exact membership and outcome receipt for this shard.
    receipt: P1ShardReceipt,
    /// Successful scenario samples assigned to this shard.
    samples: Vec<MatrixSample>,
    /// Typed scenario failures assigned to this shard.
    failures: Vec<MatrixFailure>,
}

/// Complete outcomes produced by the one authoritative eight-shard merge.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MergedP1ShardReports {
    /// Exact environment shared by every independently produced shard.
    pub(crate) environment: PerfEnvironmentIdentity,
    /// Exact receipts ordered by zero-based shard index.
    pub(crate) receipts: Vec<P1ShardReceipt>,
    /// Successful samples ordered by stable scenario identity.
    pub(crate) samples: Vec<MatrixSample>,
    /// Typed failures ordered by stable scenario identity.
    pub(crate) failures: Vec<MatrixFailure>,
}

/// Build one current-format P1 shard report from its deterministic outcomes.
///
/// # Errors
///
/// Returns a typed validation error when the shard index, declaration, outcome
/// membership, build profile, phase ownership, or reconciliation is invalid.
pub(crate) fn build_p1_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    environment: PerfEnvironmentIdentity,
    shard_index: u8,
    declared_ids: &[&str],
    samples: Vec<MatrixSample>,
    failures: Vec<MatrixFailure>,
) -> Result<P1ShardReport, P1ShardReportValidationError> {
    let successful_ids = samples
        .iter()
        .map(|sample| sample.key.as_str())
        .collect::<Vec<_>>();
    let failed_ids = failures
        .iter()
        .map(|failure| failure.key.as_str())
        .collect::<Vec<_>>();
    let receipt = p1_shard_receipt(
        profile,
        shard_index,
        declared_ids,
        &successful_ids,
        &failed_ids,
    )
    .map_err(P1ShardReportValidationError::InvalidReceipt)?;
    let report = P1ShardReport {
        performance_profile_version: profile.version(),
        expected_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
        canister_wasm_profile: required_wasm_profile.to_string(),
        phase_ownership: current_phase_ownership(),
        measurement_coverage: current_measurement_coverage(),
        environment,
        receipt,
        samples,
        failures,
    };
    validate_p1_shard_report(profile, required_wasm_profile, declared_ids, &report)?;

    Ok(report)
}

/// Validate one P1 shard against the current declaration and its serialized outcomes.
///
/// # Errors
///
/// Returns a typed validation error for identity, build-profile, phase, receipt,
/// membership, or outcome drift.
pub(crate) fn validate_p1_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declared_ids: &[&str],
    report: &P1ShardReport,
) -> Result<(), P1ShardReportValidationError> {
    if report.performance_profile_version != profile.version() {
        return Err(P1ShardReportValidationError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.expected_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(P1ShardReportValidationError::ScenarioSetHash {
            expected: profile.expected_scenario_set_hash(),
            actual: report.expected_scenario_set_hash.clone(),
        });
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(P1ShardReportValidationError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.phase_ownership != current_phase_ownership() {
        return Err(P1ShardReportValidationError::PhaseOwnershipDrift);
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(P1ShardReportValidationError::MeasurementCoverageDrift);
    }
    validate_perf_environment(profile, &report.environment)
        .map_err(P1ShardReportValidationError::InvalidEnvironment)?;
    for sample in &report.samples {
        let observed = [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ];
        if observed != expected_phase_reconciliations(sample) {
            return Err(P1ShardReportValidationError::PhaseReconciliationDrift(
                sample.key.clone(),
            ));
        }
    }

    let successful_ids = report
        .samples
        .iter()
        .map(|sample| sample.key.as_str())
        .collect::<Vec<_>>();
    let failed_ids = report
        .failures
        .iter()
        .map(|failure| failure.key.as_str())
        .collect::<Vec<_>>();
    let expected_receipt = p1_shard_receipt(
        profile,
        report.receipt.shard_index,
        declared_ids,
        &successful_ids,
        &failed_ids,
    )
    .map_err(P1ShardReportValidationError::InvalidReceipt)?;
    if report.receipt != expected_receipt {
        return Err(P1ShardReportValidationError::ReceiptDrift(
            report.receipt.shard_index,
        ));
    }

    Ok(())
}

/// Merge exactly one complete report for every deterministic P1 shard.
///
/// # Errors
///
/// Returns a typed merge error for an incomplete, duplicate, invalid, or
/// aggregate-inconsistent report set.
pub(crate) fn merge_p1_shard_reports(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declared_ids: &[&str],
    reports: Vec<P1ShardReport>,
) -> Result<MergedP1ShardReports, P1ShardMergeError> {
    if reports.len() != usize::from(profile.shard_count()) {
        return Err(P1ShardMergeError::ReportCountMismatch {
            expected: profile.shard_count(),
            actual: reports.len(),
        });
    }

    let mut by_shard = BTreeMap::new();
    let mut environment = None;
    for report in reports {
        let shard_index = report.receipt.shard_index;
        validate_p1_shard_report(profile, required_wasm_profile, declared_ids, &report).map_err(
            |source| P1ShardMergeError::InvalidReport {
                shard_index,
                source,
            },
        )?;
        if environment
            .as_ref()
            .is_some_and(|expected| expected != &report.environment)
        {
            return Err(P1ShardMergeError::EnvironmentDrift(shard_index));
        }
        environment.get_or_insert_with(|| report.environment.clone());
        if by_shard.insert(shard_index, report).is_some() {
            return Err(P1ShardMergeError::DuplicateReport(shard_index));
        }
    }

    let mut receipts = Vec::with_capacity(usize::from(profile.shard_count()));
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for shard_index in 0..profile.shard_count() {
        let report = by_shard
            .remove(&shard_index)
            .ok_or(P1ShardMergeError::MissingReport(shard_index))?;
        receipts.push(report.receipt);
        samples.extend(report.samples);
        failures.extend(report.failures);
    }
    validate_p1_shard_receipts(profile, &receipts).map_err(P1ShardMergeError::InvalidReceipts)?;
    samples.sort_by(|left, right| left.key.cmp(&right.key));
    failures.sort_by(|left, right| left.key.cmp(&right.key));

    Ok(MergedP1ShardReports {
        environment: environment.ok_or(P1ShardMergeError::MissingEnvironment)?,
        receipts,
        samples,
        failures,
    })
}

/// Write one validated shard artifact using the current strict bounded format.
///
/// # Errors
///
/// Returns a typed artifact error for invalid evidence, encoding, size-budget,
/// directory, or write failure.
pub(crate) fn write_p1_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declared_ids: &[&str],
    report: &P1ShardReport,
) -> Result<(), P1ShardArtifactError> {
    validate_p1_shard_report(profile, required_wasm_profile, declared_ids, report)
        .map_err(P1ShardArtifactError::InvalidReport)?;
    let encoded =
        serde_json::to_vec_pretty(report).map_err(|source| P1ShardArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_p1_shard_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| P1ShardArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| P1ShardArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one strict bounded shard artifact.
///
/// # Errors
///
/// Returns a typed artifact error for open, read, size, strict-decoding, or
/// current-profile validation failure.
pub(crate) fn read_p1_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declared_ids: &[&str],
) -> Result<P1ShardReport, P1ShardArtifactError> {
    let file = fs::File::open(path).map_err(|source| P1ShardArtifactError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| P1ShardArtifactError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_p1_shard_artifact_size(path, bytes.len(), max_bytes)?;
    let report = serde_json::from_slice(&bytes).map_err(|source| P1ShardArtifactError::Decode {
        path: path.to_path_buf(),
        source,
    })?;
    validate_p1_shard_report(profile, required_wasm_profile, declared_ids, &report)
        .map_err(P1ShardArtifactError::InvalidReport)?;

    Ok(report)
}

/// Enforce the checked-in byte budget for one P1 shard artifact.
///
/// # Errors
///
/// Returns a typed oversize error when the observed artifact exceeds the limit.
pub(crate) fn validate_p1_shard_artifact_size(
    path: &Path,
    observed_bytes: usize,
    max_bytes: usize,
) -> Result<(), P1ShardArtifactError> {
    if observed_bytes > max_bytes {
        return Err(P1ShardArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

/// Typed failure when one P1 shard report is not current complete evidence.
#[derive(Debug)]
pub(crate) enum P1ShardReportValidationError {
    /// The report names a performance profile version other than the current one.
    ProfileVersion {
        /// Current checked-in version.
        expected: u32,
        /// Serialized version.
        actual: u32,
    },
    /// The report's full scenario-set identity differs from the profile.
    ScenarioSetHash {
        /// Current checked-in identity.
        expected: &'static str,
        /// Serialized identity.
        actual: String,
    },
    /// The report was not measured with the required canister profile.
    UnsupportedWasmProfile(String),
    /// The report's phase-ownership table differs from the current schema.
    PhaseOwnershipDrift,
    /// The report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,
    /// The report's complete environment identity is invalid.
    InvalidEnvironment(PerfEnvironmentError),
    /// One sample's serialized reconciliation differs from its raw counters.
    PhaseReconciliationDrift(String),
    /// The outcomes do not form the exact deterministic shard membership.
    InvalidReceipt(P1ReceiptError),
    /// The serialized receipt differs from the receipt derived from its outcomes.
    ReceiptDrift(u8),
}

impl Display for P1ShardReportValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "P1 shard profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ScenarioSetHash { expected, actual } => write!(
                formatter,
                "P1 shard scenario-set hash drifted: expected {expected}, observed {actual}",
            ),
            Self::UnsupportedWasmProfile(profile) => {
                write!(formatter, "unsupported P1 shard wasm profile {profile:?}")
            }
            Self::PhaseOwnershipDrift => {
                formatter.write_str("P1 shard phase-ownership table drifted")
            }
            Self::MeasurementCoverageDrift => {
                formatter.write_str("P1 shard measurement coverage drifted")
            }
            Self::InvalidEnvironment(error) => {
                write!(formatter, "invalid P1 shard environment: {error}")
            }
            Self::PhaseReconciliationDrift(scenario_id) => write!(
                formatter,
                "P1 shard phase reconciliation drifted for scenario {scenario_id:?}",
            ),
            Self::InvalidReceipt(error) => write!(formatter, "invalid P1 shard receipt: {error}"),
            Self::ReceiptDrift(shard_index) => write!(
                formatter,
                "P1 shard {shard_index} receipt differs from its serialized outcomes",
            ),
        }
    }
}

impl Error for P1ShardReportValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidEnvironment(error) => Some(error),
            Self::InvalidReceipt(error) => Some(error),
            Self::ProfileVersion { .. }
            | Self::ScenarioSetHash { .. }
            | Self::UnsupportedWasmProfile(_)
            | Self::PhaseOwnershipDrift
            | Self::MeasurementCoverageDrift
            | Self::PhaseReconciliationDrift(_)
            | Self::ReceiptDrift(_) => None,
        }
    }
}

/// Typed failure while merging independently produced P1 shards.
#[derive(Debug)]
pub(crate) enum P1ShardMergeError {
    /// The merge input count differs from the checked-in shard count.
    ReportCountMismatch {
        /// Required shard count.
        expected: u8,
        /// Observed report count.
        actual: usize,
    },
    /// More than one report claims the same deterministic shard.
    DuplicateReport(u8),
    /// Independently produced shards do not describe the same environment and subject.
    EnvironmentDrift(u8),
    /// One required deterministic shard has no report.
    MissingReport(u8),
    /// No environment identity survived an otherwise-empty merge input.
    MissingEnvironment,
    /// One shard report failed current-profile validation.
    InvalidReport {
        /// Shard index claimed by the invalid report.
        shard_index: u8,
        /// Typed validation cause.
        source: P1ShardReportValidationError,
    },
    /// The merged receipts are not complete aggregate evidence.
    InvalidReceipts(P1ReceiptError),
}

impl Display for P1ShardMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReportCountMismatch { expected, actual } => write!(
                formatter,
                "P1 shard report count drifted: expected {expected}, observed {actual}",
            ),
            Self::DuplicateReport(shard_index) => {
                write!(formatter, "duplicate P1 shard report {shard_index}")
            }
            Self::EnvironmentDrift(shard_index) => {
                write!(formatter, "P1 shard {shard_index} environment drifted")
            }
            Self::MissingReport(shard_index) => {
                write!(formatter, "missing P1 shard report {shard_index}")
            }
            Self::MissingEnvironment => {
                formatter.write_str("merged P1 reports have no environment identity")
            }
            Self::InvalidReport {
                shard_index,
                source,
            } => write!(formatter, "invalid P1 shard report {shard_index}: {source}"),
            Self::InvalidReceipts(error) => {
                write!(formatter, "invalid merged P1 shard receipts: {error}")
            }
        }
    }
}

impl Error for P1ShardMergeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidReport { source, .. } => Some(source),
            Self::InvalidReceipts(error) => Some(error),
            Self::ReportCountMismatch { .. }
            | Self::DuplicateReport(_)
            | Self::EnvironmentDrift(_)
            | Self::MissingReport(_)
            | Self::MissingEnvironment => None,
        }
    }
}

/// Typed failure while encoding, publishing, or reading one P1 shard artifact.
#[derive(Debug)]
pub(crate) enum P1ShardArtifactError {
    /// The in-memory or decoded report is not complete current-profile evidence.
    InvalidReport(P1ShardReportValidationError),
    /// One artifact filesystem operation failed.
    Io {
        /// Artifact path.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// Filesystem cause.
        source: io::Error,
    },
    /// The artifact exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Observed bytes, capped at one byte beyond the limit while reading.
        observed_bytes: usize,
        /// Checked-in maximum bytes.
        max_bytes: usize,
    },
    /// The in-memory report could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },
    /// The artifact is not the one current strict JSON shape.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON decoding cause.
        source: serde_json::Error,
    },
}

impl Display for P1ShardArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReport(error) => write!(formatter, "invalid P1 shard report: {error}"),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "P1 shard artifact {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "P1 shard artifact {} exceeds its byte budget: observed at least {observed_bytes}, maximum {max_bytes}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "P1 shard artifact {} could not be encoded: {source}",
                path.display(),
            ),
            Self::Decode { path, source } => write!(
                formatter,
                "P1 shard artifact {} is not current-format JSON: {source}",
                path.display(),
            ),
        }
    }
}

impl Error for P1ShardArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidReport(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Encode { source, .. } | Self::Decode { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}
