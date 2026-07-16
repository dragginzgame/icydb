//! Module: sql_perf_instrumentation
//! Responsibility: diagnostics-attribution instruction-overhead calibration evidence.
//! Does not own: production attribution, P1/P2 sampling, or regression thresholds.
//! Boundary: validates the fixed narrow sentinel and emits a strict current-format artifact.

use crate::{
    sql_perf_environment::{
        PerfEnvironmentError, PerfEnvironmentIdentity, validate_perf_environment,
    },
    sql_perf_measurement::{PerformanceMeasurementCoverage, current_measurement_coverage},
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError},
};

use std::{
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// Current hard-cut instrumentation-calibration artifact version.
const INSTRUMENTATION_CALIBRATION_VERSION: u32 = 2;

/// Stable scenario selected as the narrowest trustworthy SQL attribution sentinel.
pub(crate) const INSTRUMENTATION_SENTINEL_SCENARIO_ID: &str = "user.select.pk.all.pk_asc.limit1";

/// Exact SQL executed by both attributed and total-only calibration paths.
pub(crate) const INSTRUMENTATION_SENTINEL_SQL: &str =
    "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1";

///
/// InstrumentationPathSample
///
/// One isolated cold result and canister-local instruction measurement.
/// Owned by the calibration boundary; callers cannot omit semantic identity.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstrumentationPathSample {
    /// Canonical complete result signature for the sentinel query.
    pub(crate) result_signature: String,

    /// Canister-local instructions measured with performance counter type 1.
    pub(crate) instructions: u64,
}

///
/// InstrumentationCalibrationDisposition
///
/// Reviewed threshold status for attribution overhead.
/// The initial evidence is deliberately non-gating until repeated clean runs exist.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InstrumentationCalibrationDisposition {
    /// Evidence is retained, but no reviewed instruction-overhead budget exists yet.
    ObservationOnly,
}

///
/// InstrumentationCalibrationReport
///
/// Strict evidence comparing attributed and ordinary execution of one narrow sentinel.
/// Owned by Tier D performance calibration and independent of baseline regression deltas.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstrumentationCalibrationReport {
    /// Current hard-cut artifact version.
    artifact_version: u32,

    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Required canister WASM build profile.
    canister_wasm_profile: String,

    /// Full environment and measured raw-WASM subject identity.
    pub(crate) environment: PerfEnvironmentIdentity,

    /// Canonical measured and explicitly unmeasured resource dimensions.
    pub(crate) measurement_coverage: PerformanceMeasurementCoverage,

    /// Stable matrix scenario selected for calibration.
    sentinel_scenario_id: String,

    /// Exact SQL shared by both execution paths.
    sentinel_sql: String,

    /// Required isolated cold sample count for each path.
    sample_count_per_path: u8,

    /// Attributed-path cold samples in capture order.
    pub(crate) attributed_samples: Vec<InstrumentationPathSample>,

    /// Ordinary total-only-path cold samples in capture order.
    pub(crate) total_only_samples: Vec<InstrumentationPathSample>,

    /// Canonical result signature shared by every retained sample.
    pub(crate) sentinel_result_signature: String,

    /// Median attributed-path instruction count.
    pub(crate) attributed_median_instructions: u64,

    /// Median ordinary-path instruction count.
    pub(crate) total_only_median_instructions: u64,

    /// Signed attributed-minus-ordinary median instruction delta.
    pub(crate) overhead_instructions: i128,

    /// Signed overhead relative to the ordinary median, in basis points.
    pub(crate) overhead_basis_points: i128,

    /// Explicit threshold status for the measured overhead.
    pub(crate) disposition: InstrumentationCalibrationDisposition,
}

/// Build and validate one current instrumentation-calibration report.
///
/// # Errors
///
/// Returns a typed error for profile, environment, sample-count, instruction,
/// semantic, or derived-summary drift.
pub(crate) fn build_instrumentation_calibration_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    environment: PerfEnvironmentIdentity,
    attributed_samples: Vec<InstrumentationPathSample>,
    total_only_samples: Vec<InstrumentationPathSample>,
) -> Result<InstrumentationCalibrationReport, InstrumentationCalibrationError> {
    let sentinel_result_signature =
        shared_result_signature(&attributed_samples, &total_only_samples)?;
    let attributed_median_instructions = median_instructions(&attributed_samples)?;
    let total_only_median_instructions = median_instructions(&total_only_samples)?;
    let overhead_instructions =
        i128::from(attributed_median_instructions) - i128::from(total_only_median_instructions);
    let overhead_basis_points =
        (overhead_instructions * 10_000) / i128::from(total_only_median_instructions);
    let report = InstrumentationCalibrationReport {
        artifact_version: INSTRUMENTATION_CALIBRATION_VERSION,
        performance_profile_version: profile.version(),
        canister_wasm_profile: required_wasm_profile.to_string(),
        environment,
        measurement_coverage: current_measurement_coverage(),
        sentinel_scenario_id: INSTRUMENTATION_SENTINEL_SCENARIO_ID.to_string(),
        sentinel_sql: INSTRUMENTATION_SENTINEL_SQL.to_string(),
        sample_count_per_path: profile.cold_samples_per_confirmation(),
        attributed_samples,
        total_only_samples,
        sentinel_result_signature,
        attributed_median_instructions,
        total_only_median_instructions,
        overhead_instructions,
        overhead_basis_points,
        disposition: InstrumentationCalibrationDisposition::ObservationOnly,
    };
    validate_instrumentation_calibration_report(profile, required_wasm_profile, &report)?;

    Ok(report)
}

/// Validate a decoded calibration report against the exact current contract.
///
/// # Errors
///
/// Returns a typed error when any serialized fact differs from current profile,
/// sentinel, semantic, or derived-summary authority.
pub(crate) fn validate_instrumentation_calibration_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    report: &InstrumentationCalibrationReport,
) -> Result<(), InstrumentationCalibrationError> {
    profile
        .validate()
        .map_err(InstrumentationCalibrationError::InvalidProfile)?;
    validate_perf_environment(profile, &report.environment)
        .map_err(InstrumentationCalibrationError::InvalidEnvironment)?;
    if report.artifact_version != INSTRUMENTATION_CALIBRATION_VERSION {
        return Err(InstrumentationCalibrationError::ArtifactVersion {
            expected: INSTRUMENTATION_CALIBRATION_VERSION,
            actual: report.artifact_version,
        });
    }
    if report.performance_profile_version != profile.version() {
        return Err(InstrumentationCalibrationError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(InstrumentationCalibrationError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(InstrumentationCalibrationError::MeasurementCoverageDrift);
    }
    if report.sentinel_scenario_id != INSTRUMENTATION_SENTINEL_SCENARIO_ID
        || report.sentinel_sql != INSTRUMENTATION_SENTINEL_SQL
        || report.sample_count_per_path != profile.cold_samples_per_confirmation()
        || report.disposition != InstrumentationCalibrationDisposition::ObservationOnly
    {
        return Err(InstrumentationCalibrationError::ContractDrift);
    }
    validate_sample_count(
        InstrumentationPath::Attributed,
        profile.cold_samples_per_confirmation(),
        &report.attributed_samples,
    )?;
    validate_sample_count(
        InstrumentationPath::TotalOnly,
        profile.cold_samples_per_confirmation(),
        &report.total_only_samples,
    )?;
    let signature =
        shared_result_signature(&report.attributed_samples, &report.total_only_samples)?;
    let attributed_median = median_instructions(&report.attributed_samples)?;
    let total_only_median = median_instructions(&report.total_only_samples)?;
    let overhead = i128::from(attributed_median) - i128::from(total_only_median);
    let overhead_basis_points = (overhead * 10_000) / i128::from(total_only_median);
    if report.sentinel_result_signature != signature
        || report.attributed_median_instructions != attributed_median
        || report.total_only_median_instructions != total_only_median
        || report.overhead_instructions != overhead
        || report.overhead_basis_points != overhead_basis_points
    {
        return Err(InstrumentationCalibrationError::DerivedSummaryDrift);
    }

    Ok(())
}

fn validate_sample_count(
    path: InstrumentationPath,
    expected: u8,
    samples: &[InstrumentationPathSample],
) -> Result<(), InstrumentationCalibrationError> {
    if samples.len() != usize::from(expected) {
        return Err(InstrumentationCalibrationError::SampleCount {
            path,
            expected,
            actual: samples.len(),
        });
    }

    Ok(())
}

fn shared_result_signature(
    attributed: &[InstrumentationPathSample],
    total_only: &[InstrumentationPathSample],
) -> Result<String, InstrumentationCalibrationError> {
    let Some(expected) = attributed
        .first()
        .map(|sample| sample.result_signature.as_str())
    else {
        return Err(InstrumentationCalibrationError::MissingSamples(
            InstrumentationPath::Attributed,
        ));
    };
    if expected.is_empty() {
        return Err(InstrumentationCalibrationError::EmptyResultSignature {
            path: InstrumentationPath::Attributed,
            index: 0,
        });
    }
    validate_path_samples(InstrumentationPath::Attributed, attributed, expected)?;
    validate_path_samples(InstrumentationPath::TotalOnly, total_only, expected)?;

    Ok(expected.to_string())
}

fn validate_path_samples(
    path: InstrumentationPath,
    samples: &[InstrumentationPathSample],
    expected_signature: &str,
) -> Result<(), InstrumentationCalibrationError> {
    if samples.is_empty() {
        return Err(InstrumentationCalibrationError::MissingSamples(path));
    }
    for (index, sample) in samples.iter().enumerate() {
        if sample.result_signature.is_empty() {
            return Err(InstrumentationCalibrationError::EmptyResultSignature { path, index });
        }
        if sample.result_signature != expected_signature {
            return Err(InstrumentationCalibrationError::SemanticDrift { path, index });
        }
        if sample.instructions == 0 {
            return Err(InstrumentationCalibrationError::ZeroInstructions { path, index });
        }
    }

    Ok(())
}

fn median_instructions(
    samples: &[InstrumentationPathSample],
) -> Result<u64, InstrumentationCalibrationError> {
    let mut values = samples
        .iter()
        .map(|sample| sample.instructions)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(InstrumentationCalibrationError::MissingSamples(
            InstrumentationPath::Attributed,
        ));
    }
    values.sort_unstable();

    Ok(values[values.len() / 2])
}

/// Write one validated, bounded current-format calibration artifact.
///
/// # Errors
///
/// Returns a typed validation, encoding, size, or I/O error.
pub(crate) fn write_instrumentation_calibration_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    report: &InstrumentationCalibrationReport,
) -> Result<(), InstrumentationCalibrationArtifactError> {
    validate_instrumentation_calibration_report(profile, required_wasm_profile, report)
        .map_err(InstrumentationCalibrationArtifactError::InvalidReport)?;
    let encoded = serde_json::to_vec_pretty(report).map_err(|source| {
        InstrumentationCalibrationArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    validate_artifact_size(path, profile, encoded.len())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            InstrumentationCalibrationArtifactError::Io {
                path: parent.to_path_buf(),
                operation: "created",
                source,
            }
        })?;
    }
    fs::write(path, encoded).map_err(|source| InstrumentationCalibrationArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one bounded current-format calibration artifact.
///
/// # Errors
///
/// Returns a typed I/O, size, decoding, or report-validation error.
pub(crate) fn read_instrumentation_calibration_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
) -> Result<InstrumentationCalibrationReport, InstrumentationCalibrationArtifactError> {
    let file =
        fs::File::open(path).map_err(|source| InstrumentationCalibrationArtifactError::Io {
            path: path.to_path_buf(),
            operation: "opened",
            source,
        })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut encoded = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut encoded)
        .map_err(|source| InstrumentationCalibrationArtifactError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_artifact_size(path, profile, encoded.len())?;
    let report = serde_json::from_slice(&encoded).map_err(|source| {
        InstrumentationCalibrationArtifactError::Decode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    validate_instrumentation_calibration_report(profile, required_wasm_profile, &report)
        .map_err(InstrumentationCalibrationArtifactError::InvalidReport)?;

    Ok(report)
}

fn validate_artifact_size(
    path: &Path,
    profile: PerformanceProfile,
    observed_bytes: usize,
) -> Result<(), InstrumentationCalibrationArtifactError> {
    if observed_bytes > profile.max_artifact_bytes() {
        return Err(InstrumentationCalibrationArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes: profile.max_artifact_bytes(),
        });
    }

    Ok(())
}

/// Path being calibrated against the common sentinel result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstrumentationPath {
    /// Production execution with detailed typed attribution enabled.
    Attributed,

    /// Production execution using only an outer total instruction counter.
    TotalOnly,
}

/// Typed failure when calibration evidence is not exact current-format truth.
#[derive(Debug)]
pub(crate) enum InstrumentationCalibrationError {
    /// The artifact format version differs from current authority.
    ArtifactVersion {
        /// Current version.
        expected: u32,
        /// Serialized version.
        actual: u32,
    },

    /// A fixed sentinel, sample-count, or disposition fact drifted.
    ContractDrift,

    /// A serialized median, delta, or shared signature does not match raw samples.
    DerivedSummaryDrift,

    /// One sample omitted its semantic result identity.
    EmptyResultSignature {
        /// Measured execution path.
        path: InstrumentationPath,
        /// Zero-based sample index.
        index: usize,
    },

    /// The complete environment identity is invalid.
    InvalidEnvironment(PerfEnvironmentError),

    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),

    /// One execution path has no samples.
    MissingSamples(InstrumentationPath),

    /// The report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,

    /// The artifact names a performance profile version other than current.
    ProfileVersion {
        /// Current profile version.
        expected: u32,
        /// Serialized profile version.
        actual: u32,
    },

    /// One execution path retained the wrong number of isolated samples.
    SampleCount {
        /// Measured execution path.
        path: InstrumentationPath,
        /// Required count.
        expected: u8,
        /// Retained count.
        actual: usize,
    },

    /// One sample returned a different semantic result.
    SemanticDrift {
        /// Measured execution path.
        path: InstrumentationPath,
        /// Zero-based sample index.
        index: usize,
    },

    /// The report was not captured with the required canister profile.
    UnsupportedWasmProfile(String),

    /// One sample reported no measured local instructions.
    ZeroInstructions {
        /// Measured execution path.
        path: InstrumentationPath,
        /// Zero-based sample index.
        index: usize,
    },
}

impl Display for InstrumentationCalibrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactVersion { expected, actual } => write!(
                formatter,
                "instrumentation artifact version drifted: expected {expected}, observed {actual}",
            ),
            Self::ContractDrift => {
                formatter.write_str("instrumentation calibration contract drifted")
            }
            Self::DerivedSummaryDrift => {
                formatter.write_str("instrumentation calibration summary differs from raw samples")
            }
            Self::EmptyResultSignature { path, index } => write!(
                formatter,
                "instrumentation {path:?} sample {index} has no result signature",
            ),
            Self::InvalidEnvironment(source) => {
                write!(formatter, "invalid instrumentation environment: {source}")
            }
            Self::InvalidProfile(source) => {
                write!(formatter, "invalid instrumentation profile: {source}")
            }
            Self::MissingSamples(path) => {
                write!(formatter, "instrumentation {path:?} path has no samples")
            }
            Self::MeasurementCoverageDrift => {
                formatter.write_str("instrumentation measurement coverage drifted")
            }
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "instrumentation profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::SampleCount {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "instrumentation {path:?} sample count drifted: expected {expected}, observed {actual}",
            ),
            Self::SemanticDrift { path, index } => write!(
                formatter,
                "instrumentation {path:?} sample {index} returned a different result",
            ),
            Self::UnsupportedWasmProfile(profile) => write!(
                formatter,
                "unsupported instrumentation-calibration wasm profile {profile:?}",
            ),
            Self::ZeroInstructions { path, index } => write!(
                formatter,
                "instrumentation {path:?} sample {index} reported zero instructions",
            ),
        }
    }
}

impl Error for InstrumentationCalibrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidEnvironment(source) => Some(source),
            Self::InvalidProfile(source) => Some(source),
            Self::ArtifactVersion { .. }
            | Self::ContractDrift
            | Self::DerivedSummaryDrift
            | Self::EmptyResultSignature { .. }
            | Self::MissingSamples(_)
            | Self::MeasurementCoverageDrift
            | Self::ProfileVersion { .. }
            | Self::SampleCount { .. }
            | Self::SemanticDrift { .. }
            | Self::UnsupportedWasmProfile(_)
            | Self::ZeroInstructions { .. } => None,
        }
    }
}

/// Typed artifact persistence failure preserving validation and I/O causes.
#[derive(Debug)]
pub(crate) enum InstrumentationCalibrationArtifactError {
    /// Strict JSON decoding failed.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON cause.
        source: serde_json::Error,
    },

    /// JSON encoding failed.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON cause.
        source: serde_json::Error,
    },

    /// The decoded report violates current calibration authority.
    InvalidReport(InstrumentationCalibrationError),

    /// Artifact filesystem access failed.
    Io {
        /// Affected path.
        path: PathBuf,
        /// Human-readable operation.
        operation: &'static str,
        /// I/O cause.
        source: io::Error,
    },

    /// The artifact exceeds the checked-in maximum encoded size.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Encoded byte length.
        observed_bytes: usize,
        /// Checked-in maximum byte length.
        max_bytes: usize,
    },
}

impl Display for InstrumentationCalibrationArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { path, source } => write!(
                formatter,
                "instrumentation artifact {} could not be decoded: {source}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "instrumentation artifact {} could not be encoded: {source}",
                path.display(),
            ),
            Self::InvalidReport(source) => {
                write!(formatter, "invalid instrumentation artifact: {source}")
            }
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "instrumentation artifact {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "instrumentation artifact {} is {observed_bytes} bytes; maximum is {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for InstrumentationCalibrationArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::InvalidReport(source) => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{sql_perf_environment::tests::identity, sql_perf_profile::SQL_PERFORMANCE_PROFILE};

    use super::*;

    fn samples(values: [u64; 5]) -> Vec<InstrumentationPathSample> {
        values
            .into_iter()
            .map(|instructions| InstrumentationPathSample {
                result_signature: "projection|PerfAuditUser|id|1|1".to_string(),
                instructions,
            })
            .collect()
    }

    fn report() -> InstrumentationCalibrationReport {
        build_instrumentation_calibration_report(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            identity(),
            samples([119, 121, 120, 123, 118]),
            samples([99, 101, 100, 103, 98]),
        )
        .expect("current calibration evidence should build")
    }

    #[test]
    fn calibration_uses_exact_semantics_and_median_overhead() {
        let report = report();

        assert_eq!(report.attributed_median_instructions, 120);
        assert_eq!(report.total_only_median_instructions, 100);
        assert_eq!(report.overhead_instructions, 20);
        assert_eq!(report.overhead_basis_points, 2_000);
        assert_eq!(
            report.disposition,
            InstrumentationCalibrationDisposition::ObservationOnly,
        );
    }

    #[test]
    fn calibration_rejects_incomplete_or_semantically_different_paths() {
        let mut total_only = samples([99, 101, 100, 103, 98]);
        total_only[2].result_signature = "different".to_string();
        assert!(matches!(
            build_instrumentation_calibration_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                identity(),
                samples([119, 121, 120, 123, 118]),
                total_only,
            ),
            Err(InstrumentationCalibrationError::SemanticDrift {
                path: InstrumentationPath::TotalOnly,
                index: 2,
            })
        ));

        assert!(matches!(
            build_instrumentation_calibration_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                identity(),
                samples([119, 121, 120, 123, 118]),
                Vec::new(),
            ),
            Err(InstrumentationCalibrationError::MissingSamples(
                InstrumentationPath::TotalOnly
            ))
        ));
    }

    #[test]
    fn calibration_format_rejects_unknown_fields_and_summary_drift() {
        let report = report();
        let path = std::env::temp_dir().join(format!(
            "icydb-sql-perf-instrumentation-{}.json",
            std::process::id(),
        ));
        write_instrumentation_calibration_report(
            &path,
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &report,
        )
        .expect("current calibration artifact should write");
        let decoded =
            read_instrumentation_calibration_report(&path, SQL_PERFORMANCE_PROFILE, "wasm-release")
                .expect("current calibration artifact should read");
        assert_eq!(decoded, report);
        fs::remove_file(&path).expect("temporary calibration artifact should be removed");

        let mut encoded = serde_json::to_value(&report).expect("report should encode");
        encoded["legacy_overhead"] = serde_json::json!(1);
        assert!(serde_json::from_value::<InstrumentationCalibrationReport>(encoded).is_err());

        let mut coverage_drifted = report.clone();
        coverage_drifted.measurement_coverage.peak_heap_bytes =
            crate::sql_perf_measurement::PerformanceMeasurementStatus::Measured;
        assert!(matches!(
            validate_instrumentation_calibration_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &coverage_drifted,
            ),
            Err(InstrumentationCalibrationError::MeasurementCoverageDrift)
        ));

        let mut drifted = report;
        drifted.overhead_instructions += 1;
        assert!(matches!(
            validate_instrumentation_calibration_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &drifted,
            ),
            Err(InstrumentationCalibrationError::DerivedSummaryDrift)
        ));
    }
}
