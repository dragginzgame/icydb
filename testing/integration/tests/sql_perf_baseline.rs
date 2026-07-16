//! Module: sql_perf_baseline
//! Responsibility: comparable P2 median deltas and the current regression verdict.
//! Does not own: sampling, candidate selection, environment capture, or baseline updates.
//! Boundary: validates both merged artifacts, rejects incomparable inputs, and gates typed metrics.

use crate::{
    MatrixSample, MatrixScenario,
    sql_perf_environment::{
        PerfEnvironmentIdentity, PerfEnvironmentMismatch, require_comparable_environment,
    },
    sql_perf_p2::P2RawMetric,
    sql_perf_p2_confirmation::{P2SampleMode, P2SampleSet, P2WarmEvidence, same_semantic_result},
    sql_perf_p2_shard::{MergedP2ShardReports, P2ShardMergeError, validate_merged_p2_report},
    sql_perf_profile::{PerformanceProfile, PerformanceThreshold},
    sql_perf_scale_baseline::{
        ScaleBaselineComparison, ScaleBaselineComparisonError, compare_scale_baseline,
    },
    sql_perf_scale_shard::MergedScaleShardReports,
};

use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{self, Display},
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

///
/// P2BaselineMetric
///
/// Typed metric retained in comparable P2 deltas.
/// Owned by baseline reporting; instruction members reuse P2 ranking authority.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "metric", rename_all = "snake_case")]
pub(crate) enum P2BaselineMetric {
    /// One instruction metric already owned by P2 raw ranking.
    Instruction(P2RawMetric),

    /// Rows ingested by scalar aggregate reducers.
    ScalarAggregateRowsIngested,

    /// Hybrid-covering path selections.
    HybridCoveringPathHits,

    /// Hybrid-covering index-backed field accesses.
    HybridCoveringIndexFieldAccesses,

    /// Hybrid-covering row-backed field accesses.
    HybridCoveringRowFieldAccesses,

    /// Kernel retained-layout selections.
    KernelRowRetainedLayoutHits,

    /// Kernel retained slot values.
    KernelRowRetainedSlotValues,

    /// Kernel retained octet-length values.
    KernelRowRetainedOctetLengthValues,

    /// Data-store point reads.
    DataStoreGetCalls,

    /// Index-store point reads.
    IndexStoreGetCalls,

    /// Index-store range scans.
    IndexStoreRangeScanCalls,

    /// Index entries read.
    IndexStoreEntryReads,

    /// Compiled SQL cache hits.
    SqlCompiledCommandHits,

    /// Compiled SQL cache misses.
    SqlCompiledCommandMisses,

    /// Shared plan-cache hits.
    SharedQueryPlanHits,

    /// Shared plan-cache misses.
    SharedQueryPlanMisses,

    /// Blob values projected into the result.
    OutputBlobValues,

    /// Raw projected blob bytes.
    OutputBlobBytes,

    /// Hex-rendered blob bytes.
    OutputBlobHexBytes,

    /// Rows returned by the query result.
    RowsReturned,
}

impl P2BaselineMetric {
    fn value(self, sample: &MatrixSample) -> u64 {
        match self {
            Self::Instruction(metric) => metric.value(sample),
            Self::ScalarAggregateRowsIngested => sample.scalar_aggregate_rows_ingested,
            Self::HybridCoveringPathHits => sample.hybrid_covering_path_hits,
            Self::HybridCoveringIndexFieldAccesses => sample.hybrid_covering_index_field_accesses,
            Self::HybridCoveringRowFieldAccesses => sample.hybrid_covering_row_field_accesses,
            Self::KernelRowRetainedLayoutHits => sample.kernel_row_retained_layout_hits,
            Self::KernelRowRetainedSlotValues => sample.kernel_row_retained_slot_values,
            Self::KernelRowRetainedOctetLengthValues => {
                sample.kernel_row_retained_octet_length_values
            }
            Self::DataStoreGetCalls => sample.data_store_get_calls,
            Self::IndexStoreGetCalls => sample.index_store_get_calls,
            Self::IndexStoreRangeScanCalls => sample.index_store_range_scan_calls,
            Self::IndexStoreEntryReads => sample.index_store_entry_reads,
            Self::SqlCompiledCommandHits => sample.sql_compiled_command_hits,
            Self::SqlCompiledCommandMisses => sample.sql_compiled_command_misses,
            Self::SharedQueryPlanHits => sample.shared_query_plan_hits,
            Self::SharedQueryPlanMisses => sample.shared_query_plan_misses,
            Self::OutputBlobValues => sample.output_blob_values,
            Self::OutputBlobBytes => sample.output_blob_bytes,
            Self::OutputBlobHexBytes => sample.output_blob_hex_bytes,
            Self::RowsReturned => u64::try_from(sample.outcome.row_count).unwrap_or(u64::MAX),
        }
    }

    const fn threshold(self, profile: PerformanceProfile) -> Option<PerformanceThreshold> {
        match self {
            Self::Instruction(P2RawMetric::Total) => {
                Some(profile.total_instruction_regression_threshold())
            }
            _ => None,
        }
    }
}

const P2_NON_INSTRUCTION_METRICS: &[P2BaselineMetric] = &[
    P2BaselineMetric::ScalarAggregateRowsIngested,
    P2BaselineMetric::HybridCoveringPathHits,
    P2BaselineMetric::HybridCoveringIndexFieldAccesses,
    P2BaselineMetric::HybridCoveringRowFieldAccesses,
    P2BaselineMetric::KernelRowRetainedLayoutHits,
    P2BaselineMetric::KernelRowRetainedSlotValues,
    P2BaselineMetric::KernelRowRetainedOctetLengthValues,
    P2BaselineMetric::DataStoreGetCalls,
    P2BaselineMetric::IndexStoreGetCalls,
    P2BaselineMetric::IndexStoreRangeScanCalls,
    P2BaselineMetric::IndexStoreEntryReads,
    P2BaselineMetric::SqlCompiledCommandHits,
    P2BaselineMetric::SqlCompiledCommandMisses,
    P2BaselineMetric::SharedQueryPlanHits,
    P2BaselineMetric::SharedQueryPlanMisses,
    P2BaselineMetric::OutputBlobValues,
    P2BaselineMetric::OutputBlobBytes,
    P2BaselineMetric::OutputBlobHexBytes,
    P2BaselineMetric::RowsReturned,
];

///
/// P2MetricDisposition
///
/// Explicit threshold status for one retained median delta.
/// Metrics without reviewed budgets remain observation-only and never imply a gate.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum P2MetricDisposition {
    /// Both an absolute and relative threshold are checked.
    Gated {
        /// Reviewed absolute increase threshold.
        absolute_threshold: u64,
        /// Reviewed relative increase threshold in basis points.
        relative_threshold_basis_points: u16,
        /// Whether both thresholds were reached.
        regression: bool,
    },

    /// No reviewed threshold exists yet.
    ObservationOnly,
}

///
/// P2MetricDelta
///
/// One comparable baseline/current median pair for a scenario, cache mode, and metric.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2MetricDelta {
    /// Stable selected scenario identity.
    pub(crate) scenario_id: String,

    /// Proven cache mode.
    pub(crate) mode: P2SampleMode,

    /// Typed measured metric.
    pub(crate) metric: P2BaselineMetric,

    /// Baseline median.
    pub(crate) baseline: u64,

    /// Current median.
    pub(crate) current: u64,

    /// Signed current-minus-baseline delta.
    pub(crate) delta: i128,

    /// Signed relative delta in basis points, absent for a zero baseline.
    pub(crate) delta_basis_points: Option<i128>,

    /// Reviewed gate or explicit observation-only status.
    pub(crate) disposition: P2MetricDisposition,
}

/// One threshold regression that makes the P2 baseline verdict fail.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2RegressionCause {
    /// Stable selected scenario identity.
    pub(crate) scenario_id: String,

    /// Proven cache mode.
    pub(crate) mode: P2SampleMode,

    /// Gated metric.
    pub(crate) metric: P2BaselineMetric,

    /// Signed current-minus-baseline delta.
    pub(crate) delta: i128,

    /// Signed relative delta in basis points.
    pub(crate) delta_basis_points: i128,
}

/// Current P2 regression verdict after all comparability checks pass.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "causes", rename_all = "snake_case")]
pub(crate) enum P2BaselineVerdict {
    /// Every currently gated metric stayed within its reviewed threshold.
    Passed,

    /// One or more gated metrics reached both regression thresholds.
    Failed(Vec<P2RegressionCause>),
}

///
/// PerformanceBaselineComparison
///
/// Machine-readable comparison of two independently validated merged P2 artifacts.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerformanceBaselineComparison {
    /// Complete baseline environment and measured subject.
    pub(crate) baseline_environment: PerfEnvironmentIdentity,

    /// Complete current environment and measured subject.
    pub(crate) current_environment: PerfEnvironmentIdentity,

    /// Exact candidate-set identity shared by both artifacts.
    pub(crate) p2_scenario_set_hash: String,

    /// Exact number of compared candidates.
    pub(crate) candidate_count: usize,

    /// Number of distinct metrics without reviewed thresholds.
    pub(crate) observation_only_metric_count: usize,

    /// Complete comparable scale totals, normalized costs, and slopes.
    pub(crate) scale: ScaleBaselineComparison,

    /// Stable scenario/mode/metric deltas.
    pub(crate) deltas: Vec<P2MetricDelta>,

    /// Current threshold verdict; comparability failures produce no report.
    pub(crate) verdict: P2BaselineVerdict,
}

/// Compare two complete P2 reports under one comparable environment.
///
/// # Errors
///
/// Returns a typed error before producing deltas when either artifact is invalid,
/// environments are incomparable, candidate or mode membership differs, or
/// semantic result/route identity changed.
pub(crate) fn compare_performance_baseline(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    baseline: &MergedP2ShardReports,
    current: &MergedP2ShardReports,
    baseline_scale: &MergedScaleShardReports,
    current_scale: &MergedScaleShardReports,
) -> Result<PerformanceBaselineComparison, PerformanceBaselineComparisonError> {
    validate_merged_p2_report(profile, required_wasm_profile, scenarios, baseline)
        .map_err(PerformanceBaselineComparisonError::InvalidBaseline)?;
    validate_merged_p2_report(profile, required_wasm_profile, scenarios, current)
        .map_err(PerformanceBaselineComparisonError::InvalidCurrent)?;
    require_comparable_environment(&baseline.environment, &current.environment)
        .map_err(PerformanceBaselineComparisonError::IncomparableEnvironment)?;
    let scale = compare_same_subject_scale(
        profile,
        required_wasm_profile,
        scenarios,
        baseline,
        current,
        baseline_scale,
        current_scale,
    )?;
    let deltas = compare_confirmation_deltas(profile, baseline, current)?;

    let causes = deltas
        .iter()
        .filter_map(|delta| match delta.disposition {
            P2MetricDisposition::Gated {
                regression: true, ..
            } => Some(P2RegressionCause {
                scenario_id: delta.scenario_id.clone(),
                mode: delta.mode,
                metric: delta.metric,
                delta: delta.delta,
                delta_basis_points: delta.delta_basis_points.unwrap_or_default(),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    let p2_observation_only_metric_count = deltas
        .iter()
        .filter_map(|delta| {
            matches!(delta.disposition, P2MetricDisposition::ObservationOnly)
                .then_some(delta.metric)
        })
        .collect::<BTreeSet<_>>()
        .len();
    let scale_observation_only_metric_count = 2 + scale
        .normalized
        .iter()
        .map(|delta| delta.denominator)
        .collect::<BTreeSet<_>>()
        .len();
    let verdict = if causes.is_empty() {
        P2BaselineVerdict::Passed
    } else {
        P2BaselineVerdict::Failed(causes)
    };

    Ok(PerformanceBaselineComparison {
        baseline_environment: baseline.environment.clone(),
        current_environment: current.environment.clone(),
        p2_scenario_set_hash: baseline.p2_scenario_set_hash().to_string(),
        candidate_count: baseline.confirmations.len(),
        observation_only_metric_count: p2_observation_only_metric_count
            + scale_observation_only_metric_count,
        scale,
        deltas,
        verdict,
    })
}

fn compare_same_subject_scale(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    baseline: &MergedP2ShardReports,
    current: &MergedP2ShardReports,
    baseline_scale: &MergedScaleShardReports,
    current_scale: &MergedScaleShardReports,
) -> Result<ScaleBaselineComparison, PerformanceBaselineComparisonError> {
    if baseline.environment != baseline_scale.environment {
        return Err(PerformanceBaselineComparisonError::SubjectEnvironmentDrift(
            "baseline",
        ));
    }
    if current.environment != current_scale.environment {
        return Err(PerformanceBaselineComparisonError::SubjectEnvironmentDrift(
            "current",
        ));
    }

    compare_scale_baseline(
        profile,
        required_wasm_profile,
        scenarios,
        baseline_scale,
        current_scale,
    )
    .map_err(PerformanceBaselineComparisonError::InvalidScaleComparison)
}

fn compare_confirmation_deltas(
    profile: PerformanceProfile,
    baseline: &MergedP2ShardReports,
    current: &MergedP2ShardReports,
) -> Result<Vec<P2MetricDelta>, PerformanceBaselineComparisonError> {
    let baseline_ids = confirmation_ids(baseline);
    let current_ids = confirmation_ids(current);
    if baseline_ids != current_ids {
        return Err(PerformanceBaselineComparisonError::CandidateSetDrift {
            baseline: baseline_ids,
            current: current_ids,
        });
    }

    let mut deltas = Vec::new();
    for (baseline_confirmation, current_confirmation) in
        baseline.confirmations.iter().zip(&current.confirmations)
    {
        let scenario_id = baseline_confirmation.candidate.scenario_id.as_str();
        append_sample_set_deltas(
            profile,
            scenario_id,
            &baseline_confirmation.cold,
            &current_confirmation.cold,
            &mut deltas,
        )?;
        match (&baseline_confirmation.warm, &current_confirmation.warm) {
            (P2WarmEvidence::Confirmed(baseline), P2WarmEvidence::Confirmed(current)) => {
                append_sample_set_deltas(profile, scenario_id, baseline, current, &mut deltas)?;
            }
            (
                P2WarmEvidence::NotApplicable(baseline_reason),
                P2WarmEvidence::NotApplicable(current_reason),
            ) if baseline_reason == current_reason => {}
            _ => {
                return Err(PerformanceBaselineComparisonError::SampleModeDrift(
                    scenario_id.to_string(),
                ));
            }
        }
    }

    Ok(deltas)
}

fn append_sample_set_deltas(
    profile: PerformanceProfile,
    scenario_id: &str,
    baseline: &P2SampleSet,
    current: &P2SampleSet,
    deltas: &mut Vec<P2MetricDelta>,
) -> Result<(), PerformanceBaselineComparisonError> {
    let mode = baseline.cache_proof.mode;
    if current.cache_proof.mode != mode {
        return Err(PerformanceBaselineComparisonError::SampleModeDrift(
            scenario_id.to_string(),
        ));
    }
    if !same_semantic_result(&baseline.samples[0], &current.samples[0]) {
        return Err(PerformanceBaselineComparisonError::SemanticDrift {
            scenario_id: scenario_id.to_string(),
            mode,
        });
    }
    for metric in P2RawMetric::all()
        .iter()
        .copied()
        .map(P2BaselineMetric::Instruction)
        .chain(P2_NON_INSTRUCTION_METRICS.iter().copied())
    {
        let baseline = median_metric(baseline, metric);
        let current = median_metric(current, metric);
        let delta = i128::from(current) - i128::from(baseline);
        let delta_basis_points = relative_delta_basis_points(baseline, current);
        let disposition =
            metric
                .threshold(profile)
                .map_or(P2MetricDisposition::ObservationOnly, |threshold| {
                    P2MetricDisposition::Gated {
                        absolute_threshold: threshold.absolute_instructions(),
                        relative_threshold_basis_points: threshold.relative_basis_points(),
                        regression: delta >= i128::from(threshold.absolute_instructions())
                            && delta_basis_points.is_some_and(|basis_points| {
                                basis_points >= i128::from(threshold.relative_basis_points())
                            }),
                    }
                });
        deltas.push(P2MetricDelta {
            scenario_id: scenario_id.to_string(),
            mode,
            metric,
            baseline,
            current,
            delta,
            delta_basis_points,
            disposition,
        });
    }

    Ok(())
}

fn median_metric(sample_set: &P2SampleSet, metric: P2BaselineMetric) -> u64 {
    let mut values = sample_set
        .samples
        .iter()
        .map(|sample| metric.value(sample))
        .collect::<Vec<_>>();
    values.sort_unstable();

    values[values.len() / 2]
}

fn relative_delta_basis_points(baseline: u64, current: u64) -> Option<i128> {
    if baseline == 0 {
        return None;
    }

    Some((i128::from(current) - i128::from(baseline)).saturating_mul(10_000) / i128::from(baseline))
}

fn confirmation_ids(report: &MergedP2ShardReports) -> Vec<String> {
    report
        .confirmations
        .iter()
        .map(|confirmation| confirmation.candidate.scenario_id.clone())
        .collect()
}

/// Write one bounded machine-readable P2 comparison artifact.
///
/// # Errors
///
/// Returns a typed error for encoding, size-budget, directory, or write failure.
pub(crate) fn write_performance_baseline_comparison(
    path: &Path,
    profile: PerformanceProfile,
    report: &PerformanceBaselineComparison,
) -> Result<(), PerformanceBaselineArtifactError> {
    let encoded = serde_json::to_vec_pretty(report).map_err(|source| {
        PerformanceBaselineArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if encoded.len() > profile.max_artifact_bytes() {
        return Err(PerformanceBaselineArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes: encoded.len(),
            max_bytes: profile.max_artifact_bytes(),
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| PerformanceBaselineArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| PerformanceBaselineArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Typed failure that prevents a meaningful P2 comparison report.
#[derive(Debug)]
pub(crate) enum PerformanceBaselineComparisonError {
    /// Baseline and current artifacts select different scenario IDs.
    CandidateSetDrift {
        /// Baseline scenario IDs.
        baseline: Vec<String>,
        /// Current scenario IDs.
        current: Vec<String>,
    },

    /// The two artifacts do not share a comparable environment.
    IncomparableEnvironment(PerfEnvironmentMismatch),

    /// The baseline merged artifact is invalid.
    InvalidBaseline(P2ShardMergeError),

    /// The current merged artifact is invalid.
    InvalidCurrent(P2ShardMergeError),

    /// The required scale pair is invalid or incomparable.
    InvalidScaleComparison(ScaleBaselineComparisonError),

    /// Baseline and current disagree about one scenario's maintained cache modes.
    SampleModeDrift(String),

    /// One P2 report and its same-subject scale report have different identities.
    SubjectEnvironmentDrift(&'static str),

    /// Result, route, window, or declaration identity changed between subjects.
    SemanticDrift {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Proven cache mode.
        mode: P2SampleMode,
    },
}

impl Display for PerformanceBaselineComparisonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateSetDrift { baseline, current } => write!(
                formatter,
                "P2 candidate sets differ: baseline {baseline:?}, current {current:?}",
            ),
            Self::IncomparableEnvironment(error) => {
                write!(formatter, "P2 environments are incomparable: {error}")
            }
            Self::InvalidBaseline(error) => write!(formatter, "invalid P2 baseline: {error}"),
            Self::InvalidCurrent(error) => write!(formatter, "invalid current P2 report: {error}"),
            Self::InvalidScaleComparison(error) => {
                write!(formatter, "invalid scale comparison: {error}")
            }
            Self::SampleModeDrift(scenario_id) => {
                write!(
                    formatter,
                    "P2 cache-mode membership drifted for {scenario_id:?}"
                )
            }
            Self::SubjectEnvironmentDrift(subject) => write!(
                formatter,
                "{subject} P2 and scale reports describe different environments or subjects",
            ),
            Self::SemanticDrift { scenario_id, mode } => write!(
                formatter,
                "P2 semantic identity drifted for {scenario_id:?} in {mode:?} mode",
            ),
        }
    }
}

impl Error for PerformanceBaselineComparisonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IncomparableEnvironment(error) => Some(error),
            Self::InvalidBaseline(error) | Self::InvalidCurrent(error) => Some(error),
            Self::InvalidScaleComparison(error) => Some(error),
            Self::CandidateSetDrift { .. }
            | Self::SampleModeDrift(_)
            | Self::SubjectEnvironmentDrift(_)
            | Self::SemanticDrift { .. } => None,
        }
    }
}

/// Typed failure while writing a P2 comparison artifact.
#[derive(Debug)]
pub(crate) enum PerformanceBaselineArtifactError {
    /// The in-memory report could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// One artifact filesystem operation failed.
    Io {
        /// Affected path.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// I/O cause.
        source: io::Error,
    },

    /// The encoded comparison exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Encoded bytes.
        observed_bytes: usize,
        /// Maximum encoded bytes.
        max_bytes: usize,
    },
}

impl Display for PerformanceBaselineArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode { path, source } => write!(
                formatter,
                "P2 comparison {} could not be encoded: {source}",
                path.display(),
            ),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "P2 comparison {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "P2 comparison {} is {observed_bytes} bytes; maximum is {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for PerformanceBaselineArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode { source, .. } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        deterministic_matrix, fill_matrix_phase_reconciliation,
        sql_perf_p2_shard::tests::complete_report, sql_perf_profile::SQL_PERFORMANCE_PROFILE,
        sql_perf_scale_shard::tests::complete_report as complete_scale_report,
    };

    use super::*;

    #[test]
    fn comparable_p2_reports_emit_gated_and_observation_only_deltas() {
        let scenarios = deterministic_matrix();
        let baseline = complete_report(&scenarios);
        let mut current = baseline.clone();
        current.environment.subject.source_revision = "66".repeat(20);
        current.environment.subject.raw_wasm_sha256 = "77".repeat(32);
        let (_, baseline_scale) = complete_scale_report();
        let mut current_scale = baseline_scale.clone();
        current_scale.environment.subject.source_revision = "66".repeat(20);
        current_scale.environment.subject.raw_wasm_sha256 = "77".repeat(32);
        let comparison = compare_performance_baseline(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &baseline,
            &current,
            &baseline_scale,
            &current_scale,
        )
        .expect("subject identity may differ in a comparable pair");

        assert_eq!(comparison.verdict, P2BaselineVerdict::Passed);
        assert!(comparison.observation_only_metric_count > 0);
        assert!(comparison.deltas.iter().any(|delta| {
            delta.metric == P2BaselineMetric::Instruction(P2RawMetric::Total)
                && matches!(delta.disposition, P2MetricDisposition::Gated { .. })
        }));
        assert!(
            comparison
                .deltas
                .iter()
                .any(|delta| { matches!(delta.disposition, P2MetricDisposition::ObservationOnly) })
        );
    }

    #[test]
    fn comparison_rejects_environment_and_semantic_drift_before_deltas() {
        let scenarios = deterministic_matrix();
        let baseline = complete_report(&scenarios);
        let (_, scale) = complete_scale_report();
        let mut current = baseline.clone();
        current.environment.comparable.accepted_snapshot_hash = "00".repeat(32);
        assert!(matches!(
            compare_performance_baseline(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &baseline,
                &current,
                &scale,
                &scale,
            ),
            Err(PerformanceBaselineComparisonError::IncomparableEnvironment(
                _
            ))
        ));

        let mut current = baseline.clone();
        for sample in &mut current.confirmations[0].cold.samples {
            sample.result_signature = Some("changed".to_string());
        }
        if let P2WarmEvidence::Confirmed(warm) = &mut current.confirmations[0].warm {
            for sample in &mut warm.samples {
                sample.result_signature = Some("changed".to_string());
            }
        }
        assert!(matches!(
            compare_performance_baseline(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &baseline,
                &current,
                &scale,
                &scale,
            ),
            Err(PerformanceBaselineComparisonError::SemanticDrift { .. })
        ));
    }

    #[test]
    fn total_instruction_dual_threshold_uses_confirmed_medians() {
        let scenarios = deterministic_matrix();
        let baseline = complete_report(&scenarios);
        let (_, scale) = complete_scale_report();
        let mut current = baseline.clone();
        let scenario_id = current.confirmations[0].candidate.scenario_id.clone();
        {
            let confirmation = &mut current.confirmations[0];
            for sample in &mut confirmation.cold.samples {
                sample.total_local_instructions += 200_000;
                fill_matrix_phase_reconciliation(sample);
            }
            confirmation.cold.min_total_local_instructions += 200_000;
            confirmation.cold.median_total_local_instructions += 200_000;
            confirmation.cold.max_total_local_instructions += 200_000;
        }
        let comparison = compare_performance_baseline(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &baseline,
            &current,
            &scale,
            &scale,
        )
        .expect("a stable instruction regression should remain comparable");

        assert!(matches!(comparison.verdict, P2BaselineVerdict::Failed(_)));
        assert!(comparison.deltas.iter().any(|delta| {
            delta.scenario_id == scenario_id
                && delta.mode == P2SampleMode::Cold
                && delta.metric == P2BaselineMetric::Instruction(P2RawMetric::Total)
                && matches!(
                    delta.disposition,
                    P2MetricDisposition::Gated {
                        regression: true,
                        ..
                    }
                )
        }));
    }
}
