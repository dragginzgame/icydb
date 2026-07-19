//! Module: sql_perf_calibration
//! Responsibility: strict three-run SQL performance calibration review.
//! Does not own: measurement, threshold selection, baseline blessing, or profile mutation.
//! Boundary: accepts exactly one clean ordinal 1/2/3 cohort and projects review evidence.

use crate::{
    MatrixSample, MatrixScenario,
    sql_perf_baseline::P2BaselineMetric,
    sql_perf_environment::PerfEnvironmentIdentity,
    sql_perf_instrumentation::{
        InstrumentationCalibrationError, InstrumentationCalibrationReport,
        validate_instrumentation_calibration_report,
    },
    sql_perf_p2::{P2BaselineBasis, P2CalibrationRun, P2CandidateReason},
    sql_perf_p2_confirmation::{P2SampleMode, P2SampleSet, P2WarmEvidence, same_semantic_result},
    sql_perf_p2_shard::{MergedP2ShardReports, P2ShardMergeError, validate_merged_p2_report},
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError},
    sql_perf_scale::NormalizedDenominator,
    sql_perf_scale_shard::{
        MergedScaleShardReports, ScaleShardError, validate_merged_scale_report,
    },
};

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
    fs, io,
    path::{Path, PathBuf},
};

use serde::Serialize;

const CALIBRATION_REVIEW_VERSION: u32 = 1;
const REQUIRED_CALIBRATION_RUNS: [P2CalibrationRun; 3] = [
    P2CalibrationRun::One,
    P2CalibrationRun::Two,
    P2CalibrationRun::Three,
];

///
/// CalibrationRunArtifacts
///
/// Complete P2, scale, and instrumentation evidence from one scheduled run.
/// Owned by calibration review and accepted only when all components name one subject.
///

pub(crate) struct CalibrationRunArtifacts {
    p2: MergedP2ShardReports,
    scale: MergedScaleShardReports,
    instrumentation: InstrumentationCalibrationReport,
}

impl CalibrationRunArtifacts {
    /// Group the three independently published components from one workflow run.
    pub(crate) const fn new(
        p2: MergedP2ShardReports,
        scale: MergedScaleShardReports,
        instrumentation: InstrumentationCalibrationReport,
    ) -> Self {
        Self {
            p2,
            scale,
            instrumentation,
        }
    }
}

///
/// CalibrationHotspotDisposition
///
/// Review status of a scenario recurring in a raw or normalized top-20 set.
/// This is evidence for a profile edit, never an automatic profile mutation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CalibrationHotspotDisposition {
    /// The current profile already retains the recurring scenario as required evidence.
    AlreadyRetained,

    /// A reviewer must decide whether to add the recurring scenario to the focused set.
    RequiresPromotionReview,
}

///
/// CalibrationHotspotRun
///
/// Top-20 ranking evidence retained for one recurring scenario in one cohort member.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationHotspotRun {
    /// Exact cohort ordinal carrying the ranking.
    run: P2CalibrationRun,

    /// Raw and normalized ranking reasons from that run.
    reasons: Vec<P2CandidateReason>,
}

///
/// CalibrationRecurringHotspot
///
/// Scenario appearing in a raw or normalized top-20 set in at least two clean runs.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationRecurringHotspot {
    /// Stable P1/P2 scenario identity.
    scenario_id: String,

    /// Exact per-run ranking evidence in ordinal order.
    runs: Vec<CalibrationHotspotRun>,

    /// Whether current checked-in focused evidence already covers the scenario.
    disposition: CalibrationHotspotDisposition,
}

///
/// CalibrationU64Observation
///
/// One unsigned measured value associated with an exact cohort ordinal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationU64Observation {
    /// Exact clean run ordinal.
    run: P2CalibrationRun,

    /// Measured value for that run.
    value: u64,
}

///
/// CalibrationI128Observation
///
/// One signed measured value associated with an exact cohort ordinal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationI128Observation {
    /// Exact clean run ordinal.
    run: P2CalibrationRun,

    /// Measured signed value for that run.
    value: i128,
}

///
/// CalibrationP2Envelope
///
/// Cross-run median envelope for one focused or recurring P2 scenario metric.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationP2Envelope {
    /// Stable scenario identity.
    scenario_id: String,

    /// Proven cold or warm cache mode.
    mode: P2SampleMode,

    /// Raw instruction, residual, or typed counter metric.
    metric: P2BaselineMetric,

    /// Per-run confirmed medians in ordinal order.
    observations: Vec<CalibrationU64Observation>,

    /// Smallest confirmed median across retained runs.
    minimum: u64,

    /// Middle confirmed median across retained runs.
    median: u64,

    /// Largest confirmed median across retained runs.
    maximum: u64,

    /// Whether all three clean cohort members retained this scenario and mode.
    complete_three_run: bool,
}

///
/// CalibrationScaleTotalEnvelope
///
/// Three-run total-instruction envelope for one exact-cardinality scale scenario.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationScaleTotalEnvelope {
    /// Stable scale-sentinel family identity.
    sentinel_id: String,

    /// Stable exact-cardinality scenario identity.
    scenario_id: String,

    /// Exact fixture row count.
    fixture_rows: u32,

    /// Per-run total instruction observations.
    observations: Vec<CalibrationU64Observation>,

    /// Smallest observed total.
    minimum: u64,

    /// Middle observed total.
    median: u64,

    /// Largest observed total.
    maximum: u64,
}

///
/// CalibrationNormalizedObservation
///
/// One exact rational normalized cost associated with a cohort ordinal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationNormalizedObservation {
    /// Exact clean run ordinal.
    run: P2CalibrationRun,

    /// Total instruction numerator.
    local_instructions: u64,

    /// Nonzero typed-unit denominator.
    units: u64,
}

///
/// CalibrationNormalizedValue
///
/// Exact rational cost projected without floating-point rounding.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationNormalizedValue {
    /// Total instruction numerator.
    local_instructions: u64,

    /// Nonzero typed-unit denominator.
    units: u64,
}

///
/// CalibrationScaleNormalizedEnvelope
///
/// Three-run rational normalized-cost envelope for one scale scenario and unit.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationScaleNormalizedEnvelope {
    /// Stable exact-cardinality scale scenario identity.
    scenario_id: String,

    /// Typed normalization unit.
    denominator: NormalizedDenominator,

    /// Exact per-run rational observations in ordinal order.
    observations: Vec<CalibrationNormalizedObservation>,

    /// Smallest exact rational observation.
    minimum: CalibrationNormalizedValue,

    /// Middle exact rational observation.
    median: CalibrationNormalizedValue,

    /// Largest exact rational observation.
    maximum: CalibrationNormalizedValue,
}

///
/// CalibrationScaleSlopeEnvelope
///
/// Three-run instruction-change envelope for one adjacent scale-cardinality pair.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationScaleSlopeEnvelope {
    /// Stable scale-sentinel family identity.
    sentinel_id: String,

    /// Lower fixture cardinality.
    from_fixture_rows: u32,

    /// Higher fixture cardinality.
    to_fixture_rows: u32,

    /// Positive fixture-row difference.
    row_delta: u32,

    /// Per-run signed instruction changes.
    observations: Vec<CalibrationI128Observation>,

    /// Smallest signed instruction change.
    minimum: i128,

    /// Middle signed instruction change.
    median: i128,

    /// Largest signed instruction change.
    maximum: i128,
}

///
/// CalibrationInstrumentationObservation
///
/// Attribution-overhead evidence from one exact cohort member.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationInstrumentationObservation {
    /// Exact clean run ordinal.
    run: P2CalibrationRun,

    /// Median attributed-path instructions.
    attributed_median_instructions: u64,

    /// Median total-only-path instructions.
    total_only_median_instructions: u64,

    /// Signed attributed-minus-total-only overhead.
    overhead_instructions: i128,

    /// Signed overhead relative to total-only work in basis points.
    overhead_basis_points: i128,
}

///
/// CalibrationInstrumentationEnvelope
///
/// Complete three-run observation envelope for diagnostics-attribution overhead.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationInstrumentationEnvelope {
    /// Exact per-run instrumentation observations.
    observations: Vec<CalibrationInstrumentationObservation>,

    /// Smallest observed instruction overhead.
    minimum_overhead_instructions: i128,

    /// Middle observed instruction overhead.
    median_overhead_instructions: i128,

    /// Largest observed instruction overhead.
    maximum_overhead_instructions: i128,

    /// Smallest observed relative overhead.
    minimum_overhead_basis_points: i128,

    /// Middle observed relative overhead.
    median_overhead_basis_points: i128,

    /// Largest observed relative overhead.
    maximum_overhead_basis_points: i128,
}

///
/// CalibrationRunSummary
///
/// Stable identity and high-level counts for one validated cohort member.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CalibrationRunSummary {
    /// Exact clean run ordinal.
    run: P2CalibrationRun,

    /// Exact selected P2 scenario-set identity for this discovery pass.
    p2_scenario_set_hash: String,

    /// Number of confirmed P2 candidates.
    candidate_count: usize,

    /// Attribution-overhead instruction delta.
    instrumentation_overhead_instructions: i128,
}

///
/// CalibrationCohortReview
///
/// Bounded machine-readable projection of one exact three-run calibration cohort.
/// It exposes observations and promotion work but deliberately chooses no thresholds.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CalibrationCohortReview {
    /// Current hard-cut calibration-review artifact version.
    artifact_version: u32,

    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Reviewer-chosen cohort identity shared by ordinals one through three.
    cohort: String,

    /// Exact comparable environment and measured subject shared by every component.
    environment: PerfEnvironmentIdentity,

    /// One stable summary for every exact ordinal.
    runs: Vec<CalibrationRunSummary>,

    /// Raw/normalized top-20 scenarios recurring in at least two runs.
    recurring_hotspots: Vec<CalibrationRecurringHotspot>,

    /// Number of recurring scenarios not yet in the checked-in focused set.
    unresolved_promotion_count: usize,

    /// Confirmed metric envelopes for focused, regression, and recurring scenarios.
    p2_envelopes: Vec<CalibrationP2Envelope>,

    /// Exact total-instruction envelopes for every scale scenario.
    scale_totals: Vec<CalibrationScaleTotalEnvelope>,

    /// Exact normalized-cost envelopes for every eligible scale scenario and unit.
    scale_normalized: Vec<CalibrationScaleNormalizedEnvelope>,

    /// Exact adjacent-cardinality slope envelopes.
    scale_slopes: Vec<CalibrationScaleSlopeEnvelope>,

    /// Three-run diagnostics-attribution overhead envelope.
    instrumentation: CalibrationInstrumentationEnvelope,
}

impl CalibrationCohortReview {
    /// Borrow the reviewed cohort identity.
    pub(crate) fn cohort(&self) -> &str {
        &self.cohort
    }

    /// Return how many recurring hotspots still require a profile decision.
    pub(crate) const fn unresolved_promotion_count(&self) -> usize {
        self.unresolved_promotion_count
    }

    /// Return how many P2 metric envelopes were retained for budget review.
    pub(crate) const fn p2_envelope_count(&self) -> usize {
        self.p2_envelopes.len()
    }
}

/// Build one strict review from exactly three clean scheduled cohort members.
///
/// The result is observational. A reviewer must still choose checked-in budgets,
/// select the initial baseline, and resolve every promotion candidate explicitly.
///
/// # Errors
///
/// Returns a typed error for invalid component evidence, wrong cohort ordinals,
/// mixed environments or subjects, missing focused sentinels, or semantic drift.
pub(crate) fn build_calibration_cohort_review(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    runs: [CalibrationRunArtifacts; 3],
) -> Result<CalibrationCohortReview, CalibrationCohortError> {
    profile
        .validate()
        .map_err(CalibrationCohortError::InvalidProfile)?;
    let cohort = validate_cohort(profile, required_wasm_profile, scenarios, &runs)?;
    validate_cross_run_semantics(&runs)?;

    let recurring_hotspots = recurring_hotspots(profile, &runs);
    let unresolved_promotion_count = recurring_hotspots
        .iter()
        .filter(|hotspot| {
            hotspot.disposition == CalibrationHotspotDisposition::RequiresPromotionReview
        })
        .count();
    let calibration_sentinels = calibration_sentinel_ids(profile, &recurring_hotspots);
    validate_required_sentinels(profile, &runs)?;

    Ok(CalibrationCohortReview {
        artifact_version: CALIBRATION_REVIEW_VERSION,
        performance_profile_version: profile.version(),
        cohort,
        environment: runs[0].p2.environment.clone(),
        runs: run_summaries(&runs),
        recurring_hotspots,
        unresolved_promotion_count,
        p2_envelopes: p2_envelopes(&runs, &calibration_sentinels),
        scale_totals: scale_total_envelopes(&runs),
        scale_normalized: scale_normalized_envelopes(&runs),
        scale_slopes: scale_slope_envelopes(&runs),
        instrumentation: instrumentation_envelope(&runs),
    })
}

/// Write one review artifact without changing any baseline or threshold authority.
///
/// # Errors
///
/// Returns a typed error for encoding, size-budget, directory, or write failure.
pub(crate) fn write_calibration_cohort_review(
    path: &Path,
    profile: PerformanceProfile,
    review: &CalibrationCohortReview,
) -> Result<(), CalibrationCohortArtifactError> {
    let encoded = serde_json::to_vec_pretty(review).map_err(|source| {
        CalibrationCohortArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if encoded.len() > profile.max_artifact_bytes() {
        return Err(CalibrationCohortArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes: encoded.len(),
            max_bytes: profile.max_artifact_bytes(),
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CalibrationCohortArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| CalibrationCohortArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

fn validate_cohort(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    runs: &[CalibrationRunArtifacts; 3],
) -> Result<String, CalibrationCohortError> {
    let mut expected_cohort = None;
    let mut expected_environment = None;
    for (index, artifacts) in runs.iter().enumerate() {
        let expected_run = REQUIRED_CALIBRATION_RUNS[index];
        validate_merged_p2_report(profile, required_wasm_profile, scenarios, &artifacts.p2)
            .map_err(|source| CalibrationCohortError::InvalidP2 {
                run: expected_run,
                source,
            })?;
        validate_merged_scale_report(profile, required_wasm_profile, scenarios, &artifacts.scale)
            .map_err(|source| CalibrationCohortError::InvalidScale {
            run: expected_run,
            source,
        })?;
        validate_instrumentation_calibration_report(
            profile,
            required_wasm_profile,
            &artifacts.instrumentation,
        )
        .map_err(|source| CalibrationCohortError::InvalidInstrumentation {
            run: expected_run,
            source,
        })?;

        if artifacts.p2.environment != artifacts.scale.environment
            || artifacts.p2.environment != artifacts.instrumentation.environment
        {
            return Err(CalibrationCohortError::ComponentEnvironmentDrift(
                expected_run,
            ));
        }
        if expected_environment
            .as_ref()
            .is_some_and(|environment| environment != &artifacts.p2.environment)
        {
            return Err(CalibrationCohortError::EnvironmentDrift(expected_run));
        }
        expected_environment.get_or_insert_with(|| artifacts.p2.environment.clone());

        let P2BaselineBasis::InitialCalibration { cohort, run } = artifacts.p2.baseline_basis()
        else {
            return Err(CalibrationCohortError::NotInitialCalibration(expected_run));
        };
        if *run != expected_run {
            return Err(CalibrationCohortError::OrdinalDrift {
                expected: expected_run,
                actual: *run,
            });
        }
        if expected_cohort
            .as_ref()
            .is_some_and(|expected| expected != cohort)
        {
            return Err(CalibrationCohortError::CohortDrift {
                expected: expected_cohort.unwrap_or_default(),
                actual: cohort.clone(),
            });
        }
        expected_cohort.get_or_insert_with(|| cohort.clone());
    }

    expected_cohort.ok_or(CalibrationCohortError::MissingCohort)
}

fn validate_cross_run_semantics(
    runs: &[CalibrationRunArtifacts; 3],
) -> Result<(), CalibrationCohortError> {
    let mut p2_semantics = BTreeMap::<(String, P2SampleMode), MatrixSample>::new();
    let mut scale_semantics = BTreeMap::<String, MatrixSample>::new();
    let expected_instrumentation_signature =
        runs[0].instrumentation.sentinel_result_signature.as_str();

    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for confirmation in &artifacts.p2.confirmations {
            validate_sample_set_semantics(&mut p2_semantics, run, &confirmation.cold)?;
            if let P2WarmEvidence::Confirmed(warm) = &confirmation.warm {
                validate_sample_set_semantics(&mut p2_semantics, run, warm)?;
            }
        }
        for observation in &artifacts.scale.observations {
            if let Some(expected) = scale_semantics.get(&observation.scenario_id) {
                if !same_semantic_result(expected, &observation.sample) {
                    return Err(CalibrationCohortError::ScaleSemanticDrift {
                        run,
                        scenario_id: observation.scenario_id.clone(),
                    });
                }
            } else {
                scale_semantics.insert(observation.scenario_id.clone(), observation.sample.clone());
            }
        }
        if artifacts.instrumentation.sentinel_result_signature != expected_instrumentation_signature
        {
            return Err(CalibrationCohortError::InstrumentationSemanticDrift(run));
        }
    }

    Ok(())
}

fn validate_sample_set_semantics(
    expected: &mut BTreeMap<(String, P2SampleMode), MatrixSample>,
    run: P2CalibrationRun,
    samples: &P2SampleSet,
) -> Result<(), CalibrationCohortError> {
    let key = (samples.scenario_id.clone(), samples.cache_proof.mode);
    if let Some(reference) = expected.get(&key) {
        if !same_semantic_result(reference, &samples.samples[0]) {
            return Err(CalibrationCohortError::P2SemanticDrift {
                run,
                scenario_id: samples.scenario_id.clone(),
                mode: samples.cache_proof.mode,
            });
        }
    } else {
        expected.insert(key, samples.samples[0].clone());
    }

    Ok(())
}

fn recurring_hotspots(
    profile: PerformanceProfile,
    runs: &[CalibrationRunArtifacts; 3],
) -> Vec<CalibrationRecurringHotspot> {
    let focused = profile
        .focused_hotspot_scenario_ids()
        .iter()
        .chain(profile.regression_sentinel_scenario_ids())
        .chain(profile.contract_sentinel_scenario_ids())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut evidence = BTreeMap::<String, Vec<CalibrationHotspotRun>>::new();
    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for confirmation in &artifacts.p2.confirmations {
            let reasons = confirmation
                .candidate
                .reasons
                .iter()
                .filter(|reason| {
                    matches!(
                        reason,
                        P2CandidateReason::RawMetric { .. }
                            | P2CandidateReason::NormalizedMetric { .. }
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            if !reasons.is_empty() {
                evidence
                    .entry(confirmation.candidate.scenario_id.clone())
                    .or_default()
                    .push(CalibrationHotspotRun { run, reasons });
            }
        }
    }

    evidence
        .into_iter()
        .filter_map(|(scenario_id, runs)| {
            (runs.len() >= 2).then(|| CalibrationRecurringHotspot {
                disposition: if focused.contains(scenario_id.as_str()) {
                    CalibrationHotspotDisposition::AlreadyRetained
                } else {
                    CalibrationHotspotDisposition::RequiresPromotionReview
                },
                scenario_id,
                runs,
            })
        })
        .collect()
}

fn calibration_sentinel_ids(
    profile: PerformanceProfile,
    recurring: &[CalibrationRecurringHotspot],
) -> BTreeSet<String> {
    profile
        .focused_hotspot_scenario_ids()
        .iter()
        .chain(profile.regression_sentinel_scenario_ids())
        .chain(profile.contract_sentinel_scenario_ids())
        .map(|scenario_id| (*scenario_id).to_string())
        .chain(recurring.iter().map(|hotspot| hotspot.scenario_id.clone()))
        .collect()
}

fn validate_required_sentinels(
    profile: PerformanceProfile,
    runs: &[CalibrationRunArtifacts; 3],
) -> Result<(), CalibrationCohortError> {
    for scenario_id in profile
        .focused_hotspot_scenario_ids()
        .iter()
        .chain(profile.regression_sentinel_scenario_ids())
        .chain(profile.contract_sentinel_scenario_ids())
    {
        for (index, artifacts) in runs.iter().enumerate() {
            if !artifacts
                .p2
                .confirmations
                .iter()
                .any(|confirmation| confirmation.candidate.scenario_id == *scenario_id)
            {
                return Err(CalibrationCohortError::MissingRequiredSentinel {
                    run: REQUIRED_CALIBRATION_RUNS[index],
                    scenario_id: (*scenario_id).to_string(),
                });
            }
        }
    }

    Ok(())
}

fn run_summaries(runs: &[CalibrationRunArtifacts; 3]) -> Vec<CalibrationRunSummary> {
    runs.iter()
        .enumerate()
        .map(|(index, artifacts)| CalibrationRunSummary {
            run: REQUIRED_CALIBRATION_RUNS[index],
            p2_scenario_set_hash: artifacts.p2.p2_scenario_set_hash().to_string(),
            candidate_count: artifacts.p2.confirmations.len(),
            instrumentation_overhead_instructions: artifacts.instrumentation.overhead_instructions,
        })
        .collect()
}

fn p2_envelopes(
    runs: &[CalibrationRunArtifacts; 3],
    sentinel_ids: &BTreeSet<String>,
) -> Vec<CalibrationP2Envelope> {
    let mut observations =
        BTreeMap::<(String, P2SampleMode, P2BaselineMetric), Vec<CalibrationU64Observation>>::new();
    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for confirmation in &artifacts.p2.confirmations {
            if !sentinel_ids.contains(&confirmation.candidate.scenario_id) {
                continue;
            }
            append_p2_envelope_observations(&mut observations, run, &confirmation.cold);
            if let P2WarmEvidence::Confirmed(warm) = &confirmation.warm {
                append_p2_envelope_observations(&mut observations, run, warm);
            }
        }
    }

    observations
        .into_iter()
        .map(|((scenario_id, mode, metric), observations)| {
            let (minimum, median, maximum) = u64_envelope(&observations);
            CalibrationP2Envelope {
                scenario_id,
                mode,
                metric,
                complete_three_run: observations.len() == REQUIRED_CALIBRATION_RUNS.len(),
                observations,
                minimum,
                median,
                maximum,
            }
        })
        .collect()
}

fn append_p2_envelope_observations(
    observations: &mut BTreeMap<
        (String, P2SampleMode, P2BaselineMetric),
        Vec<CalibrationU64Observation>,
    >,
    run: P2CalibrationRun,
    samples: &P2SampleSet,
) {
    for metric in P2BaselineMetric::all() {
        observations
            .entry((
                samples.scenario_id.clone(),
                samples.cache_proof.mode,
                metric,
            ))
            .or_default()
            .push(CalibrationU64Observation {
                run,
                value: metric.median(samples),
            });
    }
}

fn scale_total_envelopes(
    runs: &[CalibrationRunArtifacts; 3],
) -> Vec<CalibrationScaleTotalEnvelope> {
    let mut observations = BTreeMap::<String, (String, u32, Vec<CalibrationU64Observation>)>::new();
    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for observation in &artifacts.scale.observations {
            observations
                .entry(observation.scenario_id.clone())
                .or_insert_with(|| {
                    (
                        observation.sentinel_id.clone(),
                        observation.fixture.fixture_rows,
                        Vec::new(),
                    )
                })
                .2
                .push(CalibrationU64Observation {
                    run,
                    value: observation.sample.total_local_instructions,
                });
        }
    }

    observations
        .into_iter()
        .map(|(scenario_id, (sentinel_id, fixture_rows, observations))| {
            let (minimum, median, maximum) = u64_envelope(&observations);
            CalibrationScaleTotalEnvelope {
                sentinel_id,
                scenario_id,
                fixture_rows,
                observations,
                minimum,
                median,
                maximum,
            }
        })
        .collect()
}

fn scale_normalized_envelopes(
    runs: &[CalibrationRunArtifacts; 3],
) -> Vec<CalibrationScaleNormalizedEnvelope> {
    let mut observations =
        BTreeMap::<(String, NormalizedDenominator), Vec<CalibrationNormalizedObservation>>::new();
    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for observation in &artifacts.scale.normalized_costs {
            observations
                .entry((observation.scenario_id.clone(), observation.denominator))
                .or_default()
                .push(CalibrationNormalizedObservation {
                    run,
                    local_instructions: observation.cost.local_instructions,
                    units: observation.cost.units.get(),
                });
        }
    }

    observations
        .into_iter()
        .map(|((scenario_id, denominator), observations)| {
            let (minimum, median, maximum) = normalized_envelope(&observations);
            CalibrationScaleNormalizedEnvelope {
                scenario_id,
                denominator,
                observations,
                minimum,
                median,
                maximum,
            }
        })
        .collect()
}

fn scale_slope_envelopes(
    runs: &[CalibrationRunArtifacts; 3],
) -> Vec<CalibrationScaleSlopeEnvelope> {
    let mut observations = BTreeMap::<(String, u32, u32), Vec<CalibrationI128Observation>>::new();
    for (index, artifacts) in runs.iter().enumerate() {
        let run = REQUIRED_CALIBRATION_RUNS[index];
        for slope in &artifacts.scale.slopes {
            observations
                .entry((
                    slope.sentinel_id.clone(),
                    slope.from_fixture_rows,
                    slope.to_fixture_rows,
                ))
                .or_default()
                .push(CalibrationI128Observation {
                    run,
                    value: slope.instruction_delta,
                });
        }
    }

    observations
        .into_iter()
        .map(
            |((sentinel_id, from_fixture_rows, to_fixture_rows), observations)| {
                let (minimum, median, maximum) = i128_envelope(&observations);
                CalibrationScaleSlopeEnvelope {
                    sentinel_id,
                    from_fixture_rows,
                    to_fixture_rows,
                    row_delta: to_fixture_rows - from_fixture_rows,
                    observations,
                    minimum,
                    median,
                    maximum,
                }
            },
        )
        .collect()
}

fn instrumentation_envelope(
    runs: &[CalibrationRunArtifacts; 3],
) -> CalibrationInstrumentationEnvelope {
    let observations = runs
        .iter()
        .enumerate()
        .map(|(index, artifacts)| CalibrationInstrumentationObservation {
            run: REQUIRED_CALIBRATION_RUNS[index],
            attributed_median_instructions: artifacts
                .instrumentation
                .attributed_median_instructions,
            total_only_median_instructions: artifacts
                .instrumentation
                .total_only_median_instructions,
            overhead_instructions: artifacts.instrumentation.overhead_instructions,
            overhead_basis_points: artifacts.instrumentation.overhead_basis_points,
        })
        .collect::<Vec<_>>();
    let overhead_instructions = observations
        .iter()
        .map(|observation| CalibrationI128Observation {
            run: observation.run,
            value: observation.overhead_instructions,
        })
        .collect::<Vec<_>>();
    let overhead_basis_points = observations
        .iter()
        .map(|observation| CalibrationI128Observation {
            run: observation.run,
            value: observation.overhead_basis_points,
        })
        .collect::<Vec<_>>();
    let (
        minimum_overhead_instructions,
        median_overhead_instructions,
        maximum_overhead_instructions,
    ) = i128_envelope(&overhead_instructions);
    let (
        minimum_overhead_basis_points,
        median_overhead_basis_points,
        maximum_overhead_basis_points,
    ) = i128_envelope(&overhead_basis_points);

    CalibrationInstrumentationEnvelope {
        observations,
        minimum_overhead_instructions,
        median_overhead_instructions,
        maximum_overhead_instructions,
        minimum_overhead_basis_points,
        median_overhead_basis_points,
        maximum_overhead_basis_points,
    }
}

fn u64_envelope(observations: &[CalibrationU64Observation]) -> (u64, u64, u64) {
    let mut values = observations
        .iter()
        .map(|observation| observation.value)
        .collect::<Vec<_>>();
    values.sort_unstable();

    (
        values[0],
        values[values.len() / 2],
        values[values.len() - 1],
    )
}

fn i128_envelope(observations: &[CalibrationI128Observation]) -> (i128, i128, i128) {
    let mut values = observations
        .iter()
        .map(|observation| observation.value)
        .collect::<Vec<_>>();
    values.sort_unstable();

    (
        values[0],
        values[values.len() / 2],
        values[values.len() - 1],
    )
}

fn normalized_envelope(
    observations: &[CalibrationNormalizedObservation],
) -> (
    CalibrationNormalizedValue,
    CalibrationNormalizedValue,
    CalibrationNormalizedValue,
) {
    let mut values = observations
        .iter()
        .map(|observation| CalibrationNormalizedValue {
            local_instructions: observation.local_instructions,
            units: observation.units,
        })
        .collect::<Vec<_>>();
    values.sort_by(compare_normalized_values);

    (
        values[0],
        values[values.len() / 2],
        values[values.len() - 1],
    )
}

fn compare_normalized_values(
    left: &CalibrationNormalizedValue,
    right: &CalibrationNormalizedValue,
) -> Ordering {
    (u128::from(left.local_instructions) * u128::from(right.units))
        .cmp(&(u128::from(right.local_instructions) * u128::from(left.units)))
        .then(left.local_instructions.cmp(&right.local_instructions))
        .then(left.units.cmp(&right.units))
}

///
/// CalibrationCohortError
///
/// Typed failure proving three artifacts cannot form one calibration cohort.
///

#[derive(Debug)]
pub(crate) enum CalibrationCohortError {
    /// Calibration cohort identities differ across ordinals.
    CohortDrift {
        /// First accepted cohort identity.
        expected: String,
        /// Later conflicting cohort identity.
        actual: String,
    },

    /// P2, scale, and instrumentation components within one run name different subjects.
    ComponentEnvironmentDrift(P2CalibrationRun),

    /// Complete environment or subject identity differs across cohort members.
    EnvironmentDrift(P2CalibrationRun),

    /// Instrumentation result identity differs across cohort members.
    InstrumentationSemanticDrift(P2CalibrationRun),

    /// One instrumentation component is not valid current evidence.
    InvalidInstrumentation {
        /// Expected cohort ordinal.
        run: P2CalibrationRun,
        /// Typed validation cause.
        source: InstrumentationCalibrationError,
    },

    /// One P2 component is not valid current evidence.
    InvalidP2 {
        /// Expected cohort ordinal.
        run: P2CalibrationRun,
        /// Typed validation cause.
        source: P2ShardMergeError,
    },

    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),

    /// One scale component is not valid current evidence.
    InvalidScale {
        /// Expected cohort ordinal.
        run: P2CalibrationRun,
        /// Typed validation cause.
        source: ScaleShardError,
    },

    /// No cohort identity survived otherwise valid input.
    MissingCohort,

    /// One checked-in focused or regression sentinel is absent from a run.
    MissingRequiredSentinel {
        /// Exact cohort ordinal missing the sentinel.
        run: P2CalibrationRun,
        /// Stable required scenario identity.
        scenario_id: String,
    },

    /// One P2 component names comparable-baseline rather than calibration evidence.
    NotInitialCalibration(P2CalibrationRun),

    /// A serialized calibration ordinal differs from its exact input position.
    OrdinalDrift {
        /// Required ordinal for this input position.
        expected: P2CalibrationRun,
        /// Serialized ordinal.
        actual: P2CalibrationRun,
    },

    /// A shared P2 scenario and cache mode changed semantic result or route identity.
    P2SemanticDrift {
        /// Exact cohort ordinal exposing drift.
        run: P2CalibrationRun,
        /// Stable scenario identity.
        scenario_id: String,
        /// Proven cache mode.
        mode: P2SampleMode,
    },

    /// One exact scale scenario changed semantic result or route identity.
    ScaleSemanticDrift {
        /// Exact cohort ordinal exposing drift.
        run: P2CalibrationRun,
        /// Stable scale scenario identity.
        scenario_id: String,
    },
}

impl Display for CalibrationCohortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CohortDrift { expected, actual } => write!(
                formatter,
                "calibration cohort drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::ComponentEnvironmentDrift(run) => write!(
                formatter,
                "calibration run {run:?} components describe different measured subjects",
            ),
            Self::EnvironmentDrift(run) => write!(
                formatter,
                "calibration run {run:?} differs from the cohort environment or subject",
            ),
            Self::InstrumentationSemanticDrift(run) => write!(
                formatter,
                "calibration run {run:?} instrumentation result identity drifted",
            ),
            Self::InvalidInstrumentation { run, source } => write!(
                formatter,
                "calibration run {run:?} instrumentation evidence is invalid: {source}",
            ),
            Self::InvalidP2 { run, source } => {
                write!(
                    formatter,
                    "calibration run {run:?} P2 evidence is invalid: {source}"
                )
            }
            Self::InvalidProfile(source) => {
                write!(
                    formatter,
                    "calibration performance profile is invalid: {source}"
                )
            }
            Self::InvalidScale { run, source } => write!(
                formatter,
                "calibration run {run:?} scale evidence is invalid: {source}",
            ),
            Self::MissingCohort => formatter.write_str("calibration cohort identity is missing"),
            Self::MissingRequiredSentinel { run, scenario_id } => write!(
                formatter,
                "calibration run {run:?} is missing required sentinel {scenario_id:?}",
            ),
            Self::NotInitialCalibration(run) => write!(
                formatter,
                "calibration run {run:?} uses comparable-baseline evidence",
            ),
            Self::OrdinalDrift { expected, actual } => write!(
                formatter,
                "calibration ordinal drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::P2SemanticDrift {
                run,
                scenario_id,
                mode,
            } => write!(
                formatter,
                "calibration run {run:?} P2 scenario {scenario_id:?} {mode:?} semantics drifted",
            ),
            Self::ScaleSemanticDrift { run, scenario_id } => write!(
                formatter,
                "calibration run {run:?} scale scenario {scenario_id:?} semantics drifted",
            ),
        }
    }
}

impl Error for CalibrationCohortError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidInstrumentation { source, .. } => Some(source),
            Self::InvalidP2 { source, .. } => Some(source),
            Self::InvalidProfile(source) => Some(source),
            Self::InvalidScale { source, .. } => Some(source),
            _ => None,
        }
    }
}

///
/// CalibrationCohortArtifactError
///
/// Typed failure while publishing the bounded diagnostic review projection.
///

#[derive(Debug)]
pub(crate) enum CalibrationCohortArtifactError {
    /// The review could not be encoded as JSON.
    Encode {
        /// Intended output path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// The output directory or file operation failed.
    Io {
        /// Directory or file path involved.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// Filesystem cause.
        source: io::Error,
    },

    /// Encoded output exceeds the profile-owned artifact byte budget.
    TooLarge {
        /// Intended output path.
        path: PathBuf,
        /// Encoded bytes observed.
        observed_bytes: usize,
        /// Maximum permitted bytes.
        max_bytes: usize,
    },
}

impl Display for CalibrationCohortArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode { path, source } => write!(
                formatter,
                "calibration review {} could not be encoded: {source}",
                path.display(),
            ),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "calibration review {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "calibration review {} is {observed_bytes} bytes; maximum is {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for CalibrationCohortArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode { source, .. } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        deterministic_matrix,
        sql_perf_environment::tests::identity,
        sql_perf_instrumentation::{
            InstrumentationPathSample, build_instrumentation_calibration_report,
        },
        sql_perf_p2_shard::tests::complete_report as complete_p2_report,
        sql_perf_profile::SQL_PERFORMANCE_PROFILE,
        sql_perf_scale_shard::tests::complete_report as complete_scale_report,
    };

    use super::*;

    fn instrumentation_report() -> InstrumentationCalibrationReport {
        let attributed = [119, 121, 120, 123, 118]
            .into_iter()
            .map(|instructions| InstrumentationPathSample {
                result_signature: "projection|PerfAuditUser|id|1|1".to_string(),
                instructions,
            })
            .collect();
        let total_only = [99, 101, 100, 103, 98]
            .into_iter()
            .map(|instructions| InstrumentationPathSample {
                result_signature: "projection|PerfAuditUser|id|1|1".to_string(),
                instructions,
            })
            .collect();

        build_instrumentation_calibration_report(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            identity(),
            attributed,
            total_only,
        )
        .expect("test instrumentation evidence should build")
    }

    fn cohort_runs() -> [CalibrationRunArtifacts; 3] {
        let scenarios = deterministic_matrix();
        let p2 = complete_p2_report(&scenarios);
        let (_, scale) = complete_scale_report();
        let instrumentation = instrumentation_report();

        REQUIRED_CALIBRATION_RUNS.map(|run| {
            let mut p2 = p2.clone();
            p2.baseline_basis =
                P2BaselineBasis::initial_calibration("test-calibration".to_string(), run);
            CalibrationRunArtifacts::new(p2, scale.clone(), instrumentation.clone())
        })
    }

    #[test]
    fn exact_three_run_cohort_projects_budget_and_promotion_evidence() {
        let scenarios = deterministic_matrix();
        let review = build_calibration_cohort_review(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            cohort_runs(),
        )
        .expect("one exact clean cohort should review");

        assert_eq!(review.cohort(), "test-calibration");
        assert_eq!(review.runs.len(), 3);
        assert!(!review.recurring_hotspots.is_empty());
        assert!(review.unresolved_promotion_count() > 0);
        assert!(review.recurring_hotspots.iter().any(|hotspot| {
            hotspot.disposition == CalibrationHotspotDisposition::AlreadyRetained
                && SQL_PERFORMANCE_PROFILE
                    .regression_sentinel_scenario_ids()
                    .contains(&hotspot.scenario_id.as_str())
        }));
        assert!(review.p2_envelope_count() > 0);
        assert_eq!(review.scale_totals.len(), 72);
        assert_eq!(review.scale_slopes.len(), 48);
        assert_eq!(review.instrumentation.observations.len(), 3);

        let path = std::env::temp_dir().join(format!(
            "icydb-sql-perf-calibration-review-{}.json",
            std::process::id(),
        ));
        write_calibration_cohort_review(&path, SQL_PERFORMANCE_PROFILE, &review)
            .expect("bounded calibration review should write");
        assert!(
            fs::metadata(&path)
                .expect("written review should have metadata")
                .len()
                <= u64::try_from(SQL_PERFORMANCE_PROFILE.max_artifact_bytes())
                    .expect("artifact budget should fit u64"),
        );
        fs::remove_file(path).expect("temporary review should be removable");
    }

    #[test]
    fn cohort_rejects_wrong_identity_ordinal_and_subject() {
        let scenarios = deterministic_matrix();
        let mut wrong_cohort = cohort_runs();
        wrong_cohort[2].p2.baseline_basis = P2BaselineBasis::initial_calibration(
            "different-calibration".to_string(),
            P2CalibrationRun::Three,
        );
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                wrong_cohort,
            ),
            Err(CalibrationCohortError::CohortDrift { .. })
        ));

        let mut wrong_ordinal = cohort_runs();
        wrong_ordinal[1].p2.baseline_basis = P2BaselineBasis::initial_calibration(
            "test-calibration".to_string(),
            P2CalibrationRun::Three,
        );
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                wrong_ordinal,
            ),
            Err(CalibrationCohortError::OrdinalDrift {
                expected: P2CalibrationRun::Two,
                actual: P2CalibrationRun::Three,
            })
        ));

        let mut mixed_components = cohort_runs();
        mixed_components[1]
            .scale
            .environment
            .subject
            .raw_wasm_sha256 = "66".repeat(32);
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                mixed_components,
            ),
            Err(CalibrationCohortError::ComponentEnvironmentDrift(
                P2CalibrationRun::Two
            ))
        ));

        let mut mixed_subjects = cohort_runs();
        mixed_subjects[2].p2.environment.subject.raw_wasm_sha256 = "77".repeat(32);
        mixed_subjects[2].scale.environment.subject.raw_wasm_sha256 = "77".repeat(32);
        mixed_subjects[2]
            .instrumentation
            .environment
            .subject
            .raw_wasm_sha256 = "77".repeat(32);
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                mixed_subjects,
            ),
            Err(CalibrationCohortError::EnvironmentDrift(
                P2CalibrationRun::Three
            ))
        ));
    }

    #[test]
    fn cohort_rejects_cross_run_p2_scale_and_instrumentation_semantic_drift() {
        let scenarios = deterministic_matrix();
        let mut p2_drift = cohort_runs();
        let confirmation = &mut p2_drift[1].p2.confirmations[0];
        for sample in &mut confirmation.cold.samples {
            sample.result_signature = Some("p2-drift".to_string());
        }
        if let P2WarmEvidence::Confirmed(warm) = &mut confirmation.warm {
            for sample in &mut warm.samples {
                sample.result_signature = Some("p2-drift".to_string());
            }
        }
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                p2_drift,
            ),
            Err(CalibrationCohortError::P2SemanticDrift {
                run: P2CalibrationRun::Two,
                ..
            })
        ));

        let mut scale_drift = cohort_runs();
        scale_drift[2].scale.observations[0].sample.result_signature =
            Some("scale-drift".to_string());
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                scale_drift,
            ),
            Err(CalibrationCohortError::ScaleSemanticDrift {
                run: P2CalibrationRun::Three,
                ..
            })
        ));

        let mut instrumentation_drift = cohort_runs();
        let instrumentation = &mut instrumentation_drift[1].instrumentation;
        for sample in instrumentation
            .attributed_samples
            .iter_mut()
            .chain(&mut instrumentation.total_only_samples)
        {
            sample.result_signature = "instrumentation-drift".to_string();
        }
        instrumentation.sentinel_result_signature = "instrumentation-drift".to_string();
        assert!(matches!(
            build_calibration_cohort_review(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                instrumentation_drift,
            ),
            Err(CalibrationCohortError::InstrumentationSemanticDrift(
                P2CalibrationRun::Two
            ))
        ));
    }

    #[test]
    fn normalized_envelope_orders_exact_rationals_without_float_rounding() {
        let observations = [
            CalibrationNormalizedObservation {
                run: P2CalibrationRun::One,
                local_instructions: 10,
                units: 3,
            },
            CalibrationNormalizedObservation {
                run: P2CalibrationRun::Two,
                local_instructions: 7,
                units: 2,
            },
            CalibrationNormalizedObservation {
                run: P2CalibrationRun::Three,
                local_instructions: 13,
                units: 4,
            },
        ];

        let (minimum, median, maximum) = normalized_envelope(&observations);
        assert_eq!((minimum.local_instructions, minimum.units), (13, 4));
        assert_eq!((median.local_instructions, median.units), (10, 3));
        assert_eq!((maximum.local_instructions, maximum.units), (7, 2));
    }
}
