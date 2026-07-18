//! Module: sql_perf_p2
//! Responsibility: deterministic P2 candidate selection and strict candidate artifacts.
//! Does not own: P1 execution, repeated sampling, baseline comparison, or P2 verdicts.
//! Boundary: unions every required reason, enforces the hard cap, and revalidates serialized facts.

use crate::{
    MatrixSample, MatrixScenario,
    sql_harness::{PredicateFamily, QueryShape, StatementFamily, ValueTypeFamily, WindowBehavior},
    sql_perf_environment::{
        PerfEnvironmentError, PerfEnvironmentIdentity, PerfEnvironmentMismatch,
        PerfSubjectStateError, require_clean_perf_subject, require_comparable_environment,
        validate_perf_environment,
    },
    sql_perf_profile::{
        PerformanceProfile, PerformanceProfileError, SQL_PERFORMANCE_PROFILE, scenario_set_hash,
    },
    sql_perf_scale::{NORMALIZED_DENOMINATORS, NormalizedDenominator, compare_normalized_cost},
};

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

macro_rules! define_p2_raw_metrics {
    ($(#[$variant_doc:meta] $variant:ident => $field:ident),+ $(,)?) => {

        ///
        /// P2RawMetric
        ///
        /// Instruction phase whose P1 top ranking feeds P2 confirmation.
        /// Owned by P2 selection and serialized in candidate inclusion reasons.
        ///

        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(rename_all = "snake_case")]
        pub(crate) enum P2RawMetric {
            $(#[$variant_doc] $variant),+
        }

        impl P2RawMetric {
            /// Return every instruction metric required by P2 ranking and deltas.
            pub(crate) const fn all() -> &'static [Self] {
                P2_RAW_METRICS
            }

            /// Return the stable machine-readable metric code.
            pub(crate) const fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => stringify!($field)),+
                }
            }

            /// Read this metric from one retained matrix sample.
            pub(crate) const fn value(self, sample: &MatrixSample) -> u64 {
                match self {
                    $(Self::$variant => sample.$field),+
                }
            }
        }

        const P2_RAW_METRICS: &[P2RawMetric] = &[$(P2RawMetric::$variant),+];
    };
}

define_p2_raw_metrics! {
    /// Complete query execution.
    Total => total_local_instructions,
    /// SQL compilation.
    Compile => compile_local_instructions,
    /// Compile cache-key construction.
    CompileCacheKey => compile_cache_key_local_instructions,
    /// Compile cache lookup.
    CompileCacheLookup => compile_cache_lookup_local_instructions,
    /// SQL parsing.
    CompileParse => compile_parse_local_instructions,
    /// SQL tokenization.
    CompileParseTokenize => compile_parse_tokenize_local_instructions,
    /// SELECT parsing.
    CompileParseSelect => compile_parse_select_local_instructions,
    /// Expression parsing.
    CompileParseExpr => compile_parse_expr_local_instructions,
    /// Predicate parsing.
    CompileParsePredicate => compile_parse_predicate_local_instructions,
    /// Aggregate-lane classification.
    CompileAggregateLaneCheck => compile_aggregate_lane_check_local_instructions,
    /// Semantic preparation.
    CompilePrepare => compile_prepare_local_instructions,
    /// Semantic lowering.
    CompileLower => compile_lower_local_instructions,
    /// Parameter binding.
    CompileBind => compile_bind_local_instructions,
    /// Compiled-command cache insertion.
    CompileCacheInsert => compile_cache_insert_local_instructions,
    /// Executable query work after compilation.
    Execute => execute_local_instructions,
    /// Planner work.
    Planner => planner_local_instructions,
    /// Planner schema-info projection.
    PlannerSchemaInfo => planner_schema_info_local_instructions,
    /// Planner preparation.
    PlannerPrepare => planner_prepare_local_instructions,
    /// Plan-cache key construction.
    PlannerCacheKey => planner_cache_key_local_instructions,
    /// Plan-cache lookup.
    PlannerCacheLookup => planner_cache_lookup_local_instructions,
    /// Physical plan construction.
    PlannerPlanBuild => planner_plan_build_local_instructions,
    /// Plan-cache insertion.
    PlannerCacheInsert => planner_cache_insert_local_instructions,
    /// Store work owned outside executor runtime.
    Store => store_local_instructions,
    /// Complete executor invocation, including store work.
    ExecutorInvocation => executor_invocation_local_instructions,
    /// Executor runtime excluding store work.
    ExecutorRuntime => executor_local_instructions,
    /// SQL response finalization.
    ResponseFinalization => response_finalization_local_instructions,
    /// Grouped input streaming.
    GroupedStream => grouped_stream_local_instructions,
    /// Grouped aggregate folding.
    GroupedFold => grouped_fold_local_instructions,
    /// Grouped response finalization.
    GroupedFinalize => grouped_finalize_local_instructions,
    /// Scalar-aggregate base-row preparation.
    ScalarAggregateBaseRow => scalar_aggregate_base_row_local_instructions,
    /// Scalar-aggregate reducer work.
    ScalarAggregateReducerFold => scalar_aggregate_reducer_fold_local_instructions,
    /// Pure-covering value decoding.
    PureCoveringDecode => pure_covering_decode_local_instructions,
    /// Pure-covering response assembly.
    PureCoveringRowAssembly => pure_covering_row_assembly_local_instructions,
    /// Direct-row scan work.
    DirectDataRowScan => direct_data_row_scan_local_instructions,
    /// Direct-row key-stream work.
    DirectDataRowKeyStream => direct_data_row_key_stream_local_instructions,
    /// Direct-row value reads.
    DirectDataRowRowRead => direct_data_row_row_read_local_instructions,
    /// Direct-row key encoding.
    DirectDataRowKeyEncode => direct_data_row_key_encode_local_instructions,
    /// Direct-row store lookups.
    DirectDataRowStoreGet => direct_data_row_store_get_local_instructions,
    /// Direct-row ordered-window work.
    DirectDataRowOrderWindow => direct_data_row_order_window_local_instructions,
    /// Direct-row page-window work.
    DirectDataRowPageWindow => direct_data_row_page_window_local_instructions,
    /// Kernel-row scan work.
    KernelRowScan => kernel_row_scan_local_instructions,
    /// Kernel-row key-stream work.
    KernelRowKeyStream => kernel_row_key_stream_local_instructions,
    /// Kernel-row value reads.
    KernelRowRowRead => kernel_row_row_read_local_instructions,
    /// Kernel-row ordered-window work.
    KernelRowOrderWindow => kernel_row_order_window_local_instructions,
    /// Kernel-row page-window work.
    KernelRowPageWindow => kernel_row_page_window_local_instructions,
}

///
/// P2StratumDimension
///
/// Declared performance dimension whose worst P1 observation requires confirmation.
/// Owned by P2 selection and serialized in stratum inclusion reasons.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum P2StratumDimension {
    /// Fixture and storage surface.
    Surface,

    /// Top-level SQL statement family.
    Statement,

    /// Scalar, aggregate, grouped, or metadata result shape.
    Shape,

    /// Coarse projected value family.
    ValueType,

    /// Semantic predicate family.
    Predicate,

    /// Ordering and bounding behavior.
    Window,

    /// Observed typed execution-route family.
    Route,
}

///
/// P2CandidateReason
///
/// One typed reason a P1 scenario must be rerun by P2.
/// Owned by P2 selection and retained by confirmation artifacts and reports.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum P2CandidateReason {
    /// The scenario ranked in the checked-in top set for one raw phase.
    RawMetric {
        /// Ranked instruction phase.
        metric: P2RawMetric,
        /// Stable one-based rank.
        rank: usize,
    },

    /// The scenario ranked in the checked-in top set for one normalized cost.
    NormalizedMetric {
        /// Measured nonzero denominator.
        denominator: NormalizedDenominator,
        /// Stable one-based rank.
        rank: usize,
    },

    /// The scenario was the highest-cost observation in one declared stratum.
    StratumWorst {
        /// Typed stratum dimension.
        dimension: P2StratumDimension,
        /// Stable value within the dimension.
        value: String,
    },

    /// The scenario crossed one checked-in comparable-baseline threshold.
    BaselineThreshold {
        /// Typed thresholded instruction metric.
        metric: P2RawMetric,
    },

    /// The scenario represents one required scale stratum.
    ScaleStratum {
        /// Stable scale-stratum identifier.
        stratum: String,
    },

    /// The checked-in profile marks the scenario as a regression sentinel.
    RegressionSentinel,

    /// The checked-in profile marks the scenario as a focused hotspot.
    FocusedHotspot,
}

///
/// P2Candidate
///
/// One deduplicated P2 scenario with every inclusion reason retained.
/// Owned by P2 selection and consumed unchanged by confirmation sharding.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2Candidate {
    /// Stable scenario identity shared with P1.
    pub(crate) scenario_id: String,

    /// Deterministic zero-based P2 shard assignment.
    pub(crate) shard_index: u8,

    /// Stable sorted reasons requiring confirmation.
    pub(crate) reasons: Vec<P2CandidateReason>,
}

///
/// P2CalibrationRun
///
/// Exact ordinal in one reviewed three-run initial-calibration cohort.
/// Owned by P2 selection and retained in calibration evidence.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum P2CalibrationRun {
    /// First clean run.
    One,

    /// Second clean run.
    Two,

    /// Third clean run.
    Three,
}

impl P2CalibrationRun {
    /// Parse the command-boundary ordinal used by the scheduled runner.
    pub(crate) const fn from_ordinal(value: &str) -> Option<Self> {
        match value.as_bytes() {
            b"1" => Some(Self::One),
            b"2" => Some(Self::Two),
            b"3" => Some(Self::Three),
            _ => None,
        }
    }
}

///
/// P2BaselineBasis
///
/// Reviewed baseline authority used while deriving one P2 candidate set.
/// Owned by P2 selection and consumed by merged evidence and comparison.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum P2BaselineBasis {
    /// P1 discovery compared the current scan to one clean comparable baseline.
    Comparable {
        /// Complete reviewed baseline identity used for discovery.
        /// Boxed only to keep the calibration variant's stack footprint small.
        baseline_environment: Box<PerfEnvironmentIdentity>,
        /// Exact typed threshold-crossing reasons supplied to selection.
        threshold_crossing_count: usize,
    },

    /// No historical delta exists; this artifact belongs to a three-run cohort.
    InitialCalibration {
        /// Stable reviewer-chosen cohort identity shared by exactly three runs.
        cohort: String,
        /// Exact clean run ordinal within the cohort.
        run: P2CalibrationRun,
    },
}

impl P2BaselineBasis {
    /// Construct a comparable-baseline basis with its exact crossing count.
    pub(crate) fn comparable(
        baseline_environment: PerfEnvironmentIdentity,
        threshold_crossing_count: usize,
    ) -> Self {
        Self::Comparable {
            baseline_environment: Box::new(baseline_environment),
            threshold_crossing_count,
        }
    }

    /// Construct one explicitly identified initial-calibration run.
    pub(crate) const fn initial_calibration(cohort: String, run: P2CalibrationRun) -> Self {
        Self::InitialCalibration { cohort, run }
    }
}

///
/// P2CandidateSelection
///
/// Exact current P2 candidate set derived from one complete P1 report.
/// Owned by P2 selection and consumed by every P2 shard and merge boundary.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2CandidateSelection {
    /// Checked-in performance profile version.
    pub(crate) performance_profile_version: u32,

    /// Complete P1 scenario-set identity used for discovery.
    pub(crate) p1_scenario_set_hash: String,

    /// Canonical identity of the selected P2 scenario IDs.
    pub(crate) p2_scenario_set_hash: String,

    /// Complete P1/scale environment from which this selection was derived.
    pub(crate) environment: PerfEnvironmentIdentity,

    /// Comparable-baseline or explicit initial-calibration basis.
    pub(crate) baseline_basis: P2BaselineBasis,

    /// Exact selected scenario count.
    pub(crate) candidate_count: usize,

    /// Candidates ordered by stable scenario identity.
    pub(crate) candidates: Vec<P2Candidate>,
}

///
/// P2ThresholdCrossing
///
/// One scenario and metric known to cross a comparable-baseline threshold.
/// Owned by baseline comparison and supplied as typed input to P2 selection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct P2ThresholdCrossing {
    /// Stable P1 scenario identity.
    pub(crate) scenario_id: String,

    /// Typed thresholded instruction metric.
    pub(crate) metric: P2RawMetric,
}

///
/// P2ScaleRepresentative
///
/// One scenario selected as the worst representative of a required scale stratum.
/// Owned by scale evidence and supplied as typed input to P2 selection.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2ScaleRepresentative {
    /// Stable P1 scenario identity.
    pub(crate) scenario_id: String,

    /// Stable scale-stratum identifier.
    pub(crate) stratum: String,
}

///
/// P2SelectionRequirements
///
/// Required candidates supplied by baseline, scale, and checked-in profile owners.
/// Owned by P2 selection as its explicit non-ranking input contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct P2SelectionRequirements {
    /// Reviewed basis governing whether threshold crossings may be absent.
    baseline_basis: P2BaselineBasis,

    /// Comparable-baseline threshold crossings.
    threshold_crossings: Vec<P2ThresholdCrossing>,

    /// Required scale-stratum representatives.
    scale_representatives: Vec<P2ScaleRepresentative>,

    /// Checked-in regression sentinel scenario IDs.
    regression_sentinels: Vec<String>,

    /// Checked-in focused-hotspot scenario IDs.
    focused_hotspots: Vec<String>,
}

impl P2SelectionRequirements {
    /// Construct the explicit non-ranking requirements for one selection.
    pub(crate) const fn new(
        baseline_basis: P2BaselineBasis,
        threshold_crossings: Vec<P2ThresholdCrossing>,
        scale_representatives: Vec<P2ScaleRepresentative>,
        regression_sentinels: Vec<String>,
        focused_hotspots: Vec<String>,
    ) -> Self {
        Self {
            baseline_basis,
            threshold_crossings,
            scale_representatives,
            regression_sentinels,
            focused_hotspots,
        }
    }

    /// Construct current checked-in sentinel requirements plus derived inputs.
    pub(crate) fn from_profile(
        profile: PerformanceProfile,
        baseline_basis: P2BaselineBasis,
        threshold_crossings: Vec<P2ThresholdCrossing>,
        scale_representatives: Vec<P2ScaleRepresentative>,
    ) -> Self {
        Self::new(
            baseline_basis,
            threshold_crossings,
            scale_representatives,
            profile
                .regression_sentinel_scenario_ids()
                .iter()
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
            profile
                .focused_hotspot_scenario_ids()
                .iter()
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
        )
    }
}

/// Select the exact deterministic P2 confirmation union from complete P1 evidence.
///
/// # Errors
///
/// Returns a typed selection error for incomplete P1 identity, observation drift,
/// an unobserved required metric, an invalid required candidate, or a union above
/// the checked-in hard cap. The selector never truncates.
pub(crate) fn select_p2_candidates(
    profile: PerformanceProfile,
    environment: &PerfEnvironmentIdentity,
    scenarios: &[MatrixScenario],
    samples: &[MatrixSample],
    requirements: &P2SelectionRequirements,
) -> Result<P2CandidateSelection, P2SelectionError> {
    validate_perf_environment(profile, environment)
        .map_err(P2SelectionError::InvalidEnvironment)?;
    validate_baseline_basis(
        profile,
        environment,
        &requirements.baseline_basis,
        requirements.threshold_crossings.len(),
    )?;
    profile
        .validate_scenario_set(scenarios.iter().map(|scenario| scenario.key.as_str()))
        .map_err(P2SelectionError::InvalidDeclaredScenarioSet)?;
    profile
        .validate_scenario_set(samples.iter().map(|sample| sample.key.as_str()))
        .map_err(P2SelectionError::InvalidObservationScenarioSet)?;

    let declarations = scenarios
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    for sample in samples {
        let scenario = declarations
            .get(sample.key.as_str())
            .copied()
            .ok_or_else(|| P2SelectionError::MissingDeclaration(sample.key.clone()))?;
        if sample.surface != scenario.surface.label()
            || sample.family != scenario.family
            || sample.sql != scenario.sql
        {
            return Err(P2SelectionError::ObservationDrift(sample.key.clone()));
        }
    }

    let mut selected = BTreeMap::<String, BTreeSet<P2CandidateReason>>::new();
    append_raw_rankings(profile, samples, &mut selected)?;
    append_normalized_rankings(profile, samples, &mut selected)?;
    append_stratum_worst(scenarios, samples, &mut selected)?;
    append_explicit_requirements(&declarations, requirements, &mut selected)?;

    let cap = profile.confirmation_scenario_cap();
    if selected.len() > cap {
        return Err(P2SelectionError::CandidateCapExceeded {
            cap,
            actual: selected.len(),
        });
    }

    let candidates = selected
        .into_iter()
        .map(|(scenario_id, reasons)| {
            let shard_index = profile
                .scenario_shard(&scenario_id)
                .map_err(P2SelectionError::InvalidProfile)?;
            Ok(P2Candidate {
                scenario_id,
                shard_index,
                reasons: reasons.into_iter().collect(),
            })
        })
        .collect::<Result<Vec<_>, P2SelectionError>>()?;
    let p2_scenario_set_hash = scenario_set_hash(
        candidates
            .iter()
            .map(|candidate| candidate.scenario_id.as_str()),
    )
    .map_err(P2SelectionError::InvalidCandidateScenarioSet)?;

    let selection = P2CandidateSelection {
        performance_profile_version: profile.version(),
        p1_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
        p2_scenario_set_hash,
        environment: environment.clone(),
        baseline_basis: requirements.baseline_basis.clone(),
        candidate_count: candidates.len(),
        candidates,
    };
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    validate_p2_candidate_selection(profile, &declared_ids, &selection)?;

    Ok(selection)
}

/// Validate one serialized P2 candidate set against current profile authority.
///
/// # Errors
///
/// Returns a typed selection error for profile or declaration drift, candidate
/// count/hash/order/reason drift, an unknown candidate, or a wrong shard.
pub(crate) fn validate_p2_candidate_selection(
    profile: PerformanceProfile,
    declared_ids: &[&str],
    selection: &P2CandidateSelection,
) -> Result<(), P2SelectionError> {
    profile
        .validate_scenario_set(declared_ids.iter().copied())
        .map_err(P2SelectionError::InvalidDeclaredScenarioSet)?;
    if selection.performance_profile_version != profile.version() {
        return Err(P2SelectionError::ProfileVersionDrift {
            expected: profile.version(),
            actual: selection.performance_profile_version,
        });
    }
    if selection.p1_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(P2SelectionError::P1ScenarioSetHashDrift {
            expected: profile.expected_scenario_set_hash(),
            actual: selection.p1_scenario_set_hash.clone(),
        });
    }
    validate_perf_environment(profile, &selection.environment)
        .map_err(P2SelectionError::InvalidEnvironment)?;
    let threshold_crossing_count = selection
        .candidates
        .iter()
        .flat_map(|candidate| &candidate.reasons)
        .filter(|reason| matches!(reason, P2CandidateReason::BaselineThreshold { .. }))
        .count();
    validate_baseline_basis(
        profile,
        &selection.environment,
        &selection.baseline_basis,
        threshold_crossing_count,
    )?;
    if selection.candidate_count != selection.candidates.len() {
        return Err(P2SelectionError::CandidateCountDrift {
            declared: selection.candidate_count,
            actual: selection.candidates.len(),
        });
    }
    if selection.candidates.len() > profile.confirmation_scenario_cap() {
        return Err(P2SelectionError::CandidateCapExceeded {
            cap: profile.confirmation_scenario_cap(),
            actual: selection.candidates.len(),
        });
    }
    if !selection
        .candidates
        .windows(2)
        .all(|pair| pair[0].scenario_id < pair[1].scenario_id)
    {
        return Err(P2SelectionError::CandidateOrderingDrift);
    }

    let declared = declared_ids.iter().copied().collect::<BTreeSet<_>>();
    for candidate in &selection.candidates {
        if !declared.contains(candidate.scenario_id.as_str()) {
            return Err(P2SelectionError::UnknownRequiredScenario(
                candidate.scenario_id.clone(),
            ));
        }
        if candidate.reasons.is_empty()
            || !candidate
                .reasons
                .windows(2)
                .all(|reasons| reasons[0] < reasons[1])
        {
            return Err(P2SelectionError::CandidateReasonDrift(
                candidate.scenario_id.clone(),
            ));
        }
        let expected_shard = profile
            .scenario_shard(&candidate.scenario_id)
            .map_err(P2SelectionError::InvalidProfile)?;
        if candidate.shard_index != expected_shard {
            return Err(P2SelectionError::CandidateShardDrift {
                scenario_id: candidate.scenario_id.clone(),
                expected: expected_shard,
                actual: candidate.shard_index,
            });
        }
    }

    let observed_hash = scenario_set_hash(
        selection
            .candidates
            .iter()
            .map(|candidate| candidate.scenario_id.as_str()),
    )
    .map_err(P2SelectionError::InvalidCandidateScenarioSet)?;
    if selection.p2_scenario_set_hash != observed_hash {
        return Err(P2SelectionError::P2ScenarioSetHashDrift {
            declared: selection.p2_scenario_set_hash.clone(),
            observed: observed_hash,
        });
    }

    Ok(())
}

/// Write one validated current-format P2 candidate artifact.
///
/// # Errors
///
/// Returns a typed artifact error for invalid evidence, encoding, size-budget,
/// directory, or write failure.
pub(crate) fn write_p2_candidate_selection(
    path: &Path,
    profile: PerformanceProfile,
    declared_ids: &[&str],
    selection: &P2CandidateSelection,
) -> Result<(), P2SelectionArtifactError> {
    validate_p2_candidate_selection(profile, declared_ids, selection)
        .map_err(P2SelectionArtifactError::InvalidSelection)?;
    let encoded = serde_json::to_vec_pretty(selection).map_err(|source| {
        P2SelectionArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    validate_p2_selection_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| P2SelectionArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| P2SelectionArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one strict bounded P2 candidate artifact.
///
/// # Errors
///
/// Returns a typed artifact error for open, read, size, strict-decoding, or
/// current-profile validation failure.
pub(crate) fn read_p2_candidate_selection(
    path: &Path,
    profile: PerformanceProfile,
    declared_ids: &[&str],
) -> Result<P2CandidateSelection, P2SelectionArtifactError> {
    let file = fs::File::open(path).map_err(|source| P2SelectionArtifactError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| P2SelectionArtifactError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_p2_selection_artifact_size(path, bytes.len(), max_bytes)?;
    let selection =
        serde_json::from_slice(&bytes).map_err(|source| P2SelectionArtifactError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_p2_candidate_selection(profile, declared_ids, &selection)
        .map_err(P2SelectionArtifactError::InvalidSelection)?;

    Ok(selection)
}

/// Enforce the checked-in byte budget for one P2 selection artifact.
///
/// # Errors
///
/// Returns a typed oversize error when the observed artifact exceeds the limit.
pub(crate) fn validate_p2_selection_artifact_size(
    path: &Path,
    observed_bytes: usize,
    max_bytes: usize,
) -> Result<(), P2SelectionArtifactError> {
    if observed_bytes > max_bytes {
        return Err(P2SelectionArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

/// Revalidate the exact discovery authority serialized with one P2 selection.
///
/// # Errors
///
/// Returns a typed error for dirty subjects, invalid or incomparable environment
/// identity, calibration threshold reasons, or retained crossing-count drift.
fn validate_baseline_basis(
    profile: PerformanceProfile,
    current_environment: &PerfEnvironmentIdentity,
    basis: &P2BaselineBasis,
    actual_threshold_crossing_count: usize,
) -> Result<(), P2SelectionError> {
    require_clean_perf_subject(current_environment).map_err(|source| {
        P2SelectionError::UncleanSubject {
            subject: "current",
            source,
        }
    })?;
    match basis {
        P2BaselineBasis::Comparable {
            baseline_environment,
            threshold_crossing_count,
        } => {
            validate_perf_environment(profile, baseline_environment)
                .map_err(P2SelectionError::InvalidBaselineEnvironment)?;
            require_clean_perf_subject(baseline_environment).map_err(|source| {
                P2SelectionError::UncleanSubject {
                    subject: "baseline",
                    source,
                }
            })?;
            require_comparable_environment(baseline_environment, current_environment)
                .map_err(P2SelectionError::IncomparableBaselineEnvironment)?;
            if *threshold_crossing_count != actual_threshold_crossing_count {
                return Err(P2SelectionError::ThresholdCrossingCountDrift {
                    declared: *threshold_crossing_count,
                    actual: actual_threshold_crossing_count,
                });
            }
        }
        P2BaselineBasis::InitialCalibration { cohort, .. } => {
            if !valid_calibration_cohort(cohort) {
                return Err(P2SelectionError::InvalidCalibrationCohort(cohort.clone()));
            }
            if actual_threshold_crossing_count != 0 {
                return Err(P2SelectionError::CalibrationThresholdCrossings(
                    actual_threshold_crossing_count,
                ));
            }
        }
    }

    Ok(())
}

fn valid_calibration_cohort(cohort: &str) -> bool {
    !cohort.is_empty()
        && cohort.len() <= 64
        && cohort.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

fn append_raw_rankings(
    profile: PerformanceProfile,
    samples: &[MatrixSample],
    selected: &mut BTreeMap<String, BTreeSet<P2CandidateReason>>,
) -> Result<(), P2SelectionError> {
    for metric in P2_RAW_METRICS {
        let mut ranked = samples
            .iter()
            .filter(|sample| metric.value(sample) != 0)
            .collect::<Vec<_>>();
        if ranked.is_empty() {
            return Err(P2SelectionError::UnobservedRawMetric(*metric));
        }
        ranked.sort_by(|left, right| {
            metric
                .value(right)
                .cmp(&metric.value(left))
                .then_with(|| left.key.cmp(&right.key))
        });
        for (offset, sample) in ranked
            .into_iter()
            .take(profile.confirmation_top_n_per_metric())
            .enumerate()
        {
            insert_reason(
                selected,
                &sample.key,
                P2CandidateReason::RawMetric {
                    metric: *metric,
                    rank: offset + 1,
                },
            );
        }
    }

    Ok(())
}

fn append_normalized_rankings(
    profile: PerformanceProfile,
    samples: &[MatrixSample],
    selected: &mut BTreeMap<String, BTreeSet<P2CandidateReason>>,
) -> Result<(), P2SelectionError> {
    for denominator in NORMALIZED_DENOMINATORS {
        let mut ranked = samples
            .iter()
            .filter(|sample| denominator.measured_units(sample).is_some())
            .collect::<Vec<_>>();
        if ranked.is_empty() {
            return Err(P2SelectionError::UnobservedNormalizedDenominator(
                *denominator,
            ));
        }
        ranked.sort_by(|left, right| compare_normalized_cost(*denominator, left, right));
        for (offset, sample) in ranked
            .into_iter()
            .take(profile.confirmation_top_n_per_metric())
            .enumerate()
        {
            insert_reason(
                selected,
                &sample.key,
                P2CandidateReason::NormalizedMetric {
                    denominator: *denominator,
                    rank: offset + 1,
                },
            );
        }
    }

    Ok(())
}

fn append_stratum_worst(
    scenarios: &[MatrixScenario],
    samples: &[MatrixSample],
    selected: &mut BTreeMap<String, BTreeSet<P2CandidateReason>>,
) -> Result<(), P2SelectionError> {
    let declarations = scenarios
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut worst = BTreeMap::<(P2StratumDimension, String), &MatrixSample>::new();
    for sample in samples {
        let scenario = declarations
            .get(sample.key.as_str())
            .copied()
            .ok_or_else(|| P2SelectionError::MissingDeclaration(sample.key.clone()))?;
        for stratum in scenario_strata(scenario, sample) {
            let replace = worst.get(&stratum).is_none_or(|current| {
                sample.total_local_instructions > current.total_local_instructions
                    || (sample.total_local_instructions == current.total_local_instructions
                        && sample.key < current.key)
            });
            if replace {
                worst.insert(stratum, sample);
            }
        }
    }

    for ((dimension, value), sample) in worst {
        insert_reason(
            selected,
            &sample.key,
            P2CandidateReason::StratumWorst { dimension, value },
        );
    }

    Ok(())
}

fn scenario_strata(
    scenario: &MatrixScenario,
    sample: &MatrixSample,
) -> [(P2StratumDimension, String); 7] {
    [
        (
            P2StratumDimension::Surface,
            scenario.surface.label().to_string(),
        ),
        (
            P2StratumDimension::Statement,
            statement_code(scenario.metadata.statement).to_string(),
        ),
        (
            P2StratumDimension::Shape,
            shape_code(scenario.metadata.shape).to_string(),
        ),
        (
            P2StratumDimension::ValueType,
            value_type_code(scenario.metadata.value_type).to_string(),
        ),
        (
            P2StratumDimension::Predicate,
            predicate_code(scenario.metadata.predicate).to_string(),
        ),
        (
            P2StratumDimension::Window,
            window_code(scenario.metadata.window.behavior).to_string(),
        ),
        (P2StratumDimension::Route, sample.route_family.clone()),
    ]
}

fn append_explicit_requirements(
    declarations: &BTreeMap<&str, &MatrixScenario>,
    requirements: &P2SelectionRequirements,
    selected: &mut BTreeMap<String, BTreeSet<P2CandidateReason>>,
) -> Result<(), P2SelectionError> {
    for crossing in &requirements.threshold_crossings {
        validate_requirement(declarations, &crossing.scenario_id, crossing.metric.code())?;
        insert_reason(
            selected,
            &crossing.scenario_id,
            P2CandidateReason::BaselineThreshold {
                metric: crossing.metric,
            },
        );
    }
    for representative in &requirements.scale_representatives {
        validate_requirement(
            declarations,
            &representative.scenario_id,
            &representative.stratum,
        )?;
        insert_reason(
            selected,
            &representative.scenario_id,
            P2CandidateReason::ScaleStratum {
                stratum: representative.stratum.clone(),
            },
        );
    }
    for scenario_id in &requirements.regression_sentinels {
        validate_requirement(declarations, scenario_id, "regression_sentinel")?;
        insert_reason(selected, scenario_id, P2CandidateReason::RegressionSentinel);
    }
    for scenario_id in &requirements.focused_hotspots {
        validate_requirement(declarations, scenario_id, "focused_hotspot")?;
        insert_reason(selected, scenario_id, P2CandidateReason::FocusedHotspot);
    }

    Ok(())
}

fn validate_requirement(
    declarations: &BTreeMap<&str, &MatrixScenario>,
    scenario_id: &str,
    label: &str,
) -> Result<(), P2SelectionError> {
    if label.is_empty() {
        return Err(P2SelectionError::EmptyRequirementLabel(
            scenario_id.to_string(),
        ));
    }
    if !declarations.contains_key(scenario_id) {
        return Err(P2SelectionError::UnknownRequiredScenario(
            scenario_id.to_string(),
        ));
    }

    Ok(())
}

fn insert_reason(
    selected: &mut BTreeMap<String, BTreeSet<P2CandidateReason>>,
    scenario_id: &str,
    reason: P2CandidateReason,
) {
    selected
        .entry(scenario_id.to_string())
        .or_default()
        .insert(reason);
}

const fn statement_code(statement: StatementFamily) -> &'static str {
    match statement {
        StatementFamily::Delete => "delete",
        StatementFamily::Describe => "describe",
        StatementFamily::Explain => "explain",
        StatementFamily::Insert => "insert",
        StatementFamily::Select => "select",
        StatementFamily::Show => "show",
        StatementFamily::Update => "update",
    }
}

const fn shape_code(shape: QueryShape) -> &'static str {
    match shape {
        QueryShape::GlobalAggregate => "global_aggregate",
        QueryShape::Grouped => "grouped",
        QueryShape::Metadata => "metadata",
        QueryShape::Mutation => "mutation",
        QueryShape::Scalar => "scalar",
    }
}

const fn value_type_code(value_type: ValueTypeFamily) -> &'static str {
    match value_type {
        ValueTypeFamily::Blob => "blob",
        ValueTypeFamily::Boolean => "boolean",
        ValueTypeFamily::Catalog => "catalog",
        ValueTypeFamily::Mixed => "mixed",
        ValueTypeFamily::Numeric => "numeric",
        ValueTypeFamily::Text => "text",
    }
}

const fn predicate_code(predicate: PredicateFamily) -> &'static str {
    match predicate {
        PredicateFamily::Boolean => "boolean",
        PredicateFamily::CasefoldPrefix => "casefold_prefix",
        PredicateFamily::Compound => "compound",
        PredicateFamily::FieldComparison => "field_comparison",
        PredicateFamily::Membership => "membership",
        PredicateFamily::None => "none",
        PredicateFamily::Prefix => "prefix",
        PredicateFamily::PrimaryKey => "primary_key",
        PredicateFamily::Range => "range",
        PredicateFamily::SparseMembership => "sparse_membership",
    }
}

const fn window_code(window: WindowBehavior) -> &'static str {
    match window {
        WindowBehavior::None => "none",
        WindowBehavior::Limit => "limit",
        WindowBehavior::Ordered => "ordered",
        WindowBehavior::OrderedLimit => "ordered_limit",
        WindowBehavior::OrderedLimitOffset => "ordered_limit_offset",
    }
}

///
/// P2SelectionError
///
/// Typed failure while deriving or revalidating the exact P2 confirmation set.
/// Owned by P2 selection and preserved by strict candidate artifact errors.
///

#[derive(Debug)]
pub(crate) enum P2SelectionError {
    /// Initial-calibration evidence retained historical threshold-crossing reasons.
    CalibrationThresholdCrossings(usize),

    /// The deterministic union exceeds the checked-in cap and was not truncated.
    CandidateCapExceeded {
        /// Checked-in maximum candidate count.
        cap: usize,
        /// Deterministic union size.
        actual: usize,
    },

    /// Serialized candidate count differs from retained candidates.
    CandidateCountDrift {
        /// Serialized count.
        declared: usize,
        /// Retained candidate count.
        actual: usize,
    },

    /// Serialized candidates are not in strict stable-ID order.
    CandidateOrderingDrift,

    /// One candidate has no reasons or reasons that are not strictly ordered.
    CandidateReasonDrift(String),

    /// One candidate's serialized shard differs from deterministic assignment.
    CandidateShardDrift {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Deterministic shard assignment.
        expected: u8,
        /// Serialized shard assignment.
        actual: u8,
    },

    /// An explicit threshold or scale requirement has an empty stable label.
    EmptyRequirementLabel(String),

    /// The selected P2 identities cannot be encoded canonically.
    InvalidCandidateScenarioSet(PerformanceProfileError),

    /// The declared P1 scenarios do not match the checked-in profile.
    InvalidDeclaredScenarioSet(PerformanceProfileError),

    /// Successful P1 observations do not form the complete profile.
    InvalidObservationScenarioSet(PerformanceProfileError),

    /// The checked-in performance profile itself is invalid.
    InvalidProfile(PerformanceProfileError),

    /// The P1/scale environment retained by selection is invalid.
    InvalidEnvironment(PerfEnvironmentError),

    /// The comparable P1 baseline environment is invalid.
    InvalidBaselineEnvironment(PerfEnvironmentError),

    /// The reviewer-chosen initial-calibration cohort is empty or non-canonical.
    InvalidCalibrationCohort(String),

    /// The reviewed baseline and current P1 environments are incomparable.
    IncomparableBaselineEnvironment(PerfEnvironmentMismatch),

    /// One observed scenario has no declared metadata.
    MissingDeclaration(String),

    /// One observation's surface, family, or SQL differs from its declaration.
    ObservationDrift(String),

    /// Serialized P1 identity differs from the checked-in complete profile.
    P1ScenarioSetHashDrift {
        /// Checked-in complete P1 identity.
        expected: &'static str,
        /// Serialized P1 identity.
        actual: String,
    },

    /// Serialized P2 identity differs from the retained candidate IDs.
    P2ScenarioSetHashDrift {
        /// Serialized P2 identity.
        declared: String,
        /// Identity recomputed from retained candidates.
        observed: String,
    },

    /// Serialized profile version differs from the checked-in profile.
    ProfileVersionDrift {
        /// Checked-in profile version.
        expected: u32,
        /// Serialized profile version.
        actual: u32,
    },

    /// Serialized threshold-crossing count differs from retained typed reasons.
    ThresholdCrossingCountDrift {
        /// Count declared by the comparable-baseline basis.
        declared: usize,
        /// Count reconstructed from candidate reasons.
        actual: usize,
    },

    /// One measured subject came from source state outside its recorded revision.
    UncleanSubject {
        /// Stable subject label.
        subject: &'static str,
        /// Typed source-state cause.
        source: PerfSubjectStateError,
    },

    /// An explicit requirement names a scenario outside the exact P1 profile.
    UnknownRequiredScenario(String),

    /// A required normalized denominator has no eligible nonzero observation.
    UnobservedNormalizedDenominator(NormalizedDenominator),

    /// A required raw instruction metric has no positive observation.
    UnobservedRawMetric(P2RawMetric),
}

fn write_selection_cause(
    formatter: &mut fmt::Formatter<'_>,
    context: &str,
    cause: &dyn Display,
) -> fmt::Result {
    write!(formatter, "{context}: {cause}")
}

fn fmt_cal_count(formatter: &mut fmt::Formatter<'_>, actual: usize) -> fmt::Result {
    write!(
        formatter,
        "initial-calibration P2 selection retained {actual} baseline threshold crossings",
    )
}

fn fmt_calibration_cohort(formatter: &mut fmt::Formatter<'_>, cohort: &str) -> fmt::Result {
    write!(
        formatter,
        "invalid P2 initial-calibration cohort {cohort:?}"
    )
}

impl Display for P2SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CalibrationThresholdCrossings(actual) => fmt_cal_count(formatter, *actual),
            Self::CandidateCountDrift { declared, actual } => write!(
                formatter,
                "P2 candidate count drifted: declared {declared}, retained {actual}",
            ),
            Self::CandidateOrderingDrift => {
                formatter.write_str("P2 candidates are not in strict stable-ID order")
            }
            Self::CandidateReasonDrift(scenario_id) => write!(
                formatter,
                "P2 candidate {scenario_id:?} has missing, duplicate, or unordered reasons",
            ),
            Self::CandidateShardDrift {
                scenario_id,
                expected,
                actual,
            } => write!(
                formatter,
                "P2 candidate {scenario_id:?} shard drifted: expected {expected}, observed {actual}",
            ),
            Self::InvalidProfile(error) => {
                write_selection_cause(formatter, "invalid performance profile", error)
            }
            Self::InvalidEnvironment(error) => {
                write_selection_cause(formatter, "invalid P2 selection environment", error)
            }
            Self::InvalidBaselineEnvironment(error) => {
                write_selection_cause(formatter, "invalid P2 baseline environment", error)
            }
            Self::InvalidCalibrationCohort(cohort) => fmt_calibration_cohort(formatter, cohort),
            Self::IncomparableBaselineEnvironment(error) => {
                write_selection_cause(formatter, "P2 baseline environment is incomparable", error)
            }
            Self::InvalidDeclaredScenarioSet(error) => {
                write_selection_cause(formatter, "invalid declared P1 scenario set", error)
            }
            Self::InvalidObservationScenarioSet(error) => {
                write_selection_cause(formatter, "invalid observed P1 scenario set", error)
            }
            Self::InvalidCandidateScenarioSet(error) => {
                write_selection_cause(formatter, "invalid selected P2 scenario set", error)
            }
            Self::MissingDeclaration(scenario_id) => {
                write!(
                    formatter,
                    "P1 observation {scenario_id:?} has no declaration"
                )
            }
            Self::ObservationDrift(scenario_id) => write!(
                formatter,
                "P1 observation {scenario_id:?} differs from its declaration",
            ),
            Self::UnobservedRawMetric(metric) => {
                write!(
                    formatter,
                    "P1 did not observe required raw metric {metric:?}"
                )
            }
            Self::UnobservedNormalizedDenominator(denominator) => write!(
                formatter,
                "P1 did not observe required normalized denominator {denominator:?}",
            ),
            Self::UnknownRequiredScenario(scenario_id) => write!(
                formatter,
                "P2 requirement names unknown scenario {scenario_id:?}",
            ),
            Self::EmptyRequirementLabel(scenario_id) => write!(
                formatter,
                "P2 requirement for scenario {scenario_id:?} has an empty label",
            ),
            Self::P1ScenarioSetHashDrift { expected, actual } => write!(
                formatter,
                "P2 selection P1 identity drifted: expected {expected}, observed {actual}",
            ),
            Self::P2ScenarioSetHashDrift { declared, observed } => write!(
                formatter,
                "P2 selection identity drifted: declared {declared}, observed {observed}",
            ),
            Self::ProfileVersionDrift { expected, actual } => write!(
                formatter,
                "P2 selection profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ThresholdCrossingCountDrift { declared, actual } => write!(
                formatter,
                "P2 threshold-crossing count drifted: declared {declared}, retained {actual}",
            ),
            Self::UncleanSubject { subject, source } => {
                write!(
                    formatter,
                    "unclean P2 selection {subject} subject: {source}"
                )
            }
            Self::CandidateCapExceeded { cap, actual } => write!(
                formatter,
                "P2 candidate union exceeds its hard cap: maximum {cap}, observed {actual}",
            ),
        }
    }
}

impl Error for P2SelectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidProfile(error)
            | Self::InvalidDeclaredScenarioSet(error)
            | Self::InvalidObservationScenarioSet(error)
            | Self::InvalidCandidateScenarioSet(error) => Some(error),
            Self::InvalidEnvironment(error) | Self::InvalidBaselineEnvironment(error) => {
                Some(error)
            }
            Self::IncomparableBaselineEnvironment(error) => Some(error),
            Self::UncleanSubject { source, .. } => Some(source),
            Self::MissingDeclaration(_)
            | Self::CalibrationThresholdCrossings(_)
            | Self::CandidateCountDrift { .. }
            | Self::CandidateOrderingDrift
            | Self::CandidateReasonDrift(_)
            | Self::CandidateShardDrift { .. }
            | Self::ObservationDrift(_)
            | Self::InvalidCalibrationCohort(_)
            | Self::UnobservedRawMetric(_)
            | Self::UnobservedNormalizedDenominator(_)
            | Self::UnknownRequiredScenario(_)
            | Self::EmptyRequirementLabel(_)
            | Self::P1ScenarioSetHashDrift { .. }
            | Self::P2ScenarioSetHashDrift { .. }
            | Self::ProfileVersionDrift { .. }
            | Self::ThresholdCrossingCountDrift { .. }
            | Self::CandidateCapExceeded { .. } => None,
        }
    }
}

///
/// P2SelectionArtifactError
///
/// Typed failure while reading or writing one strict P2 selection artifact.
/// Owned by candidate artifact I/O and preserves validation, JSON, and filesystem causes.
///

#[derive(Debug)]
pub(crate) enum P2SelectionArtifactError {
    /// The artifact is not the one current strict JSON shape.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON decoding cause.
        source: serde_json::Error,
    },

    /// The in-memory selection could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// The in-memory or decoded selection is not current complete evidence.
    InvalidSelection(P2SelectionError),

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
}

impl Display for P2SelectionArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { path, source } => write!(
                formatter,
                "P2 selection artifact {} could not be decoded: {source}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "P2 selection artifact {} could not be encoded: {source}",
                path.display(),
            ),
            Self::InvalidSelection(error) => write!(formatter, "invalid P2 selection: {error}"),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "P2 selection artifact {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "P2 selection artifact {} exceeds its byte budget: observed at least {observed_bytes}, maximum {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for P2SelectionArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::InvalidSelection(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{MatrixOutcome, deterministic_matrix};

    use super::*;

    fn complete_test_samples(scenarios: &[MatrixScenario]) -> Vec<MatrixSample> {
        scenarios
            .iter()
            .enumerate()
            .map(|(index, scenario)| test_sample(index, scenario))
            .collect()
    }

    fn test_sample(index: usize, scenario: &MatrixScenario) -> MatrixSample {
        let base = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let mut sample = MatrixSample {
            key: scenario.key.clone(),
            surface: scenario.surface.label().to_string(),
            family: scenario.family.clone(),
            sql: scenario.sql.clone(),
            fixture_row_count: scenario.surface.fixture_row_count(),
            route_family: scenario.metadata.route.family().code().to_string(),
            outcome: MatrixOutcome {
                result_kind: shape_code(scenario.metadata.shape).to_string(),
                entity: scenario.surface.table().to_string(),
                row_count: index % 50 + 1,
            },
            scalar_aggregate_rows_ingested: base,
            data_store_get_calls: base,
            index_store_range_scan_calls: base,
            index_store_entry_reads: base,
            output_blob_bytes: base,
            ..MatrixSample::default()
        };
        set_all_raw_metrics(&mut sample, base.saturating_mul(100));
        sample
    }

    fn set_all_raw_metrics(sample: &mut MatrixSample, value: u64) {
        sample.total_local_instructions = value;
        sample.compile_local_instructions = value;
        sample.compile_cache_key_local_instructions = value;
        sample.compile_cache_lookup_local_instructions = value;
        sample.compile_parse_local_instructions = value;
        sample.compile_parse_tokenize_local_instructions = value;
        sample.compile_parse_select_local_instructions = value;
        sample.compile_parse_expr_local_instructions = value;
        sample.compile_parse_predicate_local_instructions = value;
        sample.compile_aggregate_lane_check_local_instructions = value;
        sample.compile_prepare_local_instructions = value;
        sample.compile_lower_local_instructions = value;
        sample.compile_bind_local_instructions = value;
        sample.compile_cache_insert_local_instructions = value;
        sample.execute_local_instructions = value;
        sample.planner_local_instructions = value;
        sample.planner_schema_info_local_instructions = value;
        sample.planner_prepare_local_instructions = value;
        sample.planner_cache_key_local_instructions = value;
        sample.planner_cache_lookup_local_instructions = value;
        sample.planner_plan_build_local_instructions = value;
        sample.planner_cache_insert_local_instructions = value;
        sample.store_local_instructions = value;
        sample.executor_invocation_local_instructions = value;
        sample.executor_local_instructions = value;
        sample.response_finalization_local_instructions = value;
        sample.grouped_stream_local_instructions = value;
        sample.grouped_fold_local_instructions = value;
        sample.grouped_finalize_local_instructions = value;
        sample.scalar_aggregate_base_row_local_instructions = value;
        sample.scalar_aggregate_reducer_fold_local_instructions = value;
        sample.pure_covering_decode_local_instructions = value;
        sample.pure_covering_row_assembly_local_instructions = value;
        sample.direct_data_row_scan_local_instructions = value;
        sample.direct_data_row_key_stream_local_instructions = value;
        sample.direct_data_row_row_read_local_instructions = value;
        sample.direct_data_row_key_encode_local_instructions = value;
        sample.direct_data_row_store_get_local_instructions = value;
        sample.direct_data_row_order_window_local_instructions = value;
        sample.direct_data_row_page_window_local_instructions = value;
        sample.kernel_row_scan_local_instructions = value;
        sample.kernel_row_key_stream_local_instructions = value;
        sample.kernel_row_row_read_local_instructions = value;
        sample.kernel_row_order_window_local_instructions = value;
        sample.kernel_row_page_window_local_instructions = value;
    }

    fn requirements_with_every_explicit_reason(
        scenarios: &[MatrixScenario],
    ) -> P2SelectionRequirements {
        P2SelectionRequirements::new(
            P2BaselineBasis::comparable(crate::sql_perf_environment::tests::identity(), 1),
            vec![P2ThresholdCrossing {
                scenario_id: scenarios[0].key.clone(),
                metric: P2RawMetric::Total,
            }],
            vec![P2ScaleRepresentative {
                scenario_id: scenarios[1].key.clone(),
                stratum: "primary_order.rows16.window1".to_string(),
            }],
            vec![scenarios[2].key.clone()],
            vec![scenarios[3].key.clone()],
        )
    }

    #[test]
    fn selection_is_stable_deduplicated_and_covers_every_reason_family() {
        let mut scenarios = deterministic_matrix();
        let mut samples = complete_test_samples(&scenarios);
        let requirements = requirements_with_every_explicit_reason(&scenarios);
        let selection = select_p2_candidates(
            SQL_PERFORMANCE_PROFILE,
            &crate::sql_perf_environment::tests::identity(),
            &scenarios,
            &samples,
            &requirements,
        )
        .expect("complete P1 observations should select P2 candidates");
        scenarios.reverse();
        samples.reverse();
        let reversed = select_p2_candidates(
            SQL_PERFORMANCE_PROFILE,
            &crate::sql_perf_environment::tests::identity(),
            &scenarios,
            &samples,
            &requirements,
        )
        .expect("selection should not depend on input order");

        assert_eq!(selection, reversed);
        assert_eq!(selection.candidate_count, selection.candidates.len());
        assert_eq!(selection.candidate_count, 74);
        assert_eq!(
            selection.p2_scenario_set_hash,
            "135956065b83dc6cbc281eff5a2544e14cf9b950044e3bb07d3b0139f82c6235"
        );
        assert!(
            selection
                .candidates
                .windows(2)
                .all(|pair| pair[0].scenario_id < pair[1].scenario_id)
        );
        assert!(selection.candidates.iter().all(|candidate| {
            candidate
                .reasons
                .windows(2)
                .all(|reasons| reasons[0] < reasons[1])
        }));
        let reasons = selection
            .candidates
            .iter()
            .flat_map(|candidate| candidate.reasons.iter())
            .collect::<Vec<_>>();
        assert!(
            reasons
                .iter()
                .any(|reason| matches!(reason, P2CandidateReason::RawMetric { .. }))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| matches!(reason, P2CandidateReason::NormalizedMetric { .. }))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| matches!(reason, P2CandidateReason::StratumWorst { .. }))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| matches!(reason, P2CandidateReason::BaselineThreshold { .. }))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| matches!(reason, P2CandidateReason::ScaleStratum { .. }))
        );
        assert!(reasons.contains(&&P2CandidateReason::RegressionSentinel));
        assert!(reasons.contains(&&P2CandidateReason::FocusedHotspot));
    }

    #[test]
    fn selection_artifact_is_strict_bounded_and_revalidates_derived_identity() {
        let scenarios = deterministic_matrix();
        let samples = complete_test_samples(&scenarios);
        let requirements = requirements_with_every_explicit_reason(&scenarios);
        let selection = select_p2_candidates(
            SQL_PERFORMANCE_PROFILE,
            &crate::sql_perf_environment::tests::identity(),
            &scenarios,
            &samples,
            &requirements,
        )
        .expect("complete P1 observations should select P2 candidates");
        let declared_ids = scenarios
            .iter()
            .map(|scenario| scenario.key.as_str())
            .collect::<Vec<_>>();
        validate_p2_candidate_selection(SQL_PERFORMANCE_PROFILE, &declared_ids, &selection)
            .expect("current selection should revalidate");

        let mut unknown_field =
            serde_json::to_value(&selection).expect("current P2 selection should serialize");
        unknown_field
            .as_object_mut()
            .expect("P2 selection should be a JSON object")
            .insert("legacy_candidates".to_string(), serde_json::json!([]));
        assert!(
            serde_json::from_value::<P2CandidateSelection>(unknown_field).is_err(),
            "unknown P2 selection fields must fail current-format decoding",
        );

        let max_bytes = SQL_PERFORMANCE_PROFILE.max_artifact_bytes();
        assert!(
            validate_p2_selection_artifact_size(Path::new("p2.json"), max_bytes, max_bytes).is_ok()
        );
        assert!(matches!(
            validate_p2_selection_artifact_size(Path::new("p2.json"), max_bytes + 1, max_bytes),
            Err(P2SelectionArtifactError::TooLarge { .. })
        ));

        let mut count_drift = selection.clone();
        count_drift.candidate_count += 1;
        assert!(matches!(
            validate_p2_candidate_selection(SQL_PERFORMANCE_PROFILE, &declared_ids, &count_drift,),
            Err(P2SelectionError::CandidateCountDrift { .. })
        ));
        let mut shard_drift = selection.clone();
        shard_drift.candidates[0].shard_index =
            (shard_drift.candidates[0].shard_index + 1) % SQL_PERFORMANCE_PROFILE.shard_count();
        assert!(matches!(
            validate_p2_candidate_selection(SQL_PERFORMANCE_PROFILE, &declared_ids, &shard_drift,),
            Err(P2SelectionError::CandidateShardDrift { .. })
        ));
        let mut hash_drift = selection.clone();
        hash_drift.p2_scenario_set_hash = "0".repeat(64);
        assert!(matches!(
            validate_p2_candidate_selection(SQL_PERFORMANCE_PROFILE, &declared_ids, &hash_drift,),
            Err(P2SelectionError::P2ScenarioSetHashDrift { .. })
        ));
        let path =
            std::env::temp_dir().join(format!("icydb-p2-selection-{}.json", std::process::id()));
        write_p2_candidate_selection(&path, SQL_PERFORMANCE_PROFILE, &declared_ids, &selection)
            .expect("current P2 selection should write");
        let decoded = read_p2_candidate_selection(&path, SQL_PERFORMANCE_PROFILE, &declared_ids)
            .expect("written P2 selection should read");
        fs::remove_file(&path).expect("temporary P2 selection should be removed");
        assert_eq!(selection, decoded);
    }

    #[test]
    fn selection_rejects_tampered_baseline_and_calibration_basis() {
        let scenarios = deterministic_matrix();
        let samples = complete_test_samples(&scenarios);
        let selection = select_p2_candidates(
            SQL_PERFORMANCE_PROFILE,
            &crate::sql_perf_environment::tests::identity(),
            &scenarios,
            &samples,
            &requirements_with_every_explicit_reason(&scenarios),
        )
        .expect("complete P1 observations should select P2 candidates");
        let declared_ids = scenarios
            .iter()
            .map(|scenario| scenario.key.as_str())
            .collect::<Vec<_>>();

        let mut threshold_count_drift = selection.clone();
        threshold_count_drift.baseline_basis =
            P2BaselineBasis::comparable(crate::sql_perf_environment::tests::identity(), 2);
        assert!(matches!(
            validate_p2_candidate_selection(
                SQL_PERFORMANCE_PROFILE,
                &declared_ids,
                &threshold_count_drift,
            ),
            Err(P2SelectionError::ThresholdCrossingCountDrift {
                declared: 2,
                actual: 1,
            })
        ));

        let mut calibration_with_threshold = selection.clone();
        calibration_with_threshold.baseline_basis = P2BaselineBasis::initial_calibration(
            "test-calibration".to_string(),
            P2CalibrationRun::One,
        );
        assert!(matches!(
            validate_p2_candidate_selection(
                SQL_PERFORMANCE_PROFILE,
                &declared_ids,
                &calibration_with_threshold,
            ),
            Err(P2SelectionError::CalibrationThresholdCrossings(1))
        ));

        let mut invalid_cohort = selection;
        invalid_cohort.baseline_basis = P2BaselineBasis::initial_calibration(
            "Not Canonical".to_string(),
            P2CalibrationRun::One,
        );
        for candidate in &mut invalid_cohort.candidates {
            candidate
                .reasons
                .retain(|reason| !matches!(reason, P2CandidateReason::BaselineThreshold { .. }));
        }
        assert!(matches!(
            validate_p2_candidate_selection(
                SQL_PERFORMANCE_PROFILE,
                &declared_ids,
                &invalid_cohort,
            ),
            Err(P2SelectionError::InvalidCalibrationCohort(cohort))
                if cohort == "Not Canonical"
        ));
    }

    #[test]
    fn checked_in_focused_hotspots_are_all_required_by_profile_selection() {
        let scenarios = deterministic_matrix();
        let samples = complete_test_samples(&scenarios);
        let requirements = P2SelectionRequirements::from_profile(
            SQL_PERFORMANCE_PROFILE,
            P2BaselineBasis::initial_calibration(
                "test-calibration".to_string(),
                P2CalibrationRun::One,
            ),
            Vec::new(),
            Vec::new(),
        );
        let selection = select_p2_candidates(
            SQL_PERFORMANCE_PROFILE,
            &crate::sql_perf_environment::tests::identity(),
            &scenarios,
            &samples,
            &requirements,
        )
        .expect("checked-in focused hotspots should belong to the P1 profile");

        for scenario_id in SQL_PERFORMANCE_PROFILE.focused_hotspot_scenario_ids() {
            let candidate = selection
                .candidates
                .iter()
                .find(|candidate| candidate.scenario_id == *scenario_id)
                .unwrap_or_else(|| panic!("focused hotspot {scenario_id:?} should be selected"));
            assert!(
                candidate
                    .reasons
                    .contains(&P2CandidateReason::FocusedHotspot)
            );
        }
    }

    #[test]
    fn selection_fails_instead_of_truncating_above_the_candidate_cap() {
        let scenarios = deterministic_matrix();
        let samples = complete_test_samples(&scenarios);
        let regression_sentinels = scenarios
            .iter()
            .take(SQL_PERFORMANCE_PROFILE.confirmation_scenario_cap() + 1)
            .map(|scenario| scenario.key.clone())
            .collect();
        let requirements = P2SelectionRequirements::new(
            P2BaselineBasis::initial_calibration(
                "test-calibration".to_string(),
                P2CalibrationRun::One,
            ),
            Vec::new(),
            Vec::new(),
            regression_sentinels,
            Vec::new(),
        );

        assert!(matches!(
            select_p2_candidates(
                SQL_PERFORMANCE_PROFILE,
                &crate::sql_perf_environment::tests::identity(),
                &scenarios,
                &samples,
                &requirements,
            ),
            Err(P2SelectionError::CandidateCapExceeded { cap: 512, actual })
                if actual > 512
        ));
    }

    #[test]
    fn selection_rejects_missing_metric_evidence_and_unknown_requirements() {
        let scenarios = deterministic_matrix();
        let mut samples = complete_test_samples(&scenarios);
        for sample in &mut samples {
            sample.response_finalization_local_instructions = 0;
        }
        let no_requirements = P2SelectionRequirements::new(
            P2BaselineBasis::initial_calibration(
                "test-calibration".to_string(),
                P2CalibrationRun::One,
            ),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        assert!(matches!(
            select_p2_candidates(
                SQL_PERFORMANCE_PROFILE,
                &crate::sql_perf_environment::tests::identity(),
                &scenarios,
                &samples,
                &no_requirements,
            ),
            Err(P2SelectionError::UnobservedRawMetric(
                P2RawMetric::ResponseFinalization
            ))
        ));

        let samples = complete_test_samples(&scenarios);
        let unknown = P2SelectionRequirements::new(
            P2BaselineBasis::initial_calibration(
                "test-calibration".to_string(),
                P2CalibrationRun::One,
            ),
            Vec::new(),
            Vec::new(),
            vec!["missing.scenario".to_string()],
            Vec::new(),
        );
        assert!(matches!(
            select_p2_candidates(
                SQL_PERFORMANCE_PROFILE,
                &crate::sql_perf_environment::tests::identity(),
                &scenarios,
                &samples,
                &unknown,
            ),
            Err(P2SelectionError::UnknownRequiredScenario(scenario_id))
                if scenario_id == "missing.scenario"
        ));
    }
}
