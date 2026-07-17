//! Module: sql_perf_p2_confirmation
//! Responsibility: exact P2 cold/warm sample-set validation and stability summaries.
//! Does not own: candidate selection, PocketIC isolation, baseline comparison, or artifact I/O.
//! Boundary: accepts only five cache-proven samples per required mode and preserves raw evidence.

use crate::{
    MatrixSample, expected_phase_reconciliations,
    sql_perf_p2::P2Candidate,
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError},
};

use std::{
    error::Error,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

///
/// P2SampleMode
///
/// Cache state required for one P2 confirmation sample set.
/// Owned by P2 confirmation and proven from typed SQL cache counters.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum P2SampleMode {
    /// A query against an isolated fixture with no prior execution of its SQL.
    Cold,

    /// A query after the same SQL was committed through the update warming boundary.
    Warm,
}

///
/// P2WarmNotApplicableReason
///
/// Typed reason a selected statement has no maintained warm-cache mode.
/// Owned by the scenario declaration and retained in confirmation evidence.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum P2WarmNotApplicableReason {
    /// Only ordinary `SELECT` statements participate in the SQL read caches.
    NonSelectStatement,
}

///
/// P2WarmSampleInput
///
/// Declared warm-mode eligibility and any captured raw observations.
/// Owned by the P2 execution boundary and consumed by confirmation construction.
///

pub(crate) enum P2WarmSampleInput {
    /// The maintained path requires five independently warmed observations.
    Required(Vec<MatrixSample>),

    /// The statement has no maintained warm-cache mode.
    NotApplicable(P2WarmNotApplicableReason),
}

///
/// P2WarmEvidence
///
/// Confirmed warm samples or a typed declaration that warm mode is ineligible.
/// Owned by P2 confirmation and validated against scenario intent by each shard.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "evidence")]
pub(crate) enum P2WarmEvidence {
    /// Five independently warmed cache-hit observations.
    Confirmed(P2SampleSet),

    /// The declared statement has no maintained warm-cache mode.
    NotApplicable(P2WarmNotApplicableReason),
}

///
/// P2CacheProof
///
/// Aggregate typed cache counters proving the declared state of one sample set.
/// Owned by P2 confirmation and derived from the retained raw samples.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2CacheProof {
    /// Required cache state.
    pub(crate) mode: P2SampleMode,

    /// Total compiled-command cache hits across the sample set.
    pub(crate) sql_compiled_command_hits: u64,

    /// Total compiled-command cache misses across the sample set.
    pub(crate) sql_compiled_command_misses: u64,

    /// Total shared-plan cache hits across the sample set.
    pub(crate) shared_query_plan_hits: u64,

    /// Total shared-plan cache misses across the sample set.
    pub(crate) shared_query_plan_misses: u64,
}

///
/// P2SampleSet
///
/// Five raw P2 observations for one scenario and cache mode plus exact summary facts.
/// Owned by P2 confirmation; later verdict and artifact layers consume it unchanged.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2SampleSet {
    /// Stable scenario identity shared with the selected candidate.
    pub(crate) scenario_id: String,

    /// Cache state proved by every retained sample.
    pub(crate) cache_proof: P2CacheProof,

    /// Minimum total instruction observation.
    pub(crate) min_total_local_instructions: u64,

    /// Median total instruction observation.
    pub(crate) median_total_local_instructions: u64,

    /// Maximum total instruction observation.
    pub(crate) max_total_local_instructions: u64,

    /// Whether the observed range stays within the fixed profile threshold.
    pub(crate) stable: bool,

    /// Complete raw samples in execution order.
    pub(crate) samples: Vec<MatrixSample>,
}

///
/// P2ScenarioConfirmation
///
/// Complete cold and warm evidence for one deterministically selected candidate.
/// Owned by P2 confirmation and consumed by shard receipts and performance verdicts.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2ScenarioConfirmation {
    /// Candidate identity and every reason it required confirmation.
    pub(crate) candidate: P2Candidate,

    /// Five isolated cache-miss observations.
    pub(crate) cold: P2SampleSet,

    /// Proven warm observations or a typed ineligibility reason.
    pub(crate) warm: P2WarmEvidence,
}

/// Build one exact P2 sample set from retained raw observations.
///
/// # Errors
///
/// Returns a typed error for invalid profile state, wrong sample count, identity
/// or semantic drift, stale phase reconciliation, or cache counters that do not
/// prove the requested mode.
pub(crate) fn build_p2_sample_set(
    profile: PerformanceProfile,
    candidate: &P2Candidate,
    mode: P2SampleMode,
    samples: Vec<MatrixSample>,
) -> Result<P2SampleSet, P2ConfirmationError> {
    profile
        .validate()
        .map_err(P2ConfirmationError::InvalidProfile)?;
    let expected_count = match mode {
        P2SampleMode::Cold => profile.cold_samples_per_confirmation(),
        P2SampleMode::Warm => profile.warm_samples_per_confirmation(),
    };
    if samples.len() != usize::from(expected_count) {
        return Err(P2ConfirmationError::SampleCount {
            scenario_id: candidate.scenario_id.clone(),
            mode,
            expected: expected_count,
            actual: samples.len(),
        });
    }

    validate_sample_identity_and_semantics(candidate, &samples)?;
    validate_phase_reconciliation(&samples)?;
    let cache_proof = cache_proof(candidate, mode, &samples)?;
    let mut totals = samples
        .iter()
        .map(|sample| sample.total_local_instructions)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    let min_total_local_instructions = totals.first().copied().unwrap_or_default();
    let median_total_local_instructions = totals[totals.len() / 2];
    let max_total_local_instructions = totals.last().copied().unwrap_or_default();
    let allowed_range = stability_range(profile, median_total_local_instructions);
    let stable =
        max_total_local_instructions.saturating_sub(min_total_local_instructions) <= allowed_range;

    Ok(P2SampleSet {
        scenario_id: candidate.scenario_id.clone(),
        cache_proof,
        min_total_local_instructions,
        median_total_local_instructions,
        max_total_local_instructions,
        stable,
        samples,
    })
}

/// Build the complete cold/warm confirmation for one selected candidate.
///
/// # Errors
///
/// Returns the typed sample-set error, or semantic drift between the cold and
/// warm modes. Instability is retained honestly and rejected by the verdict
/// boundary rather than discarded during artifact construction.
pub(crate) fn build_p2_confirmation(
    profile: PerformanceProfile,
    candidate: P2Candidate,
    cold_samples: Vec<MatrixSample>,
    warm_input: P2WarmSampleInput,
) -> Result<P2ScenarioConfirmation, P2ConfirmationError> {
    let cold = build_p2_sample_set(profile, &candidate, P2SampleMode::Cold, cold_samples)?;
    let warm = match warm_input {
        P2WarmSampleInput::Required(warm_samples) => {
            let warm = build_p2_sample_set(profile, &candidate, P2SampleMode::Warm, warm_samples)?;
            if !same_semantic_result(&cold.samples[0], &warm.samples[0]) {
                return Err(P2ConfirmationError::ModeSemanticDrift(
                    candidate.scenario_id,
                ));
            }
            P2WarmEvidence::Confirmed(warm)
        }
        P2WarmSampleInput::NotApplicable(reason) => P2WarmEvidence::NotApplicable(reason),
    };

    Ok(P2ScenarioConfirmation {
        candidate,
        cold,
        warm,
    })
}

/// Recompute and validate every derived fact in one serialized confirmation.
///
/// # Errors
///
/// Returns the typed sample error or a summary-drift error when retained raw
/// samples no longer derive the serialized cache, median, range, or stability facts.
pub(crate) fn validate_p2_confirmation(
    profile: PerformanceProfile,
    confirmation: &P2ScenarioConfirmation,
) -> Result<(), P2ConfirmationError> {
    let warm_input = match &confirmation.warm {
        P2WarmEvidence::Confirmed(warm) => P2WarmSampleInput::Required(warm.samples.clone()),
        P2WarmEvidence::NotApplicable(reason) => P2WarmSampleInput::NotApplicable(*reason),
    };
    let expected = build_p2_confirmation(
        profile,
        confirmation.candidate.clone(),
        confirmation.cold.samples.clone(),
        warm_input,
    )?;
    if &expected != confirmation {
        return Err(P2ConfirmationError::SerializedSummaryDrift(
            confirmation.candidate.scenario_id.clone(),
        ));
    }

    Ok(())
}

/// Require one confirmation to satisfy the fixed stability gate.
///
/// # Errors
///
/// Returns a typed instability error for either required cache mode.
pub(crate) fn require_stable_p2_confirmation(
    confirmation: &P2ScenarioConfirmation,
) -> Result<(), P2ConfirmationError> {
    let mut sample_sets = vec![&confirmation.cold];
    if let P2WarmEvidence::Confirmed(warm) = &confirmation.warm {
        sample_sets.push(warm);
    }
    for sample_set in sample_sets {
        if !sample_set.stable {
            return Err(P2ConfirmationError::UnstableSampleSet {
                scenario_id: confirmation.candidate.scenario_id.clone(),
                mode: sample_set.cache_proof.mode,
                min: sample_set.min_total_local_instructions,
                median: sample_set.median_total_local_instructions,
                max: sample_set.max_total_local_instructions,
            });
        }
    }

    Ok(())
}

fn validate_sample_identity_and_semantics(
    candidate: &P2Candidate,
    samples: &[MatrixSample],
) -> Result<(), P2ConfirmationError> {
    let reference = &samples[0];
    for sample in samples {
        if sample.key != candidate.scenario_id {
            return Err(P2ConfirmationError::SampleIdentityDrift {
                expected: candidate.scenario_id.clone(),
                actual: sample.key.clone(),
            });
        }
        if !same_semantic_result(reference, sample) {
            return Err(P2ConfirmationError::SampleSemanticDrift(
                candidate.scenario_id.clone(),
            ));
        }
    }

    Ok(())
}

fn validate_phase_reconciliation(samples: &[MatrixSample]) -> Result<(), P2ConfirmationError> {
    for sample in samples {
        let observed = [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ];
        if observed != expected_phase_reconciliations(sample) {
            return Err(P2ConfirmationError::PhaseReconciliationDrift(
                sample.key.clone(),
            ));
        }
    }

    Ok(())
}

fn cache_proof(
    candidate: &P2Candidate,
    mode: P2SampleMode,
    samples: &[MatrixSample],
) -> Result<P2CacheProof, P2ConfirmationError> {
    let expected = match mode {
        P2SampleMode::Cold => (0, 1),
        P2SampleMode::Warm => (1, 0),
    };
    if samples.iter().any(|sample| {
        (
            sample.sql_compiled_command_hits,
            sample.sql_compiled_command_misses,
        ) != expected
    }) {
        return Err(P2ConfirmationError::CacheProof {
            scenario_id: candidate.scenario_id.clone(),
            mode,
        });
    }

    Ok(P2CacheProof {
        mode,
        sql_compiled_command_hits: samples
            .iter()
            .map(|sample| sample.sql_compiled_command_hits)
            .sum(),
        sql_compiled_command_misses: samples
            .iter()
            .map(|sample| sample.sql_compiled_command_misses)
            .sum(),
        shared_query_plan_hits: samples
            .iter()
            .map(|sample| sample.shared_query_plan_hits)
            .sum(),
        shared_query_plan_misses: samples
            .iter()
            .map(|sample| sample.shared_query_plan_misses)
            .sum(),
    })
}

/// Return whether two samples have the same declaration, route, window, and result identity.
pub(crate) fn same_semantic_result(left: &MatrixSample, right: &MatrixSample) -> bool {
    left.key == right.key
        && left.surface == right.surface
        && left.family == right.family
        && left.sql == right.sql
        && left.fixture_row_count == right.fixture_row_count
        && left.route_family == right.route_family
        && left.route_outcome == right.route_outcome
        && left.route_reason == right.route_reason
        && left.order_by_idx_hint == right.order_by_idx_hint
        && left.limit_stop_after == right.limit_stop_after
        && left.result_signature == right.result_signature
        && left.cursor_signature == right.cursor_signature
        && left.outcome == right.outcome
}

fn stability_range(profile: PerformanceProfile, median: u64) -> u64 {
    let threshold = profile.stability_threshold();
    let relative = median.saturating_mul(u64::from(threshold.relative_basis_points())) / 10_000;

    threshold.absolute_instructions().max(relative)
}

///
/// P2ConfirmationError
///
/// Typed failure while proving one P2 candidate's cold and warm evidence.
/// Owned by P2 confirmation and preserved by later shard and verdict errors.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum P2ConfirmationError {
    /// One sample has cache counters incompatible with its required mode.
    CacheProof {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Cache mode whose proof failed.
        mode: P2SampleMode,
    },

    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),

    /// Cold and warm modes returned different semantic results or routes.
    ModeSemanticDrift(String),

    /// One sample carries stale phase-reconciliation values.
    PhaseReconciliationDrift(String),

    /// A mode does not contain the exact checked-in sample count.
    SampleCount {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Cache mode being validated.
        mode: P2SampleMode,
        /// Checked-in required sample count.
        expected: u8,
        /// Observed sample count.
        actual: usize,
    },

    /// One sample names a scenario other than its selected candidate.
    SampleIdentityDrift {
        /// Selected candidate identity.
        expected: String,
        /// Observed sample identity.
        actual: String,
    },

    /// Repeated samples changed their result, route, window, or declaration facts.
    SampleSemanticDrift(String),

    /// Serialized summary fields differ from the retained raw samples.
    SerializedSummaryDrift(String),

    /// A required mode exceeds the fixed stability range.
    UnstableSampleSet {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Unstable cache mode.
        mode: P2SampleMode,
        /// Minimum total instruction observation.
        min: u64,
        /// Median total instruction observation.
        median: u64,
        /// Maximum total instruction observation.
        max: u64,
    },
}

impl Display for P2ConfirmationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CacheProof { scenario_id, mode } => write!(
                formatter,
                "P2 scenario {scenario_id:?} has cache counters incompatible with {mode:?} mode",
            ),
            Self::InvalidProfile(error) => {
                write!(formatter, "invalid performance profile: {error}")
            }
            Self::ModeSemanticDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} changed semantics between cold and warm modes",
            ),
            Self::PhaseReconciliationDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} has stale phase reconciliation",
            ),
            Self::SampleCount {
                scenario_id,
                mode,
                expected,
                actual,
            } => write!(
                formatter,
                "P2 scenario {scenario_id:?} {mode:?} sample count drifted: expected {expected}, observed {actual}",
            ),
            Self::SampleIdentityDrift { expected, actual } => write!(
                formatter,
                "P2 sample identity drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::SampleSemanticDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} changed semantics across repeated samples",
            ),
            Self::SerializedSummaryDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} serialized summary differs from its raw samples",
            ),
            Self::UnstableSampleSet {
                scenario_id,
                mode,
                min,
                median,
                max,
            } => write!(
                formatter,
                "P2 scenario {scenario_id:?} {mode:?} samples are unstable: min {min}, median {median}, max {max}",
            ),
        }
    }
}

impl Error for P2ConfirmationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidProfile(error) => Some(error),
            Self::CacheProof { .. }
            | Self::ModeSemanticDrift(_)
            | Self::PhaseReconciliationDrift(_)
            | Self::SampleCount { .. }
            | Self::SampleIdentityDrift { .. }
            | Self::SampleSemanticDrift(_)
            | Self::SerializedSummaryDrift(_)
            | Self::UnstableSampleSet { .. } => None,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        fill_matrix_phase_reconciliation, report_matrix_sample,
        sql_perf_p2::{P2CandidateReason, P2RawMetric},
        sql_perf_profile::SQL_PERFORMANCE_PROFILE,
    };

    use super::*;

    const SCENARIO_ID: &str = "user.select.pk.all.pk_asc.limit1";

    fn candidate() -> P2Candidate {
        P2Candidate {
            scenario_id: SCENARIO_ID.to_string(),
            shard_index: SQL_PERFORMANCE_PROFILE
                .scenario_shard(SCENARIO_ID)
                .expect("test candidate should shard"),
            reasons: vec![P2CandidateReason::RawMetric {
                metric: P2RawMetric::Total,
                rank: 1,
            }],
        }
    }

    fn sample(mode: P2SampleMode, total: u64) -> MatrixSample {
        let mut sample = report_matrix_sample(
            SCENARIO_ID,
            "user",
            total,
            100,
            "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        );
        match mode {
            P2SampleMode::Cold => {
                sample.sql_compiled_command_hits = 0;
                sample.sql_compiled_command_misses = 1;
                sample.shared_query_plan_hits = 0;
                sample.shared_query_plan_misses = 1;
            }
            P2SampleMode::Warm => {
                sample.sql_compiled_command_hits = 1;
                sample.sql_compiled_command_misses = 0;
                sample.shared_query_plan_hits = 1;
                sample.shared_query_plan_misses = 0;
            }
        }
        fill_matrix_phase_reconciliation(&mut sample);

        sample
    }

    fn samples(mode: P2SampleMode, totals: [u64; 5]) -> Vec<MatrixSample> {
        totals
            .into_iter()
            .map(|total| sample(mode, total))
            .collect()
    }

    #[test]
    fn confirmation_preserves_exact_samples_and_golden_summaries() {
        let confirmation = build_p2_confirmation(
            SQL_PERFORMANCE_PROFILE,
            candidate(),
            samples(
                P2SampleMode::Cold,
                [100_000, 102_000, 101_000, 99_000, 100_500],
            ),
            P2WarmSampleInput::Required(samples(
                P2SampleMode::Warm,
                [80_000, 81_000, 79_500, 80_500, 80_250],
            )),
        )
        .expect("five proven samples per mode should confirm");
        let P2WarmEvidence::Confirmed(warm) = &confirmation.warm else {
            panic!("SELECT confirmation should retain warm evidence")
        };

        assert_eq!(confirmation.cold.samples.len(), 5);
        assert_eq!(confirmation.cold.min_total_local_instructions, 99_000);
        assert_eq!(confirmation.cold.median_total_local_instructions, 100_500);
        assert_eq!(confirmation.cold.max_total_local_instructions, 102_000);
        assert_eq!(confirmation.cold.cache_proof.sql_compiled_command_misses, 5);
        assert_eq!(warm.cache_proof.sql_compiled_command_hits, 5);
        assert!(confirmation.cold.stable);
        assert!(warm.stable);
        require_stable_p2_confirmation(&confirmation)
            .expect("golden sample ranges should be stable");
    }

    #[test]
    fn confirmation_rejects_count_cache_identity_phase_and_semantic_drift() {
        let selected = candidate();
        let mut cold = samples(P2SampleMode::Cold, [100_000; 5]);
        assert!(matches!(
            build_p2_sample_set(
                SQL_PERFORMANCE_PROFILE,
                &selected,
                P2SampleMode::Cold,
                cold[..4].to_vec(),
            ),
            Err(P2ConfirmationError::SampleCount { actual: 4, .. })
        ));

        cold[0].sql_compiled_command_hits = 1;
        assert!(matches!(
            build_p2_sample_set(
                SQL_PERFORMANCE_PROFILE,
                &selected,
                P2SampleMode::Cold,
                cold.clone(),
            ),
            Err(P2ConfirmationError::CacheProof { .. })
        ));
        cold[0] = sample(P2SampleMode::Cold, 100_000);
        cold[0].key = "unknown".to_string();
        assert!(matches!(
            build_p2_sample_set(
                SQL_PERFORMANCE_PROFILE,
                &selected,
                P2SampleMode::Cold,
                cold.clone(),
            ),
            Err(P2ConfirmationError::SampleIdentityDrift { .. })
        ));
        cold[0] = sample(P2SampleMode::Cold, 100_000);
        cold[0]
            .total_phase_reconciliation
            .unaccounted_local_instructions = 1;
        assert!(matches!(
            build_p2_sample_set(
                SQL_PERFORMANCE_PROFILE,
                &selected,
                P2SampleMode::Cold,
                cold.clone(),
            ),
            Err(P2ConfirmationError::PhaseReconciliationDrift(_))
        ));
        cold[0] = sample(P2SampleMode::Cold, 100_000);
        cold[0].result_signature = Some("drifted".to_string());
        assert!(matches!(
            build_p2_sample_set(SQL_PERFORMANCE_PROFILE, &selected, P2SampleMode::Cold, cold,),
            Err(P2ConfirmationError::SampleSemanticDrift(_))
        ));
    }

    #[test]
    fn confirmation_retains_but_gate_rejects_spread_above_fixed_bound() {
        let confirmation = build_p2_confirmation(
            SQL_PERFORMANCE_PROFILE,
            candidate(),
            samples(
                P2SampleMode::Cold,
                [100_000, 100_000, 100_000, 100_000, 110_001],
            ),
            P2WarmSampleInput::Required(samples(P2SampleMode::Warm, [80_000; 5])),
        )
        .expect("instability should remain reportable evidence");

        assert!(!confirmation.cold.stable);
        assert!(matches!(
            require_stable_p2_confirmation(&confirmation),
            Err(P2ConfirmationError::UnstableSampleSet {
                mode: P2SampleMode::Cold,
                min: 100_000,
                median: 100_000,
                max: 110_001,
                ..
            })
        ));
    }

    #[test]
    fn confirmation_retains_typed_warm_ineligibility_without_invented_samples() {
        let confirmation = build_p2_confirmation(
            SQL_PERFORMANCE_PROFILE,
            candidate(),
            samples(P2SampleMode::Cold, [100_000; 5]),
            P2WarmSampleInput::NotApplicable(P2WarmNotApplicableReason::NonSelectStatement),
        )
        .expect("typed warm ineligibility should remain valid evidence");

        assert_eq!(
            confirmation.warm,
            P2WarmEvidence::NotApplicable(P2WarmNotApplicableReason::NonSelectStatement),
        );
        require_stable_p2_confirmation(&confirmation)
            .expect("warm ineligibility should leave only the cold stability gate");
        validate_p2_confirmation(SQL_PERFORMANCE_PROFILE, &confirmation)
            .expect("typed warm ineligibility should round-trip from serialized evidence");
    }
}
