//! Module: sql_perf_scale_baseline
//! Responsibility: exact comparable deltas for scale totals, normalized costs, and slopes.
//! Does not own: scale sampling, fixture authority, threshold policy, or final verdicts.
//! Boundary: validates both merged scale artifacts and refuses identity or semantic drift.

use crate::{
    MatrixScenario,
    sql_perf_environment::{
        PerfEnvironmentIdentity, PerfEnvironmentMismatch, require_comparable_environment,
    },
    sql_perf_p2_confirmation::same_semantic_result,
    sql_perf_profile::{PerformanceProfile, PerformanceThreshold},
    sql_perf_scale::NormalizedDenominator,
    sql_perf_scale_shard::{
        MergedScaleShardReports, ScaleShardError, validate_merged_scale_report,
    },
};

use std::{
    error::Error,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

/// One comparable total-instruction scale delta.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleTotalDelta {
    /// Stable exact-cardinality scenario identity.
    pub(crate) scenario_id: String,

    /// Exact fixture row count.
    pub(crate) fixture_rows: u32,

    /// Baseline total local instructions.
    pub(crate) baseline: u64,

    /// Current total local instructions.
    pub(crate) current: u64,

    /// Signed current-minus-baseline delta.
    pub(crate) delta: i128,

    /// Signed relative delta in basis points, absent for a zero baseline.
    pub(crate) delta_basis_points: Option<i128>,

    /// Reviewed absolute increase threshold.
    pub(crate) absolute_threshold: u64,

    /// Reviewed relative increase threshold in basis points.
    pub(crate) relative_threshold_basis_points: u16,

    /// Whether this exact-cardinality total reached the reviewed threshold.
    pub(crate) regression: bool,
}

/// Exact numerator and nonzero unit count for a normalized cost.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExactNormalizedCost {
    /// Total local instruction numerator.
    pub(crate) local_instructions: u64,

    /// Nonzero measured or declared units.
    pub(crate) units: u64,
}

/// One comparable exact normalized-cost delta.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleNormalizedDelta {
    /// Stable exact-cardinality scenario identity.
    pub(crate) scenario_id: String,

    /// Typed normalization unit.
    pub(crate) denominator: NormalizedDenominator,

    /// Exact baseline cost.
    pub(crate) baseline: ExactNormalizedCost,

    /// Exact current cost.
    pub(crate) current: ExactNormalizedCost,

    /// Signed rational relative delta in basis points, absent for a zero baseline numerator.
    pub(crate) delta_basis_points: Option<i128>,

    /// Reviewed rational relative increase threshold in basis points.
    pub(crate) relative_threshold_basis_points: u16,

    /// Whether this normalized cost reached the reviewed threshold.
    pub(crate) regression: bool,
}

/// One comparable adjacent-cardinality slope delta.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleSlopeDelta {
    /// Stable scale-sentinel family identity.
    pub(crate) sentinel_id: String,

    /// Lower fixture cardinality.
    pub(crate) from_fixture_rows: u32,

    /// Higher fixture cardinality.
    pub(crate) to_fixture_rows: u32,

    /// Positive fixture-row difference.
    pub(crate) row_delta: u32,

    /// Baseline signed instruction change.
    pub(crate) baseline_instruction_delta: i128,

    /// Current signed instruction change.
    pub(crate) current_instruction_delta: i128,

    /// Signed change in the instruction-delta numerator.
    pub(crate) instruction_delta_change: i128,

    /// Signed relative increase in slope magnitude, absent for a zero baseline slope.
    pub(crate) delta_basis_points: Option<i128>,

    /// Reviewed absolute instruction-delta increase threshold.
    pub(crate) absolute_threshold: u64,

    /// Reviewed relative increase threshold in basis points.
    pub(crate) relative_threshold_basis_points: u16,

    /// Whether this adjacent-cardinality slope reached the reviewed threshold.
    pub(crate) regression: bool,
}

///
/// ScaleBaselineComparison
///
/// Exact gated scale deltas for one comparable subject pair.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleBaselineComparison {
    /// Complete baseline environment and measured subject.
    pub(crate) baseline_environment: PerfEnvironmentIdentity,

    /// Complete current environment and measured subject.
    pub(crate) current_environment: PerfEnvironmentIdentity,

    /// Exact single-sample total deltas.
    pub(crate) totals: Vec<ScaleTotalDelta>,

    /// Exact normalized-cost deltas.
    pub(crate) normalized: Vec<ScaleNormalizedDelta>,

    /// Exact adjacent-cardinality slope deltas.
    pub(crate) slopes: Vec<ScaleSlopeDelta>,
}

impl ScaleBaselineComparison {
    /// Return every reviewed scale regression in stable report order.
    pub(crate) fn regression_causes(&self) -> Vec<ScaleRegressionCause> {
        self.totals
            .iter()
            .filter(|delta| delta.regression)
            .map(|delta| ScaleRegressionCause::Total {
                scenario_id: delta.scenario_id.clone(),
                fixture_rows: delta.fixture_rows,
                delta: delta.delta,
                delta_basis_points: delta.delta_basis_points.unwrap_or_default(),
            })
            .chain(
                self.normalized
                    .iter()
                    .filter(|delta| delta.regression)
                    .map(|delta| ScaleRegressionCause::Normalized {
                        scenario_id: delta.scenario_id.clone(),
                        denominator: delta.denominator,
                        delta_basis_points: delta.delta_basis_points.unwrap_or_default(),
                    }),
            )
            .chain(
                self.slopes
                    .iter()
                    .filter(|delta| delta.regression)
                    .map(|delta| ScaleRegressionCause::Slope {
                        sentinel_id: delta.sentinel_id.clone(),
                        from_fixture_rows: delta.from_fixture_rows,
                        to_fixture_rows: delta.to_fixture_rows,
                        instruction_delta_change: delta.instruction_delta_change,
                        delta_basis_points: delta.delta_basis_points.unwrap_or_default(),
                    }),
            )
            .collect()
    }
}

/// One reviewed scale regression that contributes to the final performance verdict.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "metric", rename_all = "snake_case")]
pub(crate) enum ScaleRegressionCause {
    /// Exact-cardinality total instructions crossed their dual threshold.
    Total {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Exact fixture row count.
        fixture_rows: u32,
        /// Signed current-minus-baseline instruction delta.
        delta: i128,
        /// Signed relative delta in basis points.
        delta_basis_points: i128,
    },

    /// Exact normalized cost crossed its relative threshold.
    Normalized {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Typed normalization unit.
        denominator: NormalizedDenominator,
        /// Signed rational relative delta in basis points.
        delta_basis_points: i128,
    },

    /// Adjacent-cardinality slope crossed its dual threshold.
    Slope {
        /// Stable scale-sentinel family identity.
        sentinel_id: String,
        /// Lower fixture cardinality.
        from_fixture_rows: u32,
        /// Higher fixture cardinality.
        to_fixture_rows: u32,
        /// Signed increase in the instruction-delta numerator.
        instruction_delta_change: i128,
        /// Signed relative increase in basis points.
        delta_basis_points: i128,
    },
}

/// Compare two independently validated merged scale artifacts.
///
/// # Errors
///
/// Returns a typed error before producing deltas when either artifact is invalid,
/// environments are incomparable, observation membership or semantic identity
/// differs, slope identity changes, or exact normalized arithmetic overflows.
pub(crate) fn compare_scale_baseline(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    baseline: &MergedScaleShardReports,
    current: &MergedScaleShardReports,
) -> Result<ScaleBaselineComparison, ScaleBaselineComparisonError> {
    validate_merged_scale_report(profile, required_wasm_profile, scenarios, baseline)
        .map_err(ScaleBaselineComparisonError::InvalidBaseline)?;
    validate_merged_scale_report(profile, required_wasm_profile, scenarios, current)
        .map_err(ScaleBaselineComparisonError::InvalidCurrent)?;
    require_comparable_environment(&baseline.environment, &current.environment)
        .map_err(ScaleBaselineComparisonError::IncomparableEnvironment)?;

    let totals = compare_scale_totals(profile, baseline, current)?;
    let normalized = compare_scale_normalized(baseline, current)?;
    let slopes = compare_scale_slopes(baseline, current)?;

    Ok(ScaleBaselineComparison {
        baseline_environment: baseline.environment.clone(),
        current_environment: current.environment.clone(),
        totals,
        normalized,
        slopes,
    })
}

fn compare_scale_totals(
    profile: PerformanceProfile,
    baseline: &MergedScaleShardReports,
    current: &MergedScaleShardReports,
) -> Result<Vec<ScaleTotalDelta>, ScaleBaselineComparisonError> {
    if baseline.observations.len() != current.observations.len() {
        return Err(ScaleBaselineComparisonError::ObservationSetDrift);
    }
    baseline
        .observations
        .iter()
        .zip(&current.observations)
        .map(|(baseline, current)| {
            if baseline.sentinel_id != current.sentinel_id
                || baseline.scenario_id != current.scenario_id
                || baseline.p1_scenario_id != current.p1_scenario_id
                || baseline.selectivity != current.selectivity
                || baseline.predicate_match_rows != current.predicate_match_rows
                || baseline.result_window != current.result_window
                || baseline.fixture != current.fixture
            {
                return Err(ScaleBaselineComparisonError::ObservationIdentityDrift(
                    baseline.scenario_id.clone(),
                ));
            }
            if !same_semantic_result(&baseline.sample, &current.sample) {
                return Err(ScaleBaselineComparisonError::SemanticDrift(
                    baseline.scenario_id.clone(),
                ));
            }
            let threshold = profile.total_instruction_regression_threshold();
            Ok(ScaleTotalDelta {
                scenario_id: baseline.scenario_id.clone(),
                fixture_rows: baseline.fixture.fixture_rows,
                baseline: baseline.sample.total_local_instructions,
                current: current.sample.total_local_instructions,
                delta: i128::from(current.sample.total_local_instructions)
                    - i128::from(baseline.sample.total_local_instructions),
                delta_basis_points: relative_delta_basis_points(
                    baseline.sample.total_local_instructions,
                    current.sample.total_local_instructions,
                ),
                absolute_threshold: threshold.absolute_increase(),
                relative_threshold_basis_points: threshold.relative_increase_basis_points(),
                regression: threshold.reached(
                    baseline.sample.total_local_instructions,
                    current.sample.total_local_instructions,
                ),
            })
        })
        .collect()
}

fn compare_scale_normalized(
    baseline: &MergedScaleShardReports,
    current: &MergedScaleShardReports,
) -> Result<Vec<ScaleNormalizedDelta>, ScaleBaselineComparisonError> {
    if baseline.normalized_costs.len() != current.normalized_costs.len() {
        return Err(ScaleBaselineComparisonError::NormalizedSetDrift);
    }
    baseline
        .normalized_costs
        .iter()
        .zip(&current.normalized_costs)
        .map(|(baseline, current)| {
            if baseline.scenario_id != current.scenario_id
                || baseline.denominator != current.denominator
            {
                return Err(ScaleBaselineComparisonError::NormalizedSetDrift);
            }
            let delta_basis_points = normalized_delta_basis_points(
                baseline.cost.local_instructions,
                baseline.cost.units.get(),
                current.cost.local_instructions,
                current.cost.units.get(),
            )?;
            let relative_threshold_basis_points =
                PerformanceProfile::scale_normalized_regression_basis_points();
            Ok(ScaleNormalizedDelta {
                scenario_id: baseline.scenario_id.clone(),
                denominator: baseline.denominator,
                baseline: ExactNormalizedCost {
                    local_instructions: baseline.cost.local_instructions,
                    units: baseline.cost.units.get(),
                },
                current: ExactNormalizedCost {
                    local_instructions: current.cost.local_instructions,
                    units: current.cost.units.get(),
                },
                delta_basis_points,
                relative_threshold_basis_points,
                regression: normalized_regression(
                    baseline.cost.local_instructions,
                    current.cost.local_instructions,
                    delta_basis_points,
                    relative_threshold_basis_points,
                ),
            })
        })
        .collect()
}

fn compare_scale_slopes(
    baseline: &MergedScaleShardReports,
    current: &MergedScaleShardReports,
) -> Result<Vec<ScaleSlopeDelta>, ScaleBaselineComparisonError> {
    if baseline.slopes.len() != current.slopes.len() {
        return Err(ScaleBaselineComparisonError::SlopeSetDrift);
    }
    baseline
        .slopes
        .iter()
        .zip(&current.slopes)
        .map(|(baseline, current)| {
            if baseline.sentinel_id != current.sentinel_id
                || baseline.from_fixture_rows != current.from_fixture_rows
                || baseline.to_fixture_rows != current.to_fixture_rows
                || baseline.row_delta != current.row_delta
                || baseline.from_route_family != current.from_route_family
                || baseline.to_route_family != current.to_route_family
                || baseline.route_changed != current.route_changed
            {
                return Err(ScaleBaselineComparisonError::SlopeSetDrift);
            }
            let threshold = PerformanceProfile::scale_slope_regression_threshold();
            let instruction_delta_change = current
                .instruction_delta
                .saturating_sub(baseline.instruction_delta);
            let delta_basis_points = signed_relative_delta_basis_points(
                baseline.instruction_delta,
                current.instruction_delta,
            );
            Ok(ScaleSlopeDelta {
                sentinel_id: baseline.sentinel_id.clone(),
                from_fixture_rows: baseline.from_fixture_rows,
                to_fixture_rows: baseline.to_fixture_rows,
                row_delta: baseline.row_delta,
                baseline_instruction_delta: baseline.instruction_delta,
                current_instruction_delta: current.instruction_delta,
                instruction_delta_change,
                delta_basis_points,
                absolute_threshold: threshold.absolute_increase(),
                relative_threshold_basis_points: threshold.relative_increase_basis_points(),
                regression: signed_regression_reached(
                    threshold,
                    baseline.instruction_delta,
                    instruction_delta_change,
                    delta_basis_points,
                ),
            })
        })
        .collect()
}

fn relative_delta_basis_points(baseline: u64, current: u64) -> Option<i128> {
    if baseline == 0 {
        return None;
    }

    Some((i128::from(current) - i128::from(baseline)).saturating_mul(10_000) / i128::from(baseline))
}

fn normalized_delta_basis_points(
    baseline_instructions: u64,
    baseline_units: u64,
    current_instructions: u64,
    current_units: u64,
) -> Result<Option<i128>, ScaleBaselineComparisonError> {
    let baseline_scaled = u128::from(baseline_instructions)
        .checked_mul(u128::from(current_units))
        .ok_or(ScaleBaselineComparisonError::ArithmeticOverflow)?;
    if baseline_scaled == 0 {
        return Ok(None);
    }
    let current_scaled = u128::from(current_instructions)
        .checked_mul(u128::from(baseline_units))
        .ok_or(ScaleBaselineComparisonError::ArithmeticOverflow)?;
    let (negative, magnitude) = if current_scaled >= baseline_scaled {
        (false, current_scaled - baseline_scaled)
    } else {
        (true, baseline_scaled - current_scaled)
    };
    let scaled = magnitude
        .checked_mul(10_000)
        .ok_or(ScaleBaselineComparisonError::ArithmeticOverflow)?
        / baseline_scaled;
    let signed =
        i128::try_from(scaled).map_err(|_| ScaleBaselineComparisonError::ArithmeticOverflow)?;

    Ok(Some(if negative { -signed } else { signed }))
}

fn normalized_regression(
    baseline_instructions: u64,
    current_instructions: u64,
    delta_basis_points: Option<i128>,
    relative_threshold_basis_points: u16,
) -> bool {
    if baseline_instructions == 0 {
        return current_instructions > 0;
    }

    delta_basis_points
        .is_some_and(|basis_points| basis_points >= i128::from(relative_threshold_basis_points))
}

const fn signed_relative_delta_basis_points(baseline: i128, current: i128) -> Option<i128> {
    let denominator = baseline.saturating_abs();
    if denominator == 0 {
        return None;
    }

    Some(current.saturating_sub(baseline).saturating_mul(10_000) / denominator)
}

fn signed_regression_reached(
    threshold: PerformanceThreshold,
    baseline: i128,
    increase: i128,
    delta_basis_points: Option<i128>,
) -> bool {
    if increase < i128::from(threshold.absolute_increase()) {
        return false;
    }
    if baseline == 0 {
        return true;
    }

    delta_basis_points.is_some_and(|basis_points| {
        basis_points >= i128::from(threshold.relative_increase_basis_points())
    })
}

/// Typed failure that prevents meaningful scale deltas.
#[derive(Debug)]
pub(crate) enum ScaleBaselineComparisonError {
    /// Exact rational comparison exceeded bounded integer arithmetic.
    ArithmeticOverflow,

    /// The two artifacts do not share a comparable environment.
    IncomparableEnvironment(PerfEnvironmentMismatch),

    /// The baseline merged scale artifact is invalid.
    InvalidBaseline(ScaleShardError),

    /// The current merged scale artifact is invalid.
    InvalidCurrent(ScaleShardError),

    /// One exact-cardinality observation changed declared identity.
    ObservationIdentityDrift(String),

    /// Baseline and current observation membership differs.
    ObservationSetDrift,

    /// Eligible normalized-cost membership differs.
    NormalizedSetDrift,

    /// Result, route, window, or declaration identity changed between subjects.
    SemanticDrift(String),

    /// Adjacent-cardinality slope identity or route facts differ.
    SlopeSetDrift,
}

impl Display for ScaleBaselineComparisonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow => {
                formatter.write_str("scale normalized delta arithmetic overflowed")
            }
            Self::IncomparableEnvironment(error) => {
                write!(formatter, "scale environments are incomparable: {error}")
            }
            Self::InvalidBaseline(error) => write!(formatter, "invalid scale baseline: {error}"),
            Self::InvalidCurrent(error) => {
                write!(formatter, "invalid current scale report: {error}")
            }
            Self::ObservationIdentityDrift(scenario_id) => write!(
                formatter,
                "scale observation identity drifted for {scenario_id:?}",
            ),
            Self::ObservationSetDrift => {
                formatter.write_str("scale observation membership drifted")
            }
            Self::NormalizedSetDrift => {
                formatter.write_str("scale normalized-cost membership drifted")
            }
            Self::SemanticDrift(scenario_id) => write!(
                formatter,
                "scale semantic identity drifted for {scenario_id:?}",
            ),
            Self::SlopeSetDrift => formatter.write_str("scale slope identity drifted"),
        }
    }
}

impl Error for ScaleBaselineComparisonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IncomparableEnvironment(error) => Some(error),
            Self::InvalidBaseline(error) | Self::InvalidCurrent(error) => Some(error),
            Self::ArithmeticOverflow
            | Self::ObservationIdentityDrift(_)
            | Self::ObservationSetDrift
            | Self::NormalizedSetDrift
            | Self::SemanticDrift(_)
            | Self::SlopeSetDrift => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        sql_perf_profile::SQL_PERFORMANCE_PROFILE, sql_perf_scale_shard::tests::complete_report,
    };

    use super::*;

    #[test]
    fn scale_comparison_preserves_exact_total_normalized_and_slope_deltas() {
        let (scenarios, baseline) = complete_report();
        let mut current = baseline.clone();
        current.environment.subject.source_revision = "66".repeat(20);
        current.environment.subject.raw_wasm_sha256 = "77".repeat(32);
        let comparison = compare_scale_baseline(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &baseline,
            &current,
        )
        .expect("subject identity may differ in a comparable scale pair");

        assert_eq!(comparison.totals.len(), 45);
        assert!(!comparison.normalized.is_empty());
        assert_eq!(comparison.slopes.len(), 30);
        assert!(comparison.totals.iter().all(|delta| delta.delta == 0));
        assert!(
            comparison
                .normalized
                .iter()
                .all(|delta| delta.delta_basis_points == Some(0)),
        );
    }

    #[test]
    fn scale_comparison_rejects_environment_and_route_drift() {
        let (scenarios, baseline) = complete_report();
        let mut current = baseline.clone();
        current.environment.comparable.accepted_snapshot_hash = "00".repeat(32);
        assert!(matches!(
            compare_scale_baseline(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &baseline,
                &current,
            ),
            Err(ScaleBaselineComparisonError::IncomparableEnvironment(_))
        ));

        let mut current = baseline.clone();
        current.observations[0].sample.route_family = "changed".to_string();
        assert!(matches!(
            compare_scale_baseline(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &baseline,
                &current,
            ),
            Err(ScaleBaselineComparisonError::InvalidCurrent(_))
        ));
    }

    #[test]
    fn reviewed_scale_thresholds_cover_zero_and_boundary_cases() {
        let normalized_threshold = PerformanceProfile::scale_normalized_regression_basis_points();
        assert!(!normalized_regression(
            10_000,
            10_099,
            Some(99),
            normalized_threshold,
        ));
        assert!(normalized_regression(
            10_000,
            10_100,
            Some(100),
            normalized_threshold,
        ));
        assert!(!normalized_regression(0, 0, None, normalized_threshold,));
        assert!(normalized_regression(0, 1, None, normalized_threshold,));

        let slope_threshold = PerformanceProfile::scale_slope_regression_threshold();
        assert!(!signed_regression_reached(
            slope_threshold,
            1_000_000,
            9_999,
            Some(99),
        ));
        assert!(signed_regression_reached(
            slope_threshold,
            1_000_000,
            10_000,
            Some(100),
        ));
        assert!(signed_regression_reached(slope_threshold, 0, 10_000, None,));
    }

    #[test]
    fn scale_regression_causes_include_every_gated_family() {
        let (scenarios, baseline) = complete_report();
        let mut comparison = compare_scale_baseline(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &baseline,
            &baseline,
        )
        .expect("one scale artifact should compare to itself");
        comparison.totals[0].regression = true;
        comparison.normalized[0].regression = true;
        comparison.slopes[0].regression = true;

        assert!(matches!(
            comparison.regression_causes().as_slice(),
            [
                ScaleRegressionCause::Total { .. },
                ScaleRegressionCause::Normalized { .. },
                ScaleRegressionCause::Slope { .. },
            ]
        ));
    }
}
