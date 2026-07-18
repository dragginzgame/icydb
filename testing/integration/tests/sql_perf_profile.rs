//! Module: sql_perf_profile
//! Responsibility: checked-in SQL performance profile identity and fixed execution budgets.
//! Does not own: scenario construction, PocketIC execution, sample collection, or report rendering.
//! Boundary: validates the exact broad-scan scenario set before a performance artifact can be comparable.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{self, Display},
};

use icydb_testing_sql_generator::{
    SQL_SCHEDULED_SHARD_COUNT, ScenarioShardError, scheduled_sql_scenario_shard,
};

use crate::sql_perf_regression_sentinels::REGRESSION_SENTINEL_SCENARIO_IDS;

/// Current checked-in SQL performance profile version.
pub(crate) const SQL_PERFORMANCE_PROFILE_VERSION: u32 = 1;

const EXPECTED_SCENARIO_COUNT: usize = 1_777;
const EXPECTED_SCENARIO_SET_HASH: &str =
    "aab04fd35c1d224eedc44d2075f5bdfc44256162c8a33a096aa469612504bc8c";
const EXPECTED_SCALE_SCENARIO_COUNT: usize = 45;
const EXPECTED_SCALE_SCENARIO_SET_HASH: &str =
    "aa4e78925f21c0a93dbfba3e0484c6a82b8a8a49c3c4fc4d835f71826588f6c4";
const EXPECTED_SCALE_SHARD_COUNTS: &[usize] = &[6, 6, 4, 7, 3, 4, 6, 9];
const PERFORMANCE_SHARD_COUNT: u8 = SQL_SCHEDULED_SHARD_COUNT;
const SCALE_ROW_CARDINALITIES: &[u32] = &[16, 256, 2_048];
const RESULT_WINDOW_SIZES: &[u32] = &[1, 10, 50];
const FOCUSED_HOTSPOT_SCENARIO_IDS: &[&str] = &[
    "token.collection_id.sparse_in.count",
    "token.collection_id.sparse_in.page_only.limit50",
    "token.collection_stage_id.branch_set.count",
    "token.collection_stage_id.branch_set.covering_page_only.limit50",
    "token.collection_stage_id.branch_set.duplicate_count",
    "token.collection_stage_id.branch_set.full_entity.limit50",
    "token.collection_stage_id.branch_set.index_residual_covering.limit3",
    "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
    "token.collection_stage_id.branch_set.page_only.limit50",
    "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
    "token.collection_stage_id.branch_set.wide_page_only.limit50",
    "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
    "token.collection_stage_id.overcap_fallback.page_only.limit50",
    "token.collection_stage_id.overcap_pruned.page_only.limit50",
    "token.collection_stage_id.prefixed_stage_range.page_only.limit50",
];
/// One absolute-plus-relative regression threshold.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PerformanceThreshold {
    absolute_increase: u64,
    relative_increase_basis_points: u16,
}

impl PerformanceThreshold {
    /// Return the absolute increase threshold.
    pub(crate) const fn absolute_increase(self) -> u64 {
        self.absolute_increase
    }

    /// Return the relative increase threshold in basis points.
    pub(crate) const fn relative_increase_basis_points(self) -> u16 {
        self.relative_increase_basis_points
    }

    /// Return whether one current unsigned value reaches this reviewed threshold.
    ///
    /// A zero baseline uses the absolute threshold alone because no relative
    /// percentage exists.
    pub(crate) fn reached(self, baseline: u64, current: u64) -> bool {
        let delta = i128::from(current) - i128::from(baseline);
        if delta < i128::from(self.absolute_increase) {
            return false;
        }
        if baseline == 0 {
            return true;
        }
        let basis_points = delta.saturating_mul(10_000) / i128::from(baseline);

        basis_points >= i128::from(self.relative_increase_basis_points)
    }
}

const PHASE_INSTRUCTION_REGRESSION_THRESHOLD: PerformanceThreshold = PerformanceThreshold {
    absolute_increase: 10_000,
    relative_increase_basis_points: 100,
};
const RESIDUAL_INSTRUCTION_REGRESSION_THRESHOLD: PerformanceThreshold = PerformanceThreshold {
    absolute_increase: 10_000,
    relative_increase_basis_points: 100,
};
const EXACT_COUNTER_REGRESSION_THRESHOLD: PerformanceThreshold = PerformanceThreshold {
    absolute_increase: 1,
    relative_increase_basis_points: 0,
};
const SCALE_NORMALIZED_REGRESSION_BASIS_POINTS: u16 = 100;
const SCALE_SLOPE_REGRESSION_THRESHOLD: PerformanceThreshold = PerformanceThreshold {
    absolute_increase: 10_000,
    relative_increase_basis_points: 100,
};

/// Authoritative fixed profile for SQL performance discovery and confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PerformanceProfile {
    version: u32,
    expected_scenario_count: usize,
    expected_scenario_set_hash: &'static str,
    expected_scale_scenario_count: usize,
    expected_scale_scenario_set_hash: &'static str,
    expected_scale_shard_counts: &'static [usize],
    broad_scan_requires_full_enumeration: bool,
    confirmation_top_n_per_metric: usize,
    confirmation_scenario_cap: usize,
    cold_samples_per_confirmation: u8,
    warm_samples_per_confirmation: u8,
    scale_row_cardinalities: &'static [u32],
    result_window_sizes: &'static [u32],
    focused_hotspot_scenario_ids: &'static [&'static str],
    regression_sentinel_scenario_ids: &'static [&'static str],
    shard_count: u8,
    max_artifact_bytes: usize,
    stability_threshold: PerformanceThreshold,
    total_instruction_regression_threshold: PerformanceThreshold,
}

impl PerformanceProfile {
    /// Return the profile version carried by every comparable artifact.
    pub(crate) const fn version(self) -> u32 {
        self.version
    }

    /// Return the exact broad-scan scenario count.
    pub(crate) const fn expected_scenario_count(self) -> usize {
        self.expected_scenario_count
    }

    /// Return the expected BLAKE3 scenario-set identity.
    pub(crate) const fn expected_scenario_set_hash(self) -> &'static str {
        self.expected_scenario_set_hash
    }

    /// Return the exact scale scenario count.
    pub(crate) const fn expected_scale_scenario_count(self) -> usize {
        self.expected_scale_scenario_count
    }

    /// Return the expected scale scenario-set BLAKE3 identity.
    pub(crate) const fn expected_scale_scenario_set_hash(self) -> &'static str {
        self.expected_scale_scenario_set_hash
    }

    /// Borrow the exact expected scale assignment count for every scheduled shard.
    pub(crate) const fn expected_scale_shard_counts(self) -> &'static [usize] {
        self.expected_scale_shard_counts
    }

    /// Return whether broad scan is an exact full enumeration.
    pub(crate) const fn broad_scan_requires_full_enumeration(self) -> bool {
        self.broad_scan_requires_full_enumeration
    }

    /// Return the per-metric candidate count used by confirmation selection.
    pub(crate) const fn confirmation_top_n_per_metric(self) -> usize {
        self.confirmation_top_n_per_metric
    }

    /// Return the maximum deduplicated confirmation scenario count.
    pub(crate) const fn confirmation_scenario_cap(self) -> usize {
        self.confirmation_scenario_cap
    }

    /// Return the required isolated cold sample count.
    pub(crate) const fn cold_samples_per_confirmation(self) -> u8 {
        self.cold_samples_per_confirmation
    }

    /// Return the required proven-warm sample count for eligible paths.
    pub(crate) const fn warm_samples_per_confirmation(self) -> u8 {
        self.warm_samples_per_confirmation
    }

    /// Borrow the exact row-cardinality scale ladder.
    pub(crate) const fn scale_row_cardinalities(self) -> &'static [u32] {
        self.scale_row_cardinalities
    }

    /// Borrow the exact result-window ladder.
    pub(crate) const fn result_window_sizes(self) -> &'static [u32] {
        self.result_window_sizes
    }

    /// Borrow the checked-in focused P2 hotspot scenario identities.
    pub(crate) const fn focused_hotspot_scenario_ids(self) -> &'static [&'static str] {
        self.focused_hotspot_scenario_ids
    }

    /// Borrow the checked-in P2 regression-sentinel scenario identities.
    pub(crate) const fn regression_sentinel_scenario_ids(self) -> &'static [&'static str] {
        self.regression_sentinel_scenario_ids
    }

    /// Return the exact number of required shards per scheduled stage.
    pub(crate) const fn shard_count(self) -> u8 {
        self.shard_count
    }

    /// Return the maximum encoded size of one performance evidence artifact.
    pub(crate) const fn max_artifact_bytes(self) -> usize {
        self.max_artifact_bytes
    }

    /// Return the confirmation sample-stability threshold.
    pub(crate) const fn stability_threshold(self) -> PerformanceThreshold {
        self.stability_threshold
    }

    /// Return the initial total-instruction regression threshold.
    pub(crate) const fn total_instruction_regression_threshold(self) -> PerformanceThreshold {
        self.total_instruction_regression_threshold
    }

    /// Return the reviewed non-total instruction-phase regression threshold.
    pub(crate) const fn phase_instruction_regression_threshold() -> PerformanceThreshold {
        PHASE_INSTRUCTION_REGRESSION_THRESHOLD
    }

    /// Return the reviewed attribution-residual regression threshold.
    pub(crate) const fn residual_instruction_regression_threshold() -> PerformanceThreshold {
        RESIDUAL_INSTRUCTION_REGRESSION_THRESHOLD
    }

    /// Return the reviewed exact-counter regression threshold.
    pub(crate) const fn exact_counter_regression_threshold() -> PerformanceThreshold {
        EXACT_COUNTER_REGRESSION_THRESHOLD
    }

    /// Return the reviewed normalized scale-cost threshold in basis points.
    pub(crate) const fn scale_normalized_regression_basis_points() -> u16 {
        SCALE_NORMALIZED_REGRESSION_BASIS_POINTS
    }

    /// Return the reviewed adjacent-cardinality slope regression threshold.
    pub(crate) const fn scale_slope_regression_threshold() -> PerformanceThreshold {
        SCALE_SLOPE_REGRESSION_THRESHOLD
    }

    /// Validate every fixed profile invariant.
    ///
    /// # Errors
    ///
    /// Returns a typed profile error when a checked-in budget or identity is invalid.
    pub(crate) fn validate(self) -> Result<(), PerformanceProfileError> {
        if self.version == 0 {
            return Err(PerformanceProfileError::InvalidContract(
                "profile version must be non-zero",
            ));
        }
        if self.expected_scenario_count == 0 {
            return Err(PerformanceProfileError::InvalidContract(
                "expected scenario count must be non-zero",
            ));
        }
        if self.expected_scenario_set_hash.len() != 64
            || !self
                .expected_scenario_set_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(PerformanceProfileError::InvalidContract(
                "scenario-set hash must be 64 lowercase hexadecimal characters",
            ));
        }
        if self.expected_scale_scenario_set_hash.len() != 64
            || !self
                .expected_scale_scenario_set_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(PerformanceProfileError::InvalidContract(
                "scale scenario-set hash must be 64 lowercase hexadecimal characters",
            ));
        }
        if self.expected_scale_shard_counts.len() != usize::from(self.shard_count)
            || self.expected_scale_shard_counts.iter().sum::<usize>()
                != self.expected_scale_scenario_count
        {
            return Err(PerformanceProfileError::InvalidContract(
                "scale shard counts must cover the exact scale scenario set",
            ));
        }
        validate_regression_sentinels(
            self.regression_sentinel_scenario_ids,
            self.focused_hotspot_scenario_ids,
        )?;
        if !self.broad_scan_requires_full_enumeration
            || self.confirmation_top_n_per_metric != 20
            || self.confirmation_scenario_cap != 512
            || self.cold_samples_per_confirmation != 5
            || self.warm_samples_per_confirmation != 5
            || self.expected_scale_scenario_count != EXPECTED_SCALE_SCENARIO_COUNT
            || self.expected_scale_scenario_set_hash != EXPECTED_SCALE_SCENARIO_SET_HASH
            || self.expected_scale_shard_counts != EXPECTED_SCALE_SHARD_COUNTS
            || self.scale_row_cardinalities != SCALE_ROW_CARDINALITIES
            || self.result_window_sizes != RESULT_WINDOW_SIZES
            || self.focused_hotspot_scenario_ids != FOCUSED_HOTSPOT_SCENARIO_IDS
            || self.regression_sentinel_scenario_ids != REGRESSION_SENTINEL_SCENARIO_IDS
            || self.shard_count != PERFORMANCE_SHARD_COUNT
            || self.max_artifact_bytes != 128 * 1024 * 1024
            || self.stability_threshold
                != (PerformanceThreshold {
                    absolute_increase: 10_000,
                    relative_increase_basis_points: 100,
                })
            || self.total_instruction_regression_threshold
                != (PerformanceThreshold {
                    absolute_increase: 100_000,
                    relative_increase_basis_points: 1_000,
                })
            || Self::phase_instruction_regression_threshold()
                != (PerformanceThreshold {
                    absolute_increase: 10_000,
                    relative_increase_basis_points: 100,
                })
            || Self::residual_instruction_regression_threshold()
                != (PerformanceThreshold {
                    absolute_increase: 10_000,
                    relative_increase_basis_points: 100,
                })
            || Self::exact_counter_regression_threshold()
                != (PerformanceThreshold {
                    absolute_increase: 1,
                    relative_increase_basis_points: 0,
                })
            || Self::scale_normalized_regression_basis_points() != 100
            || Self::scale_slope_regression_threshold()
                != (PerformanceThreshold {
                    absolute_increase: 10_000,
                    relative_increase_basis_points: 100,
                })
        {
            return Err(PerformanceProfileError::InvalidContract(
                "fixed performance budgets drifted from the 0.204 contract",
            ));
        }

        Ok(())
    }

    /// Validate and return the exact observed scenario-set hash.
    ///
    /// # Errors
    ///
    /// Returns a typed profile error for invalid profile state, missing or duplicate
    /// scenario identities, or count/hash drift.
    pub(crate) fn validate_scenario_set<'a>(
        self,
        scenario_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<String, PerformanceProfileError> {
        self.validate()?;
        let scenario_ids = scenario_ids.into_iter().collect::<Vec<_>>();
        if scenario_ids.len() != self.expected_scenario_count {
            return Err(PerformanceProfileError::ScenarioCountMismatch {
                expected: self.expected_scenario_count,
                actual: scenario_ids.len(),
            });
        }
        let actual = scenario_set_hash(scenario_ids)?;
        if actual != self.expected_scenario_set_hash {
            return Err(PerformanceProfileError::ScenarioSetHashMismatch {
                expected: self.expected_scenario_set_hash,
                actual,
            });
        }

        Ok(actual)
    }

    /// Return the deterministic scheduled shard for one scenario identity.
    ///
    /// # Errors
    ///
    /// Returns a typed profile error when the profile is invalid or the scenario
    /// identity cannot be encoded canonically.
    pub(crate) fn scenario_shard(self, scenario_id: &str) -> Result<u8, PerformanceProfileError> {
        self.validate()?;
        scenario_shard(scenario_id, self.shard_count)
    }
}

/// Validate the exact promoted set independently from the fixed profile fields.
fn validate_regression_sentinels(
    regression_sentinels: &[&str],
    focused_hotspots: &[&str],
) -> Result<(), PerformanceProfileError> {
    if regression_sentinels.len() != 351
        || !regression_sentinels
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        || regression_sentinels
            .iter()
            .any(|candidate| focused_hotspots.contains(candidate))
    {
        return Err(PerformanceProfileError::InvalidContract(
            "regression sentinels must be the exact sorted promotion set",
        ));
    }

    Ok(())
}

/// Current SQL performance discovery and confirmation profile.
pub(crate) const SQL_PERFORMANCE_PROFILE: PerformanceProfile = PerformanceProfile {
    version: SQL_PERFORMANCE_PROFILE_VERSION,
    expected_scenario_count: EXPECTED_SCENARIO_COUNT,
    expected_scenario_set_hash: EXPECTED_SCENARIO_SET_HASH,
    expected_scale_scenario_count: EXPECTED_SCALE_SCENARIO_COUNT,
    expected_scale_scenario_set_hash: EXPECTED_SCALE_SCENARIO_SET_HASH,
    expected_scale_shard_counts: EXPECTED_SCALE_SHARD_COUNTS,
    broad_scan_requires_full_enumeration: true,
    confirmation_top_n_per_metric: 20,
    confirmation_scenario_cap: 512,
    cold_samples_per_confirmation: 5,
    warm_samples_per_confirmation: 5,
    scale_row_cardinalities: SCALE_ROW_CARDINALITIES,
    result_window_sizes: RESULT_WINDOW_SIZES,
    focused_hotspot_scenario_ids: FOCUSED_HOTSPOT_SCENARIO_IDS,
    regression_sentinel_scenario_ids: REGRESSION_SENTINEL_SCENARIO_IDS,
    shard_count: PERFORMANCE_SHARD_COUNT,
    max_artifact_bytes: 128 * 1024 * 1024,
    stability_threshold: PerformanceThreshold {
        absolute_increase: 10_000,
        relative_increase_basis_points: 100,
    },
    total_instruction_regression_threshold: PerformanceThreshold {
        absolute_increase: 100_000,
        relative_increase_basis_points: 1_000,
    },
};

/// Typed failure while validating checked-in performance-profile authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PerformanceProfileError {
    /// The static profile violates a design invariant.
    InvalidContract(&'static str),

    /// One scenario identity is empty.
    EmptyScenarioId,

    /// One scenario identity appears more than once.
    DuplicateScenarioId(String),

    /// One scenario identity cannot fit the canonical length prefix.
    ScenarioIdTooLong(String),

    /// The generated matrix count differs from the checked-in profile.
    ScenarioCountMismatch {
        /// Checked-in count.
        expected: usize,
        /// Observed count.
        actual: usize,
    },

    /// The generated matrix identity differs from the checked-in profile.
    ScenarioSetHashMismatch {
        /// Checked-in BLAKE3 hash.
        expected: &'static str,
        /// Observed BLAKE3 hash.
        actual: String,
    },
}

impl Display for PerformanceProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContract(detail) => {
                write!(formatter, "invalid performance profile: {detail}")
            }
            Self::EmptyScenarioId => formatter.write_str("performance scenario ID is empty"),
            Self::DuplicateScenarioId(id) => {
                write!(formatter, "duplicate performance scenario ID {id:?}")
            }
            Self::ScenarioIdTooLong(id) => {
                write!(formatter, "performance scenario ID is too long: {id:?}")
            }
            Self::ScenarioCountMismatch { expected, actual } => write!(
                formatter,
                "performance scenario count drifted: expected {expected}, observed {actual}",
            ),
            Self::ScenarioSetHashMismatch { expected, actual } => write!(
                formatter,
                "performance scenario set drifted: expected {expected}, observed {actual}",
            ),
        }
    }
}

impl Error for PerformanceProfileError {}

/// Return the canonical identity for one duplicate-free scenario-ID set.
///
/// # Errors
///
/// Returns a typed profile error for an empty, duplicate, or unencodable ID.
pub(crate) fn scenario_set_hash<'a>(
    scenario_ids: impl IntoIterator<Item = &'a str>,
) -> Result<String, PerformanceProfileError> {
    let mut ids = scenario_ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    let mut unique = BTreeSet::new();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"icydb-sql-perf-scenarios/v1");
    for id in ids {
        if id.is_empty() {
            return Err(PerformanceProfileError::EmptyScenarioId);
        }
        if !unique.insert(id) {
            return Err(PerformanceProfileError::DuplicateScenarioId(id.to_string()));
        }
        let length = u32::try_from(id.len())
            .map_err(|_| PerformanceProfileError::ScenarioIdTooLong(id.to_string()))?;
        hasher.update(&length.to_be_bytes());
        hasher.update(id.as_bytes());
    }

    Ok(hasher.finalize().to_hex().to_string())
}

fn scenario_shard(scenario_id: &str, shard_count: u8) -> Result<u8, PerformanceProfileError> {
    if shard_count != SQL_SCHEDULED_SHARD_COUNT {
        return Err(PerformanceProfileError::InvalidContract(
            "performance shard count must match the shared scheduled SQL contract",
        ));
    }
    scheduled_sql_scenario_shard(scenario_id).map_err(|error| match error {
        ScenarioShardError::EmptyScenarioId => PerformanceProfileError::EmptyScenarioId,
        ScenarioShardError::ScenarioIdTooLong { .. } => {
            PerformanceProfileError::ScenarioIdTooLong(scenario_id.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_set_identity_is_order_independent_and_rejects_duplicates() {
        let first = scenario_set_hash(["scenario.b", "scenario.a"])
            .expect("distinct scenario IDs should hash");
        let second = scenario_set_hash(["scenario.a", "scenario.b"])
            .expect("scenario order should not change identity");

        assert_eq!(first, second);
        assert_eq!(
            first, "95e593742d348cbe3843efd10a9363d5dd2394d4425dca2baab84bb5b8efc3fd",
            "scenario-set encoding must remain pinned by a golden vector",
        );
        assert!(matches!(
            scenario_set_hash(["scenario.a", "scenario.a"]),
            Err(PerformanceProfileError::DuplicateScenarioId(_))
        ));
    }

    #[test]
    fn checked_in_profile_matches_design_contract() {
        let profile = SQL_PERFORMANCE_PROFILE;
        let stability = profile.stability_threshold();
        let total = profile.total_instruction_regression_threshold();
        let phase = PerformanceProfile::phase_instruction_regression_threshold();
        let residual = PerformanceProfile::residual_instruction_regression_threshold();
        let counter = PerformanceProfile::exact_counter_regression_threshold();
        let slope = PerformanceProfile::scale_slope_regression_threshold();

        assert_eq!(profile.version(), SQL_PERFORMANCE_PROFILE_VERSION);
        assert_eq!(profile.expected_scenario_count(), 1_777);
        assert_eq!(
            profile.expected_scenario_set_hash(),
            EXPECTED_SCENARIO_SET_HASH
        );
        assert!(profile.broad_scan_requires_full_enumeration());
        assert_eq!(profile.confirmation_top_n_per_metric(), 20);
        assert_eq!(profile.confirmation_scenario_cap(), 512);
        assert_eq!(profile.cold_samples_per_confirmation(), 5);
        assert_eq!(profile.warm_samples_per_confirmation(), 5);
        assert_eq!(profile.scale_row_cardinalities(), &[16, 256, 2_048]);
        assert_eq!(profile.result_window_sizes(), &[1, 10, 50]);
        assert_eq!(profile.focused_hotspot_scenario_ids().len(), 15);
        assert_eq!(profile.regression_sentinel_scenario_ids().len(), 351);
        assert_eq!(
            scenario_set_hash(profile.regression_sentinel_scenario_ids().iter().copied(),)
                .expect("reviewed regression sentinels should hash"),
            "f7acfb40711e8462cb53394fff39466a96241b06aa4523ab76813c772e139baf",
        );
        assert_eq!(profile.shard_count(), 8);
        assert_eq!(profile.max_artifact_bytes(), 128 * 1024 * 1024);
        assert_eq!(stability.absolute_increase(), 10_000);
        assert_eq!(stability.relative_increase_basis_points(), 100);
        assert_eq!(total.absolute_increase(), 100_000);
        assert_eq!(total.relative_increase_basis_points(), 1_000);
        assert_eq!(phase.absolute_increase(), 10_000);
        assert_eq!(phase.relative_increase_basis_points(), 100);
        assert_eq!(residual.absolute_increase(), 10_000);
        assert_eq!(residual.relative_increase_basis_points(), 100);
        assert_eq!(counter.absolute_increase(), 1);
        assert_eq!(counter.relative_increase_basis_points(), 0);
        assert_eq!(
            PerformanceProfile::scale_normalized_regression_basis_points(),
            100,
        );
        assert_eq!(slope.absolute_increase(), 10_000);
        assert_eq!(slope.relative_increase_basis_points(), 100);
        profile
            .validate()
            .expect("checked-in profile should be valid");
    }

    #[test]
    fn scenario_sharding_matches_golden_assignments() {
        let profile = SQL_PERFORMANCE_PROFILE;
        let assignments = ["scenario.a", "scenario.b", "scenario.c"]
            .map(|id| profile.scenario_shard(id).expect("scenario should shard"));

        assert_eq!(assignments, [0, 6, 7]);
    }

    #[test]
    fn reviewed_thresholds_are_inclusive_and_fail_closed_from_zero() {
        let total = SQL_PERFORMANCE_PROFILE.total_instruction_regression_threshold();
        assert!(!total.reached(1_000_000, 1_099_999));
        assert!(total.reached(1_000_000, 1_100_000));

        let counter = PerformanceProfile::exact_counter_regression_threshold();
        assert!(!counter.reached(0, 0));
        assert!(counter.reached(0, 1));
        assert!(counter.reached(100, 101));
    }
}
