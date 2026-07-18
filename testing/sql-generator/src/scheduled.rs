//! Module: sql_generator::scheduled
//! Responsibility: strict Tier C correctness shard evidence and exact merge completeness.
//! Does not own: scenario semantics, provider verdicts, execution, artifact paths, or CI policy.
//! Boundary: validates harness-owned scenario outcomes against the fixed current Tier C profile.

use crate::{
    MUTATION_GENERATOR_VERSION, REGRESSION_CORPUS_FORMAT_VERSION, SELECT_GENERATOR_VERSION,
    SQL_SCHEDULED_SHARD_COUNT, ScenarioShardError, SqlGeneratorError, TIER_C_ROOT_SEEDS,
    is_valid_tier_c_failure_artifact_id, replay::canonical_json_bytes,
    scheduled_sql_scenario_shard,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
};

/// Current hard-cut Tier C correctness evidence format.
pub const TIER_C_EVIDENCE_FORMAT_VERSION: u32 = 1;

/// Semantic SQL coverage-manifest revision required by current Tier C evidence.
///
/// The integration manifest gate rederives and golden-vector checks this transport
/// identity from the authoritative typed coverage cells and provider declarations.
pub const TIER_C_SQL_COVERAGE_MANIFEST_REVISION: &str =
    "0daa1e4b1f0b6ac954261e0e796e6e2066bb8b1266a9b1cd5b58d53813f3aaae";

/// Largest Tier C shard or merged artifact admitted before JSON decoding.
pub const TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES: usize = 1_048_576;

/// Domain separator for the complete Tier C correctness scenario-set identity.
const TIER_C_SCENARIO_SET_DOMAIN: &[u8] = b"icydb-sql-tier-c-scenarios/v1";

/// Domain separator for one Tier C correctness shard membership identity.
const TIER_C_SHARD_SET_DOMAIN: &[u8] = b"icydb-sql-tier-c-shard-scenarios/v1";

///
/// TierCScenarioOutcome
///
/// Compact result emitted after the harness-owned correctness verdict has run.
/// This type records the verdict; it does not decide whether a rejection is expected.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "status",
    content = "failure_artifact_id",
    deny_unknown_fields,
    rename_all = "snake_case"
)]
pub enum TierCScenarioOutcome {
    /// The scenario reached its declared typed rejection.
    ExpectedRejection,

    /// The scenario failed and references its separate minimized failure artifact.
    Failed(String),

    /// The admitted scenario and every required provider agreed.
    Passed,
}

impl TierCScenarioOutcome {
    fn validate(&self) -> Result<(), TierCEvidenceError> {
        let Self::Failed(failure_artifact_id) = self else {
            return Ok(());
        };
        if !is_valid_tier_c_failure_artifact_id(failure_artifact_id) {
            return Err(TierCEvidenceError::InvalidFailureArtifactId);
        }

        Ok(())
    }
}

///
/// TierCScenarioObservation
///
/// One harness-owned correctness outcome keyed by its stable scenario identity.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TierCScenarioObservation {
    scenario_id: String,
    outcome: TierCScenarioOutcome,
}

impl TierCScenarioObservation {
    /// Record one scenario outcome after the harness verdict has completed.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error for an invalid scenario or failure-artifact identity.
    pub fn try_new(
        scenario_id: impl Into<String>,
        outcome: TierCScenarioOutcome,
    ) -> Result<Self, TierCEvidenceError> {
        let observation = Self {
            scenario_id: scenario_id.into(),
            outcome,
        };
        observation.validate()?;

        Ok(observation)
    }

    /// Borrow the stable scenario identity.
    #[must_use]
    pub const fn scenario_id(&self) -> &str {
        self.scenario_id.as_str()
    }

    /// Borrow the harness-owned observed outcome.
    #[must_use]
    pub const fn outcome(&self) -> &TierCScenarioOutcome {
        &self.outcome
    }

    fn validate(&self) -> Result<(), TierCEvidenceError> {
        scheduled_sql_scenario_shard(self.scenario_id.as_str()).map_err(|source| {
            TierCEvidenceError::InvalidScenarioId {
                scenario_id: self.scenario_id.clone(),
                source,
            }
        })?;
        self.outcome.validate()
    }
}

///
/// TierCShardReport
///
/// One independently executable Tier C shard and its exact current-format receipt.
/// Derived hashes and counts are retained for inspection and recomputed on every read.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TierCShardReport {
    format_version: u32,
    manifest_revision: String,
    select_generator_version: u32,
    mutation_generator_version: u32,
    regression_corpus_format_version: u32,
    root_seeds: Vec<String>,
    shard_index: u8,
    shard_count: u8,
    expected_scenario_set_hash: String,
    expected_shard_hash: String,
    observed_shard_hash: String,
    expected_scenario_count: u32,
    observed_scenario_count: u32,
    passed_scenario_count: u32,
    expected_rejection_count: u32,
    failed_scenario_count: u32,
    complete: bool,
    observations: Vec<TierCScenarioObservation>,
}

impl TierCShardReport {
    /// Build one exact Tier C shard report from the complete declaration and shard outcomes.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error for an invalid declaration, shard index,
    /// duplicate or misassigned observation, or incomplete membership.
    pub fn try_new(
        shard_index: u8,
        declared_scenario_ids: &[&str],
        observations: Vec<TierCScenarioObservation>,
    ) -> Result<Self, TierCEvidenceError> {
        if shard_index >= SQL_SCHEDULED_SHARD_COUNT {
            return Err(TierCEvidenceError::InvalidShardIndex {
                shard_index,
                shard_count: SQL_SCHEDULED_SHARD_COUNT,
            });
        }
        validate_declared_scenario_ids(declared_scenario_ids)?;
        let prepared = prepare_tier_c_shard(shard_index, declared_scenario_ids, observations)?;
        let expected_scenario_set_hash = scenario_set_hash(declared_scenario_ids)?;

        Ok(Self {
            format_version: TIER_C_EVIDENCE_FORMAT_VERSION,
            manifest_revision: TIER_C_SQL_COVERAGE_MANIFEST_REVISION.to_string(),
            select_generator_version: SELECT_GENERATOR_VERSION,
            mutation_generator_version: MUTATION_GENERATOR_VERSION,
            regression_corpus_format_version: REGRESSION_CORPUS_FORMAT_VERSION,
            root_seeds: current_root_seed_ids(),
            shard_index,
            shard_count: SQL_SCHEDULED_SHARD_COUNT,
            expected_scenario_set_hash,
            expected_shard_hash: prepared.expected_shard_hash,
            observed_shard_hash: prepared.observed_shard_hash,
            expected_scenario_count: prepared.expected_scenario_count,
            observed_scenario_count: prepared.observed_scenario_count,
            passed_scenario_count: prepared.outcome_counts.passed,
            expected_rejection_count: prepared.outcome_counts.expected_rejection,
            failed_scenario_count: prepared.outcome_counts.failed,
            complete: true,
            observations: prepared.observations,
        })
    }

    /// Return the zero-based deterministic shard index.
    #[must_use]
    pub const fn shard_index(&self) -> u8 {
        self.shard_index
    }

    /// Borrow the full expected Tier C scenario-set identity.
    #[must_use]
    pub const fn expected_scenario_set_hash(&self) -> &str {
        self.expected_scenario_set_hash.as_str()
    }

    /// Return the exact observed scenario count.
    #[must_use]
    pub const fn observed_scenario_count(&self) -> u32 {
        self.observed_scenario_count
    }

    /// Return the exact passed scenario count.
    #[must_use]
    pub const fn passed_scenario_count(&self) -> u32 {
        self.passed_scenario_count
    }

    /// Return the exact expected-rejection count.
    #[must_use]
    pub const fn expected_rejection_count(&self) -> u32 {
        self.expected_rejection_count
    }

    /// Return the exact failed scenario count.
    #[must_use]
    pub const fn failed_scenario_count(&self) -> u32 {
        self.failed_scenario_count
    }

    /// Borrow the stable-order observations serialized by this shard.
    #[must_use]
    pub const fn observations(&self) -> &[TierCScenarioObservation] {
        self.observations.as_slice()
    }

    /// Encode this shard using the sole current bounded canonical JSON format.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error when current-profile validation,
    /// serialization, or the artifact byte bound fails.
    pub fn to_canonical_json(
        &self,
        declared_scenario_ids: &[&str],
    ) -> Result<Vec<u8>, TierCEvidenceError> {
        self.validate(declared_scenario_ids)?;
        let bytes = canonical_json_bytes(self).map_err(TierCEvidenceError::Serialization)?;
        validate_artifact_size(bytes.len())?;

        Ok(bytes)
    }

    /// Decode one strict bounded current-format Tier C shard artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error before decoding oversized input, or for
    /// malformed, non-canonical, stale, tampered, or incomplete evidence.
    pub fn from_canonical_json(
        bytes: &[u8],
        declared_scenario_ids: &[&str],
    ) -> Result<Self, TierCEvidenceError> {
        validate_artifact_size(bytes.len())?;
        let report = serde_json::from_slice::<Self>(bytes)
            .map_err(|source| TierCEvidenceError::Decode { source })?;
        report.validate(declared_scenario_ids)?;
        let canonical = canonical_json_bytes(&report).map_err(TierCEvidenceError::Serialization)?;
        if canonical != bytes {
            return Err(TierCEvidenceError::NonCanonicalArtifact);
        }

        Ok(report)
    }

    fn validate(&self, declared_scenario_ids: &[&str]) -> Result<(), TierCEvidenceError> {
        if self.format_version != TIER_C_EVIDENCE_FORMAT_VERSION {
            return Err(TierCEvidenceError::InvalidArtifactVersion {
                expected: TIER_C_EVIDENCE_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        let expected = Self::try_new(
            self.shard_index,
            declared_scenario_ids,
            self.observations.clone(),
        )?;
        if &expected != self {
            return Err(TierCEvidenceError::ShardReportDrift(self.shard_index));
        }

        Ok(())
    }
}

///
/// TierCMergedReport
///
/// Complete Tier C evidence produced only after all eight shard reports validate.
/// A complete report may remain red when it contains failed scenario outcomes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TierCMergedReport {
    format_version: u32,
    manifest_revision: String,
    select_generator_version: u32,
    mutation_generator_version: u32,
    regression_corpus_format_version: u32,
    root_seeds: Vec<String>,
    shard_count: u8,
    expected_scenario_set_hash: String,
    observed_scenario_count: u32,
    passed_scenario_count: u32,
    expected_rejection_count: u32,
    failed_scenario_count: u32,
    complete: bool,
    clean: bool,
    shard_reports: Vec<TierCShardReport>,
}

impl TierCMergedReport {
    /// Merge exactly one validated report for every deterministic Tier C shard.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error for missing, duplicate, tampered,
    /// inconsistent, overflowed, or aggregate-incomplete shard evidence.
    pub fn try_merge(
        declared_scenario_ids: &[&str],
        shard_reports: Vec<TierCShardReport>,
    ) -> Result<Self, TierCEvidenceError> {
        validate_declared_scenario_ids(declared_scenario_ids)?;
        if shard_reports.len() != usize::from(SQL_SCHEDULED_SHARD_COUNT) {
            return Err(TierCEvidenceError::ShardReportCountMismatch {
                expected: SQL_SCHEDULED_SHARD_COUNT,
                actual: shard_reports.len(),
            });
        }

        let mut by_shard = BTreeMap::new();
        for report in shard_reports {
            report.validate(declared_scenario_ids)?;
            let shard_index = report.shard_index();
            if by_shard.insert(shard_index, report).is_some() {
                return Err(TierCEvidenceError::DuplicateShardReport(shard_index));
            }
        }

        let mut shard_reports = Vec::with_capacity(usize::from(SQL_SCHEDULED_SHARD_COUNT));
        let mut observed_scenario_count = 0_u32;
        let mut passed_scenario_count = 0_u32;
        let mut expected_rejection_count = 0_u32;
        let mut failed_scenario_count = 0_u32;
        for shard_index in 0..SQL_SCHEDULED_SHARD_COUNT {
            let report = by_shard
                .remove(&shard_index)
                .ok_or(TierCEvidenceError::MissingShardReport(shard_index))?;
            observed_scenario_count = checked_add_count(
                observed_scenario_count,
                report.observed_scenario_count(),
                "observed merge",
            )?;
            passed_scenario_count = checked_add_count(
                passed_scenario_count,
                report.passed_scenario_count(),
                "passed merge",
            )?;
            expected_rejection_count = checked_add_count(
                expected_rejection_count,
                report.expected_rejection_count(),
                "expected-rejection merge",
            )?;
            failed_scenario_count = checked_add_count(
                failed_scenario_count,
                report.failed_scenario_count(),
                "failed merge",
            )?;
            shard_reports.push(report);
        }
        let declared_count = bounded_count(declared_scenario_ids.len(), "declared merge")?;
        let classified_count = checked_add_count(
            checked_add_count(
                passed_scenario_count,
                expected_rejection_count,
                "classified merge",
            )?,
            failed_scenario_count,
            "classified merge",
        )?;
        if observed_scenario_count != declared_count || classified_count != declared_count {
            return Err(TierCEvidenceError::AggregateScenarioCountMismatch {
                expected: declared_count,
                observed: observed_scenario_count,
                classified: classified_count,
            });
        }

        Ok(Self {
            format_version: TIER_C_EVIDENCE_FORMAT_VERSION,
            manifest_revision: TIER_C_SQL_COVERAGE_MANIFEST_REVISION.to_string(),
            select_generator_version: SELECT_GENERATOR_VERSION,
            mutation_generator_version: MUTATION_GENERATOR_VERSION,
            regression_corpus_format_version: REGRESSION_CORPUS_FORMAT_VERSION,
            root_seeds: current_root_seed_ids(),
            shard_count: SQL_SCHEDULED_SHARD_COUNT,
            expected_scenario_set_hash: scenario_set_hash(declared_scenario_ids)?,
            observed_scenario_count,
            passed_scenario_count,
            expected_rejection_count,
            failed_scenario_count,
            complete: true,
            clean: failed_scenario_count == 0,
            shard_reports,
        })
    }

    /// Borrow the complete expected Tier C scenario-set identity.
    #[must_use]
    pub const fn expected_scenario_set_hash(&self) -> &str {
        self.expected_scenario_set_hash.as_str()
    }

    /// Return the complete observed scenario count.
    #[must_use]
    pub const fn observed_scenario_count(&self) -> u32 {
        self.observed_scenario_count
    }

    /// Return the complete passed scenario count.
    #[must_use]
    pub const fn passed_scenario_count(&self) -> u32 {
        self.passed_scenario_count
    }

    /// Return the complete expected-rejection count.
    #[must_use]
    pub const fn expected_rejection_count(&self) -> u32 {
        self.expected_rejection_count
    }

    /// Return the complete failed scenario count.
    #[must_use]
    pub const fn failed_scenario_count(&self) -> u32 {
        self.failed_scenario_count
    }

    /// Return whether every declared scenario completed without a failure outcome.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.clean
    }

    /// Require the complete merged evidence to contain no failed scenario.
    ///
    /// # Errors
    ///
    /// Returns a typed red verdict carrying the exact failure count.
    pub const fn require_clean(&self) -> Result<(), TierCEvidenceError> {
        if self.clean {
            Ok(())
        } else {
            Err(TierCEvidenceError::FailedEvidence {
                failed_scenario_count: self.failed_scenario_count,
            })
        }
    }

    /// Borrow the exact shard reports ordered by zero-based shard index.
    #[must_use]
    pub const fn shard_reports(&self) -> &[TierCShardReport] {
        self.shard_reports.as_slice()
    }

    /// Encode this merge using the sole current bounded canonical JSON format.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error when current-profile validation,
    /// serialization, or the artifact byte bound fails.
    pub fn to_canonical_json(
        &self,
        declared_scenario_ids: &[&str],
    ) -> Result<Vec<u8>, TierCEvidenceError> {
        self.validate(declared_scenario_ids)?;
        let bytes = canonical_json_bytes(self).map_err(TierCEvidenceError::Serialization)?;
        validate_artifact_size(bytes.len())?;

        Ok(bytes)
    }

    /// Decode one strict bounded current-format Tier C merged artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed evidence error before decoding oversized input, or for
    /// malformed, non-canonical, stale, tampered, or aggregate-incomplete evidence.
    pub fn from_canonical_json(
        bytes: &[u8],
        declared_scenario_ids: &[&str],
    ) -> Result<Self, TierCEvidenceError> {
        validate_artifact_size(bytes.len())?;
        let report = serde_json::from_slice::<Self>(bytes)
            .map_err(|source| TierCEvidenceError::Decode { source })?;
        report.validate(declared_scenario_ids)?;
        let canonical = canonical_json_bytes(&report).map_err(TierCEvidenceError::Serialization)?;
        if canonical != bytes {
            return Err(TierCEvidenceError::NonCanonicalArtifact);
        }

        Ok(report)
    }

    fn validate(&self, declared_scenario_ids: &[&str]) -> Result<(), TierCEvidenceError> {
        if self.format_version != TIER_C_EVIDENCE_FORMAT_VERSION {
            return Err(TierCEvidenceError::InvalidArtifactVersion {
                expected: TIER_C_EVIDENCE_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        let expected = Self::try_merge(declared_scenario_ids, self.shard_reports.clone())?;
        if &expected != self {
            return Err(TierCEvidenceError::MergedReportDrift);
        }

        Ok(())
    }
}

///
/// TierCEvidenceError
///
/// Typed current-profile construction, decoding, validation, or merge failure.
///

#[derive(Debug)]
pub enum TierCEvidenceError {
    /// Merged counts do not cover the complete declared scenario set exactly once.
    AggregateScenarioCountMismatch {
        /// Complete declared scenario count.
        expected: u32,
        /// Sum of exact shard observation counts.
        observed: u32,
        /// Sum of pass, expected-rejection, and failure counts.
        classified: u32,
    },

    /// Input or output exceeded the fixed current artifact byte bound.
    ArtifactTooLarge {
        /// Observed byte count.
        observed_bytes: usize,
        /// Maximum admitted byte count.
        maximum_bytes: usize,
    },

    /// Canonical JSON could not be decoded.
    Decode {
        /// Original JSON decoding cause.
        source: serde_json::Error,
    },

    /// More than one declaration or observation used one stable scenario identity.
    DuplicateScenarioId(String),

    /// More than one report claimed the same zero-based shard.
    DuplicateShardReport(u8),

    /// Complete evidence contains one or more failed scenarios.
    FailedEvidence {
        /// Exact failed scenario count.
        failed_scenario_count: u32,
    },

    /// The current evidence format version did not match the artifact.
    InvalidArtifactVersion {
        /// Sole current format version.
        expected: u32,
        /// Decoded artifact version.
        actual: u32,
    },

    /// A failure artifact identity was not the current content-addressed form.
    InvalidFailureArtifactId,

    /// One stable scenario identity could not use the shared shard contract.
    InvalidScenarioId {
        /// Invalid scenario identity.
        scenario_id: String,
        /// Shared shard-contract cause.
        source: ScenarioShardError,
    },

    /// The requested shard index was outside the fixed current shard range.
    InvalidShardIndex {
        /// Requested zero-based shard index.
        shard_index: u8,
        /// Fixed current shard count.
        shard_count: u8,
    },

    /// A decoded merged report disagreed with recomputed current evidence.
    MergedReportDrift,

    /// Exactly one required zero-based shard had no report.
    MissingShardReport(u8),

    /// A decoded artifact was valid JSON but not canonical current JSON.
    NonCanonicalArtifact,

    /// One observation appeared in a report other than its deterministic shard.
    ScenarioAssignedToDifferentShard {
        /// Misassigned stable scenario identity.
        scenario_id: String,
        /// Shard report that attempted to record the observation.
        receipt_shard: u8,
        /// Deterministic shard derived from the scenario identity.
        assigned_shard: u8,
    },

    /// One bounded scenario count overflowed its current `u32` representation.
    ScenarioCountOverflow {
        /// Count role being accumulated.
        context: &'static str,
    },

    /// One shard's observed identities differed from its deterministic declaration.
    ScenarioMembershipMismatch {
        /// Zero-based shard index.
        shard_index: u8,
        /// Declared identities without an observation.
        missing: Vec<String>,
        /// Observed identities absent from the declaration.
        unexpected: Vec<String>,
    },

    /// The complete Tier C declaration contained no scenario.
    ScenarioSetEmpty,

    /// Canonical JSON materialization or encoding failed.
    Serialization(SqlGeneratorError),

    /// The merged report did not contain exactly the fixed eight shard reports.
    ShardReportCountMismatch {
        /// Fixed current shard count.
        expected: u8,
        /// Decoded or supplied report count.
        actual: usize,
    },

    /// One decoded shard report disagreed with its recomputed current receipt.
    ShardReportDrift(u8),
}

impl Display for TierCEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AggregateScenarioCountMismatch {
                expected,
                observed,
                classified,
            } => write!(
                formatter,
                "Tier C aggregate counts drifted: expected {expected}, observed {observed}, classified {classified}",
            ),
            Self::ArtifactTooLarge {
                observed_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "Tier C artifact has {observed_bytes} bytes, exceeding the {maximum_bytes}-byte bound",
            ),
            Self::Decode { .. } => formatter.write_str("failed to decode Tier C canonical JSON"),
            Self::DuplicateScenarioId(scenario_id) => {
                write!(formatter, "duplicate Tier C scenario ID {scenario_id:?}")
            }
            Self::DuplicateShardReport(shard_index) => {
                write!(formatter, "duplicate Tier C shard report {shard_index}")
            }
            Self::FailedEvidence {
                failed_scenario_count,
            } => write!(
                formatter,
                "Tier C evidence contains {failed_scenario_count} failed scenarios",
            ),
            Self::InvalidArtifactVersion { expected, actual } => write!(
                formatter,
                "Tier C artifact version {actual} does not match current version {expected}",
            ),
            Self::InvalidFailureArtifactId => formatter.write_str(
                "Tier C failure artifact ID must be failure. followed by 64 lowercase hexadecimal BLAKE3 digits",
            ),
            Self::InvalidScenarioId {
                scenario_id,
                source,
            } => write!(
                formatter,
                "invalid Tier C scenario ID {scenario_id:?}: {source}"
            ),
            Self::InvalidShardIndex {
                shard_index,
                shard_count,
            } => write!(
                formatter,
                "Tier C shard index {shard_index} is outside zero through {}",
                shard_count.saturating_sub(1),
            ),
            Self::MergedReportDrift => {
                formatter.write_str("Tier C merged report disagrees with recomputed evidence")
            }
            Self::MissingShardReport(shard_index) => {
                write!(formatter, "missing Tier C shard report {shard_index}")
            }
            Self::NonCanonicalArtifact => {
                formatter.write_str("Tier C artifact is not canonical current JSON")
            }
            Self::ScenarioAssignedToDifferentShard {
                scenario_id,
                receipt_shard,
                assigned_shard,
            } => write!(
                formatter,
                "Tier C scenario {scenario_id:?} belongs to shard {assigned_shard}, not report {receipt_shard}",
            ),
            Self::ScenarioCountOverflow { context } => {
                write!(formatter, "Tier C {context} scenario count overflowed")
            }
            Self::ScenarioMembershipMismatch {
                shard_index,
                missing,
                unexpected,
            } => write!(
                formatter,
                "Tier C shard {shard_index} membership drifted: missing {missing:?}, unexpected {unexpected:?}",
            ),
            Self::ScenarioSetEmpty => {
                formatter.write_str("Tier C scenario declaration must not be empty")
            }
            Self::Serialization(source) => write!(
                formatter,
                "failed to serialize Tier C canonical JSON: {source}"
            ),
            Self::ShardReportCountMismatch { expected, actual } => write!(
                formatter,
                "Tier C merge requires {expected} shard reports, observed {actual}",
            ),
            Self::ShardReportDrift(shard_index) => write!(
                formatter,
                "Tier C shard report {shard_index} disagrees with recomputed evidence",
            ),
        }
    }
}

impl Error for TierCEvidenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::InvalidScenarioId { source, .. } => Some(source),
            Self::Serialization(source) => Some(source),
            _ => None,
        }
    }
}

fn validate_declared_scenario_ids(
    declared_scenario_ids: &[&str],
) -> Result<(), TierCEvidenceError> {
    if declared_scenario_ids.is_empty() {
        return Err(TierCEvidenceError::ScenarioSetEmpty);
    }
    let mut unique = BTreeSet::new();
    for scenario_id in declared_scenario_ids {
        scheduled_sql_scenario_shard(scenario_id).map_err(|source| {
            TierCEvidenceError::InvalidScenarioId {
                scenario_id: (*scenario_id).to_string(),
                source,
            }
        })?;
        if !unique.insert(*scenario_id) {
            return Err(TierCEvidenceError::DuplicateScenarioId(
                (*scenario_id).to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_unique_observations(
    observations: &[TierCScenarioObservation],
) -> Result<(), TierCEvidenceError> {
    for adjacent in observations.windows(2) {
        if adjacent[0].scenario_id == adjacent[1].scenario_id {
            return Err(TierCEvidenceError::DuplicateScenarioId(
                adjacent[0].scenario_id.clone(),
            ));
        }
    }

    Ok(())
}

struct PreparedTierCShard {
    observations: Vec<TierCScenarioObservation>,
    expected_shard_hash: String,
    observed_shard_hash: String,
    expected_scenario_count: u32,
    observed_scenario_count: u32,
    outcome_counts: TierCOutcomeCounts,
}

struct TierCOutcomeCounts {
    passed: u32,
    expected_rejection: u32,
    failed: u32,
}

fn prepare_tier_c_shard(
    shard_index: u8,
    declared_scenario_ids: &[&str],
    mut observations: Vec<TierCScenarioObservation>,
) -> Result<PreparedTierCShard, TierCEvidenceError> {
    let expected_ids = scenario_ids_for_shard(shard_index, declared_scenario_ids)?;
    validate_observation_assignments(shard_index, &observations)?;
    observations.sort_by(|left, right| left.scenario_id.cmp(&right.scenario_id));
    validate_unique_observations(&observations)?;
    validate_shard_membership(shard_index, &expected_ids, &observations)?;

    Ok(PreparedTierCShard {
        expected_shard_hash: shard_set_hash(shard_index, expected_ids.iter().copied())?,
        observed_shard_hash: shard_set_hash(
            shard_index,
            observations
                .iter()
                .map(TierCScenarioObservation::scenario_id),
        )?,
        expected_scenario_count: bounded_count(expected_ids.len(), "expected shard")?,
        observed_scenario_count: bounded_count(observations.len(), "observed shard")?,
        outcome_counts: count_outcomes(&observations)?,
        observations,
    })
}

fn validate_observation_assignments(
    shard_index: u8,
    observations: &[TierCScenarioObservation],
) -> Result<(), TierCEvidenceError> {
    for observation in observations {
        observation.validate()?;
        let assigned =
            scheduled_sql_scenario_shard(observation.scenario_id()).map_err(|source| {
                TierCEvidenceError::InvalidScenarioId {
                    scenario_id: observation.scenario_id().to_string(),
                    source,
                }
            })?;
        if assigned != shard_index {
            return Err(TierCEvidenceError::ScenarioAssignedToDifferentShard {
                scenario_id: observation.scenario_id().to_string(),
                receipt_shard: shard_index,
                assigned_shard: assigned,
            });
        }
    }

    Ok(())
}

fn validate_shard_membership(
    shard_index: u8,
    expected_ids: &[&str],
    observations: &[TierCScenarioObservation],
) -> Result<(), TierCEvidenceError> {
    let expected = expected_ids.iter().copied().collect::<BTreeSet<_>>();
    let observed = observations
        .iter()
        .map(TierCScenarioObservation::scenario_id)
        .collect::<BTreeSet<_>>();
    if expected != observed {
        return Err(TierCEvidenceError::ScenarioMembershipMismatch {
            shard_index,
            missing: expected
                .difference(&observed)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
            unexpected: observed
                .difference(&expected)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
        });
    }

    Ok(())
}

fn count_outcomes(
    observations: &[TierCScenarioObservation],
) -> Result<TierCOutcomeCounts, TierCEvidenceError> {
    let passed = observations
        .iter()
        .filter(|observation| matches!(observation.outcome(), TierCScenarioOutcome::Passed))
        .count();
    let expected_rejection = observations
        .iter()
        .filter(|observation| {
            matches!(
                observation.outcome(),
                TierCScenarioOutcome::ExpectedRejection
            )
        })
        .count();
    let failed = observations
        .iter()
        .filter(|observation| matches!(observation.outcome(), TierCScenarioOutcome::Failed(_)))
        .count();

    Ok(TierCOutcomeCounts {
        passed: bounded_count(passed, "passed shard")?,
        expected_rejection: bounded_count(expected_rejection, "expected-rejection shard")?,
        failed: bounded_count(failed, "failed shard")?,
    })
}

fn scenario_ids_for_shard<'a>(
    shard_index: u8,
    declared_scenario_ids: &[&'a str],
) -> Result<Vec<&'a str>, TierCEvidenceError> {
    let mut selected = Vec::new();
    for scenario_id in declared_scenario_ids {
        let assigned = scheduled_sql_scenario_shard(scenario_id).map_err(|source| {
            TierCEvidenceError::InvalidScenarioId {
                scenario_id: (*scenario_id).to_string(),
                source,
            }
        })?;
        if assigned == shard_index {
            selected.push(*scenario_id);
        }
    }
    selected.sort_unstable();

    Ok(selected)
}

fn scenario_set_hash(scenario_ids: &[&str]) -> Result<String, TierCEvidenceError> {
    validate_declared_scenario_ids(scenario_ids)?;
    hash_scenario_ids(
        TIER_C_SCENARIO_SET_DOMAIN,
        None,
        scenario_ids.iter().copied(),
    )
}

fn shard_set_hash<'a>(
    shard_index: u8,
    scenario_ids: impl IntoIterator<Item = &'a str>,
) -> Result<String, TierCEvidenceError> {
    hash_scenario_ids(TIER_C_SHARD_SET_DOMAIN, Some(shard_index), scenario_ids)
}

fn hash_scenario_ids<'a>(
    domain: &[u8],
    shard_index: Option<u8>,
    scenario_ids: impl IntoIterator<Item = &'a str>,
) -> Result<String, TierCEvidenceError> {
    let mut scenario_ids = scenario_ids.into_iter().collect::<Vec<_>>();
    scenario_ids.sort_unstable();
    for adjacent in scenario_ids.windows(2) {
        if adjacent[0] == adjacent[1] {
            return Err(TierCEvidenceError::DuplicateScenarioId(
                adjacent[0].to_string(),
            ));
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    if let Some(shard_index) = shard_index {
        hasher.update(&[shard_index]);
    }
    for scenario_id in scenario_ids {
        let length = u32::try_from(scenario_id.len()).map_err(|_| {
            TierCEvidenceError::InvalidScenarioId {
                scenario_id: scenario_id.to_string(),
                source: ScenarioShardError::ScenarioIdTooLong {
                    observed_bytes: scenario_id.len(),
                },
            }
        })?;
        hasher.update(&length.to_be_bytes());
        hasher.update(scenario_id.as_bytes());
    }

    Ok(hasher.finalize().to_hex().to_string())
}

fn bounded_count(count: usize, context: &'static str) -> Result<u32, TierCEvidenceError> {
    u32::try_from(count).map_err(|_| TierCEvidenceError::ScenarioCountOverflow { context })
}

fn checked_add_count(
    left: u32,
    right: u32,
    context: &'static str,
) -> Result<u32, TierCEvidenceError> {
    left.checked_add(right)
        .ok_or(TierCEvidenceError::ScenarioCountOverflow { context })
}

fn current_root_seed_ids() -> Vec<String> {
    TIER_C_ROOT_SEEDS
        .iter()
        .map(|root_seed| format!("u64:{root_seed:016x}"))
        .collect()
}

const fn validate_artifact_size(byte_count: usize) -> Result<(), TierCEvidenceError> {
    if byte_count > TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES {
        return Err(TierCEvidenceError::ArtifactTooLarge {
            observed_bytes: byte_count,
            maximum_bytes: TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
        });
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        SQL_SCHEDULED_SHARD_COUNT, TierCEvidenceError, TierCMergedReport, TierCScenarioObservation,
        TierCScenarioOutcome, TierCShardReport, scheduled::TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
        scheduled_sql_scenario_shard,
    };

    #[test]
    fn exact_eight_shard_reports_merge_and_round_trip_canonically() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        let reports = reports_for(&declared, |shard_index| {
            if shard_index == 3 {
                TierCScenarioOutcome::ExpectedRejection
            } else {
                TierCScenarioOutcome::Passed
            }
        });
        let merged =
            TierCMergedReport::try_merge(&declared, reports).expect("exact receipts should merge");

        assert_eq!(merged.observed_scenario_count(), 8);
        assert_eq!(merged.passed_scenario_count(), 7);
        assert_eq!(merged.expected_rejection_count(), 1);
        assert_eq!(merged.failed_scenario_count(), 0);
        assert!(merged.is_clean());
        merged.require_clean().expect("clean evidence should pass");

        let encoded = merged
            .to_canonical_json(&declared)
            .expect("merged report should encode");
        let decoded = TierCMergedReport::from_canonical_json(encoded.as_slice(), &declared)
            .expect("merged report should decode");
        assert_eq!(decoded, merged);

        for report in merged.shard_reports() {
            let encoded = report
                .to_canonical_json(&declared)
                .expect("shard report should encode");
            let decoded = TierCShardReport::from_canonical_json(encoded.as_slice(), &declared)
                .expect("shard report should decode");
            assert_eq!(&decoded, report);
        }
    }

    #[test]
    fn scenario_declaration_order_does_not_change_receipt_identity() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        let reversed = declared.iter().copied().rev().collect::<Vec<_>>();
        let observations = observations_for_shard(0, &declared, TierCScenarioOutcome::Passed);

        let first = TierCShardReport::try_new(0, &declared, observations.clone())
            .expect("forward declaration should validate");
        let second = TierCShardReport::try_new(0, &reversed, observations)
            .expect("reversed declaration should validate");

        assert_eq!(first, second);
    }

    #[test]
    fn tier_c_scenario_set_identity_has_a_fixed_golden_vector() {
        let forward = super::scenario_set_hash(&["scenario.a", "scenario.b"])
            .expect("distinct scenario IDs should hash");
        let reversed = super::scenario_set_hash(&["scenario.b", "scenario.a"])
            .expect("scenario declaration order should not change identity");

        assert_eq!(forward, reversed);
        assert_eq!(
            forward,
            "eb8df215669ca3f5f36225a8a54910f26e3e1c44590e4b0a5ac65c98365bdc23",
        );
    }

    #[test]
    fn shard_report_rejects_missing_duplicate_and_misassigned_observations() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        assert!(matches!(
            TierCShardReport::try_new(0, &declared, Vec::new()),
            Err(TierCEvidenceError::ScenarioMembershipMismatch { .. })
        ));

        let mut duplicate = observations_for_shard(0, &declared, TierCScenarioOutcome::Passed);
        duplicate.push(duplicate[0].clone());
        assert!(matches!(
            TierCShardReport::try_new(0, &declared, duplicate),
            Err(TierCEvidenceError::DuplicateScenarioId(_))
        ));

        let misassigned = vec![
            TierCScenarioObservation::try_new(
                scenario_ids[1].clone(),
                TierCScenarioOutcome::Passed,
            )
            .expect("observation should construct"),
        ];
        assert!(matches!(
            TierCShardReport::try_new(0, &declared, misassigned),
            Err(TierCEvidenceError::ScenarioAssignedToDifferentShard { .. })
        ));
    }

    #[test]
    fn failed_evidence_remains_reportable_but_cannot_pass() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        assert!(matches!(
            TierCScenarioObservation::try_new(
                scenario_ids[5].clone(),
                TierCScenarioOutcome::Failed("not-content-addressed".to_string()),
            ),
            Err(TierCEvidenceError::InvalidFailureArtifactId)
        ));
        let reports = reports_for(&declared, |shard_index| {
            if shard_index == 5 {
                TierCScenarioOutcome::Failed(format!("failure.{}", "5".repeat(64)))
            } else {
                TierCScenarioOutcome::Passed
            }
        });
        let merged = TierCMergedReport::try_merge(&declared, reports)
            .expect("complete failed evidence should still merge for reporting");

        assert_eq!(merged.failed_scenario_count(), 1);
        assert!(!merged.is_clean());
        assert!(matches!(
            merged.require_clean(),
            Err(TierCEvidenceError::FailedEvidence {
                failed_scenario_count: 1
            })
        ));
    }

    #[test]
    fn strict_decode_rejects_unknown_tampered_and_oversized_artifacts() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        let report = TierCShardReport::try_new(
            0,
            &declared,
            observations_for_shard(0, &declared, TierCScenarioOutcome::Passed),
        )
        .expect("report should construct");
        let encoded = report
            .to_canonical_json(&declared)
            .expect("report should encode");

        let mut value = serde_json::from_slice::<serde_json::Value>(&encoded)
            .expect("canonical report should be JSON");
        value
            .as_object_mut()
            .expect("report should be an object")
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        let unknown = serde_json::to_vec(&value).expect("tampered report should encode");
        assert!(matches!(
            TierCShardReport::from_canonical_json(&unknown, &declared),
            Err(TierCEvidenceError::Decode { .. })
        ));

        value
            .as_object_mut()
            .expect("report should be an object")
            .remove("unknown");
        value
            .as_object_mut()
            .expect("report should be an object")
            .insert(
                "passed_scenario_count".to_string(),
                serde_json::Value::from(99_u32),
            );
        let tampered = crate::replay::canonical_json_bytes(&value)
            .expect("tampered report should canonicalize");
        assert!(matches!(
            TierCShardReport::from_canonical_json(&tampered, &declared),
            Err(TierCEvidenceError::ShardReportDrift(0))
        ));

        let oversized = vec![b' '; TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES + 1];
        assert!(matches!(
            TierCShardReport::from_canonical_json(&oversized, &declared),
            Err(TierCEvidenceError::ArtifactTooLarge { .. })
        ));
    }

    #[test]
    fn merge_rejects_missing_and_duplicate_shard_reports() {
        let scenario_ids = one_scenario_per_shard();
        let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
        let mut reports = reports_for(&declared, |_| TierCScenarioOutcome::Passed);
        reports.pop();
        assert!(matches!(
            TierCMergedReport::try_merge(&declared, reports),
            Err(TierCEvidenceError::ShardReportCountMismatch {
                expected: 8,
                actual: 7
            })
        ));

        let mut reports = reports_for(&declared, |_| TierCScenarioOutcome::Passed);
        reports[7] = reports[0].clone();
        assert!(matches!(
            TierCMergedReport::try_merge(&declared, reports),
            Err(TierCEvidenceError::DuplicateShardReport(0))
        ));
    }

    fn reports_for(
        declared: &[&str],
        outcome: impl Fn(u8) -> TierCScenarioOutcome,
    ) -> Vec<TierCShardReport> {
        (0..SQL_SCHEDULED_SHARD_COUNT)
            .map(|shard_index| {
                TierCShardReport::try_new(
                    shard_index,
                    declared,
                    observations_for_shard(shard_index, declared, outcome(shard_index)),
                )
                .expect("complete shard report should construct")
            })
            .collect()
    }

    fn observations_for_shard(
        shard_index: u8,
        declared: &[&str],
        outcome: TierCScenarioOutcome,
    ) -> Vec<TierCScenarioObservation> {
        declared
            .iter()
            .filter(|scenario_id| {
                scheduled_sql_scenario_shard(scenario_id).expect("test scenario ID should shard")
                    == shard_index
            })
            .map(|scenario_id| {
                TierCScenarioObservation::try_new((*scenario_id).to_string(), outcome.clone())
                    .expect("test observation should construct")
            })
            .collect()
    }

    fn one_scenario_per_shard() -> Vec<String> {
        (0..SQL_SCHEDULED_SHARD_COUNT)
            .map(|shard_index| {
                (0_u32..10_000)
                    .map(|candidate| format!("scenario.shard_{shard_index}.{candidate}"))
                    .find(|scenario_id| {
                        scheduled_sql_scenario_shard(scenario_id)
                            .expect("test scenario ID should shard")
                            == shard_index
                    })
                    .expect("each fixed shard should receive a finite test identity")
            })
            .collect()
    }
}
