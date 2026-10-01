//! Module: sql_generator::failure
//! Responsibility: strict bounded Tier C failure artifacts around minimized replay evidence.
//! Does not own: mismatch discovery, shrinking, execution verdicts, artifact paths, or corpus review.
//! Boundary: gives receipt failure references one deterministic current-format artifact identity.

use crate::{
    MutationReplayRecord, ScenarioShardError, SelectReplayRecord, SqlGeneratorError,
    TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES, replay::canonical_json_bytes, scheduled_sql_scenario_shard,
};

use std::{
    error::Error,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

/// Current hard-cut Tier C minimized-failure artifact format.
pub const TIER_C_FAILURE_ARTIFACT_FORMAT_VERSION: u32 = 1;

/// Domain separator for deterministic Tier C failure-artifact identities.
const TIER_C_FAILURE_ARTIFACT_ID_DOMAIN: &[u8] = b"icydb-sql-tier-c-failure-artifact/v1";

/// Canonical prefix for content-addressed Tier C failure-artifact identities.
const TIER_C_FAILURE_ARTIFACT_ID_PREFIX: &str = "failure.";

/// Exact lowercase hexadecimal BLAKE3 digest character count.
const BLAKE3_HEX_CHARACTER_COUNT: usize = 64;

/// Return whether a receipt reference has the sole current content-addressed form.
///
/// This validates syntax only. Artifact readers must still decode the referenced
/// file and prove that its canonical content derives the same identity.
#[must_use]
pub fn is_valid_tier_c_failure_artifact_id(artifact_id: &str) -> bool {
    let Some(digest) = artifact_id.strip_prefix(TIER_C_FAILURE_ARTIFACT_ID_PREFIX) else {
        return false;
    };

    digest.len() == BLAKE3_HEX_CHARACTER_COUNT
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

///
/// TierCFailureReplay
///
/// Current generated replay family embedded by one Tier C failure artifact.
/// The replay owns mismatch identity, typed outcomes, minimization state, and budgets.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "replay",
    deny_unknown_fields,
    rename_all = "snake_case"
)]
pub enum TierCFailureReplay {
    /// One independently modeled mutation failure replay.
    Mutation(Box<MutationReplayRecord>),

    /// One generated SELECT failure replay.
    Select(Box<SelectReplayRecord>),
}

impl TierCFailureReplay {
    /// Return whether shrinking reached a deterministic fixed point.
    #[must_use]
    pub const fn minimization_complete(&self) -> bool {
        match self {
            Self::Mutation(replay) => replay.minimization_complete(),
            Self::Select(replay) => replay.minimization_complete(),
        }
    }

    fn original_generated_scenario_id(&self) -> &str {
        match self {
            Self::Mutation(replay) => replay.original_sequence().identity().id(),
            Self::Select(replay) => replay.original_case().identity().id(),
        }
    }

    fn validate(&self) -> Result<(), TierCFailureArtifactError> {
        match self {
            Self::Mutation(replay) => replay
                .to_canonical_json()
                .map(|_| ())
                .map_err(TierCFailureArtifactError::Replay),
            Self::Select(replay) => replay
                .to_canonical_json()
                .map(|_| ())
                .map_err(TierCFailureArtifactError::Replay),
        }
    }
}

///
/// TierCFailureArtifact
///
/// Strict scenario-to-replay envelope referenced by a failed Tier C receipt observation.
/// Its content-derived identity prevents a receipt from naming mutable or ambiguous evidence.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TierCFailureArtifact {
    format_version: u32,
    scenario_id: String,
    replay: TierCFailureReplay,
}

impl TierCFailureArtifact {
    /// Wrap one generated SELECT replay for its scheduled scenario identity.
    ///
    /// # Errors
    ///
    /// Returns a typed artifact error when the scenario identity or replay is invalid.
    pub fn try_from_select_replay(
        scenario_id: impl Into<String>,
        replay: SelectReplayRecord,
    ) -> Result<Self, TierCFailureArtifactError> {
        Self::try_new(scenario_id, TierCFailureReplay::Select(Box::new(replay)))
    }

    /// Wrap one generated mutation replay for its scheduled scenario identity.
    ///
    /// # Errors
    ///
    /// Returns a typed artifact error when the scenario identity or replay is invalid.
    pub fn try_from_mutation_replay(
        scenario_id: impl Into<String>,
        replay: MutationReplayRecord,
    ) -> Result<Self, TierCFailureArtifactError> {
        Self::try_new(scenario_id, TierCFailureReplay::Mutation(Box::new(replay)))
    }

    /// Borrow the exact scheduled scenario identity recorded by the shard receipt.
    #[must_use]
    pub const fn scenario_id(&self) -> &str {
        self.scenario_id.as_str()
    }

    /// Borrow the complete current generated replay evidence.
    #[must_use]
    pub const fn replay(&self) -> &TierCFailureReplay {
        &self.replay
    }

    /// Borrow the original generated scenario identity embedded by the replay.
    ///
    /// This equals the scheduled scenario identity for generated matrix cases.
    /// Reviewed corpus scenarios deliberately use a separate `corpus.<review-id>`
    /// identity, so the catalog-aware receipt merge validates that mapping.
    #[must_use]
    pub fn replay_scenario_id(&self) -> &str {
        self.replay.original_generated_scenario_id()
    }

    /// Return whether shrinking reached a deterministic fixed point.
    #[must_use]
    pub const fn minimization_complete(&self) -> bool {
        self.replay.minimization_complete()
    }

    /// Derive the stable receipt reference from canonical artifact content.
    ///
    /// # Errors
    ///
    /// Returns a typed artifact error when validation, encoding, or the byte bound fails.
    pub fn artifact_id(&self) -> Result<String, TierCFailureArtifactError> {
        let bytes = self.to_canonical_json()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(TIER_C_FAILURE_ARTIFACT_ID_DOMAIN);
        hasher.update(
            &u32::try_from(bytes.len())
                .map_err(|_| TierCFailureArtifactError::ArtifactTooLarge {
                    observed_bytes: bytes.len(),
                    maximum_bytes: TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
                })?
                .to_be_bytes(),
        );
        hasher.update(bytes.as_slice());

        Ok(format!(
            "{TIER_C_FAILURE_ARTIFACT_ID_PREFIX}{}",
            hasher.finalize().to_hex()
        ))
    }

    /// Serialize this artifact as bounded canonical current-version JSON.
    ///
    /// # Errors
    ///
    /// Returns a typed artifact error when validation, canonical encoding, or
    /// the fixed one-mebibyte artifact bound fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, TierCFailureArtifactError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self).map_err(TierCFailureArtifactError::Serialization)?;
        validate_artifact_size(bytes.len())?;

        Ok(bytes)
    }

    /// Decode one strict bounded current-version Tier C failure artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed artifact error before oversized input is decoded, or for
    /// malformed, stale, non-canonical, or internally inconsistent evidence.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, TierCFailureArtifactError> {
        validate_artifact_size(bytes.len())?;
        let artifact = serde_json::from_slice::<Self>(bytes)
            .map_err(|source| TierCFailureArtifactError::Decode { source })?;
        artifact.validate()?;
        let canonical =
            canonical_json_bytes(&artifact).map_err(TierCFailureArtifactError::Serialization)?;
        if canonical != bytes {
            return Err(TierCFailureArtifactError::NonCanonicalArtifact);
        }

        Ok(artifact)
    }

    fn try_new(
        scenario_id: impl Into<String>,
        replay: TierCFailureReplay,
    ) -> Result<Self, TierCFailureArtifactError> {
        let artifact = Self {
            format_version: TIER_C_FAILURE_ARTIFACT_FORMAT_VERSION,
            scenario_id: scenario_id.into(),
            replay,
        };
        artifact.validate()?;

        Ok(artifact)
    }

    fn validate(&self) -> Result<(), TierCFailureArtifactError> {
        if self.format_version != TIER_C_FAILURE_ARTIFACT_FORMAT_VERSION {
            return Err(TierCFailureArtifactError::InvalidArtifactVersion {
                expected: TIER_C_FAILURE_ARTIFACT_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        scheduled_sql_scenario_shard(self.scenario_id.as_str()).map_err(|source| {
            TierCFailureArtifactError::InvalidScenarioId {
                scenario_id: self.scenario_id.clone(),
                source,
            }
        })?;
        self.replay.validate()
    }
}

///
/// TierCFailureArtifactError
///
/// Typed construction, encoding, decoding, or validation failure for minimized Tier C evidence.
///

#[derive(Debug)]
pub enum TierCFailureArtifactError {
    /// Input or output exceeded the fixed current artifact byte bound.
    ArtifactTooLarge {
        /// Observed byte count.
        observed_bytes: usize,

        /// Maximum admitted byte count.
        maximum_bytes: usize,
    },

    /// Strict current JSON could not be decoded.
    Decode {
        /// Original JSON decoding cause.
        source: serde_json::Error,
    },

    /// The current failure-artifact format version did not match the artifact.
    InvalidArtifactVersion {
        /// Sole current format version.
        expected: u32,

        /// Decoded artifact version.
        actual: u32,
    },

    /// One artifact used an invalid shared scheduled scenario identity.
    InvalidScenarioId {
        /// Invalid scenario identity.
        scenario_id: String,

        /// Shared shard-contract cause.
        source: ScenarioShardError,
    },

    /// A valid JSON artifact did not use deterministic current encoding.
    NonCanonicalArtifact,

    /// Embedded replay evidence violated its current typed contract.
    Replay(SqlGeneratorError),

    /// Canonical JSON materialization or encoding failed.
    Serialization(SqlGeneratorError),
}

impl Display for TierCFailureArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactTooLarge {
                observed_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "Tier C failure artifact has {observed_bytes} bytes, exceeding the {maximum_bytes}-byte bound",
            ),
            Self::Decode { .. } => formatter.write_str("failed to decode Tier C failure artifact"),
            Self::InvalidArtifactVersion { expected, actual } => write!(
                formatter,
                "Tier C failure artifact version {actual} does not match current version {expected}",
            ),
            Self::InvalidScenarioId {
                scenario_id,
                source,
            } => write!(
                formatter,
                "invalid Tier C failure scenario ID {scenario_id:?}: {source}",
            ),
            Self::NonCanonicalArtifact => {
                formatter.write_str("Tier C failure artifact is not canonical current JSON")
            }
            Self::Replay(source) => write!(
                formatter,
                "Tier C failure artifact contains invalid replay evidence: {source}",
            ),
            Self::Serialization(source) => write!(
                formatter,
                "failed to serialize Tier C failure artifact: {source}",
            ),
        }
    }
}

impl Error for TierCFailureArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::InvalidScenarioId { source, .. } => Some(source),
            Self::Replay(source) | Self::Serialization(source) => Some(source),
            Self::ArtifactTooLarge { .. }
            | Self::InvalidArtifactVersion { .. }
            | Self::NonCanonicalArtifact => None,
        }
    }
}

const fn validate_artifact_size(byte_count: usize) -> Result<(), TierCFailureArtifactError> {
    if byte_count > TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES {
        return Err(TierCFailureArtifactError::ArtifactTooLarge {
            observed_bytes: byte_count,
            maximum_bytes: TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
        });
    }

    Ok(())
}
