//! Module: sql_generator::mutation::replay
//! Responsibility: structured mutation mismatch identity and canonical bounded replay.
//! Does not own: mismatch discovery, provider execution, or shrink candidate policy.
//! Boundary: serializes only the current generator format and rejects stale or non-canonical input.

use crate::{
    GeneratedMutationSequence, SqlGeneratorError, SqlGeneratorErrorKind,
    replay::canonical_json_bytes,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Current hard-cut canonical mutation replay format.
pub const MUTATION_REPLAY_FORMAT_VERSION: u32 = 2;

/// Domain separator for canonical mutation row-set fingerprints.
const MUTATION_ROWS_FINGERPRINT_DOMAIN: &[u8] = b"icydb-sql-mutation-rows/v1";

///
/// MutationFeature
///
/// Stable current-contract DML feature participating in mismatch identity and coverage.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFeature {
    /// DELETE statement.
    Delete,

    /// INSERT statement.
    Insert,

    /// INSERT sourced from a query over current state.
    InsertFromQuery,

    /// Multi-row INSERT staging.
    MultiRowInsert,

    /// Typed expected rejection and atomicity check.
    Rejection,

    /// Full-row RETURNING projection.
    Returning,

    /// UPDATE statement.
    Update,

    /// Ordered LIMIT/OFFSET mutation selection.
    Window,
}

///
/// MutationExecutionPhase
///
/// Stable phase in which a mutation differential or invariant failure occurred.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationExecutionPhase {
    /// Typed post-execution comparison.
    Comparison,

    /// Product planning or execution.
    Execution,

    /// Independent provider setup or execution.
    Reference,
}

///
/// MutationMismatchCategory
///
/// Stable failure class that mutation shrinking must preserve exactly.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationMismatchCategory {
    /// Affected-row counts differ.
    AffectedRows,

    /// A rejected statement partially changed state.
    Atomicity,

    /// Providers disagree about acceptance.
    Acceptance,

    /// A provider exposed an internal setup or execution invariant failure.
    InternalInvariant,

    /// Complete post-state differs.
    PostState,

    /// RETURNING rows differ after contractual normalization.
    ReturnedRows,

    /// Stable typed rejection classes differ.
    TypedError,
}

///
/// MutationMismatchSignature
///
/// Structured provider-independent mutation failure identity preserved by shrinking.
/// Diagnostic values and rendered error messages are excluded deliberately.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationMismatchSignature {
    features: BTreeSet<MutationFeature>,
    phase: MutationExecutionPhase,
    subject_provider_id: String,
    comparison_provider_id: String,
    error_class_id: Option<String>,
    category: MutationMismatchCategory,
    invariant_class_id: Option<String>,
}

impl MutationMismatchSignature {
    /// Build one stable mutation mismatch identity.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error when a required or optional identifier is empty.
    pub fn try_new(
        features: BTreeSet<MutationFeature>,
        phase: MutationExecutionPhase,
        subject_provider_id: impl Into<String>,
        comparison_provider_id: impl Into<String>,
        error_class_id: Option<String>,
        category: MutationMismatchCategory,
        invariant_class_id: Option<String>,
    ) -> Result<Self, SqlGeneratorError> {
        let signature = Self {
            features,
            phase,
            subject_provider_id: subject_provider_id.into(),
            comparison_provider_id: comparison_provider_id.into(),
            error_class_id,
            category,
            invariant_class_id,
        };
        signature.validate()?;

        Ok(signature)
    }

    /// Borrow contract features participating in mismatch identity.
    #[must_use]
    pub const fn features(&self) -> &BTreeSet<MutationFeature> {
        &self.features
    }

    /// Return the failure phase.
    #[must_use]
    pub const fn phase(&self) -> MutationExecutionPhase {
        self.phase
    }

    /// Borrow subject-provider identity.
    #[must_use]
    pub const fn subject_provider_id(&self) -> &str {
        self.subject_provider_id.as_str()
    }

    /// Borrow comparison-provider identity.
    #[must_use]
    pub const fn comparison_provider_id(&self) -> &str {
        self.comparison_provider_id.as_str()
    }

    /// Borrow a stable typed error class, when relevant.
    #[must_use]
    pub fn error_class_id(&self) -> Option<&str> {
        self.error_class_id.as_deref()
    }

    /// Return the stable mismatch category.
    #[must_use]
    pub const fn category(&self) -> MutationMismatchCategory {
        self.category
    }

    /// Borrow an atomicity or boundary invariant class, when relevant.
    #[must_use]
    pub fn invariant_class_id(&self) -> Option<&str> {
        self.invariant_class_id.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.features.is_empty()
            || self.subject_provider_id.is_empty()
            || self.comparison_provider_id.is_empty()
            || self.error_class_id.as_deref().is_some_and(str::is_empty)
            || self
                .invariant_class_id
                .as_deref()
                .is_some_and(str::is_empty)
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "mutation mismatch requires features and non-empty provider/class identifiers",
            ));
        }

        Ok(())
    }
}

///
/// MutationObservedOutcome
///
/// Compact typed provider outcome embedded in mutation failure replay.
/// Result material is represented by canonical fingerprints rather than display strings.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MutationObservedOutcome {
    /// Provider completed one accepted statement.
    Accepted {
        /// Number of affected rows.
        affected_rows: u32,

        /// Canonical fingerprint of normalized RETURNING rows.
        returned_rows_fingerprint: String,

        /// Canonical fingerprint of complete post-state.
        state_fingerprint: String,
    },

    /// Provider failed outside the typed mutation contract.
    InfrastructureFailure {
        /// Stable harness-owned failure class.
        failure_class_id: String,

        /// Phase at which the provider became unavailable.
        phase: MutationExecutionPhase,
    },

    /// Provider returned a stable typed mutation rejection.
    Rejected {
        /// Stable product- or adapter-owned error class.
        error_class_id: String,

        /// Canonical fingerprint proving the observed unchanged post-state.
        state_fingerprint: String,
    },
}

impl MutationObservedOutcome {
    /// Build one compact accepted provider outcome.
    #[must_use]
    pub fn accepted(
        affected_rows: u32,
        returned_rows_fingerprint: impl Into<String>,
        state_fingerprint: impl Into<String>,
    ) -> Self {
        Self::Accepted {
            affected_rows,
            returned_rows_fingerprint: returned_rows_fingerprint.into(),
            state_fingerprint: state_fingerprint.into(),
        }
    }

    /// Build one compact rejected provider outcome.
    #[must_use]
    pub fn rejected(
        error_class_id: impl Into<String>,
        state_fingerprint: impl Into<String>,
    ) -> Self {
        Self::Rejected {
            error_class_id: error_class_id.into(),
            state_fingerprint: state_fingerprint.into(),
        }
    }

    /// Build accepted replay evidence directly from normalized typed rows.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical row fingerprinting fails.
    pub fn try_accepted_with_rows(
        affected_rows: u32,
        returned_rows: &[crate::MutationRow],
        state_after: &[crate::MutationRow],
    ) -> Result<Self, SqlGeneratorError> {
        Ok(Self::accepted(
            affected_rows,
            mutation_rows_fingerprint(b"returning", returned_rows)?,
            mutation_rows_fingerprint(b"state", state_after)?,
        ))
    }

    /// Build rejected replay evidence from one typed error class and normalized state.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical state fingerprinting fails.
    pub fn try_rejected_with_state(
        error_class_id: impl Into<String>,
        state_after: &[crate::MutationRow],
    ) -> Result<Self, SqlGeneratorError> {
        Ok(Self::rejected(
            error_class_id,
            mutation_rows_fingerprint(b"state", state_after)?,
        ))
    }

    /// Build one provider-infrastructure failure.
    #[must_use]
    pub fn infrastructure_failure(
        failure_class_id: impl Into<String>,
        phase: MutationExecutionPhase,
    ) -> Self {
        Self::InfrastructureFailure {
            failure_class_id: failure_class_id.into(),
            phase,
        }
    }

    /// Project one typed model or adapter step outcome into compact replay evidence.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical row fingerprinting fails.
    pub fn try_from_step_outcome(
        outcome: &crate::MutationStepOutcome,
    ) -> Result<Self, SqlGeneratorError> {
        match outcome {
            crate::MutationStepOutcome::Accepted {
                affected_rows,
                returned_rows,
                state_after,
            } => Ok(Self::accepted(
                *affected_rows,
                mutation_rows_fingerprint(b"returning", returned_rows)?,
                mutation_rows_fingerprint(b"state", state_after)?,
            )),
            crate::MutationStepOutcome::Rejected {
                rejection,
                state_after,
            } => Ok(Self::rejected(
                rejection.id(),
                mutation_rows_fingerprint(b"state", state_after)?,
            )),
        }
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        let valid = match self {
            Self::Accepted {
                returned_rows_fingerprint,
                state_fingerprint,
                ..
            } => !returned_rows_fingerprint.is_empty() && !state_fingerprint.is_empty(),
            Self::InfrastructureFailure {
                failure_class_id, ..
            } => !failure_class_id.is_empty(),
            Self::Rejected {
                error_class_id,
                state_fingerprint,
            } => !error_class_id.is_empty() && !state_fingerprint.is_empty(),
        };
        if !valid {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "mutation observed outcome identifiers and fingerprints must be non-empty",
            ));
        }

        Ok(())
    }
}

fn mutation_rows_fingerprint(
    role: &[u8],
    rows: &[crate::MutationRow],
) -> Result<String, SqlGeneratorError> {
    let bytes = canonical_json_bytes(rows)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(MUTATION_ROWS_FINGERPRINT_DOMAIN);
    hasher.update(role);
    hasher.update(&bytes);
    Ok(format!("blake3.{}", hasher.finalize().to_hex()))
}

///
/// MutationReplayRecord
///
/// Current-version bounded failure unit containing the original and smallest
/// signature-preserving mutation sequences plus typed provider outcomes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationReplayRecord {
    format_version: u32,
    original_sequence: GeneratedMutationSequence,
    minimized_sequence: GeneratedMutationSequence,
    snapshot_fingerprint: String,
    original_sequence_fingerprint: String,
    signature: MutationMismatchSignature,
    subject_outcome: MutationObservedOutcome,
    comparison_outcome: MutationObservedOutcome,
    minimization_complete: bool,
    shrink_candidates_attempted: u32,
    evaluations: u32,
}

impl MutationReplayRecord {
    /// Build and validate one current-version mutation replay record.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error when sequence authority, typed outcomes, or budgets disagree.
    #[expect(
        clippy::too_many_arguments,
        reason = "replay construction keeps both outcomes and shrink accounting explicit"
    )]
    pub fn try_new(
        original_sequence: GeneratedMutationSequence,
        minimized_sequence: GeneratedMutationSequence,
        signature: MutationMismatchSignature,
        subject_outcome: MutationObservedOutcome,
        comparison_outcome: MutationObservedOutcome,
        minimization_complete: bool,
        shrink_candidates_attempted: u32,
        evaluations: u32,
    ) -> Result<Self, SqlGeneratorError> {
        let snapshot_fingerprint = original_sequence.snapshot().fingerprint()?;
        let original_sequence_fingerprint = original_sequence.fingerprint()?;
        let record = Self {
            format_version: MUTATION_REPLAY_FORMAT_VERSION,
            original_sequence,
            minimized_sequence,
            snapshot_fingerprint,
            original_sequence_fingerprint,
            signature,
            subject_outcome,
            comparison_outcome,
            minimization_complete,
            shrink_candidates_attempted,
            evaluations,
        };
        record.validate()?;

        Ok(record)
    }

    /// Return the current mutation replay format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Borrow the original failing sequence.
    #[must_use]
    pub const fn original_sequence(&self) -> &GeneratedMutationSequence {
        &self.original_sequence
    }

    /// Borrow the smallest signature-preserving sequence found.
    #[must_use]
    pub const fn minimized_sequence(&self) -> &GeneratedMutationSequence {
        &self.minimized_sequence
    }

    /// Borrow the preserved mismatch signature.
    #[must_use]
    pub const fn signature(&self) -> &MutationMismatchSignature {
        &self.signature
    }

    /// Borrow the compact subject outcome recorded for the minimized failure.
    #[must_use]
    pub const fn subject_outcome(&self) -> &MutationObservedOutcome {
        &self.subject_outcome
    }

    /// Borrow the compact comparison-provider outcome recorded for the minimized failure.
    #[must_use]
    pub const fn comparison_outcome(&self) -> &MutationObservedOutcome {
        &self.comparison_outcome
    }

    /// Return whether deterministic minimization reached a fixed point.
    #[must_use]
    pub const fn minimization_complete(&self) -> bool {
        self.minimization_complete
    }

    /// Return attempted shrink candidates.
    #[must_use]
    pub const fn shrink_candidates_attempted(&self) -> u32 {
        self.shrink_candidates_attempted
    }

    /// Return complete subject-plus-provider evaluations.
    #[must_use]
    pub const fn evaluations(&self) -> u32 {
        self.evaluations
    }

    /// Serialize this record as bounded current-version canonical JSON.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error for invalid state, serialization failure, or artifact overflow.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SqlGeneratorError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self)?;
        let byte_count = u32::try_from(bytes.len()).map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "canonical mutation replay exceeds u32 byte accounting",
            )
        })?;
        if byte_count > self.original_sequence.budgets().max_artifact_bytes() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "canonical mutation replay has {byte_count} bytes, exceeding its {}-byte budget",
                    self.original_sequence.budgets().max_artifact_bytes(),
                ),
            ));
        }

        Ok(bytes)
    }

    /// Decode exactly one canonical current-version mutation replay record.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error for malformed, stale, non-canonical, or inconsistent input.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, SqlGeneratorError> {
        let record = serde_json::from_slice::<Self>(bytes).map_err(|source| {
            SqlGeneratorError::with_json_source(
                SqlGeneratorErrorKind::CanonicalReplay,
                "failed to decode canonical mutation replay",
                source,
            )
        })?;
        if record.format_version != MUTATION_REPLAY_FORMAT_VERSION {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                format!(
                    "mutation replay version {} is unsupported; current version is {MUTATION_REPLAY_FORMAT_VERSION}",
                    record.format_version,
                ),
            ));
        }
        record.validate()?;
        if record.to_canonical_json()? != bytes {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "mutation replay input is not canonical JSON",
            ));
        }

        Ok(record)
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.format_version != MUTATION_REPLAY_FORMAT_VERSION {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "mutation replay does not use the current hard-cut format",
            ));
        }
        self.original_sequence.validate()?;
        self.minimized_sequence.validate()?;
        self.signature.validate()?;
        self.subject_outcome.validate()?;
        self.comparison_outcome.validate()?;
        if self.original_sequence.identity() != self.minimized_sequence.identity()
            || self.original_sequence.snapshot() != self.minimized_sequence.snapshot()
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "minimized mutation replay changed sequence identity or accepted-snapshot authority",
            ));
        }
        if self.snapshot_fingerprint != self.original_sequence.snapshot().fingerprint()?
            || self.original_sequence_fingerprint != self.original_sequence.fingerprint()?
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "mutation replay fingerprint does not match embedded original material",
            ));
        }
        let budgets = self.original_sequence.budgets();
        if self.shrink_candidates_attempted > budgets.max_shrink_candidates()
            || self.evaluations > budgets.max_evaluations()
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation replay shrink accounting exceeds its deterministic budget",
            ));
        }

        Ok(())
    }
}
