//! Module: sql_generator::replay
//! Responsibility: structured mismatch identity and canonical bounded failure replay.
//! Does not own: mismatch discovery, product execution, or shrink candidate policy.
//! Boundary: serializes the current generator format only and rejects stale/non-canonical input.

use crate::{GeneratedSelectCase, SelectFeature, SqlGeneratorError, SqlGeneratorErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// Current hard-cut canonical SELECT replay format.
pub const SELECT_REPLAY_FORMAT_VERSION: u32 = 1;

///
/// SelectExecutionPhase
///
/// Stable phase in which a generated differential or invariant failure occurred.
/// Owned by replay identity rather than inferred from product error text.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectExecutionPhase {
    /// Public or trusted SQL admission.
    Admission,

    /// Typed result comparison after both providers completed.
    Comparison,

    /// Product plan execution and result construction.
    Execution,

    /// SQL parsing and frontend validation.
    Parsing,

    /// Semantic lowering and plan construction.
    Planning,

    /// Independent reference-provider setup or execution.
    Reference,
}

///
/// SelectMismatchCategory
///
/// Stable mismatch class preserved by shrinking. Diagnostic values and raw
/// messages are deliberately excluded from this identity.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectMismatchCategory {
    /// Providers disagree about acceptance or rejection.
    Acceptance,

    /// A required semantic boundary was violated.
    Boundary,

    /// Duplicate row counts differ after typed normalization.
    DuplicateMultiplicity,

    /// A provider exposed an internal invariant failure.
    InternalInvariant,

    /// Row ordering differs where order is contractually defined.
    Ordering,

    /// A provider panicked or trapped.
    PanicOrTrap,

    /// Selected execution-route facts disagree with the declared invariant.
    Route,

    /// Result row or column shape differs.
    RowShape,

    /// A provider exceeded the declared execution timeout.
    Timeout,

    /// Providers rejected with different stable typed error classes.
    TypedError,

    /// Typed scalar values differ.
    Value,
}

///
/// SelectComparisonProvider
///
/// Exact second provider represented by one SELECT mismatch replay. This is
/// distinct from the generated case's required evidence provider because an
/// internal cold-versus-warm invariant can fail before external comparison.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectComparisonProvider {
    /// Warm IcyDB execution compared with the cold subject execution.
    IcydbWarm,

    /// Typed rejection contract attached before invalid SQL rendering.
    RejectionInvariant,

    /// Independently executed bundled SQLite result.
    SqliteReference,
}

///
/// SelectMismatchSignature
///
/// Structured failure identity that a shrink candidate must preserve exactly.
/// Provider and typed-error identifiers are stable IDs, never display strings.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectMismatchSignature {
    features: BTreeSet<SelectFeature>,
    phase: SelectExecutionPhase,
    subject_provider_id: String,
    comparison_provider: SelectComparisonProvider,
    error_class_id: Option<String>,
    category: SelectMismatchCategory,
    invariant_class_id: Option<String>,
}

impl SelectMismatchSignature {
    /// Build one structured mismatch identity.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error when a required provider or optional class
    /// identifier is empty.
    pub fn try_new(
        features: BTreeSet<SelectFeature>,
        phase: SelectExecutionPhase,
        subject_provider_id: impl Into<String>,
        comparison_provider: SelectComparisonProvider,
        error_class_id: Option<String>,
        category: SelectMismatchCategory,
        invariant_class_id: Option<String>,
    ) -> Result<Self, SqlGeneratorError> {
        let signature = Self {
            features,
            phase,
            subject_provider_id: subject_provider_id.into(),
            comparison_provider,
            error_class_id,
            category,
            invariant_class_id,
        };
        signature.validate()?;

        Ok(signature)
    }

    /// Borrow contract features that participate in mismatch identity.
    #[must_use]
    pub const fn features(&self) -> &BTreeSet<SelectFeature> {
        &self.features
    }

    /// Return the stable execution phase.
    #[must_use]
    pub const fn phase(&self) -> SelectExecutionPhase {
        self.phase
    }

    /// Borrow the stable subject-provider identity.
    #[must_use]
    pub const fn subject_provider_id(&self) -> &str {
        self.subject_provider_id.as_str()
    }

    /// Return the comparison provider.
    #[must_use]
    pub const fn comparison_provider(&self) -> SelectComparisonProvider {
        self.comparison_provider
    }

    /// Borrow the stable typed error class, when relevant.
    #[must_use]
    pub fn error_class_id(&self) -> Option<&str> {
        self.error_class_id.as_deref()
    }

    /// Return the mismatch category.
    #[must_use]
    pub const fn category(&self) -> SelectMismatchCategory {
        self.category
    }

    /// Borrow a route or boundary invariant class, when relevant.
    #[must_use]
    pub fn invariant_class_id(&self) -> Option<&str> {
        self.invariant_class_id.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.subject_provider_id.is_empty()
            || self.error_class_id.as_deref().is_some_and(str::is_empty)
            || self
                .invariant_class_id
                .as_deref()
                .is_some_and(str::is_empty)
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "SELECT mismatch identifiers must be non-empty",
            ));
        }

        Ok(())
    }
}

///
/// SelectObservedOutcome
///
/// Compact typed provider outcome embedded in failure replay. Result payloads
/// are represented by canonical fingerprints rather than display-formatted rows.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SelectObservedOutcome {
    /// Provider completed with a typed normalized result.
    Accepted {
        /// BLAKE3 fingerprint of canonical typed rows.
        result_fingerprint: String,

        /// Number of result rows before fingerprinting.
        row_count: u32,
    },

    /// Provider failed outside the product's typed rejection contract.
    InfrastructureFailure {
        /// Stable harness-owned infrastructure class.
        failure_class_id: String,

        /// Phase at which the provider became unavailable.
        phase: SelectExecutionPhase,
    },

    /// Provider returned a stable typed rejection.
    Rejected {
        /// Stable product- or adapter-owned error class.
        error_class_id: String,

        /// Phase that authoritatively produced the rejection.
        phase: SelectExecutionPhase,
    },
}

impl SelectObservedOutcome {
    /// Build a compact accepted outcome.
    #[must_use]
    pub fn accepted(result_fingerprint: impl Into<String>, row_count: u32) -> Self {
        Self::Accepted {
            result_fingerprint: result_fingerprint.into(),
            row_count,
        }
    }

    /// Build a stable typed rejection outcome.
    #[must_use]
    pub fn rejected(error_class_id: impl Into<String>, phase: SelectExecutionPhase) -> Self {
        Self::Rejected {
            error_class_id: error_class_id.into(),
            phase,
        }
    }

    /// Build a stable infrastructure-failure outcome.
    #[must_use]
    pub fn infrastructure_failure(
        failure_class_id: impl Into<String>,
        phase: SelectExecutionPhase,
    ) -> Self {
        Self::InfrastructureFailure {
            failure_class_id: failure_class_id.into(),
            phase,
        }
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        let identifier = match self {
            Self::Accepted {
                result_fingerprint, ..
            } => result_fingerprint,
            Self::InfrastructureFailure {
                failure_class_id, ..
            } => failure_class_id,
            Self::Rejected { error_class_id, .. } => error_class_id,
        };
        if identifier.is_empty() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "SELECT observed outcome identifier must be non-empty",
            ));
        }

        Ok(())
    }
}

///
/// SelectReplayRecord
///
/// Current-version bounded failure unit containing the original generated case,
/// smallest signature-preserving case, typed outcomes, and shrink accounting.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectReplayRecord {
    format_version: u32,
    original_case: GeneratedSelectCase,
    minimized_case: GeneratedSelectCase,
    snapshot_fingerprint: String,
    fixture_fingerprint: String,
    signature: SelectMismatchSignature,
    subject_outcome: SelectObservedOutcome,
    comparison_outcome: SelectObservedOutcome,
    minimization_complete: bool,
    shrink_candidates_attempted: u32,
    evaluations: u32,
}

impl SelectReplayRecord {
    /// Build and validate one current-version replay record.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error if either case is invalid, the minimized
    /// case changes stable identity/authority, or deterministic budgets are exceeded.
    #[expect(
        clippy::too_many_arguments,
        reason = "replay construction keeps both outcomes and shrink accounting explicit"
    )]
    pub fn try_new(
        original_case: GeneratedSelectCase,
        minimized_case: GeneratedSelectCase,
        signature: SelectMismatchSignature,
        subject_outcome: SelectObservedOutcome,
        comparison_outcome: SelectObservedOutcome,
        minimization_complete: bool,
        shrink_candidates_attempted: u32,
        evaluations: u32,
    ) -> Result<Self, SqlGeneratorError> {
        let snapshot_fingerprint = original_case.snapshot().fingerprint()?;
        let fixture_fingerprint = original_case.fixture().fingerprint()?;
        let record = Self {
            format_version: SELECT_REPLAY_FORMAT_VERSION,
            original_case,
            minimized_case,
            snapshot_fingerprint,
            fixture_fingerprint,
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

    /// Return the current replay format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Borrow the original failing generated case.
    #[must_use]
    pub const fn original_case(&self) -> &GeneratedSelectCase {
        &self.original_case
    }

    /// Borrow the smallest signature-preserving case found.
    #[must_use]
    pub const fn minimized_case(&self) -> &GeneratedSelectCase {
        &self.minimized_case
    }

    /// Borrow the preserved mismatch signature.
    #[must_use]
    pub const fn signature(&self) -> &SelectMismatchSignature {
        &self.signature
    }

    /// Borrow the compact subject outcome recorded for the minimized failure.
    #[must_use]
    pub const fn subject_outcome(&self) -> &SelectObservedOutcome {
        &self.subject_outcome
    }

    /// Borrow the compact comparison-provider outcome recorded for the minimized failure.
    #[must_use]
    pub const fn comparison_outcome(&self) -> &SelectObservedOutcome {
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
    /// Returns a typed replay error when validation, serialization, or the
    /// record's artifact-size budget fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SqlGeneratorError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self)?;
        let byte_count = u32::try_from(bytes.len()).map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "canonical SELECT replay exceeds u32 byte accounting",
            )
        })?;
        if byte_count > self.original_case.budgets().max_artifact_bytes() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "canonical SELECT replay has {byte_count} bytes, exceeding its {}-byte budget",
                    self.original_case.budgets().max_artifact_bytes(),
                ),
            ));
        }

        Ok(bytes)
    }

    /// Decode exactly one canonical current-version replay record.
    ///
    /// # Errors
    ///
    /// Returns a typed replay error for malformed JSON, unknown versions,
    /// non-canonical bytes, stale fingerprints, invalid cases, or exceeded bounds.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, SqlGeneratorError> {
        let record = serde_json::from_slice::<Self>(bytes).map_err(|source| {
            SqlGeneratorError::with_json_source(
                SqlGeneratorErrorKind::CanonicalReplay,
                "failed to decode canonical SELECT replay",
                source,
            )
        })?;
        if record.format_version != SELECT_REPLAY_FORMAT_VERSION {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                format!(
                    "SELECT replay version {} is unsupported; current version is {SELECT_REPLAY_FORMAT_VERSION}",
                    record.format_version,
                ),
            ));
        }
        record.validate()?;
        let canonical = record.to_canonical_json()?;
        if canonical != bytes {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "SELECT replay input is not RFC 8785 canonical JSON",
            ));
        }

        Ok(record)
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.format_version != SELECT_REPLAY_FORMAT_VERSION {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "SELECT replay does not use the current hard-cut format",
            ));
        }
        self.original_case.validate()?;
        self.minimized_case.validate()?;
        self.signature.validate()?;
        self.subject_outcome.validate()?;
        self.comparison_outcome.validate()?;
        if self.original_case.identity() != self.minimized_case.identity()
            || self.original_case.snapshot() != self.minimized_case.snapshot()
            || self.original_case.expected() != self.minimized_case.expected()
            || self.original_case.provider() != self.minimized_case.provider()
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "minimized SELECT replay changed case identity, snapshot authority, or evidence intent",
            ));
        }
        if self.snapshot_fingerprint != self.original_case.snapshot().fingerprint()?
            || self.fixture_fingerprint != self.original_case.fixture().fingerprint()?
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalReplay,
                "SELECT replay fingerprint does not match embedded original material",
            ));
        }
        let budgets = self.original_case.budgets();
        if self.shrink_candidates_attempted > budgets.max_shrink_candidates()
            || self.evaluations > budgets.max_evaluations()
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "SELECT replay shrink accounting exceeds its deterministic budget",
            ));
        }

        Ok(())
    }
}

pub(crate) fn canonical_json_bytes<T>(value: &T) -> Result<Vec<u8>, SqlGeneratorError>
where
    T: Serialize + ?Sized,
{
    let value = serde_json::to_value(value).map_err(|source| {
        SqlGeneratorError::with_json_source(
            SqlGeneratorErrorKind::Serialization,
            "failed to materialize canonical SQL JSON",
            source,
        )
    })?;
    let canonical = canonicalize_value(value);
    serde_json::to_vec(&canonical).map_err(|source| {
        SqlGeneratorError::with_json_source(
            SqlGeneratorErrorKind::Serialization,
            "failed to encode canonical SQL JSON",
            source,
        )
    })
}

fn canonicalize_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_value).collect()),
        Value::Object(values) => {
            let mut sorted = values.into_iter().collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            let canonical = sorted
                .into_iter()
                .map(|(key, value)| (key, canonicalize_value(value)))
                .collect::<Map<_, _>>();
            Value::Object(canonical)
        }
        scalar => scalar,
    }
}
