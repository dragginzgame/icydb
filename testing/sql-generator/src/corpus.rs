//! Module: sql_generator::corpus
//! Responsibility: one bounded current-format checked-in SQL regression case.
//! Does not own: failure discovery, mismatch evaluation, or corpus filesystem policy.
//! Boundary: converts complete minimized replay evidence into a current-behavior replay input.

use crate::{
    GeneratedMutationSequence, GeneratedSelectCase, MutationReplayRecord, SelectReplayRecord,
    SqlGeneratorError, SqlGeneratorErrorKind, replay::canonical_json_bytes,
};
use serde::{Deserialize, Serialize};

/// Current hard-cut checked-in regression corpus format.
pub const REGRESSION_CORPUS_FORMAT_VERSION: u32 = 1;

/// Largest current-format corpus entry admitted before JSON decoding.
pub const REGRESSION_CORPUS_MAX_ENTRY_BYTES: usize = 1_048_576;

/// Largest stable regression identifier admitted by the corpus format.
const MAX_REGRESSION_ID_BYTES: usize = 128;

///
/// RegressionCorpusCase
///
/// Minimized current-behavior input executed by the scheduled corpus lane.
/// The generator owns its embedded expected behavior; this wrapper does not
/// preserve the historical failing outcome as an allowlist.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "case", rename_all = "snake_case")]
pub enum RegressionCorpusCase {
    /// One typed mutation sequence compared with its independent state model.
    Mutation(Box<GeneratedMutationSequence>),

    /// One typed SELECT case compared with its declared evidence provider.
    Select(Box<GeneratedSelectCase>),
}

impl RegressionCorpusCase {
    /// Borrow the stable generated scenario or sequence identity.
    #[must_use]
    pub const fn generated_id(&self) -> &str {
        match self {
            Self::Mutation(sequence) => sequence.identity().id(),
            Self::Select(case) => case.identity().id(),
        }
    }

    const fn artifact_byte_limit(&self) -> u32 {
        match self {
            Self::Mutation(sequence) => sequence.budgets().max_artifact_bytes(),
            Self::Select(case) => case.budgets().max_artifact_bytes(),
        }
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        match self {
            Self::Mutation(sequence) => sequence.validate(),
            Self::Select(case) => case.validate(),
        }
    }
}

///
/// RegressionCorpusEntry
///
/// One reviewed minimized SQL regression in the sole current checked-in format.
/// Owned by the test generator and consumed by deterministic replay lanes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegressionCorpusEntry {
    format_version: u32,
    regression_id: String,
    regression_case: RegressionCorpusCase,
}

impl RegressionCorpusEntry {
    /// Convert a completely minimized SELECT failure into a current-behavior corpus input.
    ///
    /// The historical mismatch and observed outcomes are deliberately not retained:
    /// a corpus entry executes the embedded case's current acceptance and provider contract.
    ///
    /// # Errors
    ///
    /// Returns a typed corpus error when minimization is incomplete, the identifier
    /// is invalid, or the minimized case no longer validates.
    pub fn try_from_select_replay(
        regression_id: impl Into<String>,
        replay: &SelectReplayRecord,
    ) -> Result<Self, SqlGeneratorError> {
        if !replay.minimization_complete() {
            return Err(incomplete_minimization_error("SELECT"));
        }
        Self::try_new(
            regression_id,
            RegressionCorpusCase::Select(Box::new(replay.minimized_case().clone())),
        )
    }

    /// Convert a completely minimized mutation failure into a current-behavior corpus input.
    ///
    /// The historical mismatch and observed outcomes are deliberately not retained:
    /// a corpus entry executes the sequence's independently modeled current outcomes.
    ///
    /// # Errors
    ///
    /// Returns a typed corpus error when minimization is incomplete, the identifier
    /// is invalid, or the minimized sequence no longer validates.
    pub fn try_from_mutation_replay(
        regression_id: impl Into<String>,
        replay: &MutationReplayRecord,
    ) -> Result<Self, SqlGeneratorError> {
        if !replay.minimization_complete() {
            return Err(incomplete_minimization_error("mutation"));
        }
        Self::try_new(
            regression_id,
            RegressionCorpusCase::Mutation(Box::new(replay.minimized_sequence().clone())),
        )
    }

    /// Return the sole current corpus format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Borrow the stable reviewed regression identity.
    #[must_use]
    pub const fn regression_id(&self) -> &str {
        self.regression_id.as_str()
    }

    /// Borrow the minimized current-behavior case.
    #[must_use]
    pub const fn regression_case(&self) -> &RegressionCorpusCase {
        &self.regression_case
    }

    /// Serialize this entry as bounded current-version canonical JSON.
    ///
    /// # Errors
    ///
    /// Returns a typed corpus error when validation, canonical serialization, or
    /// the embedded lane's artifact-size bound fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SqlGeneratorError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self)?;
        let byte_count = u32::try_from(bytes.len()).map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "canonical regression corpus entry exceeds u32 byte accounting",
            )
        })?;
        if byte_count > self.regression_case.artifact_byte_limit() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "canonical regression corpus entry has {byte_count} bytes, exceeding its {}-byte budget",
                    self.regression_case.artifact_byte_limit(),
                ),
            ));
        }

        Ok(bytes)
    }

    /// Decode exactly one bounded canonical current-version corpus entry.
    ///
    /// # Errors
    ///
    /// Returns a typed corpus error before decoding oversized input, or for
    /// malformed, stale, non-canonical, or internally inconsistent JSON.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, SqlGeneratorError> {
        if bytes.len() > REGRESSION_CORPUS_MAX_ENTRY_BYTES {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "regression corpus entry has at least {} bytes, exceeding the {REGRESSION_CORPUS_MAX_ENTRY_BYTES}-byte decode bound",
                    bytes.len(),
                ),
            ));
        }
        let entry = serde_json::from_slice::<Self>(bytes).map_err(|source| {
            SqlGeneratorError::with_json_source(
                SqlGeneratorErrorKind::CanonicalCorpus,
                "failed to decode canonical regression corpus entry",
                source,
            )
        })?;
        entry.validate()?;
        if entry.to_canonical_json()? != bytes {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalCorpus,
                "regression corpus entry is not RFC 8785 canonical JSON",
            ));
        }

        Ok(entry)
    }

    fn try_new(
        regression_id: impl Into<String>,
        regression_case: RegressionCorpusCase,
    ) -> Result<Self, SqlGeneratorError> {
        let entry = Self {
            format_version: REGRESSION_CORPUS_FORMAT_VERSION,
            regression_id: regression_id.into(),
            regression_case,
        };
        entry.validate()?;

        Ok(entry)
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.format_version != REGRESSION_CORPUS_FORMAT_VERSION {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::CanonicalCorpus,
                "regression corpus entry does not use the current hard-cut format",
            ));
        }
        validate_regression_id(self.regression_id.as_str())?;
        self.regression_case.validate()
    }
}

/// Enforce the filename-safe stable vocabulary shared by corpus metadata and paths.
fn validate_regression_id(regression_id: &str) -> Result<(), SqlGeneratorError> {
    let valid = !regression_id.is_empty()
        && regression_id.len() <= MAX_REGRESSION_ID_BYTES
        && regression_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
        && regression_id
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase);
    if !valid {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::CanonicalCorpus,
            "regression ID must begin with a lowercase ASCII letter and contain at most 128 lowercase ASCII letters, digits, dots, underscores, or hyphens",
        ));
    }

    Ok(())
}

fn incomplete_minimization_error(case_kind: &str) -> SqlGeneratorError {
    SqlGeneratorError::new(
        SqlGeneratorErrorKind::CanonicalCorpus,
        format!("incomplete {case_kind} minimization cannot enter the reviewed corpus"),
    )
}
