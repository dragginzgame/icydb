//! Module: sql_generator::error
//! Responsibility: stable failure taxonomy for generator, replay, and shrink operations.
//! Does not own: product SQL failures or reference-adapter failures.
//! Boundary: preserves typed harness causes without matching rendered error strings.

use std::{
    error::Error,
    fmt::{self, Display},
};

///
/// SqlGeneratorErrorKind
///
/// Stable class for one test-generator infrastructure or contract failure.
/// Owned by the SQL generator and consumed by required correctness lanes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlGeneratorErrorKind {
    /// A configured artifact, fixture, expression, or evaluation budget was exceeded.
    Budget,

    /// Canonical replay JSON was malformed, non-canonical, or used an unknown version.
    CanonicalReplay,

    /// A typed expression or SELECT case violated the current generator contract.
    InvalidCase,

    /// Accepted-snapshot test material was incomplete, ambiguous, or inconsistent.
    InvalidSnapshot,

    /// A zero bound, zero total weight, or overflowing weight sum was requested.
    RandomChoice,

    /// Current-contract SQL rendering could not represent a typed case.
    Rendering,

    /// Canonical serialization or fingerprinting failed.
    Serialization,
}

///
/// SqlGeneratorError
///
/// Typed generator failure with stable classification, contextual detail, and
/// an optional original serialization cause.
///

#[derive(Debug)]
pub struct SqlGeneratorError {
    kind: SqlGeneratorErrorKind,
    detail: String,
    source: Option<serde_json::Error>,
}

impl SqlGeneratorError {
    pub(crate) fn new(kind: SqlGeneratorErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            source: None,
        }
    }

    pub(crate) fn with_json_source(
        kind: SqlGeneratorErrorKind,
        detail: impl Into<String>,
        source: serde_json::Error,
    ) -> Self {
        Self {
            kind,
            detail: detail.into(),
            source: Some(source),
        }
    }

    /// Return the stable generator failure class.
    #[must_use]
    pub const fn kind(&self) -> SqlGeneratorErrorKind {
        self.kind
    }

    /// Borrow the contextual failure detail.
    #[must_use]
    pub const fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

impl Display for SqlGeneratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}

impl Error for SqlGeneratorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &dyn Error)
    }
}
