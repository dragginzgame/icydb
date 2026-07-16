//! Module: sqlite_reference::error
//! Responsibility: typed SQLite adapter and environment failures.
//! Does not own: correctness verdicts or failure-owner policy.
//! Boundary: preserves the underlying SQLite cause while exposing stable error kinds.

use std::{
    error::Error,
    fmt::{self, Display},
};

///
/// SqliteAdapterErrorKind
///
/// Stable failure class emitted by the test-only SQLite reference adapter.
/// Correctness runners use this type instead of matching rendered error text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteAdapterErrorKind {
    /// The bundled in-memory SQLite connection could not be opened.
    Connection,

    /// The bundled SQLite identity or connection policy does not match the contract.
    Environment,

    /// The reference fixture could not be created or populated.
    Fixture,

    /// A typed generated case violated the reference-adapter boundary.
    GeneratedCase,

    /// A requested fixture entity name is not a safe SQL identifier.
    Identifier,

    /// Generated mutation setup, execution, rejection, or state extraction failed.
    Mutation,

    /// A reference statement could not be prepared or executed.
    Query,

    /// A reference result could not be read or did not match its declared shape.
    Result,

    /// A reference transaction could not be started or committed.
    Transaction,
}

///
/// SqliteAdapterError
///
/// Typed SQLite reference failure with human-readable context and an optional
/// original `rusqlite` cause. The adapter owns this error; correctness runners
/// decide whether its kind represents adapter or infrastructure ownership.
///

#[derive(Debug)]
pub struct SqliteAdapterError {
    kind: SqliteAdapterErrorKind,
    detail: String,
    source: Option<rusqlite::Error>,
}

impl SqliteAdapterError {
    pub(crate) fn new(kind: SqliteAdapterErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            source: None,
        }
    }

    pub(crate) fn with_source(
        kind: SqliteAdapterErrorKind,
        detail: impl Into<String>,
        source: rusqlite::Error,
    ) -> Self {
        Self {
            kind,
            detail: detail.into(),
            source: Some(source),
        }
    }

    /// Return the stable adapter failure class.
    #[must_use]
    pub const fn kind(&self) -> SqliteAdapterErrorKind {
        self.kind
    }

    /// Borrow the human-readable failure context.
    #[must_use]
    pub const fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

impl Error for SqliteAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &dyn Error)
    }
}

impl Display for SqliteAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}
