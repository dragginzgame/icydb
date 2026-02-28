use crate::{db::codec::cursor::CursorDecodeError, value::Value};
use thiserror::Error as ThisError;

///
/// ContinuationSignature
///
/// Stable, deterministic hash of continuation-relevant plan semantics.
/// Excludes windowing state (`limit`, `offset`) and cursor boundaries.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContinuationSignature([u8; 32]);

impl ContinuationSignature {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn as_hex(&self) -> String {
        crate::db::codec::cursor::encode_cursor(&self.0)
    }
}

impl std::fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

///
/// CursorPlanError
///
/// Cursor token and continuation boundary validation failures.
///

#[derive(Debug, ThisError)]
pub enum CursorPlanError {
    /// Cursor token could not be decoded.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursor { reason: CursorDecodeError },

    /// Cursor token payload/semantics are invalid after token decode.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursorPayload { reason: String },

    /// Cursor token version is unsupported.
    #[error("unsupported continuation cursor version: {version}")]
    ContinuationCursorVersionMismatch { version: u8 },

    /// Cursor token does not belong to this canonical query shape.
    #[error(
        "continuation cursor does not match query plan signature for '{entity_path}': expected={expected}, actual={actual}"
    )]
    ContinuationCursorSignatureMismatch {
        entity_path: &'static str,
        expected: String,
        actual: String,
    },

    /// Cursor boundary width does not match canonical order width.
    #[error("continuation cursor boundary arity mismatch: expected {expected}, found {found}")]
    ContinuationCursorBoundaryArityMismatch { expected: usize, found: usize },

    /// Cursor window offset does not match the current query window shape.
    #[error(
        "continuation cursor offset mismatch: expected {expected_offset}, found {actual_offset}"
    )]
    ContinuationCursorWindowMismatch {
        expected_offset: u32,
        actual_offset: u32,
    },

    /// Cursor boundary value type mismatch for a non-primary-key ordered field.
    #[error(
        "continuation cursor boundary type mismatch for field '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorBoundaryTypeMismatch {
        field: String,
        expected: String,
        value: Value,
    },

    /// Cursor primary-key boundary does not match the entity key type.
    #[error(
        "continuation cursor primary key type mismatch for '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorPrimaryKeyTypeMismatch {
        field: String,
        expected: String,
        value: Option<Value>,
    },
}
