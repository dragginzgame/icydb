use crate::{
    db::{
        codec::cursor::CursorDecodeError,
        cursor::{ContinuationSignature, TokenWireError},
    },
    value::Value,
};
use thiserror::Error as ThisError;

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

impl CursorPlanError {
    // Construct one invalid cursor-token decode error.
    pub(in crate::db) const fn invalid_continuation_cursor(reason: CursorDecodeError) -> Self {
        Self::InvalidContinuationCursor { reason }
    }

    // Construct the canonical invalid-continuation payload error variant.
    pub(in crate::db) fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> Self {
        Self::InvalidContinuationCursorPayload {
            reason: reason.into(),
        }
    }

    // Construct one cursor version mismatch error.
    pub(in crate::db) const fn continuation_cursor_version_mismatch(version: u8) -> Self {
        Self::ContinuationCursorVersionMismatch { version }
    }

    // Construct one cursor-signature mismatch error for the current entity path.
    pub(in crate::db) fn continuation_cursor_signature_mismatch(
        entity_path: &'static str,
        expected: &ContinuationSignature,
        actual: &ContinuationSignature,
    ) -> Self {
        Self::ContinuationCursorSignatureMismatch {
            entity_path,
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    // Construct one cursor boundary arity mismatch error.
    pub(in crate::db) const fn continuation_cursor_boundary_arity_mismatch(
        expected: usize,
        found: usize,
    ) -> Self {
        Self::ContinuationCursorBoundaryArityMismatch { expected, found }
    }

    // Construct one cursor window mismatch error.
    pub(in crate::db) const fn continuation_cursor_window_mismatch(
        expected_offset: u32,
        actual_offset: u32,
    ) -> Self {
        Self::ContinuationCursorWindowMismatch {
            expected_offset,
            actual_offset,
        }
    }

    // Construct one non-primary-key boundary type mismatch error.
    pub(in crate::db) fn continuation_cursor_boundary_type_mismatch(
        field: impl Into<String>,
        expected: impl Into<String>,
        value: Value,
    ) -> Self {
        Self::ContinuationCursorBoundaryTypeMismatch {
            field: field.into(),
            expected: expected.into(),
            value,
        }
    }

    // Construct one primary-key boundary type mismatch error.
    pub(in crate::db) fn continuation_cursor_primary_key_type_mismatch(
        field: impl Into<String>,
        expected: impl Into<String>,
        value: Option<Value>,
    ) -> Self {
        Self::ContinuationCursorPrimaryKeyTypeMismatch {
            field: field.into(),
            expected: expected.into(),
            value,
        }
    }

    // Map cursor token decode failures into canonical plan-surface cursor errors.
    pub(in crate::db) fn from_token_wire_error(err: TokenWireError) -> Self {
        match err {
            TokenWireError::Encode(message) | TokenWireError::Decode(message) => {
                Self::invalid_continuation_cursor_payload(message)
            }
            TokenWireError::UnsupportedVersion { version } => {
                Self::continuation_cursor_version_mismatch(version)
            }
        }
    }
}
