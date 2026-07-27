//! Module: cursor::error
//! Responsibility: cursor-domain typed error taxonomy and invariant construction helpers.
//! Does not own: planner policy derivation or runtime execution routing semantics.
//! Boundary: classifies continuation token/anchor/order/window failures for cursor consumers.

use crate::{
    db::cursor::{ContinuationSignature, CursorDecodeError, TokenWireError},
    error::InternalError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorPayloadErrorCode(u8);

impl CursorPayloadErrorCode {
    pub(crate) const GROUPED_DIRECTION_MISMATCH: Self = Self(2);
    pub(crate) const TOKEN_ENCODE: Self = Self(18);
    pub(crate) const TOKEN_DECODE: Self = Self(19);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorSignaturePrefix(u32);

impl CursorSignaturePrefix {
    #[cfg(test)]
    pub(crate) const UNKNOWN: Self = Self(0);

    pub(crate) const fn from_signature(signature: &ContinuationSignature) -> Self {
        let bytes = (*signature).into_bytes();
        Self(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

///
/// CursorPlanError
///
/// Cursor token and continuation boundary validation failures.
///

#[derive(Debug)]
pub enum CursorPlanError {
    /// Cursor token could not be decoded.
    InvalidContinuationCursor { reason: CursorDecodeError },

    /// Cursor token payload/semantics are invalid after token decode.
    InvalidContinuationCursorPayload {
        reason: CursorPayloadErrorCode,
        index: Option<usize>,
    },

    /// Cursor plan/runtime contract invariants were violated.
    ContinuationCursorInvariantViolation,

    /// Cursor token does not belong to this canonical query shape.
    ContinuationCursorSignatureMismatch {
        expected: CursorSignaturePrefix,
        actual: CursorSignaturePrefix,
    },

    /// Cursor window offset does not match the current query window shape.
    ContinuationCursorWindowMismatch {
        expected_offset: u32,
        actual_offset: u32,
    },
}

impl CursorPlanError {
    /// Return whether this error represents invalid external continuation state.
    #[must_use]
    pub(crate) const fn is_invalid_continuation_cursor(&self) -> bool {
        match self {
            Self::InvalidContinuationCursor { .. }
            | Self::InvalidContinuationCursorPayload { .. }
            | Self::ContinuationCursorSignatureMismatch { .. }
            | Self::ContinuationCursorWindowMismatch { .. } => true,
            Self::ContinuationCursorInvariantViolation => false,
        }
    }

    /// Construct one invalid cursor-token decode error.
    pub(in crate::db) const fn invalid_continuation_cursor(reason: CursorDecodeError) -> Self {
        Self::InvalidContinuationCursor { reason }
    }

    /// Construct the canonical invalid-continuation payload error variant.
    pub(in crate::db) const fn invalid_continuation_cursor_payload(
        reason: CursorPayloadErrorCode,
    ) -> Self {
        Self::InvalidContinuationCursorPayload {
            reason,
            index: None,
        }
    }

    /// Construct one grouped-cursor direction mismatch payload error.
    pub(crate) const fn grouped_continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::GROUPED_DIRECTION_MISMATCH,
        )
    }

    /// Construct one cursor invariant-violation error variant.
    pub(in crate::db) const fn continuation_cursor_invariant() -> Self {
        Self::ContinuationCursorInvariantViolation
    }

    /// Construct one invariant error for empty cursor ORDER BY specifications.
    pub(in crate::db) const fn cursor_requires_non_empty_order() -> Self {
        Self::continuation_cursor_invariant()
    }

    /// Construct one cursor-signature mismatch error for the current entity path.
    pub(in crate::db) const fn continuation_cursor_signature_mismatch(
        _entity_path: &'static str,
        expected: &ContinuationSignature,
        actual: &ContinuationSignature,
    ) -> Self {
        Self::ContinuationCursorSignatureMismatch {
            expected: CursorSignaturePrefix::from_signature(expected),
            actual: CursorSignaturePrefix::from_signature(actual),
        }
    }

    /// Construct one cursor window mismatch error.
    pub(in crate::db) const fn continuation_cursor_window_mismatch(
        expected_offset: u32,
        actual_offset: u32,
    ) -> Self {
        Self::ContinuationCursorWindowMismatch {
            expected_offset,
            actual_offset,
        }
    }

    /// Map cursor token decode failures into canonical plan-surface cursor errors.
    pub(in crate::db) const fn from_token_wire_error(err: TokenWireError) -> Self {
        match err {
            TokenWireError::Encode => {
                Self::invalid_continuation_cursor_payload(CursorPayloadErrorCode::TOKEN_ENCODE)
            }
            TokenWireError::Decode => {
                Self::invalid_continuation_cursor_payload(CursorPayloadErrorCode::TOKEN_DECODE)
            }
        }
    }

    /// Map cursor-plan failures into runtime taxonomy classes.
    ///
    /// Cursor token/version/signature/window/payload mismatches are external
    /// input failures (`Unsupported` at cursor origin). Only explicit
    /// continuation invariant violations remain invariant-class failures.
    pub(crate) fn into_internal_error(self) -> InternalError {
        if self.is_invalid_continuation_cursor() {
            InternalError::cursor_invalid_continuation()
        } else {
            InternalError::cursor_executor_invariant()
        }
    }
}
