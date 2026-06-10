//! Module: cursor::error
//! Responsibility: cursor-domain typed error taxonomy and invariant construction helpers.
//! Does not own: planner policy derivation or runtime execution routing semantics.
//! Boundary: classifies continuation token/anchor/order/window failures for cursor consumers.

use crate::{
    db::cursor::{ContinuationSignature, CursorDecodeError, TokenWireError},
    error::InternalError,
};
use thiserror::Error as ThisError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorPayloadErrorCode(u8);

impl CursorPayloadErrorCode {
    pub(crate) const UNKNOWN: Self = Self(0);
    pub(crate) const DIRECTION_MISMATCH: Self = Self(1);
    pub(crate) const GROUPED_DIRECTION_MISMATCH: Self = Self(2);
    pub(crate) const UNKNOWN_ORDER_FIELD: Self = Self(3);
    pub(crate) const PRIMARY_KEY_TIE_BREAK_REQUIRED: Self = Self(4);
    pub(crate) const INDEX_RANGE_ANCHOR_DECODE_FAILED: Self = Self(5);
    pub(crate) const INDEX_RANGE_ANCHOR_CANONICAL_ENCODING_MISMATCH: Self = Self(6);
    pub(crate) const INDEX_RANGE_ANCHOR_INDEX_ID_MISMATCH: Self = Self(7);
    pub(crate) const INDEX_RANGE_ANCHOR_KEY_NAMESPACE_MISMATCH: Self = Self(8);
    pub(crate) const INDEX_RANGE_ANCHOR_COMPONENT_ARITY_MISMATCH: Self = Self(9);
    pub(crate) const INDEX_RANGE_ANCHOR_OUTSIDE_ENVELOPE: Self = Self(10);
    pub(crate) const UNEXPECTED_INDEX_RANGE_ANCHOR_FOR_COMPOSITE_PLAN: Self = Self(11);
    pub(crate) const INDEX_RANGE_ANCHOR_SEMANTIC_BOUNDS_REQUIRED: Self = Self(12);
    pub(crate) const INDEX_RANGE_ANCHOR_REQUIRED: Self = Self(13);
    pub(crate) const UNEXPECTED_INDEX_RANGE_ANCHOR_FOR_NON_RANGE_PATH: Self = Self(14);
    pub(crate) const INDEX_RANGE_ANCHOR_PRIMARY_KEY_DECODE_FAILED: Self = Self(15);
    #[cfg(test)]
    pub(crate) const BOUNDARY_PRIMARY_KEY_DECODE_FAILED: Self = Self(16);
    pub(crate) const INDEX_RANGE_BOUNDARY_ANCHOR_MISMATCH: Self = Self(17);
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

#[derive(Debug, ThisError)]
pub enum CursorPlanError {
    /// Cursor token could not be decoded.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursor { reason: CursorDecodeError },

    /// Cursor token payload/semantics are invalid after token decode.
    #[error("invalid continuation cursor payload")]
    InvalidContinuationCursorPayload {
        reason: CursorPayloadErrorCode,
        index: Option<usize>,
    },

    /// Cursor plan/runtime contract invariants were violated.
    #[error("continuation cursor invariant violation")]
    ContinuationCursorInvariantViolation,

    /// Cursor token does not belong to this canonical query shape.
    #[error("continuation cursor signature mismatch")]
    ContinuationCursorSignatureMismatch {
        expected: CursorSignaturePrefix,
        actual: CursorSignaturePrefix,
    },

    /// Cursor boundary width does not match canonical order width.
    #[error("continuation cursor boundary arity mismatch")]
    ContinuationCursorBoundaryArityMismatch { expected: usize, found: usize },

    /// Cursor window offset does not match the current query window shape.
    #[error("continuation cursor window mismatch")]
    ContinuationCursorWindowMismatch {
        expected_offset: u32,
        actual_offset: u32,
    },

    /// Cursor boundary value type mismatch for a non-primary-key ordered field.
    #[error("continuation cursor boundary type mismatch")]
    ContinuationCursorBoundaryTypeMismatch { index: usize },

    /// Cursor primary-key boundary does not match the entity key type.
    #[error("continuation cursor primary key type mismatch")]
    ContinuationCursorPrimaryKeyTypeMismatch { index: Option<usize> },
}

impl CursorPlanError {
    /// Canonical policy text for missing cursor ORDER BY requirements.
    pub(in crate::db) const fn cursor_requires_order_message() -> &'static str {
        "cursor pagination requires an explicit ordering"
    }

    /// Canonical policy text for missing cursor LIMIT requirements.
    pub(in crate::db) const fn cursor_requires_limit_message() -> &'static str {
        "cursor pagination requires a limit"
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

    /// Construct one indexed invalid-continuation payload error variant.
    pub(in crate::db) const fn invalid_continuation_cursor_payload_at(
        reason: CursorPayloadErrorCode,
        index: usize,
    ) -> Self {
        Self::InvalidContinuationCursorPayload {
            reason,
            index: Some(index),
        }
    }

    /// Construct one cursor-direction mismatch payload error.
    pub(in crate::db) const fn continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(CursorPayloadErrorCode::DIRECTION_MISMATCH)
    }

    /// Construct one grouped-cursor direction mismatch payload error.
    pub(in crate::db) const fn grouped_continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::GROUPED_DIRECTION_MISMATCH,
        )
    }

    /// Construct one unknown ORDER BY field payload error.
    pub(in crate::db) const fn continuation_cursor_unknown_order_field(_field: &str) -> Self {
        Self::invalid_continuation_cursor_payload(CursorPayloadErrorCode::UNKNOWN_ORDER_FIELD)
    }

    /// Construct one indexed unknown ORDER BY field payload error.
    pub(in crate::db) const fn continuation_cursor_unknown_order_field_at(
        _field: &str,
        index: usize,
    ) -> Self {
        Self::invalid_continuation_cursor_payload_at(
            CursorPayloadErrorCode::UNKNOWN_ORDER_FIELD,
            index,
        )
    }

    /// Construct one deterministic tie-break payload error.
    pub(in crate::db) const fn continuation_cursor_primary_key_tie_break_required(
        _pk_field: &str,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::PRIMARY_KEY_TIE_BREAK_REQUIRED,
        )
    }

    /// Construct one anchor decode failure payload error.
    pub(in crate::db) fn index_range_anchor_decode_failed(_reason: impl Into<String>) -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_DECODE_FAILED,
        )
    }

    /// Construct one canonical-anchor encoding mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_canonical_encoding_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_CANONICAL_ENCODING_MISMATCH,
        )
    }

    /// Construct one anchor index-id mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_index_id_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_INDEX_ID_MISMATCH,
        )
    }

    /// Construct one anchor key-namespace mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_key_namespace_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_KEY_NAMESPACE_MISMATCH,
        )
    }

    /// Construct one anchor component-arity mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_component_arity_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_COMPONENT_ARITY_MISMATCH,
        )
    }

    /// Construct one out-of-envelope anchor payload error.
    pub(in crate::db) const fn index_range_anchor_outside_envelope() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_OUTSIDE_ENVELOPE,
        )
    }

    /// Construct one composite-plan anchor rejection payload error.
    pub(in crate::db) const fn unexpected_index_range_anchor_for_composite_plan() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::UNEXPECTED_INDEX_RANGE_ANCHOR_FOR_COMPOSITE_PLAN,
        )
    }

    /// Construct one missing semantic-bounds payload error.
    pub(in crate::db) const fn index_range_anchor_semantic_bounds_required() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_SEMANTIC_BOUNDS_REQUIRED,
        )
    }

    /// Construct one missing raw anchor payload error.
    pub(in crate::db) const fn index_range_anchor_required() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_REQUIRED,
        )
    }

    /// Construct one non-index-range path anchor rejection payload error.
    pub(in crate::db) const fn unexpected_index_range_anchor_for_non_range_path() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::UNEXPECTED_INDEX_RANGE_ANCHOR_FOR_NON_RANGE_PATH,
        )
    }

    /// Construct one anchor-primary-key decode failure payload error.
    pub(in crate::db) fn index_range_anchor_primary_key_decode_failed(
        _reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_ANCHOR_PRIMARY_KEY_DECODE_FAILED,
        )
    }

    /// Construct one boundary-primary-key decode failure payload error.
    #[cfg(test)]
    pub(in crate::db) fn index_range_boundary_primary_key_decode_failed(
        _reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::BOUNDARY_PRIMARY_KEY_DECODE_FAILED,
        )
    }

    /// Construct one boundary/anchor mismatch payload error.
    pub(in crate::db) const fn index_range_boundary_anchor_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            CursorPayloadErrorCode::INDEX_RANGE_BOUNDARY_ANCHOR_MISMATCH,
        )
    }

    /// Construct one cursor invariant-violation error variant.
    pub(in crate::db) const fn continuation_cursor_invariant() -> Self {
        Self::ContinuationCursorInvariantViolation
    }

    /// Construct one invariant error for missing explicit cursor ordering.
    pub(in crate::db) const fn cursor_requires_order() -> Self {
        Self::continuation_cursor_invariant()
    }

    /// Construct one invariant error for cursor surfaces that require either
    /// explicit scalar ordering or canonical grouped ordering.
    #[cfg(test)]
    pub(in crate::db) const fn cursor_requires_explicit_or_grouped_ordering() -> Self {
        Self::continuation_cursor_invariant()
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

    /// Construct one cursor boundary arity mismatch error.
    pub(in crate::db) const fn continuation_cursor_boundary_arity_mismatch(
        expected: usize,
        found: usize,
    ) -> Self {
        Self::ContinuationCursorBoundaryArityMismatch { expected, found }
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

    /// Construct one indexed non-primary-key boundary type mismatch error.
    pub(in crate::db) const fn continuation_cursor_boundary_type_mismatch_at(index: usize) -> Self {
        Self::ContinuationCursorBoundaryTypeMismatch { index }
    }

    /// Construct one primary-key boundary type mismatch error.
    pub(in crate::db) const fn continuation_cursor_primary_key_type_mismatch() -> Self {
        Self::ContinuationCursorPrimaryKeyTypeMismatch { index: None }
    }

    /// Construct one indexed primary-key boundary type mismatch error.
    pub(in crate::db) const fn continuation_cursor_primary_key_type_mismatch_at(
        index: usize,
    ) -> Self {
        Self::ContinuationCursorPrimaryKeyTypeMismatch { index: Some(index) }
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

    /// Map one primary-key cursor decode failure into the executor-facing
    /// internal invariant taxonomy used by storage-key boundary adapters.
    #[cfg(test)]
    pub(in crate::db) fn into_pk_cursor_decode_internal_error(self) -> InternalError {
        let _ = self;
        InternalError::cursor_executor_invariant()
    }

    /// Map cursor-plan failures into runtime taxonomy classes.
    ///
    /// Cursor token/version/signature/window/payload mismatches are external
    /// input failures (`Unsupported` at cursor origin). Only explicit
    /// continuation invariant violations remain invariant-class failures.
    pub(crate) fn into_internal_error(self) -> InternalError {
        match self {
            Self::ContinuationCursorInvariantViolation => {
                InternalError::cursor_executor_invariant()
            }
            Self::InvalidContinuationCursor { .. }
            | Self::InvalidContinuationCursorPayload { .. }
            | Self::ContinuationCursorSignatureMismatch { .. }
            | Self::ContinuationCursorBoundaryArityMismatch { .. }
            | Self::ContinuationCursorWindowMismatch { .. }
            | Self::ContinuationCursorBoundaryTypeMismatch { .. }
            | Self::ContinuationCursorPrimaryKeyTypeMismatch { .. } => {
                InternalError::cursor_unsupported()
            }
        }
    }
}
