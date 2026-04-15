//! Module: cursor::error
//! Responsibility: cursor-domain typed error taxonomy and invariant construction helpers.
//! Does not own: planner policy derivation or runtime execution routing semantics.
//! Boundary: classifies continuation token/anchor/order/window failures for cursor consumers.

use crate::{
    db::{
        codec::cursor::CursorDecodeError,
        cursor::{ContinuationSignature, TokenWireError},
    },
    error::InternalError,
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

    /// Cursor plan/runtime contract invariants were violated.
    #[error("{reason}")]
    ContinuationCursorInvariantViolation { reason: String },

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
    /// Canonical policy text for missing cursor ORDER BY requirements.
    pub(crate) const fn cursor_requires_order_message() -> &'static str {
        "cursor pagination requires an explicit ordering"
    }

    /// Canonical invariant text for cursor surfaces that require either
    /// explicit scalar ordering or canonical grouped ordering.
    pub(crate) const fn cursor_requires_explicit_or_grouped_ordering_message() -> &'static str {
        "cursor pagination requires explicit or grouped ordering"
    }

    /// Canonical policy text for missing cursor LIMIT requirements.
    pub(crate) const fn cursor_requires_limit_message() -> &'static str {
        "cursor pagination requires a limit"
    }

    /// Canonical payload text for empty cursor ORDER BY specifications.
    pub(crate) const fn cursor_requires_non_empty_order_message() -> &'static str {
        "cursor pagination requires non-empty ordering"
    }

    /// Construct one invalid cursor-token decode error.
    pub(in crate::db) const fn invalid_continuation_cursor(reason: CursorDecodeError) -> Self {
        Self::InvalidContinuationCursor { reason }
    }

    /// Construct the canonical invalid-continuation payload error variant.
    pub(in crate::db) fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> Self {
        Self::InvalidContinuationCursorPayload {
            reason: reason.into(),
        }
    }

    /// Construct one cursor-direction mismatch payload error.
    pub(in crate::db) fn continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "continuation cursor direction does not match executable plan direction",
        )
    }

    /// Construct one grouped-cursor direction mismatch payload error.
    pub(in crate::db) fn grouped_continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "grouped continuation cursor direction does not match executable plan direction",
        )
    }

    /// Construct one unknown ORDER BY field payload error.
    pub(in crate::db) fn continuation_cursor_unknown_order_field(field: &str) -> Self {
        Self::invalid_continuation_cursor_payload(format!("unknown order field '{field}'"))
    }

    /// Construct one deterministic tie-break payload error.
    pub(in crate::db) fn continuation_cursor_primary_key_tie_break_required(
        pk_field: &str,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(format!(
            "order specification must end with primary key '{pk_field}' as deterministic tie-break"
        ))
    }

    /// Construct one anchor decode failure payload error.
    pub(in crate::db) fn index_range_anchor_decode_failed(reason: impl Into<String>) -> Self {
        Self::invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor decode failed: {}",
            reason.into(),
        ))
    }

    /// Construct one canonical-anchor encoding mismatch payload error.
    pub(in crate::db) fn index_range_anchor_canonical_encoding_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation anchor canonical encoding mismatch",
        )
    }

    /// Construct one anchor index-id mismatch payload error.
    pub(in crate::db) fn index_range_anchor_index_id_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation anchor index id mismatch",
        )
    }

    /// Construct one anchor key-namespace mismatch payload error.
    pub(in crate::db) fn index_range_anchor_key_namespace_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation anchor key namespace mismatch",
        )
    }

    /// Construct one anchor component-arity mismatch payload error.
    pub(in crate::db) fn index_range_anchor_component_arity_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation anchor component arity mismatch",
        )
    }

    /// Construct one out-of-envelope anchor payload error.
    pub(in crate::db) fn index_range_anchor_outside_envelope() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation anchor is outside the original range envelope",
        )
    }

    /// Construct one composite-plan anchor rejection payload error.
    pub(in crate::db) fn unexpected_index_range_anchor_for_composite_plan() -> Self {
        Self::invalid_continuation_cursor_payload(
            "unexpected index-range continuation anchor for composite access plan",
        )
    }

    /// Construct one missing semantic-bounds payload error.
    pub(in crate::db) fn index_range_anchor_semantic_bounds_required() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation validation is missing semantic bounds payload",
        )
    }

    /// Construct one missing raw anchor payload error.
    pub(in crate::db) fn index_range_anchor_required() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation cursor is missing a raw-key anchor",
        )
    }

    /// Construct one non-index-range path anchor rejection payload error.
    pub(in crate::db) fn unexpected_index_range_anchor_for_non_range_path() -> Self {
        Self::invalid_continuation_cursor_payload(
            "unexpected index-range continuation anchor for non-index-range access path",
        )
    }

    /// Construct one anchor-primary-key decode failure payload error.
    pub(in crate::db) fn index_range_anchor_primary_key_decode_failed(
        reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(format!(
            "index-range continuation anchor primary key decode failed: {reason}",
        ))
    }

    /// Construct one boundary-primary-key decode failure payload error.
    pub(in crate::db) fn index_range_boundary_primary_key_decode_failed(
        reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload(format!(
            "index-range continuation boundary primary key decode failed: {reason}",
        ))
    }

    /// Construct one boundary/anchor mismatch payload error.
    pub(in crate::db) fn index_range_boundary_anchor_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload(
            "index-range continuation boundary/anchor mismatch",
        )
    }

    /// Construct one cursor invariant-violation error variant.
    pub(in crate::db) fn continuation_cursor_invariant(reason: impl Into<String>) -> Self {
        Self::ContinuationCursorInvariantViolation {
            reason: reason.into(),
        }
    }

    /// Construct one invariant error for missing explicit cursor ordering.
    pub(in crate::db) fn cursor_requires_order() -> Self {
        Self::continuation_cursor_invariant(Self::cursor_requires_order_message())
    }

    /// Construct one invariant error for cursor surfaces that require either
    /// explicit scalar ordering or canonical grouped ordering.
    #[cfg(test)]
    pub(in crate::db) fn cursor_requires_explicit_or_grouped_ordering() -> Self {
        Self::continuation_cursor_invariant(
            Self::cursor_requires_explicit_or_grouped_ordering_message(),
        )
    }

    /// Construct one invariant error for empty cursor ORDER BY specifications.
    pub(in crate::db) fn cursor_requires_non_empty_order() -> Self {
        Self::continuation_cursor_invariant(Self::cursor_requires_non_empty_order_message())
    }

    /// Construct one cursor-signature mismatch error for the current entity path.
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

    /// Construct one non-primary-key boundary type mismatch error.
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

    /// Construct one primary-key boundary type mismatch error.
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

    /// Map cursor token decode failures into canonical plan-surface cursor errors.
    pub(in crate::db) fn from_token_wire_error(err: TokenWireError) -> Self {
        match err {
            TokenWireError::Encode(message) | TokenWireError::Decode(message) => {
                Self::invalid_continuation_cursor_payload(message)
            }
        }
    }

    /// Map one primary-key cursor decode failure into the executor-facing
    /// internal invariant taxonomy used by storage-key boundary adapters.
    #[cfg(test)]
    pub(in crate::db) fn into_pk_cursor_decode_internal_error(self) -> InternalError {
        match self {
            Self::InvalidContinuationCursor { reason } => InternalError::cursor_executor_invariant(
                format!("pk cursor decode rejected invalid continuation cursor: {reason}"),
            ),
            Self::InvalidContinuationCursorPayload { reason } => {
                InternalError::cursor_executor_invariant(format!(
                    "pk cursor decode rejected invalid continuation payload: {reason}"
                ))
            }
            Self::ContinuationCursorSignatureMismatch { .. } => {
                InternalError::cursor_executor_invariant(
                    "pk cursor decode encountered continuation signature mismatch",
                )
            }
            Self::ContinuationCursorBoundaryArityMismatch { expected, found } => {
                InternalError::cursor_executor_invariant(format!(
                    "pk cursor boundary arity mismatch: expected {expected}, found {found}"
                ))
            }
            Self::ContinuationCursorWindowMismatch {
                expected_offset,
                actual_offset,
            } => InternalError::cursor_executor_invariant(format!(
                "pk cursor window mismatch: expected_offset={expected_offset}, actual_offset={actual_offset}"
            )),
            Self::ContinuationCursorBoundaryTypeMismatch { field, .. } => {
                InternalError::cursor_executor_invariant(format!(
                    "pk cursor boundary type mismatch on field '{field}'"
                ))
            }
            Self::ContinuationCursorPrimaryKeyTypeMismatch { value: None, .. } => {
                InternalError::cursor_executor_invariant("pk cursor slot must be present")
            }
            Self::ContinuationCursorPrimaryKeyTypeMismatch { value: Some(_), .. } => {
                InternalError::cursor_executor_invariant("pk cursor slot type mismatch")
            }
            Self::ContinuationCursorInvariantViolation { reason } => {
                InternalError::cursor_executor_invariant(reason)
            }
        }
    }

    /// Map cursor-plan failures into runtime taxonomy classes.
    ///
    /// Cursor token/version/signature/window/payload mismatches are external
    /// input failures (`Unsupported` at cursor origin). Only explicit
    /// continuation invariant violations remain invariant-class failures.
    pub(crate) fn into_internal_error(self) -> InternalError {
        match self {
            Self::ContinuationCursorInvariantViolation { reason } => {
                InternalError::cursor_executor_invariant(reason)
            }
            Self::InvalidContinuationCursor { .. }
            | Self::InvalidContinuationCursorPayload { .. }
            | Self::ContinuationCursorSignatureMismatch { .. }
            | Self::ContinuationCursorBoundaryArityMismatch { .. }
            | Self::ContinuationCursorWindowMismatch { .. }
            | Self::ContinuationCursorBoundaryTypeMismatch { .. }
            | Self::ContinuationCursorPrimaryKeyTypeMismatch { .. } => {
                InternalError::cursor_unsupported(self.to_string())
            }
        }
    }
}
