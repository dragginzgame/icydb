//! Module: cursor::error
//! Responsibility: cursor-domain typed error taxonomy and invariant construction helpers.
//! Does not own: planner policy derivation or runtime execution routing semantics.
//! Boundary: classifies continuation token/anchor/order/window failures for cursor consumers.

use crate::{
    db::cursor::{ContinuationSignature, CursorDecodeError, TokenWireError},
    error::InternalError,
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
    #[error("invalid continuation cursor payload")]
    InvalidContinuationCursorPayload,

    /// Cursor plan/runtime contract invariants were violated.
    #[error("continuation cursor invariant violation")]
    ContinuationCursorInvariantViolation,

    /// Cursor token does not belong to this canonical query shape.
    #[error("continuation cursor signature mismatch")]
    ContinuationCursorSignatureMismatch,

    /// Cursor boundary width does not match canonical order width.
    #[error("continuation cursor boundary arity mismatch")]
    ContinuationCursorBoundaryArityMismatch,

    /// Cursor window offset does not match the current query window shape.
    #[error("continuation cursor window mismatch")]
    ContinuationCursorWindowMismatch,

    /// Cursor boundary value type mismatch for a non-primary-key ordered field.
    #[error("continuation cursor boundary type mismatch")]
    ContinuationCursorBoundaryTypeMismatch,

    /// Cursor primary-key boundary does not match the entity key type.
    #[error("continuation cursor primary key type mismatch")]
    ContinuationCursorPrimaryKeyTypeMismatch,
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
    pub(in crate::db) const fn invalid_continuation_cursor_payload() -> Self {
        Self::InvalidContinuationCursorPayload
    }

    /// Construct one cursor-direction mismatch payload error.
    pub(in crate::db) const fn continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one grouped-cursor direction mismatch payload error.
    pub(in crate::db) const fn grouped_continuation_cursor_direction_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one unknown ORDER BY field payload error.
    pub(in crate::db) const fn continuation_cursor_unknown_order_field(_field: &str) -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one deterministic tie-break payload error.
    pub(in crate::db) const fn continuation_cursor_primary_key_tie_break_required(
        _pk_field: &str,
    ) -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one anchor decode failure payload error.
    pub(in crate::db) fn index_range_anchor_decode_failed(_reason: impl Into<String>) -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one canonical-anchor encoding mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_canonical_encoding_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one anchor index-id mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_index_id_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one anchor key-namespace mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_key_namespace_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one anchor component-arity mismatch payload error.
    pub(in crate::db) const fn index_range_anchor_component_arity_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one out-of-envelope anchor payload error.
    pub(in crate::db) const fn index_range_anchor_outside_envelope() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one composite-plan anchor rejection payload error.
    pub(in crate::db) const fn unexpected_index_range_anchor_for_composite_plan() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one missing semantic-bounds payload error.
    pub(in crate::db) const fn index_range_anchor_semantic_bounds_required() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one missing raw anchor payload error.
    pub(in crate::db) const fn index_range_anchor_required() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one non-index-range path anchor rejection payload error.
    pub(in crate::db) const fn unexpected_index_range_anchor_for_non_range_path() -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one anchor-primary-key decode failure payload error.
    pub(in crate::db) fn index_range_anchor_primary_key_decode_failed(
        _reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one boundary-primary-key decode failure payload error.
    #[cfg(test)]
    pub(in crate::db) fn index_range_boundary_primary_key_decode_failed(
        _reason: impl std::fmt::Display,
    ) -> Self {
        Self::invalid_continuation_cursor_payload()
    }

    /// Construct one boundary/anchor mismatch payload error.
    pub(in crate::db) const fn index_range_boundary_anchor_mismatch() -> Self {
        Self::invalid_continuation_cursor_payload()
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
        _expected: &ContinuationSignature,
        _actual: &ContinuationSignature,
    ) -> Self {
        Self::ContinuationCursorSignatureMismatch
    }

    /// Construct one cursor boundary arity mismatch error.
    pub(in crate::db) const fn continuation_cursor_boundary_arity_mismatch(
        _expected: usize,
        _found: usize,
    ) -> Self {
        Self::ContinuationCursorBoundaryArityMismatch
    }

    /// Construct one cursor window mismatch error.
    pub(in crate::db) const fn continuation_cursor_window_mismatch(
        _expected_offset: u32,
        _actual_offset: u32,
    ) -> Self {
        Self::ContinuationCursorWindowMismatch
    }

    /// Construct one non-primary-key boundary type mismatch error.
    pub(in crate::db) const fn continuation_cursor_boundary_type_mismatch() -> Self {
        Self::ContinuationCursorBoundaryTypeMismatch
    }

    /// Construct one primary-key boundary type mismatch error.
    pub(in crate::db) const fn continuation_cursor_primary_key_type_mismatch() -> Self {
        Self::ContinuationCursorPrimaryKeyTypeMismatch
    }

    /// Map cursor token decode failures into canonical plan-surface cursor errors.
    pub(in crate::db) const fn from_token_wire_error(err: TokenWireError) -> Self {
        match err {
            TokenWireError::Encode | TokenWireError::Decode => {
                Self::invalid_continuation_cursor_payload()
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
            | Self::InvalidContinuationCursorPayload
            | Self::ContinuationCursorSignatureMismatch
            | Self::ContinuationCursorBoundaryArityMismatch
            | Self::ContinuationCursorWindowMismatch
            | Self::ContinuationCursorBoundaryTypeMismatch
            | Self::ContinuationCursorPrimaryKeyTypeMismatch => InternalError::cursor_unsupported(),
        }
    }
}
