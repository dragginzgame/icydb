//! Cursor pagination policy rules shared by fluent/intent entry surfaces.
//!
//! Plan-shape policy validation (ORDER emptiness, delete-limit/order coupling,
//! unordered pagination) is owned by `query::plan::validate`.

use crate::db::query::intent::LoadSpec;
use thiserror::Error as ThisError;

///
/// CursorPagingPolicyError
/// Canonical policy failures for cursor-pagination readiness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum CursorPagingPolicyError {
    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    #[error("cursor pagination requires a limit")]
    CursorRequiresLimit,
}

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }

    Ok(())
}
