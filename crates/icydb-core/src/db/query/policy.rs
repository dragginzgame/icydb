//! Cursor pagination policy rules shared by fluent/intent entry surfaces.
//!
//! Plan-shape policy validation (ORDER emptiness, delete-limit/order coupling,
//! unordered pagination) is owned by `query::plan::validate`.

use crate::db::query::plan::{
    CursorPagingPolicyError, LoadSpec,
    validate_cursor_paging_requirements as validate_cursor_paging_requirements_shared,
};

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
#[allow(dead_code)]
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    validate_cursor_paging_requirements_shared(has_order, spec)
}
