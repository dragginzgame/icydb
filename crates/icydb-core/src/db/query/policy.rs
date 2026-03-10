//! Module: db::query::policy
//! Responsibility: module-local ownership and contracts for db::query::policy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Cursor pagination policy rules shared by fluent/intent entry surfaces.
//!
//! Plan-shape policy validation (ORDER emptiness, delete-limit/order coupling,
//! unordered pagination) is owned by `query::plan::validate`.

use crate::db::query::plan::{
    CursorPagingPolicyError, LoadSpec,
    validate_cursor_paging_requirements as validate_cursor_paging_requirements_shared,
};

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
#[expect(dead_code)]
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    validate_cursor_paging_requirements_shared(has_order, spec)
}
