//! Module: db::test_support
//! Responsibility: db-local test helper modules.
//! Does not own: runtime test fixtures or production support APIs.
//! Boundary: exposes helpers only inside db test code.

pub(in crate::db) mod index;
pub(in crate::db) mod source_guard;

use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

/// Construct the shared uniform test profile with one resource overridden.
///
/// Exact-limit tests keep unrelated resource ceilings and failure headroom
/// fixed while exercising their selected admission boundary.
#[must_use]
pub(in crate::db) fn request_with_limit(
    resource: DiagnosticExecutionBudgetResource,
    limit: u64,
) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}
