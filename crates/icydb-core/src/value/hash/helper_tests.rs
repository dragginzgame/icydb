//! Test-only helpers for the parent module.

use super::*;

/// Fixed typed failure used to qualify consumers without spending real IC work.
pub(crate) fn test_hash_budget_error() -> InternalError {
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
        DiagnosticExecutionLane as Lane,
    };
    InternalError::execution_budget_exceeded(
        Resource::TemporaryBytes,
        10,
        11,
        Scope::Execution,
        Lane::Mutation,
        7,
    )
}

pub(super) fn test_hash_override() -> Option<Result<[u8; 16], InternalError>> {
    TEST_HASH_OVERRIDE.with(|cell| {
        cell.get()
            .map(|result| result.map_err(|make_error| make_error()))
    })
}

pub(crate) fn with_test_hash_override<T>(
    override_hash: TestHashOverride,
    f: impl FnOnce() -> T + std::panic::UnwindSafe,
) -> T {
    let previous = TEST_HASH_OVERRIDE.with(|cell| cell.replace(Some(override_hash)));
    let result = std::panic::catch_unwind(f);
    TEST_HASH_OVERRIDE.with(|cell| cell.set(previous));
    match result {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

pub(super) type TestHashOverride = Result<[u8; 16], fn() -> InternalError>;

thread_local! {
    pub(super) static TEST_HASH_OVERRIDE: std::cell::Cell<Option<TestHashOverride>> =
        const { std::cell::Cell::new(None) };
}
