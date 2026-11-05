//! Test-only constructors and adapters for the parent module.

use super::*;

impl InternalError {
    /// Construct a query-origin internal error.
    pub(crate) fn query_internal() -> Self {
        Self::new(ErrorClass::Internal, ErrorOrigin::Query)
    }

    /// Construct an index-plan invariant violation error with a canonical prefix.
    pub(crate) fn index_plan_invariant(origin: ErrorOrigin) -> Self {
        Self::new(ErrorClass::InvariantViolation, origin)
    }

    /// Construct an index-plan invariant violation error for store-origin failures.
    pub(crate) fn index_plan_store_invariant() -> Self {
        Self::index_plan_invariant(ErrorOrigin::Store)
    }
}
