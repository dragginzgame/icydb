//! Module: db::error::access
//!
//! Responsibility: access-planning error conversions into runtime invariants.
//! Does not own: access planning itself.
//! Boundary: access validation failures are mapped to runtime taxonomy here.

use crate::{db::access::AccessPlanError, error::InternalError};

/// Map shared access-validation failures into query-boundary invariants.
pub(crate) fn from_executor_access_plan_error(err: AccessPlanError) -> InternalError {
    InternalError::query_invariant(err.to_string())
}
