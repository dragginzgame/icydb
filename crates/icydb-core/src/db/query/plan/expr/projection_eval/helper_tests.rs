//! Test-only constructors and adapters for the parent module.

use super::*;

impl ProjectionFunctionEvalError {
    /// Convert this function-evaluation failure into the query-facing error
    /// taxonomy used by builder preview paths.
    pub(in crate::db) fn into_query_error(self) -> QueryError {
        match self {
            Self::InvalidCall => QueryError::invariant(),
            Self::InvalidProjection(reason) => QueryError::unsupported_projection(reason),
            Self::Numeric(err) => QueryError::execute(err.into_internal_error()),
            Self::Budget(err) => QueryError::execute(err.into()),
        }
    }
}

/// Evaluate one bounded projection-function call over already-evaluated
/// argument values.
pub(in crate::db) fn eval_projection_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, QueryError> {
    eval_projection_function_call_checked(function, args)
        .map_err(ProjectionFunctionEvalError::into_query_error)
}
