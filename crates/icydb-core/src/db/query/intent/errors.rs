//! Module: query::intent::errors
//! Responsibility: query-intent-facing typed error taxonomy and domain conversions.
//! Does not own: planner rule evaluation or runtime execution policy decisions.
//! Boundary: unifies intent/planner/cursor/resource errors into query API error classes.

use crate::{
    db::{
        cursor::CursorPlanError,
        query::plan::{
            CursorPagingPolicyError, FluentLoadPolicyViolation, IntentKeyAccessPolicyViolation,
            PlanError, PlannerError, PolicyPlanError,
        },
        response::ResponseError,
        schema::ValidateError,
    },
    error::{ErrorClass, InternalError},
};
use thiserror::Error as ThisError;

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("{0}")]
    Validate(#[from] ValidateError),

    #[error("{0}")]
    Plan(Box<PlanError>),

    #[error("{0}")]
    Intent(#[from] IntentError),

    #[error("{0}")]
    Response(#[from] ResponseError),

    #[error("{0}")]
    Execute(#[from] QueryExecutionError),
}

impl QueryError {
    /// Construct an execution-domain query error from one classified runtime error.
    pub(crate) fn execute(err: InternalError) -> Self {
        Self::Execute(QueryExecutionError::from(err))
    }
}

///
/// QueryExecutionError
///

#[derive(Debug, ThisError)]
pub enum QueryExecutionError {
    #[error("{0}")]
    Corruption(InternalError),

    #[error("{0}")]
    IncompatiblePersistedFormat(InternalError),

    #[error("{0}")]
    InvariantViolation(InternalError),

    #[error("{0}")]
    Conflict(InternalError),

    #[error("{0}")]
    NotFound(InternalError),

    #[error("{0}")]
    Unsupported(InternalError),

    #[error("{0}")]
    Internal(InternalError),
}

impl QueryExecutionError {
    /// Borrow the wrapped classified runtime error.
    #[must_use]
    pub const fn as_internal(&self) -> &InternalError {
        match self {
            Self::Corruption(err)
            | Self::IncompatiblePersistedFormat(err)
            | Self::InvariantViolation(err)
            | Self::Conflict(err)
            | Self::NotFound(err)
            | Self::Unsupported(err)
            | Self::Internal(err) => err,
        }
    }
}

impl From<InternalError> for QueryExecutionError {
    fn from(err: InternalError) -> Self {
        match err.class {
            ErrorClass::Corruption => Self::Corruption(err),
            ErrorClass::IncompatiblePersistedFormat => Self::IncompatiblePersistedFormat(err),
            ErrorClass::InvariantViolation => Self::InvariantViolation(err),
            ErrorClass::Conflict => Self::Conflict(err),
            ErrorClass::NotFound => Self::NotFound(err),
            ErrorClass::Unsupported => Self::Unsupported(err),
            ErrorClass::Internal => Self::Internal(err),
        }
    }
}

impl From<PlannerError> for QueryError {
    fn from(err: PlannerError) -> Self {
        match err {
            PlannerError::Plan(err) => Self::from(*err),
            PlannerError::Internal(err) => Self::execute(*err),
        }
    }
}

impl From<PlanError> for QueryError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}

///
/// IntentError
///

#[derive(Clone, Copy, Debug, ThisError)]
pub enum IntentError {
    #[error("{0}")]
    PlanShape(#[from] PolicyPlanError),

    #[error("by_ids() cannot be combined with predicates")]
    ByIdsWithPredicate,

    #[error("only() cannot be combined with predicates")]
    OnlyWithPredicate,

    #[error("multiple key access methods were used on the same query")]
    KeyAccessConflict,

    #[error("{0}")]
    InvalidPagingShape(#[from] PagingIntentError),

    #[error("grouped queries require execute_grouped(...)")]
    GroupedRequiresExecuteGrouped,

    #[error("HAVING requires GROUP BY")]
    HavingRequiresGroupBy,
}

///
/// PagingIntentError
///
/// Canonical intent-level paging contract failures shared by planner and
/// fluent/execution boundary gates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
#[expect(clippy::enum_variant_names)]
pub enum PagingIntentError {
    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_order_message()
    )]
    CursorRequiresOrder,

    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_limit_message()
    )]
    CursorRequiresLimit,

    #[error("cursor tokens can only be used with .page().execute()")]
    CursorRequiresPagedExecution,
}

impl From<CursorPagingPolicyError> for PagingIntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        match err {
            CursorPagingPolicyError::CursorRequiresOrder => Self::CursorRequiresOrder,
            CursorPagingPolicyError::CursorRequiresLimit => Self::CursorRequiresLimit,
        }
    }
}

impl From<CursorPagingPolicyError> for IntentError {
    fn from(err: CursorPagingPolicyError) -> Self {
        Self::InvalidPagingShape(PagingIntentError::from(err))
    }
}

impl From<IntentKeyAccessPolicyViolation> for IntentError {
    fn from(err: IntentKeyAccessPolicyViolation) -> Self {
        match err {
            IntentKeyAccessPolicyViolation::KeyAccessConflict => Self::KeyAccessConflict,
            IntentKeyAccessPolicyViolation::ByIdsWithPredicate => Self::ByIdsWithPredicate,
            IntentKeyAccessPolicyViolation::OnlyWithPredicate => Self::OnlyWithPredicate,
        }
    }
}

impl From<FluentLoadPolicyViolation> for IntentError {
    fn from(err: FluentLoadPolicyViolation) -> Self {
        match err {
            FluentLoadPolicyViolation::CursorRequiresPagedExecution => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresPagedExecution)
            }
            FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped => {
                Self::GroupedRequiresExecuteGrouped
            }
            FluentLoadPolicyViolation::CursorRequiresOrder => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
            }
            FluentLoadPolicyViolation::CursorRequiresLimit => {
                Self::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
            }
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorOrigin;

    fn assert_execute_variant_for_class(err: &QueryExecutionError, class: ErrorClass) {
        match class {
            ErrorClass::Corruption => assert!(matches!(err, QueryExecutionError::Corruption(_))),
            ErrorClass::IncompatiblePersistedFormat => {
                assert!(matches!(
                    err,
                    QueryExecutionError::IncompatiblePersistedFormat(_)
                ));
            }
            ErrorClass::InvariantViolation => {
                assert!(matches!(err, QueryExecutionError::InvariantViolation(_)));
            }
            ErrorClass::Conflict => assert!(matches!(err, QueryExecutionError::Conflict(_))),
            ErrorClass::NotFound => assert!(matches!(err, QueryExecutionError::NotFound(_))),
            ErrorClass::Unsupported => assert!(matches!(err, QueryExecutionError::Unsupported(_))),
            ErrorClass::Internal => assert!(matches!(err, QueryExecutionError::Internal(_))),
        }
    }

    #[test]
    fn query_execute_error_from_internal_preserves_class_and_origin_matrix() {
        let cases = [
            (ErrorClass::Corruption, ErrorOrigin::Store),
            (
                ErrorClass::IncompatiblePersistedFormat,
                ErrorOrigin::Serialize,
            ),
            (ErrorClass::InvariantViolation, ErrorOrigin::Query),
            (ErrorClass::InvariantViolation, ErrorOrigin::Recovery),
            (ErrorClass::Conflict, ErrorOrigin::Executor),
            (ErrorClass::NotFound, ErrorOrigin::Identity),
            (ErrorClass::Unsupported, ErrorOrigin::Cursor),
            (ErrorClass::Internal, ErrorOrigin::Serialize),
            (ErrorClass::Internal, ErrorOrigin::Planner),
        ];

        for (class, origin) in cases {
            let internal = InternalError::classified(class, origin, "matrix");
            let mapped = QueryExecutionError::from(internal);

            assert_execute_variant_for_class(&mapped, class);
            assert_eq!(mapped.as_internal().class, class);
            assert_eq!(mapped.as_internal().origin, origin);
        }
    }

    #[test]
    fn planner_internal_mapping_preserves_runtime_class_and_origin() {
        let planner_internal = PlannerError::Internal(Box::new(InternalError::classified(
            ErrorClass::Unsupported,
            ErrorOrigin::Cursor,
            "cursor payload mismatch",
        )));
        let query_err = QueryError::from(planner_internal);

        assert!(matches!(
            query_err,
            QueryError::Execute(QueryExecutionError::Unsupported(inner))
                if inner.class == ErrorClass::Unsupported
                    && inner.origin == ErrorOrigin::Cursor
        ));
    }

    #[test]
    fn planner_plan_mapping_stays_in_query_plan_error_boundary() {
        let planner_plan = PlannerError::Plan(Box::new(PlanError::from(
            PolicyPlanError::UnorderedPagination,
        )));
        let query_err = QueryError::from(planner_plan);

        assert!(
            matches!(query_err, QueryError::Plan(_)),
            "planner plan errors must remain in query plan boundary, not execution boundary",
        );
    }

    #[test]
    fn planner_internal_mapping_preserves_boundary_origins_for_telemetry_matrix() {
        let cases = [
            (ErrorClass::Internal, ErrorOrigin::Planner),
            (ErrorClass::Unsupported, ErrorOrigin::Cursor),
            (ErrorClass::InvariantViolation, ErrorOrigin::Recovery),
            (ErrorClass::Corruption, ErrorOrigin::Identity),
        ];

        for (class, origin) in cases {
            let planner_internal = PlannerError::Internal(Box::new(InternalError::classified(
                class, origin, "matrix",
            )));
            let query_err = QueryError::from(planner_internal);

            let QueryError::Execute(execute_err) = query_err else {
                panic!("planner internal errors must map to query execute errors");
            };
            assert_execute_variant_for_class(&execute_err, class);
            assert_eq!(
                execute_err.as_internal().origin,
                origin,
                "planner-internal mapping must preserve telemetry origin for {origin:?}",
            );
        }
    }

    #[test]
    fn query_execute_storage_and_index_errors_stay_in_execution_boundary() {
        let cases = [
            InternalError::store_internal("store internal"),
            InternalError::index_internal("index internal"),
            InternalError::store_corruption("store corruption"),
            InternalError::index_corruption("index corruption"),
            InternalError::store_unsupported("store unsupported"),
            InternalError::index_unsupported("index unsupported"),
        ];

        for internal in cases {
            let class = internal.class;
            let origin = internal.origin;

            let query_err = QueryError::execute(internal);
            let QueryError::Execute(execute_err) = query_err else {
                panic!("storage/index runtime failures must stay in execution boundary");
            };

            assert_execute_variant_for_class(&execute_err, class);
            assert_eq!(
                execute_err.as_internal().origin,
                origin,
                "storage/index runtime failures must preserve origin taxonomy",
            );
        }
    }

    #[test]
    fn cursor_paging_policy_maps_to_invalid_paging_shape_intent_error() {
        let order = IntentError::from(CursorPagingPolicyError::CursorRequiresOrder);
        let limit = IntentError::from(CursorPagingPolicyError::CursorRequiresLimit);

        assert!(matches!(
            order,
            IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
        ));
        assert!(matches!(
            limit,
            IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
        ));
    }

    #[test]
    fn fluent_paging_policy_maps_to_invalid_paging_shape_or_grouped_contract() {
        let non_paged = IntentError::from(FluentLoadPolicyViolation::CursorRequiresPagedExecution);
        let grouped = IntentError::from(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);

        assert!(matches!(
            non_paged,
            IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresPagedExecution)
        ));
        assert!(matches!(
            grouped,
            IntentError::GroupedRequiresExecuteGrouped
        ));
    }

    #[test]
    fn fluent_cursor_order_and_limit_policy_map_to_intent_paging_shape() {
        let requires_order = IntentError::from(FluentLoadPolicyViolation::CursorRequiresOrder);
        let requires_limit = IntentError::from(FluentLoadPolicyViolation::CursorRequiresLimit);

        assert!(matches!(
            requires_order,
            IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
        ));
        assert!(matches!(
            requires_limit,
            IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
        ));
    }
}
