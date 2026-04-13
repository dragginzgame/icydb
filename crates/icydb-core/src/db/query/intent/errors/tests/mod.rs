//! Module: db::query::intent::errors::tests
//! Covers query-intent error mapping and user-facing error classification.
//! Does not own: production error behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::query::intent::{IntentError, QueryError, QueryExecutionError},
    error::{ErrorClass, ErrorOrigin, InternalError},
};

use super::*;

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
        let planner_internal =
            PlannerError::Internal(Box::new(InternalError::classified(class, origin, "matrix")));
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
fn query_intent_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::GroupedRequiresDirectExecute);

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::GroupedRequiresDirectExecute)
    ));
}

#[test]
fn having_requires_group_by_constructor_keeps_intent_boundary() {
    let err = IntentError::having_requires_group_by();

    assert!(matches!(err, IntentError::HavingRequiresGroupBy));
}

#[test]
fn query_invariant_constructor_preserves_query_invariant_boundary() {
    let query_err = QueryError::invariant("route contract mismatch");

    assert!(matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::InvariantViolation(inner))
            if inner.class == ErrorClass::InvariantViolation
                && inner.origin == ErrorOrigin::Query
    ));
}

#[test]
fn query_serialize_internal_constructor_preserves_serialize_internal_boundary() {
    let query_err = QueryError::serialize_internal("cursor token encode failed");

    assert!(matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Internal(inner))
            if inner.class == ErrorClass::Internal
                && inner.origin == ErrorOrigin::Serialize
    ));
}

#[test]
fn continuation_kind_mismatch_helpers_preserve_invariant_messages() {
    let scalar = QueryError::scalar_paged_emitted_grouped_continuation();
    let grouped = QueryError::grouped_paged_emitted_scalar_continuation();

    let QueryError::Execute(QueryExecutionError::InvariantViolation(scalar_inner)) = scalar else {
        panic!("scalar continuation kind mismatch must stay in invariant boundary");
    };
    let QueryError::Execute(QueryExecutionError::InvariantViolation(grouped_inner)) = grouped
    else {
        panic!("grouped continuation kind mismatch must stay in invariant boundary");
    };

    assert_eq!(
        scalar_inner.message,
        "executor invariant violated: scalar load pagination emitted grouped continuation token",
    );
    assert_eq!(
        grouped_inner.message,
        "executor invariant violated: grouped pagination emitted scalar continuation token",
    );
}

#[cfg(feature = "sql")]
#[test]
fn unsupported_sql_feature_preserves_query_unsupported_execution_boundary() {
    let query_err = QueryError::unsupported_sql_feature("JOIN");

    assert!(matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(inner))
            if inner.class == ErrorClass::Unsupported
                && inner.origin == ErrorOrigin::Query
    ));
}

#[test]
fn unknown_aggregate_target_field_preserves_query_unsupported_execution_boundary() {
    let query_err = QueryError::unknown_aggregate_target_field("missing");

    assert!(matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(ref inner))
            if inner.class == ErrorClass::Unsupported
                && inner.origin == ErrorOrigin::Query
    ));

    let QueryError::Execute(QueryExecutionError::Unsupported(inner)) = query_err else {
        panic!("unknown aggregate target field must map to query unsupported execution error");
    };

    assert_eq!(inner.message, "unknown aggregate target field: missing");
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
    let grouped = IntentError::from(FluentLoadPolicyViolation::GroupedRequiresDirectExecute);

    assert!(matches!(
        non_paged,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresPagedExecution)
    ));
    assert!(matches!(grouped, IntentError::GroupedRequiresDirectExecute));
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
