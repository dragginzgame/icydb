//! Module: db::query::intent::errors::tests
//! Covers query-intent error mapping and user-facing error classification.
//! Does not own: production error behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

#[cfg(feature = "sql")]
use crate::error::{ErrorDetail, QueryErrorDetail, SchemaDdlAdmissionError};
use crate::{
    db::{
        query::intent::{IntentError, QueryError, QueryExecutionError},
        response::ResponseError,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};

use super::*;

fn assert_execute_variant_for_class(err: &QueryExecutionError, class: ErrorClass) {
    match class {
        ErrorClass::Corruption => std::assert_matches!(err, QueryExecutionError::Corruption(_)),
        ErrorClass::IncompatiblePersistedFormat => {
            std::assert_matches!(err, QueryExecutionError::IncompatiblePersistedFormat(_));
        }
        ErrorClass::InvariantViolation => {
            std::assert_matches!(err, QueryExecutionError::InvariantViolation(_));
        }
        ErrorClass::Conflict => std::assert_matches!(err, QueryExecutionError::Conflict(_)),
        ErrorClass::NotFound => std::assert_matches!(err, QueryExecutionError::NotFound(_)),
        ErrorClass::Unsupported => std::assert_matches!(err, QueryExecutionError::Unsupported(_)),
        ErrorClass::Internal => std::assert_matches!(err, QueryExecutionError::Internal(_)),
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
        let internal = InternalError::classified(class, origin);
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
    )));
    let query_err = QueryError::from(planner_internal);

    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(inner))
            if inner.class == ErrorClass::Unsupported
                && inner.origin == ErrorOrigin::Cursor
    );
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
fn query_plan_unordered_pagination_exposes_compact_diagnostic_code() {
    let query_err = QueryError::from(PlanError::from(PolicyPlanError::UnorderedPagination));
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnorderedPagination
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::QueryKind {
            kind: icydb_diagnostic_code::QueryErrorKind::UnorderedPagination,
        }),
    );
}

#[test]
fn query_response_error_exposes_response_origin_compact_diagnostic() {
    let query_err = QueryError::from(ResponseError::not_unique("User", 2));
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryNotUnique
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Response
    );
}

#[test]
fn query_read_admission_error_exposes_compact_diagnostic_detail() {
    let query_err =
        QueryError::from(icydb_diagnostic_code::QueryReadAdmissionCode::PublicQueryOffsetRejected);
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::PublicQueryOffsetRejected,
            }
        ),
    );
    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(_))
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
            PlannerError::Internal(Box::new(InternalError::classified(class, origin)));
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
        InternalError::store_internal(),
        InternalError::index_internal(),
        InternalError::store_corruption(),
        InternalError::index_corruption(),
        InternalError::store_unsupported(),
        InternalError::index_unsupported(),
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

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::GroupedRequiresDirectExecute)
    );
}

#[test]
fn raw_limit_before_exists_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_exists_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeExistsTerminal)
    );
}

#[test]
fn raw_limit_before_page_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_page_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforePageTerminal)
    );
}

#[test]
fn cursor_before_page_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::cursor_before_page_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::CursorBeforePageTerminal)
    );
}

#[test]
fn raw_limit_before_count_exact_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_count_exact_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeCountExactTerminal)
    );
}

#[test]
fn raw_limit_before_sum_exact_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_sum_exact_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeSumExactTerminal)
    );
}

#[test]
fn raw_limit_before_min_exact_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_min_exact_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeMinExactTerminal)
    );
}

#[test]
fn raw_limit_before_max_exact_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_max_exact_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeMaxExactTerminal)
    );
}

#[test]
fn raw_limit_before_avg_exact_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_avg_exact_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeAvgExactTerminal)
    );
}

#[test]
fn raw_limit_before_collect_complete_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_collect_complete_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeCollectCompleteTerminal)
    );
}

#[test]
fn raw_limit_before_admin_batch_terminal_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::raw_limit_before_admin_batch_terminal());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeAdminBatchTerminal)
    );
}

#[test]
fn admin_batch_requires_trusted_read_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::admin_batch_requires_trusted_read());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::AdminBatchRequiresTrustedRead)
    );
}

#[test]
fn complete_read_too_many_rows_constructor_keeps_intent_boundary() {
    let err = QueryError::intent(IntentError::complete_read_too_many_rows());

    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::CompleteReadTooManyRows)
    );
}

#[test]
fn having_requires_group_by_constructor_keeps_intent_boundary() {
    let err = IntentError::having_requires_group_by();

    std::assert_matches!(err, IntentError::HavingRequiresGroupBy);
}

#[test]
fn query_invariant_constructor_preserves_query_invariant_boundary() {
    let query_err = QueryError::invariant();

    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::InvariantViolation(inner))
            if inner.class == ErrorClass::InvariantViolation
                && inner.origin == ErrorOrigin::Query
    );
}

#[test]
fn query_serialize_internal_constructor_preserves_serialize_internal_boundary() {
    let query_err = QueryError::serialize_internal();

    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Internal(inner))
            if inner.class == ErrorClass::Internal
                && inner.origin == ErrorOrigin::Serialize
    );
}

#[test]
fn continuation_kind_mismatch_helpers_preserve_invariant_diagnostics() {
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
        scalar_inner.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
    assert_eq!(
        grouped_inner.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[cfg(feature = "sql")]
#[test]
fn unsupported_sql_feature_preserves_query_unsupported_execution_boundary() {
    let query_err =
        QueryError::unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);

    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(inner))
            if inner.class == ErrorClass::Unsupported
                && inner.origin == ErrorOrigin::Query
    );
}

#[cfg(feature = "sql")]
#[test]
fn unsupported_sql_feature_query_error_exposes_compact_feature_code() {
    let query_err =
        QueryError::unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::UnsupportedSqlFeature {
                feature: icydb_diagnostic_code::SqlFeatureCode::Join,
            }
        ),
    );
}

#[test]
fn unsupported_projection_query_error_exposes_compact_projection_code() {
    let query_err = QueryError::unsupported_projection(
        icydb_diagnostic_code::QueryProjectionCode::NumericScaleArguments,
    );
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedProjection
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::QueryProjection {
            reason: icydb_diagnostic_code::QueryProjectionCode::NumericScaleArguments,
        }),
    );
}

#[test]
fn result_shape_mismatch_query_error_exposes_compact_shape_code() {
    let query_err = QueryError::result_shape_mismatch(
        icydb_diagnostic_code::QueryResultShapeCode::ExpectedGroupedRows,
    );
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryResultShapeMismatch
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::QueryResultShape {
            reason: icydb_diagnostic_code::QueryResultShapeCode::ExpectedGroupedRows,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_lowering_query_error_exposes_compact_lowering_code() {
    let query_err = QueryError::from_sql_lowering_error(
        crate::db::sql::lowering::SqlLoweringError::UnsupportedWhereExpression,
    );
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlLowering {
            reason: icydb_diagnostic_code::SqlLoweringCode::WhereExpressionShape,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_surface_mismatch_query_error_exposes_compact_mismatch_code() {
    let query_err = QueryError::sql_surface_mismatch(
        icydb_diagnostic_code::SqlSurfaceMismatchCode::QueryRejectsInsert,
    );
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlSurfaceMismatch
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: icydb_diagnostic_code::SqlSurfaceMismatchCode::QueryRejectsInsert,
            }
        ),
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_write_boundary_query_error_exposes_compact_boundary_code() {
    let query_err = QueryError::sql_write_boundary(
        icydb_diagnostic_code::SqlWriteBoundaryCode::MissingRequiredFields,
    );
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::MissingRequiredFields,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_ddl_publication_race_maps_to_query_admission_detail() {
    let query_err = QueryError::from_sql_ddl_execution_error(
        InternalError::schema_ddl_publication_race_lost("User"),
    );

    let QueryError::Execute(QueryExecutionError::Unsupported(inner)) = query_err else {
        panic!("SQL DDL race loss must stay an unsupported execution error");
    };

    assert_eq!(inner.class, ErrorClass::Unsupported);
    assert_eq!(inner.origin, ErrorOrigin::Query);
    assert!(
        matches!(
            inner.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::SchemaDdlAdmission {
                error: SchemaDdlAdmissionError::PublicationRaceLost
            }))
        ),
        "SQL DDL race loss should surface as a DDL admission detail, got {:?}",
        inner.detail(),
    );
}

#[cfg(feature = "sql")]
#[test]
fn unknown_aggregate_target_field_preserves_query_unsupported_execution_boundary() {
    let query_err = QueryError::unknown_aggregate_target_field("missing");
    let diagnostic = query_err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnknownAggregateTargetField
    );
    assert_eq!(diagnostic.detail(), None);

    std::assert_matches!(
        query_err,
        QueryError::Execute(QueryExecutionError::Unsupported(ref inner))
            if inner.class == ErrorClass::Unsupported
                && inner.origin == ErrorOrigin::Query
    );

    let QueryError::Execute(QueryExecutionError::Unsupported(inner)) = query_err else {
        panic!("unknown aggregate target field must map to query unsupported execution error");
    };

    assert!(matches!(
        inner.detail(),
        Some(ErrorDetail::Query(
            QueryErrorDetail::UnknownAggregateTargetField
        ))
    ));
}

#[test]
fn cursor_paging_policy_maps_to_invalid_paging_shape_intent_error() {
    let order = IntentError::from(CursorPagingPolicyError::CursorRequiresOrder);
    let limit = IntentError::from(CursorPagingPolicyError::CursorRequiresLimit);

    std::assert_matches!(
        order,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
    );
    std::assert_matches!(
        limit,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
    );
}

#[test]
fn fluent_paging_policy_maps_to_invalid_paging_shape_or_grouped_contract() {
    let non_paged = IntentError::from(FluentLoadPolicyViolation::CursorRequiresPagedExecution);
    let grouped = IntentError::from(FluentLoadPolicyViolation::GroupedRequiresDirectExecute);

    std::assert_matches!(
        non_paged,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresPagedExecution)
    );
    std::assert_matches!(grouped, IntentError::GroupedRequiresDirectExecute);
}

#[test]
fn fluent_cursor_order_and_limit_policy_map_to_intent_paging_shape() {
    let requires_order = IntentError::from(FluentLoadPolicyViolation::CursorRequiresOrder);
    let requires_limit = IntentError::from(FluentLoadPolicyViolation::CursorRequiresLimit);

    std::assert_matches!(
        requires_order,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresOrder)
    );
    std::assert_matches!(
        requires_limit,
        IntentError::InvalidPagingShape(PagingIntentError::CursorRequiresLimit)
    );
}
