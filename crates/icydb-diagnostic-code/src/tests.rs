use super::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorClass, ErrorCode, ErrorOrigin,
    QueryProjectionCode, QueryReadAdmissionCode, SqlFeatureCode, SqlLoweringCode,
    SqlWriteBoundaryCode,
    registry::{DETAIL_ERROR_CODES, ORDERED_ERROR_CODES},
};

#[test]
fn diagnostic_from_code_uses_default_origin() {
    let diagnostic = Diagnostic::from_code(DiagnosticCode::QueryPlan);

    assert_eq!(diagnostic.code(), DiagnosticCode::QueryPlan);
    assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
}

#[test]
fn diagnostic_code_reports_broad_class() {
    assert_eq!(
        DiagnosticCode::QueryUnsupportedSqlFeature.class(),
        ErrorClass::Unsupported
    );
    assert_eq!(
        DiagnosticCode::QuerySqlSurfaceMismatch.class(),
        ErrorClass::Unsupported
    );
    assert_eq!(DiagnosticCode::QueryPlan.class(), ErrorClass::Query);
    assert_eq!(
        DiagnosticCode::StoreCorruption.class(),
        ErrorClass::Corruption
    );
}

#[test]
fn class_and_origin_wire_codes_round_trip() {
    for (class, raw) in [
        (ErrorClass::Query, 1),
        (ErrorClass::Corruption, 2),
        (ErrorClass::IncompatiblePersistedFormat, 3),
        (ErrorClass::NotFound, 4),
        (ErrorClass::Internal, 5),
        (ErrorClass::Conflict, 6),
        (ErrorClass::Unsupported, 7),
        (ErrorClass::InvariantViolation, 8),
    ] {
        assert_eq!(class.wire_code(), raw);
        assert_eq!(ErrorClass::from_wire_code(raw), Some(class));
        assert_eq!(format!("{class:?}"), raw.to_string());
    }

    for (origin, raw) in [
        (ErrorOrigin::Cursor, 1),
        (ErrorOrigin::Executor, 2),
        (ErrorOrigin::Identity, 3),
        (ErrorOrigin::Index, 4),
        (ErrorOrigin::Interface, 5),
        (ErrorOrigin::Planner, 6),
        (ErrorOrigin::Query, 7),
        (ErrorOrigin::Recovery, 8),
        (ErrorOrigin::Response, 9),
        (ErrorOrigin::Runtime, 10),
        (ErrorOrigin::Serialize, 11),
        (ErrorOrigin::Store, 12),
    ] {
        assert_eq!(origin.wire_code(), raw);
        assert_eq!(ErrorOrigin::from_known_wire_code(raw), Some(origin));
        assert_eq!(ErrorOrigin::from_wire_code(raw), origin);
        assert_eq!(format!("{origin:?}"), raw.to_string());
    }

    assert_eq!(ErrorClass::from_wire_code(0), None);
    assert_eq!(ErrorOrigin::from_known_wire_code(0), None);
    assert_eq!(ErrorOrigin::from_wire_code(0), ErrorOrigin::Runtime);
}

#[test]
fn public_error_codes_are_sequential() {
    let first = ORDERED_ERROR_CODES
        .first()
        .expect("public error-code registry is non-empty")
        .raw();

    assert_eq!(first, 1);

    for (index, code) in ORDERED_ERROR_CODES.iter().enumerate() {
        let expected = first + u16::try_from(index).expect("test error-code index fits u16");
        assert_eq!(code.raw(), expected);
        assert_eq!(ErrorCode::known(code.raw()), Some(*code));
        assert!(code.is_known());
    }

    let last = ORDERED_ERROR_CODES
        .last()
        .expect("public error-code registry is non-empty")
        .raw();

    assert_eq!(last, 293);
}

#[test]
fn all_public_error_codes_round_trip_through_diagnostic_parts() {
    let first = ORDERED_ERROR_CODES
        .first()
        .expect("public error-code registry is non-empty")
        .raw();
    let last = ORDERED_ERROR_CODES
        .last()
        .expect("public error-code registry is non-empty")
        .raw();

    for raw in first..=last {
        let code = ErrorCode::from_raw(raw);
        let diagnostic_code = code.diagnostic_code();
        let diagnostic_detail = code.diagnostic_detail();
        let rebuilt = ErrorCode::from_parts(diagnostic_code, diagnostic_detail);

        assert_eq!(rebuilt.raw(), raw);

        let diagnostic = code.diagnostic(ErrorOrigin::Runtime);

        assert_eq!(diagnostic.code(), diagnostic_code);
        assert_eq!(diagnostic.detail(), diagnostic_detail.as_ref());
        assert_eq!(diagnostic.error_code().raw(), raw);
    }
}

#[test]
fn invalid_raw_error_codes_fail_closed_to_runtime_internal() {
    let first_unknown = ORDERED_ERROR_CODES
        .last()
        .expect("public error-code registry is non-empty")
        .raw()
        .checked_add(1)
        .expect("public error-code registry retains an unknown successor");

    for raw in [0, first_unknown, u16::MAX] {
        let code = ErrorCode::from_raw(raw);

        assert_eq!(ErrorCode::known(raw), None);
        assert!(!code.is_known());
        assert_eq!(code.diagnostic_code(), DiagnosticCode::RuntimeInternal);
        assert_eq!(code.diagnostic_detail(), None);
        assert_eq!(code.class(), ErrorClass::Internal);

        let diagnostic = code.diagnostic(ErrorOrigin::Query);

        assert_eq!(diagnostic.code(), DiagnosticCode::RuntimeInternal);
        assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
        assert_eq!(diagnostic.detail(), None);
        assert_eq!(diagnostic.error_code(), ErrorCode::RUNTIME_INTERNAL);
    }
}

#[test]
fn from_parts_requires_detail_to_match_broad_code() {
    let detail = Some(DiagnosticDetail::UnsupportedSqlFeature {
        feature: SqlFeatureCode::Join,
    });

    assert_eq!(
        ErrorCode::from_parts(DiagnosticCode::QueryUnsupportedSqlFeature, detail),
        ErrorCode::SQL_FEATURE_JOIN
    );
    assert_eq!(
        ErrorCode::from_parts(DiagnosticCode::QueryPlan, detail),
        ErrorCode::QUERY_PLAN
    );
}

#[test]
fn detail_bearing_registry_entries_round_trip_directly() {
    assert!(!DETAIL_ERROR_CODES.is_empty());

    for &(code, diagnostic_code, detail) in DETAIL_ERROR_CODES {
        assert_eq!(ErrorCode::from_parts(diagnostic_code, Some(detail)), code);
        assert_eq!(code.diagnostic_code(), diagnostic_code);
        assert_eq!(code.diagnostic_detail(), Some(detail));
        assert_eq!(detail.diagnostic_code(), diagnostic_code);
    }
}

#[test]
fn diagnostic_detail_reports_generated_broad_code() {
    let detail = DiagnosticDetail::UnsupportedSqlFeature {
        feature: SqlFeatureCode::Join,
    };

    assert_eq!(
        detail.diagnostic_code(),
        DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(format!("{detail:?}"), "61");
}

#[test]
fn public_error_codes_reconstruct_shifted_details() {
    assert_eq!(
        ErrorCode::QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD.diagnostic_code(),
        DiagnosticCode::QueryUnknownAggregateTargetField
    );
    assert_eq!(
        ErrorCode::SQL_FEATURE_JOIN.diagnostic_detail(),
        Some(DiagnosticDetail::UnsupportedSqlFeature {
            feature: SqlFeatureCode::Join,
        })
    );
    assert_eq!(
        ErrorCode::QUERY_PROJECTION_NUMERIC_LITERAL_REQUIRED.diagnostic_detail(),
        Some(DiagnosticDetail::QueryProjection {
            reason: QueryProjectionCode::NumericLiteralRequired,
        })
    );
    assert_eq!(
        ErrorCode::QUERY_READ_PUBLIC_REQUIRES_LIMIT.diagnostic_detail(),
        Some(DiagnosticDetail::QueryReadAdmission {
            reason: QueryReadAdmissionCode::PublicQueryRequiresLimit,
        })
    );
    assert_eq!(
        ErrorCode::SQL_LOWERING_DISTINCT_ORDER_BY_PROJECTION.diagnostic_detail(),
        Some(DiagnosticDetail::SqlLowering {
            reason: SqlLoweringCode::DistinctOrderByProjection,
        })
    );
    assert_eq!(
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE.diagnostic_detail(),
        Some(DiagnosticDetail::SqlWriteBoundary {
            boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge,
        })
    );
    assert_eq!(
        ErrorCode::SQL_WRITE_RETURNING_ROWS_TOO_MANY.diagnostic_detail(),
        Some(DiagnosticDetail::SqlWriteBoundary {
            boundary: SqlWriteBoundaryCode::ReturningRowsTooMany,
        })
    );
}
