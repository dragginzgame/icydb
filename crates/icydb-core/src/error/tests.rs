//! Module: error::tests
//! Covers the error taxonomy mapping and constructor invariants defined by the
//! core error surface.

use std::mem::size_of;

use super::*;
use crate::db::{
    access::AccessPlanError,
    cursor::{CursorPayloadErrorCode, CursorPlanError, CursorSignaturePrefix},
    query::plan::{
        PlanError, PolicyPlanError,
        validate::{GroupPlanError, OrderPlanError, PlanPolicyError, PlanUserError},
    },
};

#[test]
fn internal_error_taxonomy_axes_remain_one_byte() {
    assert_eq!(size_of::<ErrorClass>(), 1);
    assert_eq!(size_of::<ErrorOrigin>(), 1);
    assert_eq!(format!("{:?}", ErrorClass::Corruption), "0");
    assert_eq!(format!("{:?}", ErrorOrigin::Serialize), "0");
}

fn from_group_plan_error(err: PlanError) -> InternalError {
    match err {
        PlanError::User(inner) => match *inner {
            PlanUserError::Group(_) => InternalError::query_invalid_logical_plan(),
            _ => InternalError::planner_executor_invariant(),
        },
        PlanError::Policy(inner) => match *inner {
            PlanPolicyError::Group(_) => InternalError::query_invalid_logical_plan(),
            PlanPolicyError::Policy(_) => InternalError::planner_executor_invariant(),
        },
        PlanError::Cursor(_) => InternalError::planner_executor_invariant(),
    }
}

fn plan_invariant_violation(err: PolicyPlanError) -> InternalError {
    let _ = err;
    InternalError::planner_executor_invariant()
}

fn assert_runtime_invariant(err: &InternalError, origin: ErrorOrigin) {
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, origin);

    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
    );
    assert_eq!(diagnostic.origin(), origin.diagnostic_origin());
    assert_eq!(diagnostic.detail(), None);
}

fn assert_runtime_corruption(err: &InternalError, origin: ErrorOrigin) {
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, origin);

    let diagnostic = err.diagnostic();
    let expected_code = if matches!(origin, ErrorOrigin::Store) {
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption
    } else {
        icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption
    };
    assert_eq!(diagnostic.code(), expected_code);
    assert_eq!(diagnostic.origin(), origin.diagnostic_origin());
}

const fn cursor_payload_error() -> CursorPlanError {
    CursorPlanError::InvalidContinuationCursorPayload {
        reason: CursorPayloadErrorCode::UNKNOWN,
        index: None,
    }
}

const fn cursor_signature_mismatch_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorSignatureMismatch {
        expected: CursorSignaturePrefix::UNKNOWN,
        actual: CursorSignaturePrefix::UNKNOWN,
    }
}

const fn cursor_boundary_arity_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorBoundaryArityMismatch {
        expected: 2,
        found: 1,
    }
}

const fn cursor_window_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorWindowMismatch {
        expected_offset: 4,
        actual_offset: 2,
    }
}

const fn cursor_boundary_type_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorBoundaryTypeMismatch { index: 0 }
}

const fn cursor_primary_key_type_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { index: Some(1) }
}

#[test]
fn index_plan_index_corruption_uses_index_origin() {
    let err = InternalError::index_plan_index_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Index);
}

#[test]
fn index_plan_store_corruption_uses_store_origin() {
    let err = InternalError::index_plan_store_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Store);
}

#[test]
fn index_plan_serialize_corruption_uses_serialize_origin() {
    let err = InternalError::index_plan_serialize_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Serialize);
}

#[test]
fn serialize_incompatible_persisted_format_uses_serialize_origin() {
    let err = InternalError::serialize_incompatible_persisted_format();
    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeIncompatiblePersistedFormat,
    );
}

#[test]
fn index_plan_store_invariant_uses_store_origin() {
    let err = InternalError::index_plan_store_invariant();
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
}

#[test]
fn query_executor_invariant_uses_invariant_violation_class() {
    let err = InternalError::query_executor_invariant();
    assert_runtime_invariant(&err, ErrorOrigin::Query);
}

#[test]
fn cursor_executor_invariant_uses_cursor_origin() {
    let err = InternalError::cursor_executor_invariant();
    assert_runtime_invariant(&err, ErrorOrigin::Cursor);
}

#[test]
fn query_unsupported_uses_query_origin() {
    let err = InternalError::query_unsupported();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_unsupported_sql_feature_preserves_query_detail_label() {
    let err =
        InternalError::query_unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert!(
        matches!(
            err.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                if feature == &icydb_diagnostic_code::SqlFeatureCode::Join
        ),
        "query unsupported SQL feature helper should preserve structured feature code detail",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_unsupported_sql_feature_exposes_compact_diagnostic_detail() {
    let err =
        InternalError::query_unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query
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

#[cfg(feature = "sql")]
#[test]
fn query_sql_lowering_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_lowering(
        icydb_diagnostic_code::SqlLoweringCode::DistinctOrderByProjection,
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlLowering {
            reason: icydb_diagnostic_code::SqlLoweringCode::DistinctOrderByProjection,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_sql_surface_mismatch_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_surface_mismatch(
        icydb_diagnostic_code::SqlSurfaceMismatchCode::QueryRejectsInsert,
    );
    let diagnostic = err.diagnostic();

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
fn query_sql_write_boundary_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_write_boundary(
        icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_schema_ddl_admission_exposes_compact_diagnostic_detail() {
    let err =
        InternalError::query_schema_ddl_admission(SchemaDdlAdmissionError::PublicationRaceLost);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            }
        ),
    );
}

#[test]
fn schema_ddl_publication_race_exposes_compact_admission_detail() {
    let err = InternalError::schema_ddl_publication_race_lost("User");
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Store
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            }
        ),
    );
}

#[test]
fn internal_error_without_detail_uses_class_origin_compact_code() {
    let err = InternalError::classified(ErrorClass::InvariantViolation, ErrorOrigin::Planner);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Planner
    );
    assert_eq!(diagnostic.detail(), None);
}

#[test]
fn executor_access_plan_error_mapping_stays_invariant_violation() {
    let err = AccessPlanError::IndexPrefixEmpty.into_internal_error();
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn plan_policy_error_mapping_uses_runtime_invariant_code() {
    let err = plan_invariant_violation(PolicyPlanError::DeleteWindowRequiresOrder);
    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_uses_runtime_invariant_code() {
    let err = from_group_plan_error(PlanError::from(GroupPlanError::UnknownGroupField {
        field: "tenant".to_string(),
    }));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_non_group_user_variant() {
    let err = from_group_plan_error(PlanError::from(PlanUserError::Order(Box::new(
        OrderPlanError::UnknownField { term_index: 0 },
    ))));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_non_group_policy_variant() {
    let err = from_group_plan_error(PlanError::from(PlanPolicyError::Policy(Box::new(
        PolicyPlanError::UnorderedPagination,
    ))));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_cursor_variant() {
    let err = from_group_plan_error(PlanError::from(cursor_window_error()));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn cursor_plan_error_mapping_classifies_invalid_payload_as_e6() {
    let err = cursor_payload_error().into_internal_error();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Cursor,
    );
    assert_eq!(diagnostic.detail(), None);
    assert_eq!(
        diagnostic.error_code(),
        icydb_diagnostic_code::ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
    );
}

#[test]
fn cursor_plan_error_mapping_classifies_signature_mismatch_as_unsupported() {
    let err = cursor_signature_mismatch_error().into_internal_error();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    assert_eq!(
        err.diagnostic().code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
}

#[test]
fn cursor_plan_error_mapping_keeps_invariant_violation_class() {
    let err = CursorPlanError::ContinuationCursorInvariantViolation.into_internal_error();

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Cursor,
    );
    assert_eq!(diagnostic.detail(), None);
}

#[test]
fn classification_integrity_helpers_preserve_error_class() {
    let classes = [
        ErrorClass::Corruption,
        ErrorClass::IncompatiblePersistedFormat,
        ErrorClass::NotFound,
        ErrorClass::Internal,
        ErrorClass::Conflict,
        ErrorClass::Unsupported,
        ErrorClass::InvariantViolation,
    ];

    for class in classes {
        let base = InternalError::classified(class, ErrorOrigin::Query);
        let reorigined = base.with_origin(ErrorOrigin::Store);
        assert_eq!(
            reorigined.class, class,
            "class must be preserved across helper relabeling operations",
        );
    }
}

#[test]
fn classification_integrity_cursor_conversion_matrix_is_restricted() {
    fn expected_class_from_cursor_variant(err: &CursorPlanError) -> ErrorClass {
        match err {
            CursorPlanError::InvalidContinuationCursor { .. }
            | CursorPlanError::InvalidContinuationCursorPayload { .. }
            | CursorPlanError::ContinuationCursorSignatureMismatch { .. }
            | CursorPlanError::ContinuationCursorBoundaryArityMismatch { .. }
            | CursorPlanError::ContinuationCursorWindowMismatch { .. }
            | CursorPlanError::ContinuationCursorBoundaryTypeMismatch { .. }
            | CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { .. } => {
                ErrorClass::Unsupported
            }
            CursorPlanError::ContinuationCursorInvariantViolation => ErrorClass::InvariantViolation,
        }
    }

    let cases = vec![
        cursor_payload_error(),
        CursorPlanError::ContinuationCursorInvariantViolation,
        cursor_signature_mismatch_error(),
        cursor_boundary_arity_error(),
        cursor_window_error(),
        cursor_boundary_type_error(),
        cursor_primary_key_type_error(),
    ];

    for cursor_err in cases {
        let expected_class = expected_class_from_cursor_variant(&cursor_err);
        let expected_code = match expected_class {
            ErrorClass::Unsupported => {
                icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
            }
            ErrorClass::InvariantViolation => {
                icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
            }
            _ => unreachable!("cursor conversion matrix only permits two error classes"),
        };
        let err = cursor_err.into_internal_error();
        assert_eq!(err.origin, ErrorOrigin::Cursor);
        assert_eq!(
            err.class, expected_class,
            "cursor conversion class must remain stable for each cursor variant: {err:?}",
        );
        assert_eq!(
            err.diagnostic().code(),
            expected_code,
            "cursor conversion diagnostic must remain stable for each cursor variant: {err:?}",
        );
    }
}

#[test]
fn classification_integrity_access_plan_conversion_stays_invariant() {
    let err = AccessPlanError::InvalidKeyRange.into_internal_error();

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn classification_integrity_corruption_constructors_never_downgrade() {
    let corruption_cases = [
        InternalError::store_corruption(),
        InternalError::index_corruption(),
        InternalError::serialize_corruption(),
        InternalError::identity_corruption(),
        InternalError::index_plan_index_corruption(),
        InternalError::index_plan_store_corruption(),
        InternalError::index_plan_serialize_corruption(),
    ];

    for err in corruption_cases {
        assert_eq!(
            err.class,
            ErrorClass::Corruption,
            "corruption constructors must remain corruption-classed",
        );
        assert!(
            !matches!(err.class, ErrorClass::Unsupported),
            "corruption constructors must never downgrade to unsupported",
        );
    }
}

#[test]
fn mutation_unknown_field_uses_compact_executor_invariant() {
    let err = InternalError::mutation_structural_field_unknown("tests::User", "missing_name");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[test]
fn mutation_invalid_result_uses_compact_executor_invariant() {
    let err =
        InternalError::mutation_structural_after_image_invalid("tests::User", "abc123", "detail");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}
