//! Module: error::tests
//! Responsibility: module-local ownership and contracts for error::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::{
    access::AccessPlanError,
    cursor::CursorPlanError,
    query::plan::{
        PlanError, PolicyPlanError,
        validate::{GroupPlanError, OrderPlanError, PlanPolicyError, PlanUserError},
    },
};

#[test]
fn index_plan_index_corruption_uses_index_origin() {
    let err = InternalError::index_plan_index_corruption("broken key payload");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Index);
    assert_eq!(
        err.message,
        "corruption detected (index): broken key payload"
    );
}

#[test]
fn index_plan_store_corruption_uses_store_origin() {
    let err = InternalError::index_plan_store_corruption("row/key mismatch");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(err.message, "corruption detected (store): row/key mismatch");
}

#[test]
fn index_plan_serialize_corruption_uses_serialize_origin() {
    let err = InternalError::index_plan_serialize_corruption("decode failed");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert_eq!(
        err.message,
        "corruption detected (serialize): decode failed"
    );
}

#[test]
fn serialize_incompatible_persisted_format_uses_serialize_origin() {
    let err = InternalError::serialize_incompatible_persisted_format("row format version 7");
    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert_eq!(err.message, "row format version 7");
}

#[test]
fn index_plan_store_invariant_uses_store_origin() {
    let err = InternalError::index_plan_store_invariant("row/key mismatch");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.message,
        "invariant violation detected (store): row/key mismatch"
    );
}

#[test]
fn query_executor_invariant_uses_invariant_violation_class() {
    let err = crate::db::error::query_executor_invariant("route contract mismatch");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn query_unsupported_sql_feature_preserves_query_detail_label() {
    let err = InternalError::query_unsupported_sql_feature("JOIN");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert!(
        matches!(
            err.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                if *feature == "JOIN"
        ),
        "query unsupported SQL feature helper should preserve structured feature label detail",
    );
}

#[test]
fn executor_access_plan_error_mapping_stays_invariant_violation() {
    let err = crate::db::error::from_executor_access_plan_error(AccessPlanError::IndexPrefixEmpty);
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn plan_policy_error_mapping_uses_executor_invariant_prefix() {
    let err = crate::db::error::plan_invariant_violation(PolicyPlanError::DeleteLimitRequiresOrder);
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert_eq!(
        err.message,
        "executor invariant violated: delete limit requires explicit ordering",
    );
}

#[test]
fn group_plan_error_mapping_uses_invalid_logical_plan_prefix() {
    let err = crate::db::error::from_group_plan_error(PlanError::from(
        GroupPlanError::UnknownGroupField {
            field: "tenant".to_string(),
        },
    ));

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert_eq!(
        err.message,
        "invalid logical plan: unknown group field 'tenant'",
    );
}

#[test]
fn group_plan_error_mapping_rejects_non_group_user_variant() {
    let err = crate::db::error::from_group_plan_error(PlanError::from(PlanUserError::Order(
        Box::new(OrderPlanError::UnknownField {
            field: "tenant".to_string(),
        }),
    )));

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert!(
        err.message
            .contains("group-plan error conversion received non-group user variant"),
        "non-group user variant mapping should fail closed with explicit domain message: {err:?}",
    );
}

#[test]
fn group_plan_error_mapping_rejects_non_group_policy_variant() {
    let err = crate::db::error::from_group_plan_error(PlanError::from(PlanPolicyError::Policy(
        Box::new(PolicyPlanError::UnorderedPagination),
    )));

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert!(
        err.message
            .contains("group-plan error conversion received non-group policy variant"),
        "non-group policy variant mapping should fail closed with explicit domain message: {err:?}",
    );
}

#[test]
fn group_plan_error_mapping_rejects_cursor_variant() {
    let err = crate::db::error::from_group_plan_error(PlanError::from(
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset: 8,
            actual_offset: 3,
        },
    ));

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert!(
        err.message
            .contains("group-plan error conversion received cursor variant"),
        "cursor variant mapping should fail closed with explicit domain message: {err:?}",
    );
}

#[test]
fn cursor_plan_error_mapping_classifies_invalid_payload_as_unsupported() {
    let err = crate::db::error::from_cursor_plan_error(
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: "bad payload".to_string(),
        },
    );

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    assert!(err.message.contains("invalid continuation cursor"));
}

#[test]
fn cursor_plan_error_mapping_classifies_signature_mismatch_as_unsupported() {
    let err = crate::db::error::from_cursor_plan_error(
        CursorPlanError::ContinuationCursorSignatureMismatch {
            entity_path: "tests::Entity",
            expected: "aa".to_string(),
            actual: "bb".to_string(),
        },
    );

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
}

#[test]
fn cursor_plan_error_mapping_keeps_invariant_violation_class() {
    let err = crate::db::error::from_cursor_plan_error(
        CursorPlanError::ContinuationCursorInvariantViolation {
            reason: "runtime cursor contract violated".to_string(),
        },
    );

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    assert!(err.message.contains("runtime cursor contract violated"));
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
        let base = InternalError::classified(class, ErrorOrigin::Query, "base");
        let relabeled_message = base.with_message("updated");
        let reorigined = relabeled_message.with_origin(ErrorOrigin::Store);
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
            | CursorPlanError::ContinuationCursorVersionMismatch { .. }
            | CursorPlanError::ContinuationCursorSignatureMismatch { .. }
            | CursorPlanError::ContinuationCursorBoundaryArityMismatch { .. }
            | CursorPlanError::ContinuationCursorWindowMismatch { .. }
            | CursorPlanError::ContinuationCursorBoundaryTypeMismatch { .. }
            | CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { .. } => {
                ErrorClass::Unsupported
            }
            CursorPlanError::ContinuationCursorInvariantViolation { .. } => {
                ErrorClass::InvariantViolation
            }
        }
    }

    let cases = vec![
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: "payload".to_string(),
        },
        CursorPlanError::ContinuationCursorInvariantViolation {
            reason: "invariant".to_string(),
        },
        CursorPlanError::ContinuationCursorVersionMismatch { version: 9 },
        CursorPlanError::ContinuationCursorSignatureMismatch {
            entity_path: "tests::Entity",
            expected: "aabb".to_string(),
            actual: "ccdd".to_string(),
        },
        CursorPlanError::ContinuationCursorBoundaryArityMismatch {
            expected: 2,
            found: 1,
        },
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset: 10,
            actual_offset: 2,
        },
        CursorPlanError::ContinuationCursorBoundaryTypeMismatch {
            field: "rank".to_string(),
            expected: "u64".to_string(),
            value: crate::value::Value::Text("x".to_string()),
        },
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
            field: "id".to_string(),
            expected: "Ulid".to_string(),
            value: Some(crate::value::Value::Text("x".to_string())),
        },
    ];

    for cursor_err in cases {
        let expected_class = expected_class_from_cursor_variant(&cursor_err);
        let err = crate::db::error::from_cursor_plan_error(cursor_err);
        assert_eq!(err.origin, ErrorOrigin::Cursor);
        assert_eq!(
            err.class, expected_class,
            "cursor conversion class must remain stable for each cursor variant: {err:?}",
        );
    }
}

#[test]
fn classification_integrity_access_plan_conversion_stays_invariant() {
    let err = crate::db::error::from_executor_access_plan_error(AccessPlanError::InvalidKeyRange);

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn classification_integrity_corruption_constructors_never_downgrade() {
    let corruption_cases = [
        InternalError::store_corruption("store"),
        InternalError::index_corruption("index"),
        InternalError::serialize_corruption("serialize"),
        InternalError::identity_corruption("identity"),
        InternalError::index_plan_index_corruption("index-plan-index"),
        InternalError::index_plan_store_corruption("index-plan-store"),
        InternalError::index_plan_serialize_corruption("index-plan-serialize"),
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
