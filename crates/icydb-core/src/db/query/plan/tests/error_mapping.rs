use crate::db::{
    access::AccessPlanError,
    cursor::CursorPlanError,
    query::plan::validate::{OrderPlanError, PlanError, PolicyPlanError},
};

#[test]
fn plan_error_from_order_maps_to_order_domain_variant() {
    let err = PlanError::from(OrderPlanError::UnorderableField {
        field: "rank".to_string(),
    });

    assert!(matches!(
        err,
        PlanError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::UnorderableField { field } if field == "rank"
            )
    ));
}

#[test]
fn plan_error_from_access_maps_to_access_domain_variant() {
    let err = PlanError::from(AccessPlanError::InvalidKeyRange);

    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::InvalidKeyRange
    )));
}

#[test]
fn plan_error_from_policy_maps_to_policy_domain_variant() {
    let err = PlanError::from(PolicyPlanError::UnorderedPagination);

    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::UnorderedPagination
    )));
}

#[test]
fn plan_error_from_cursor_maps_to_cursor_domain_variant() {
    let err = PlanError::from(CursorPlanError::ContinuationCursorBoundaryArityMismatch {
        expected: 2,
        found: 1,
    });

    assert!(matches!(
        err,
        PlanError::Cursor(inner)
            if matches!(
                inner.as_ref(),
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
    ));
}
