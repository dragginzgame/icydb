use crate::{
    diagnostic::StyleDiagnosticCode,
    plan::{PlanRoute, PlanRouteKind},
};

#[test]
fn route_labels_are_normalized() {
    let route = PlanRoute::new(PlanRouteKind::CatalogRead, " players ")
        .expect("trimmed route labels should be valid");

    assert_eq!(route.label(), "players");
    assert!(!route.kind().is_write());
}

#[test]
fn empty_route_labels_return_typed_diagnostic() {
    let err = PlanRoute::new(PlanRouteKind::CatalogRead, " ")
        .expect_err("empty route labels should fail");

    assert_eq!(err.code(), StyleDiagnosticCode::EmptyPlanRoute);
}
