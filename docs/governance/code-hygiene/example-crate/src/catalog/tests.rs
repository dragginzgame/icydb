//! Boundary-level tests for catalog admission and accepted snapshot ownership.

use crate::{
    catalog::{CatalogAdmission, CatalogExample},
    diagnostic::StyleDiagnosticCode,
};

#[test]
fn admits_snapshot_through_catalog_owner() {
    let mut catalog = CatalogExample::default();

    let report = catalog
        .admit("players", 7)
        .expect("valid admission should succeed");

    assert_eq!(report.admission().entity_name(), "players");
    assert_eq!(report.route().label(), "players");
    assert_eq!(catalog.snapshot_entity_name("players"), Some("players"));
    assert_eq!(catalog.snapshot_version("players"), Some(7));
}

#[test]
fn rejects_empty_entity_name_without_matching_messages() {
    let err = CatalogAdmission::new("   ", 1).expect_err("blank names should fail");

    assert_eq!(err.code(), StyleDiagnosticCode::EmptyEntityName);
}
