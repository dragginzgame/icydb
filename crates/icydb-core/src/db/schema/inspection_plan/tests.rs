use super::*;
use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision, FieldId,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaRowLayout,
            SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    types::EntityTag,
};

fn identity(
    revision: AcceptedSchemaRevision,
    fingerprint: CommitSchemaFingerprint,
) -> AcceptedCatalogIdentity {
    AcceptedCatalogIdentity::new(
        EntityTag::new(17),
        "tests::InspectionEntity",
        "tests::InspectionStore",
        revision,
        SchemaVersion::initial(),
        fingerprint,
    )
}

fn value_catalog(revision: AcceptedSchemaRevision) -> AcceptedValueCatalogHandle {
    AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        revision,
    )
}

fn snapshot() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::InspectionEntity".to_string(),
        "InspectionEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_test_field(
            1,
            "id",
            0,
            AcceptedFieldKind::Nat64,
            false,
        )],
    ))
}

#[test]
fn accepted_inspection_plan_compiles_the_write_admission_program_once() {
    let revision = AcceptedSchemaRevision::INITIAL;
    let identity = identity(revision, [0x11; 16]);

    let plan = AcceptedInspectionPlan::compile_relation_free_for_tests(
        identity.clone(),
        Rc::new(snapshot()),
        value_catalog(revision),
    )
    .expect("verified accepted inputs should compile one inspection plan");

    assert_eq!(plan.identity(), identity);
    assert!(!plan.write_constraints().is_empty());
    assert_eq!(plan.write_constraints().required_slots(), &[0]);
    assert_eq!(plan.write_constraints().integrity_constraint_count(), 0);
    assert_ne!(plan.fingerprint().to_bytes(), [0; 32]);
}

#[test]
fn accepted_inspection_plan_fingerprint_binds_schema_and_root_identity() {
    let revision = AcceptedSchemaRevision::INITIAL;
    let baseline = accepted_inspection_plan_fingerprint(&identity(revision, [0x11; 16]), {
        AcceptedSchemaFingerprint::new([0x22; 32])
    });

    assert_eq!(
        baseline,
        accepted_inspection_plan_fingerprint(&identity(revision, [0x11; 16]), {
            AcceptedSchemaFingerprint::new([0x22; 32])
        }),
    );
    assert_ne!(
        baseline,
        accepted_inspection_plan_fingerprint(&identity(revision, [0x33; 16]), {
            AcceptedSchemaFingerprint::new([0x22; 32])
        }),
    );
    assert_ne!(
        baseline,
        accepted_inspection_plan_fingerprint(&identity(revision, [0x11; 16]), {
            AcceptedSchemaFingerprint::new([0x44; 32])
        }),
    );
}

#[test]
fn accepted_inspection_plan_rejects_mismatched_catalog_revision() {
    let error = AcceptedInspectionPlan::compile_relation_free_for_tests(
        identity(AcceptedSchemaRevision::INITIAL, [0x11; 16]),
        Rc::new(snapshot()),
        value_catalog(AcceptedSchemaRevision::new(2)),
    )
    .expect_err("a plan must not combine different accepted revisions");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
}
