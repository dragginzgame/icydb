use super::*;
use crate::{
    db::schema::{FieldStorageDecode, LeafCodec, ScalarCodec},
    db::{
        commit::CommitSchemaFingerprint,
        schema::{
            AcceptedCatalogIdentity, AcceptedCompositeCatalog, AcceptedFieldKind,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            IdentityState, IdentityStateOwner, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    types::EntityTag,
};

fn plan() -> AcceptedInspectionPlan {
    let revision = AcceptedSchemaRevision::INITIAL;
    let identity = AcceptedCatalogIdentity::new(
        EntityTag::new(23),
        "tests::QuickEntity",
        "tests::QuickStore",
        revision,
        SchemaVersion::initial(),
        CommitSchemaFingerprint::from([0x44; 16]),
    );
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::QuickEntity".to_string(),
        "QuickEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    ));
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        revision,
    );

    AcceptedInspectionPlan::compile_relation_free_for_tests(
        identity,
        snapshot.into(),
        value_catalog,
    )
    .expect("accepted Quick plan should compile")
}

fn finding(plan: &AcceptedInspectionPlan) -> IntegrityFinding {
    IntegrityFinding {
        diagnostic_code: icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind: IntegrityFindingKind::MalformedRow,
        entity: IntegrityEntityIdentity::from_plan(plan),
        store_path: plan.identity().store_path().to_string(),
        phase: IntegrityPhase::Rows,
        verifier_family: IntegrityVerifierFamily::RowEnvelope,
        physical_key: vec![1],
        primary_key: None,
        field_paths: Vec::new(),
        value_path: None,
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: None,
        observed: None,
    }
}

#[test]
fn database_incarnation_rejects_zero_and_round_trips_current_bytes() {
    assert!(DatabaseIncarnationId::try_from_bytes([0; 16]).is_err());

    let identity = DatabaseIncarnationId::for_tests(7);
    assert_eq!(
        DatabaseIncarnationId::try_from_bytes(identity.to_bytes())
            .expect("nonzero incarnation should decode"),
        identity,
    );
}

#[test]
fn integrity_finding_wire_preserves_targeted_constraint_path() {
    let plan = plan();
    let mut finding = finding(&plan);
    let path = ConstraintValuePath::new(vec![
        crate::error::ConstraintValuePathComponent::RootField { field_id: 1 },
        crate::error::ConstraintValuePathComponent::ListElement { index: 2 },
    ]);
    finding.kind = IntegrityFindingKind::ConstraintViolation;
    finding.value_path = Some(Box::new(path.clone()));
    finding.constraint_id = Some(7);
    finding.constraint_name = Some("nested_limit".to_string());

    let bytes = icydb_schema::wire::encode_one(&finding).expect("integrity finding should encode");
    let decoded: IntegrityFinding =
        icydb_schema::wire::decode_one(&bytes).expect("integrity finding should decode");
    assert_eq!(decoded.value_path(), Some(&path));
    assert_eq!(decoded.constraint_id(), Some(7));
    assert_eq!(decoded.constraint_name(), Some("nested_limit"));
}

#[test]
fn quick_clean_result_binds_incarnation_and_accepted_plan_identity() {
    let plan = plan();
    let incarnation = DatabaseIncarnationId::for_tests(8);
    let result = QuickIntegrityAccumulator::new()
        .complete(&plan, incarnation)
        .expect("clean Quick accounting should remain valid");

    assert_eq!(result.status(), &QuickIntegrityStatus::CompleteClean);
    assert_eq!(result.database_incarnation_id(), incarnation);
    assert_eq!(result.accepted_schema_version(), 1);
    assert_eq!(result.accepted_schema_fingerprint(), [0x44; 16]);
    assert_eq!(result.total_findings(), 0);
    assert_eq!(result.omitted_findings(), 0);
}

#[test]
fn quick_findings_keep_a_bounded_prefix_and_exact_omitted_count() {
    let plan = plan();
    let mut accumulator = QuickIntegrityAccumulator::new();
    for _ in 0..=MAX_QUICK_RETURNED_FINDINGS {
        accumulator
            .record(finding(&plan))
            .expect("bounded test finding count should fit");
    }
    let result = accumulator
        .complete(&plan, DatabaseIncarnationId::for_tests(9))
        .expect("one-over-cap Quick accounting should remain valid");

    assert_eq!(result.status(), &QuickIntegrityStatus::CompleteWithFindings,);
    assert_eq!(result.total_findings(), 65);
    assert_eq!(result.findings().len(), MAX_QUICK_RETURNED_FINDINGS);
    assert_eq!(result.omitted_findings(), 1);
    assert_eq!(
        result.total_findings(),
        result.findings().len() as u64 + result.omitted_findings(),
    );
}

#[test]
fn quick_findings_at_the_exact_returned_cap_have_no_omissions() {
    let plan = plan();
    let mut accumulator = QuickIntegrityAccumulator::new();
    for _ in 0..MAX_QUICK_RETURNED_FINDINGS {
        accumulator
            .record(finding(&plan))
            .expect("exact-cap finding count should fit");
    }
    let result = accumulator
        .complete(&plan, DatabaseIncarnationId::for_tests(10))
        .expect("exact-cap Quick accounting should remain valid");

    assert_eq!(result.total_findings(), 64);
    assert_eq!(result.findings().len(), MAX_QUICK_RETURNED_FINDINGS);
    assert_eq!(result.omitted_findings(), 0);
}

#[test]
fn quick_resource_limit_preserves_identity_and_accumulated_findings() {
    let plan = plan();
    let incarnation = DatabaseIncarnationId::for_tests(10);
    let mut accumulator = QuickIntegrityAccumulator {
        total_findings: u64::MAX,
        findings: vec![finding(&plan)],
    };
    let diagnostic = accumulator
        .record(finding(&plan))
        .expect_err("counter overflow must refuse the next finding");
    let result = accumulator
        .resource_limited(&plan, incarnation, diagnostic.clone())
        .expect("retained accounting should remain valid after refusal");

    assert_eq!(
        result.status(),
        &QuickIntegrityStatus::ResourceLimited(diagnostic)
    );
    assert_eq!(result.database_incarnation_id(), incarnation);
    assert_eq!(result.accepted_schema_version(), 1);
    assert_eq!(result.accepted_schema_fingerprint(), [0x44; 16]);
    assert_eq!(result.total_findings(), u64::MAX);
    assert_eq!(result.omitted_findings(), u64::MAX - 1);
    assert_eq!(result.findings(), &[finding(&plan)]);
}

#[test]
fn quick_selected_authority_failure_is_not_a_clean_completion() {
    let plan = plan();
    let error = InternalError::accepted_row_constraint_program_corrupt();
    let result = uninspectable_quick_integrity(
        plan.identity(),
        DatabaseIncarnationId::for_tests(11),
        IntegrityAuthorityDiagnostic::from_internal(&error),
    );

    assert!(matches!(
        result.status(),
        QuickIntegrityStatus::Uninspectable(IntegrityAuthorityDiagnostic {
            class: IntegrityAuthorityClass::Corruption,
            ..
        }),
    ));
    assert_eq!(result.total_findings(), 0);
    assert_eq!(result.omitted_findings(), 0);
}

#[test]
fn quick_identity_inventory_rejects_active_retired_owner_collision_first() {
    let incarnation = DatabaseIncarnationId::for_tests(12);
    let owner = IdentityStateOwner::try_new(incarnation, EntityTag::new(31), FieldId::new(1))
        .expect("identity owner should admit");
    let active = IdentityState::new_active(owner, AcceptedFieldKind::Nat64)
        .expect("active identity state should admit");
    let retired = active.retire().expect("active state should retire");
    let mut owners = BTreeMap::new();

    record_quick_identity_owner(&mut owners, "tests::first", &active)
        .expect("the first owner should admit");
    let error = record_quick_identity_owner(&mut owners, "tests::second", &retired)
        .expect_err("an active/retired owner collision must reject");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}
