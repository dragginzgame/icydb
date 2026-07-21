use super::{
    SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionReason,
    classify_schema_admission_rejection, schema_admission_rejection,
};
use crate::{
    db::schema::{
        AcceptedFieldKind, FieldId, MutationPublicationPreflight, PersistedFieldOrigin,
        PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
        SchemaFieldSlot, SchemaFieldWritePolicy, SchemaInsertDefault, SchemaRowLayout,
        SchemaTransitionDecision, SchemaTransitionPlanKind, SchemaVersion,
        decide_schema_transition, derive_generated_accepted_candidate,
        transition::SchemaTransitionRejectionKind,
    },
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
};

// Build the stable two-field snapshot used by transition-policy tests.
// Keeping the fixture local avoids depending on reconciliation test entities.
fn expected_snapshot() -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::SchemaReconcileEntity".to_string(),
        "SchemaReconcileEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    )
}

fn accepted_generated_additive_candidate(
    stored: &PersistedSchemaSnapshot,
    generated: PersistedSchemaSnapshot,
) -> PersistedSchemaSnapshot {
    derive_generated_accepted_candidate(stored, &generated)
        .expect("test layout version should advance")
        .expect("generated append-only fixture should derive accepted temporal facts")
}

// Preserve the expected snapshot shape except for entity name so tests can
// assert that transition diagnostics report the first rejected identity fact.
fn changed_entity_name_snapshot(expected: &PersistedSchemaSnapshot) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        "ChangedSchemaReconcileEntity".to_string(),
        expected.first_primary_key_field_id(),
        expected.row_layout().clone(),
        expected.fields().to_vec(),
    )
}

fn snapshot_with_version(
    snapshot: &PersistedSchemaSnapshot,
    version: SchemaVersion,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.primary_key_field_ids().to_vec(),
        snapshot.row_layout().clone(),
        snapshot.fields().to_vec(),
        snapshot.indexes().to_vec(),
    )
    .with_relations(snapshot.relations().to_vec())
}

fn snapshot_with_ddl_nickname_field(
    snapshot: &PersistedSchemaSnapshot,
    version: SchemaVersion,
) -> PersistedSchemaSnapshot {
    let mut fields = snapshot.fields().to_vec();
    fields.push(
        PersistedFieldSnapshot::new_initial_with_write_policy_and_origin(
            FieldId::new(3),
            "nickname".to_string(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Text { max_len: None },
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::none(),
            PersistedFieldOrigin::SqlDdl,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ),
    );

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        fields,
        snapshot.indexes().to_vec(),
    )
    .with_relations(snapshot.relations().to_vec())
}

fn snapshot_with_renamed_name_field(
    snapshot: &PersistedSchemaSnapshot,
    name: &str,
) -> PersistedSchemaSnapshot {
    let mut changed_fields = snapshot.fields().to_vec();
    changed_fields[1] = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        name.to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );

    PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        snapshot.row_layout().clone(),
        changed_fields,
    )
}

fn snapshot_with_name_kind(
    snapshot: &PersistedSchemaSnapshot,
    kind: AcceptedFieldKind,
) -> PersistedSchemaSnapshot {
    let mut fields = snapshot.fields().to_vec();
    fields[1] = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "name".to_string(),
        SchemaFieldSlot::new(1),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );

    PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        snapshot.row_layout().clone(),
        fields,
    )
}

#[test]
fn admission_identity_carries_version_method_and_shape_fingerprint() {
    let stored = expected_snapshot();
    let candidate = snapshot_with_version(&stored, SchemaVersion::new(2));
    let comparison = SchemaAdmissionIdentityComparison::from_snapshots(&stored, &candidate)
        .expect("admission identity should hash snapshots");

    assert_eq!(comparison.stored.schema_version, SchemaVersion::initial());
    assert_eq!(comparison.candidate.schema_version, SchemaVersion::new(2));
    assert_eq!(
        comparison.stored.fingerprint_method_version,
        comparison.candidate.fingerprint_method_version,
    );
    assert_eq!(
        comparison.stored.schema_fingerprint, comparison.candidate.schema_fingerprint,
        "schema_version must not be part of the admission shape fingerprint",
    );
}

#[test]
fn admission_identity_fingerprint_changes_with_accepted_shape() {
    let stored = expected_snapshot();
    let changed = snapshot_with_renamed_name_field(&stored, "display_name");
    let comparison = SchemaAdmissionIdentityComparison::from_snapshots(&stored, &changed)
        .expect("admission identity should hash snapshots");

    assert_ne!(
        comparison.stored.schema_fingerprint, comparison.candidate.schema_fingerprint,
        "field-name changes are accepted-shape changes for admission",
    );
}

#[test]
fn admission_matrix_accepts_same_identity_and_single_version_shape_change() {
    let stored = expected_snapshot();
    let same = expected_snapshot();
    let changed = snapshot_with_version(
        &snapshot_with_renamed_name_field(&stored, "display_name"),
        SchemaVersion::new(2),
    );

    assert!(
        schema_admission_rejection(
            SchemaAdmissionIdentityComparison::from_snapshots(&stored, &same)
                .expect("same admission identity should hash"),
        )
        .is_none(),
        "same version, method, and shape should enter compatibility classification",
    );
    assert!(
        schema_admission_rejection(
            SchemaAdmissionIdentityComparison::from_snapshots(&stored, &changed)
                .expect("changed admission identity should hash"),
        )
        .is_none(),
        "exactly N+1 with changed shape should enter compatibility classification",
    );
}

#[test]
fn admission_matrix_rejects_missing_bump_empty_bump_gap_and_rollback() {
    let stored = expected_snapshot();
    let changed_without_bump = snapshot_with_renamed_name_field(&stored, "display_name");
    let empty_bump = snapshot_with_version(&stored, SchemaVersion::new(2));
    let version_gap = snapshot_with_version(&changed_without_bump, SchemaVersion::new(3));
    let rollback = snapshot_with_version(&stored, SchemaVersion::new(0));

    for (candidate, expected_reason, expected_next) in [
        (
            changed_without_bump,
            SchemaAdmissionRejectionReason::MissingVersionBump,
            None,
        ),
        (
            empty_bump,
            SchemaAdmissionRejectionReason::EmptyVersionBump,
            None,
        ),
        (
            version_gap,
            SchemaAdmissionRejectionReason::VersionGap,
            Some(2),
        ),
        (
            rollback,
            SchemaAdmissionRejectionReason::VersionRollback,
            None,
        ),
    ] {
        let comparison = SchemaAdmissionIdentityComparison::from_snapshots(&stored, &candidate)
            .expect("admission identity should hash");
        let classification = classify_schema_admission_rejection(comparison)
            .expect("candidate should fail admission classification");
        assert_eq!(classification.reason, expected_reason);
        assert_eq!(classification.expected_next, expected_next);

        let rejection =
            schema_admission_rejection(comparison).expect("candidate should fail admission");
        assert_eq!(
            rejection.admission(),
            Some(classification),
            "schema-version rejection should retain the structured admission classification",
        );

        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::SchemaVersion
        );
        assert!(
            rejection.detail().contains("stored_version=1")
                && rejection.detail().contains("candidate_version=")
                && rejection.detail().contains("stored_method=")
                && rejection.detail().contains("candidate_method=")
                && rejection.detail().contains("stored_fingerprint=")
                && rejection.detail().contains("candidate_fingerprint="),
            "schema admission diagnostics should include compared identity facts, got '{}'",
            rejection.detail(),
        );
        if expected_reason == SchemaAdmissionRejectionReason::VersionGap {
            assert!(
                rejection.detail().contains("expected_next=2"),
                "version-gap diagnostics should include the expected next version, got '{}'",
                rejection.detail(),
            );
        }
    }
}

#[test]
fn admission_matrix_rejects_fingerprint_method_mismatch() {
    let stored = expected_snapshot();
    let candidate = snapshot_with_version(
        &snapshot_with_renamed_name_field(&stored, "display_name"),
        SchemaVersion::new(2),
    );
    let mut comparison = SchemaAdmissionIdentityComparison::from_snapshots(&stored, &candidate)
        .expect("admission identity should hash");
    comparison.candidate.fingerprint_method_version = comparison
        .candidate
        .fingerprint_method_version
        .saturating_add(1);
    let classification = classify_schema_admission_rejection(comparison)
        .expect("method mismatch should fail admission classification");
    assert_eq!(
        classification.reason,
        SchemaAdmissionRejectionReason::FingerprintMethodMismatch,
    );
    assert_eq!(classification.expected_next, None);

    let rejection = schema_admission_rejection(comparison).expect("method mismatch should reject");
    assert_eq!(
        rejection.admission(),
        Some(classification),
        "method mismatch rejection should retain the structured admission classification",
    );

    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::SchemaVersion
    );
    assert!(
        rejection.detail().contains("stored_method=")
            && rejection.detail().contains("candidate_method=")
            && rejection.detail().contains("stored_fingerprint=")
            && rejection.detail().contains("candidate_fingerprint="),
        "method mismatch diagnostics should include compared identity facts, got '{}'",
        rejection.detail(),
    );
}

fn name_field_path_index(name: &str) -> PersistedIndexSnapshot {
    name_field_path_index_in_store(name, format!("test::SchemaReconcileEntity::{name}"))
}

fn name_field_path_index_in_store(name: &str, store: String) -> PersistedIndexSnapshot {
    named_field_path_index_with_ordinal(name, 1, store)
}

fn named_field_path_index_with_ordinal(
    name: &str,
    ordinal: u16,
    store: String,
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        ordinal,
        name.to_string(),
        store,
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        )]),
        None,
    )
}

fn snapshot_with_indexes(
    snapshot: &PersistedSchemaSnapshot,
    indexes: Vec<PersistedIndexSnapshot>,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_indexes(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        snapshot.row_layout().clone(),
        snapshot.fields().to_vec(),
        indexes,
    )
}

#[test]
fn schema_transition_policy_accepts_metadata_only_generated_index_rename() {
    let base = expected_snapshot();
    let store = "test::SchemaReconcileEntity::name_index".to_string();
    let stored = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "SchemaReconcileEntity|name",
            store.clone(),
        )],
    );
    let generated = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "idx_schema_reconcile_entity__name",
            store,
        )],
    );

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &generated)
    else {
        panic!("index name-only drift should be a metadata-only accepted transition");
    };

    assert_eq!(
        plan.kind(),
        SchemaTransitionPlanKind::MetadataOnlyIndexRename
    );
    assert_eq!(
        plan.publication_preflight(),
        MutationPublicationPreflight::PublishableNow,
    );
}

#[test]
fn schema_transition_policy_accepts_generated_index_rename_with_extra_ddl_indexes() {
    let base = expected_snapshot();
    let generated_store = "test::SchemaReconcileEntity::name_index".to_string();
    let ddl_index = named_field_path_index_with_ordinal(
        "ddl_name_idx",
        2,
        "test::SchemaReconcileEntity::ddl_name_idx".to_string(),
    );
    let stored = snapshot_with_indexes(
        &base,
        vec![
            name_field_path_index_in_store("SchemaReconcileEntity|name", generated_store.clone()),
            ddl_index,
        ],
    );
    let generated = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "idx_schema_reconcile_entity__name",
            generated_store,
        )],
    );

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &generated)
    else {
        panic!("generated index rename with extra DDL index should be metadata-only accepted");
    };

    assert_eq!(
        plan.kind(),
        SchemaTransitionPlanKind::MetadataOnlyIndexRename
    );
    assert_eq!(
        plan.publication_preflight(),
        MutationPublicationPreflight::PublishableNow,
    );
}

#[test]
fn schema_transition_policy_accepts_exact_snapshot_match() {
    let expected = expected_snapshot();

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&expected, &expected)
    else {
        panic!("exact snapshot match should produce an accepted transition plan");
    };
    assert_eq!(plan.kind(), SchemaTransitionPlanKind::ExactMatch);

    let changed = changed_entity_name_snapshot(&expected);
    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&changed, &expected)
    else {
        panic!("changed schema snapshot should be rejected");
    };
    assert!(
        rejection
            .detail()
            .contains("entity name changed: stored='ChangedSchemaReconcileEntity' generated='SchemaReconcileEntity'"),
        "transition rejection should retain the first schema mismatch detail",
    );
}

#[test]
fn schema_transition_policy_accepts_supported_ddl_indexes_absent_from_generated_model() {
    let generated = expected_snapshot();
    let accepted = snapshot_with_indexes(&generated, vec![name_field_path_index("name_idx")]);
    let accepted = snapshot_with_version(&accepted, SchemaVersion::new(2));

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&accepted, &generated)
    else {
        panic!("supported accepted DDL index should remain compatible with generated metadata");
    };

    assert_eq!(plan.kind(), SchemaTransitionPlanKind::ExactMatch);
    assert_eq!(
        plan.publication_preflight(),
        MutationPublicationPreflight::PublishableNow,
    );
}

#[test]
fn schema_transition_policy_accepts_supported_ddl_fields_absent_from_generated_model() {
    let generated = expected_snapshot();
    let accepted = snapshot_with_ddl_nickname_field(&generated, SchemaVersion::new(2));

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&accepted, &generated)
    else {
        panic!("supported accepted DDL field should remain compatible with generated metadata");
    };

    assert_eq!(plan.kind(), SchemaTransitionPlanKind::ExactMatch);
    assert_eq!(
        plan.publication_preflight(),
        MutationPublicationPreflight::PublishableNow,
    );
}

#[test]
fn schema_transition_policy_accepts_append_only_fields() {
    let stored = expected_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "nickname".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    ));
    let generated = accepted_generated_additive_candidate(
        &stored,
        PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.first_primary_key_field_id(),
            SchemaRowLayout::initial(vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ]),
            generated_fields,
        ),
    );

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &generated)
    else {
        panic!("append-only nullable generated field should be an accepted transition");
    };

    assert_eq!(plan.kind(), SchemaTransitionPlanKind::AppendOnlyFields);
    assert_eq!(
        plan.publication_preflight(),
        MutationPublicationPreflight::PublishableNow
    );
}

#[test]
fn schema_transition_policy_rejects_existing_enum_field_type_rebind() {
    let base = expected_snapshot();
    let stored = snapshot_with_name_kind(
        &base,
        AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
        },
    );
    let rebound = snapshot_with_name_kind(
        &base,
        AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(2).expect("test enum type ID should be valid"),
        },
    );

    assert!(matches!(
        decide_schema_transition(&stored, &rebound),
        SchemaTransitionDecision::Rejected(_)
    ));
}

#[test]
fn schema_transition_policy_accepts_append_only_defaulted_fields() {
    let stored = expected_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::SlotPayload(vec![0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0]),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let generated = accepted_generated_additive_candidate(
        &stored,
        PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.first_primary_key_field_id(),
            SchemaRowLayout::initial(vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ]),
            generated_fields,
        ),
    );

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &generated)
    else {
        panic!("append-only defaulted generated field should be an accepted transition");
    };

    assert_eq!(plan.kind(), SchemaTransitionPlanKind::AppendOnlyFields);
}

#[test]
fn schema_transition_policy_rejects_malformed_append_only_default_payloads() {
    let stored = expected_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::SlotPayload(vec![0x00]),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let generated = accepted_generated_additive_candidate(
        &stored,
        PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.first_primary_key_field_id(),
            SchemaRowLayout::initial(vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ]),
            generated_fields,
        ),
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&stored, &generated)
    else {
        panic!("malformed append-only default payload should be rejected");
    };

    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::FieldContract
    );
    assert!(
        rejection
            .detail()
            .contains("field must be nullable without a default or carry a valid explicit persisted default payload"),
        "unexpected malformed default payload rejection detail: {}",
        rejection.detail(),
    );
}

#[test]
fn schema_transition_policy_reports_row_layout_mismatch_after_entity_identity() {
    let expected = expected_snapshot();
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(1)),
            (FieldId::new(2), SchemaFieldSlot::new(0)),
        ]),
        expected.fields().to_vec(),
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&changed, &expected)
    else {
        panic!("changed row layout should be rejected");
    };

    assert!(
        rejection.detail().contains("row layout changed"),
        "row-layout drift should be reported before field metadata drift",
    );
    assert!(
        rejection
            .detail()
            .contains("stored_fields=2 generated_fields=2"),
        "row-layout drift should summarize layout sizes",
    );
    assert!(
        rejection.detail().contains(
            "first_difference=row_layout[0] stored_field_id=1 stored_slot=1 stored_name='id' stored_kind=Ulid; generated_field_id=1 generated_slot=0 generated_name='id' generated_kind=Ulid"
        ),
        "row-layout drift should identify the first changed field/slot pair",
    );
    assert!(
        !rejection.detail().contains("SchemaRowLayout"),
        "row-layout drift should not dump raw layout debug output",
    );
}

#[test]
fn schema_transition_policy_rejects_primary_key_field_changes() {
    let expected = expected_snapshot();
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        FieldId::new(2),
        expected.row_layout().clone(),
        expected.fields().to_vec(),
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&changed, &expected)
    else {
        panic!("primary-key field drift should be rejected");
    };

    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::EntityIdentity
    );
    assert!(
        rejection
            .detail()
            .contains("primary key field ids changed: stored=[2] generated=[1]"),
        "primary-key drift should be identified before row decode can run",
    );
}

#[test]
fn schema_transition_policy_rejects_field_type_changes() {
    let expected = expected_snapshot();
    let mut changed_fields = expected.fields().to_vec();
    changed_fields[1] = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "name".to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    );
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        expected.row_layout().clone(),
        changed_fields,
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&changed, &expected)
    else {
        panic!("field type drift should be rejected");
    };

    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::FieldContract
    );
    assert!(
        rejection
            .detail()
            .contains("field[1] kind changed: stored=Nat64 generated=Text"),
        "field type drift should name the first changed field contract",
    );
}

#[test]
fn schema_transition_policy_accepts_generated_field_default_changes_as_metadata_only() {
    let stored = expected_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields[1] = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "name".to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        false,
        SchemaInsertDefault::SlotPayload(vec![0xFF, 0x01, b'A', b'd', b'a']),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let generated = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.first_primary_key_field_id(),
        stored.row_layout().clone(),
        generated_fields,
    );

    let candidate = derive_generated_accepted_candidate(&stored, &generated)
        .expect("metadata-only default change should not allocate a layout")
        .expect("generated default change should derive an accepted candidate");
    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &candidate)
    else {
        panic!("generated default change should be accepted");
    };

    assert_eq!(
        plan.kind(),
        SchemaTransitionPlanKind::MetadataOnlyFieldDefault,
    );
    assert_eq!(candidate.row_layout(), stored.row_layout());
    assert_eq!(
        candidate.fields()[1].historical_fill(),
        stored.fields()[1].historical_fill(),
        "future default changes must retain frozen historical fill",
    );
    assert_eq!(
        candidate.fields()[1].insert_default(),
        generated.fields()[1].insert_default(),
    );
}

#[test]
fn schema_transition_policy_types_generated_field_after_ddl_slot_collision() {
    let generated_before = expected_snapshot();
    let accepted = snapshot_with_ddl_nickname_field(&generated_before, SchemaVersion::new(2));
    let mut generated_fields = generated_before.fields().to_vec();
    generated_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let generated_after = PersistedSchemaSnapshot::new(
        SchemaVersion::new(3),
        generated_before.entity_path().to_string(),
        generated_before.entity_name().to_string(),
        generated_before.first_primary_key_field_id(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        generated_fields,
    );

    assert!(
        derive_generated_accepted_candidate(&accepted, &generated_after)
            .expect("collision classification should not allocate a layout")
            .is_none(),
        "slot collisions must not be lowered into an accepted candidate",
    );
    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&accepted, &generated_after)
    else {
        panic!("generated field after a DDL field must reject");
    };
    assert_eq!(rejection.kind(), SchemaTransitionRejectionKind::FieldSlot);
    assert!(
        rejection
            .detail()
            .contains("cannot claim a slot already owned by accepted SQL DDL"),
        "collision rejection must retain its typed slot-owner explanation: {}",
        rejection.detail(),
    );
}

#[test]
fn schema_transition_policy_reports_first_nested_leaf_mismatch() {
    let stored = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::NestedSchemaEntity".to_string(),
        "NestedSchemaEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "profile".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::test_composite(),
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["nickname".to_string()],
                    AcceptedFieldKind::Text { max_len: None },
                    false,
                )],
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    );
    let mut generated_fields = stored.fields().to_vec();
    generated_fields[1] = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "profile".to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::test_composite(),
        vec![PersistedNestedLeafSnapshot::new(
            vec!["score".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        )],
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.first_primary_key_field_id(),
        stored.row_layout().clone(),
        generated_fields,
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&stored, &generated)
    else {
        panic!("nested leaf metadata drift should be rejected");
    };

    assert!(
        rejection.detail().contains(
            "field[1] nested leaf metadata changed: stored=1 generated=1; first_difference=nested_leaf[0]"
        ),
        "nested leaf drift should identify the owning field and first changed leaf",
    );
    assert!(
        rejection.detail().contains(
            "stored_path='nickname' stored_kind=Text { max_len: None } stored_nullable=false"
        ),
        "nested leaf drift should describe the stored leaf contract",
    );
    assert!(
        rejection
            .detail()
            .contains("generated_path='score' generated_kind=Nat64 generated_nullable=false"),
        "nested leaf drift should describe the generated leaf contract",
    );
    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::FieldContract,
        "nested leaf drift remains a rejected field-contract transition",
    );
}

#[test]
fn schema_transition_policy_names_unsupported_generated_removed_fields() {
    let expected = expected_snapshot();
    let mut stored_fields = expected.fields().to_vec();
    stored_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "removed_score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        stored_fields,
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&changed, &expected)
    else {
        panic!("stored extra row-layout field should be rejected");
    };

    assert!(
        rejection.detail().contains(
            "unsupported generated field removal: stored field[2] id=3 slot=2 name='removed_score' kind=Nat64; startup reconciliation does not perform physical DDL work"
        ),
        "removed field drift should be named as an unsupported transition shape",
    );
    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::FieldContract,
        "unsupported removals are field-contract transitions, not generic row-layout mismatches",
    );
    assert_eq!(
        rejection.admission(),
        None,
        "unsupported field-contract transitions must not carry schema-version admission classification",
    );
}

#[test]
fn schema_transition_policy_rejects_unlowered_generated_additive_layout() {
    let stored = expected_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "new_score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.first_primary_key_field_id(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        generated_fields,
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&stored, &generated)
    else {
        panic!("generated additive field should be rejected until additive policy exists");
    };

    assert!(
        rejection.detail().contains(
            "row layout changed: stored_current=1 generated_current=1 stored_floor=1 generated_floor=1 stored_fields=2 generated_fields=3"
        ),
        "an unlowered generated proposal must not masquerade as accepted temporal authority",
    );
    assert_eq!(
        rejection.kind(),
        SchemaTransitionRejectionKind::RowLayout,
        "the generated proposal failed to acquire a fresh accepted layout identity",
    );
    assert_eq!(
        rejection.admission(),
        None,
        "unsupported field-contract transitions must not carry schema-version admission classification",
    );
}
