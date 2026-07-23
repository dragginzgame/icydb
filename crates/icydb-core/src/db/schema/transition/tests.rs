use super::{
    SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionReason,
    classify_schema_admission_rejection, schema_admission_rejection,
};
use crate::{
    db::schema::{
        AcceptedCheckExprV1, AcceptedConstraintCatalog, AcceptedFieldKind,
        AcceptedSchemaFingerprint, ConstraintActivationKind, ConstraintIdAllocator,
        ConstraintOrigin, FieldId, GeneratedConstraintActivationContext,
        MutationPublicationPreflight, PersistedFieldOrigin, PersistedFieldSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        RelationId, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaIndexId, SchemaInsertDefault,
        SchemaRowLayout, SchemaTransitionDecision, SchemaTransitionPlanKind, SchemaVersion,
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
    derive_generated_accepted_candidate(stored, &generated, None)
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
    .with_constraint_catalog(snapshot.constraint_catalog().clone())
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
    .with_constraint_catalog(snapshot.constraint_catalog().clone())
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

fn name_field_path_index_in_store(name: &str, store: String) -> PersistedIndexSnapshot {
    named_field_path_index_with_ordinal(name, 1, store)
}

fn named_field_path_index_with_ordinal(
    name: &str,
    ordinal: u16,
    store: String,
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(u32::from(ordinal)).expect("test index identity should be non-zero"),
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

fn sql_ddl_name_field_path_index(name: &str, store: String) -> PersistedIndexSnapshot {
    sql_ddl_named_field_path_index_with_ordinal(name, 1, store)
}

fn sql_ddl_named_field_path_index_with_ordinal(
    name: &str,
    ordinal: u16,
    store: String,
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new_sql_ddl(
        SchemaIndexId::new(u32::from(ordinal)).expect("test index identity should be non-zero"),
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
    .with_constraint_catalog(snapshot.constraint_catalog().clone())
    .with_relations(snapshot.relations().to_vec())
}

fn test_relation(id: u32, name: &str) -> PersistedRelationEdgeSnapshot {
    PersistedRelationEdgeSnapshot::new(
        RelationId::new(id).expect("test relation identity should be non-zero"),
        name.to_string(),
        "test::Owner".to_string(),
        vec![FieldId::new(1)],
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
fn schema_transition_policy_rejects_generated_rename_over_ddl_index_identity() {
    let base = expected_snapshot();
    let store = "test::SchemaReconcileEntity::name_index".to_string();
    let stored = snapshot_with_indexes(
        &base,
        vec![sql_ddl_name_field_path_index("ddl_name_idx", store.clone())],
    );
    let generated = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "idx_schema_reconcile_entity__name",
            store,
        )],
    );

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&stored, &generated)
    else {
        panic!("generated metadata must not take ownership of a DDL index with a colliding ID");
    };

    assert_eq!(rejection.kind(), SchemaTransitionRejectionKind::Snapshot);
}

#[test]
fn schema_transition_policy_accepts_generated_index_rename_with_extra_ddl_indexes() {
    let base = expected_snapshot();
    let generated_store = "test::SchemaReconcileEntity::name_index".to_string();
    let ddl_index = sql_ddl_named_field_path_index_with_ordinal(
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
fn schema_transition_policy_rejects_relation_drift_during_generated_index_rename() {
    let base = expected_snapshot();
    let store = "test::SchemaReconcileEntity::name_index".to_string();
    let stored = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "SchemaReconcileEntity|name",
            store.clone(),
        )],
    )
    .with_relations(vec![test_relation(1, "owner")]);
    let generated = snapshot_with_indexes(
        &base,
        vec![name_field_path_index_in_store(
            "idx_schema_reconcile_entity__name",
            store,
        )],
    )
    .with_relations(vec![test_relation(2, "replacement_owner")]);

    let SchemaTransitionDecision::Rejected(rejection) =
        decide_schema_transition(&stored, &generated)
    else {
        panic!("index rename must not publish an unrelated relation change");
    };

    assert_eq!(rejection.kind(), SchemaTransitionRejectionKind::Snapshot);
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
    let accepted = snapshot_with_indexes(
        &generated,
        vec![sql_ddl_name_field_path_index(
            "name_idx",
            "test::SchemaReconcileEntity::name_idx".to_string(),
        )],
    );
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
fn generated_check_change_cannot_hide_inside_append_only_field_reconciliation() {
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
    let generated_constraints = stored
        .constraint_catalog()
        .clone()
        .with_added_check(
            "name_policy".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .expect("test generated check should allocate");
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
    )
    .with_constraint_catalog(generated_constraints);

    assert_eq!(
        derive_generated_accepted_candidate(&stored, &generated, None),
        Ok(None),
        "check activation must not be discarded while an unrelated generated field is accepted",
    );
}

#[test]
fn generated_check_addition_publishes_one_stable_activation_candidate() {
    let stored = expected_snapshot();
    let generated_catalog = stored
        .constraint_catalog()
        .clone()
        .with_added_check(
            "name_policy".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .expect("generated check proposal should allocate");
    let generated = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.first_primary_key_field_id(),
        stored.row_layout().clone(),
        stored.fields().to_vec(),
    )
    .with_constraint_catalog(generated_catalog);
    let activation =
        GeneratedConstraintActivationContext::new(AcceptedSchemaFingerprint::new([0xA5; 32]), 2);

    let candidate = derive_generated_accepted_candidate(&stored, &generated, Some(activation))
        .expect("activation candidate should derive")
        .expect("generated check should require activation");
    assert_eq!(candidate.constraints(), stored.constraints());
    assert_eq!(candidate.constraint_activations().len(), 1);
    assert_eq!(candidate.constraint_activations()[0].name(), "name_policy");

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &candidate)
    else {
        panic!("activation-only candidate should be an admitted transition");
    };
    assert_eq!(plan.kind(), SchemaTransitionPlanKind::ConstraintActivation);

    let replayed = derive_generated_accepted_candidate(&candidate, &generated, Some(activation))
        .expect("matching activation should remain stable")
        .expect("matching activation should remain accepted authority");
    assert_eq!(
        replayed.constraint_catalog(),
        candidate.constraint_catalog()
    );
}

#[cfg(feature = "sql")]
#[test]
fn generated_not_null_tightening_publishes_one_stable_activation_candidate() {
    let generated = expected_snapshot();
    let stored = crate::db::schema::derive_sql_ddl_field_nullability_persisted_after(
        &generated,
        FieldId::new(2),
        true,
        generated.version(),
    )
    .expect("nullable accepted-before fixture should derive");
    let activation =
        GeneratedConstraintActivationContext::new(AcceptedSchemaFingerprint::new([0xA6; 32]), 3);

    let candidate = derive_generated_accepted_candidate(&stored, &generated, Some(activation))
        .expect("not-null activation candidate should derive")
        .expect("generated tightening should require activation");
    let [pending] = candidate.constraint_activations() else {
        panic!("generated tightening should own exactly one activation");
    };
    assert!(candidate.fields()[1].nullable());
    assert!(matches!(
        pending.kind(),
        ConstraintActivationKind::NotNull { field_id } if *field_id == FieldId::new(2)
    ));

    let replayed = derive_generated_accepted_candidate(&candidate, &generated, Some(activation))
        .expect("matching not-null activation should remain stable")
        .expect("matching not-null activation should remain accepted authority");
    assert_eq!(
        replayed.constraint_catalog(),
        candidate.constraint_catalog()
    );
}

#[test]
fn generated_unique_index_addition_reserves_one_planner_invisible_candidate() {
    let stored = expected_snapshot();
    let unique_index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        "unique_name".to_string(),
        "test::SchemaReconcileEntity::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        )]),
        None,
    );
    let generated = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::new(2),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_ids().to_vec(),
        stored.row_layout().clone(),
        stored.fields().to_vec(),
        vec![unique_index],
    );
    let generated_catalog = AcceptedConstraintCatalog::initial(
        generated.fields(),
        generated.indexes(),
        generated.relations(),
    )
    .expect("generated unique constraint catalog should build");
    let generated = generated.with_constraint_catalog(generated_catalog);
    let activation =
        GeneratedConstraintActivationContext::new(AcceptedSchemaFingerprint::new([0xB6; 32]), 4);

    let candidate = derive_generated_accepted_candidate(&stored, &generated, Some(activation))
        .expect("generated unique activation should derive")
        .expect("generated unique addition should require activation");
    assert!(candidate.indexes().is_empty());
    let [candidate_index] = candidate.candidate_indexes() else {
        panic!("one planner-invisible candidate index should be retained");
    };
    let [pending] = candidate.constraint_activations() else {
        panic!("one unique activation should reserve the candidate");
    };
    assert_eq!(candidate_index.name(), "unique_name");
    assert!(matches!(
        pending.kind(),
        ConstraintActivationKind::Unique { index_id }
            if *index_id == candidate_index.schema_id()
    ));
    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &candidate)
    else {
        panic!("generated unique activation should be an accepted transition");
    };
    assert_eq!(plan.kind(), SchemaTransitionPlanKind::ConstraintActivation);

    let replayed = derive_generated_accepted_candidate(&candidate, &generated, Some(activation))
        .expect("matching generated unique activation should remain stable")
        .expect("matching generated unique activation should remain accepted authority");
    assert_eq!(replayed, candidate);
}

#[test]
fn exact_generated_reconciliation_preserves_retired_constraint_identity_high_water() {
    let generated = expected_snapshot();
    let stored =
        generated
            .clone()
            .with_constraint_catalog(AcceptedConstraintCatalog::from_persisted_parts(
                ConstraintIdAllocator::new(17),
                generated.constraints().to_vec(),
                Vec::new(),
            ));

    let candidate = derive_generated_accepted_candidate(&stored, &generated, None)
        .expect("exact generated proposal should reconcile")
        .expect("accepted identity state should remain authoritative");

    assert_eq!(candidate, stored);
    assert_eq!(candidate.constraint_id_allocator().high_water(), 17);
}

#[test]
fn generated_additive_field_preserves_accepted_only_ddl_index() {
    let base = expected_snapshot();
    let stored = snapshot_with_indexes(
        &base,
        vec![sql_ddl_name_field_path_index(
            "ddl_name_idx",
            "test::SchemaReconcileEntity::ddl_name_idx".to_string(),
        )],
    );
    let mut generated_fields = base.fields().to_vec();
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

    let candidate = derive_generated_accepted_candidate(&stored, &generated, None)
        .expect("test layout version should advance")
        .expect("generated field should lower without taking DDL index ownership");
    assert_eq!(candidate.indexes(), stored.indexes());
    assert!(!candidate.indexes()[0].generated());

    let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&stored, &candidate)
    else {
        panic!("preserved DDL index should leave a pure append-only field transition");
    };
    assert_eq!(plan.kind(), SchemaTransitionPlanKind::AppendOnlyFields);
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

    let candidate = derive_generated_accepted_candidate(&stored, &generated, None)
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
        derive_generated_accepted_candidate(&accepted, &generated_after, None)
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
