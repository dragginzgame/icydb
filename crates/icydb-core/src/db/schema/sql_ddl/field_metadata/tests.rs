use super::validate_sql_ddl_field_drop_metadata_change;
use crate::{
    db::schema::{
        AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId, FieldStorageDecode, LeafCodec,
        PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedSchemaSnapshot, RelationIdAllocator, ScalarCodec,
        SchemaDdlAcceptedSnapshotDerivation, SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, derive_sql_ddl_field_drop_accepted_after,
    },
    error::ErrorClass,
};

fn field_drop_fixture() -> (AcceptedSchemaSnapshot, SchemaDdlAcceptedSnapshotDerivation) {
    let fields = [("id", 1, 0), ("removed", 2, 1), ("retained", 3, 2)]
        .into_iter()
        .map(|(name, id, slot)| {
            PersistedFieldSnapshot::new_initial(
                FieldId::new(id),
                name.to_string(),
                SchemaFieldSlot::new(slot),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            )
        })
        .collect::<Vec<_>>();
    let index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("index ID"),
        1,
        "by_retained".to_string(),
        "tests::Store".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(3),
            SchemaFieldSlot::new(2),
            vec!["retained".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        )]),
        None,
    );
    let layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    let before = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "tests::FieldDrop".to_string(),
        "FieldDrop".to_string(),
        FieldId::new(1),
        layout,
        fields,
        vec![index],
    ))
    .expect("accepted before snapshot");
    let derivation = derive_sql_ddl_field_drop_accepted_after(&before, "removed")
        .expect("field removal")
        .with_declared_schema_version(&before, SchemaVersion::new(2))
        .expect("declared version admission");
    (before, derivation)
}

#[test]
fn field_drop_accepts_dense_remapping_with_declared_version() {
    let (before, derivation) = field_drop_fixture();
    let after = derivation.accepted_after().persisted_snapshot();
    let target = derivation
        .admission()
        .field_drop_target()
        .expect("drop target");

    validate_sql_ddl_field_drop_metadata_change(before.persisted_snapshot(), after, target)
        .expect("canonical removal should validate");
    assert_eq!(after.version(), SchemaVersion::new(2));
    assert_eq!(after.fields()[1].name(), "retained");
    assert_eq!(after.fields()[1].id(), FieldId::new(2));
    assert_eq!(after.fields()[1].slot(), SchemaFieldSlot::new(1));
    let key = &after.indexes()[0].key().field_paths()[0];
    assert_eq!(key.field_id(), FieldId::new(2));
    assert_eq!(key.slot(), SchemaFieldSlot::new(1));
    assert_ne!(
        after.constraint_catalog(),
        before.persisted_snapshot().constraint_catalog()
    );
}

#[test]
fn field_drop_rejects_unchanged_layout_version() {
    let (before, derivation) = field_drop_fixture();
    let after = derivation.accepted_after().persisted_snapshot();
    let target = derivation
        .admission()
        .field_drop_target()
        .expect("drop target");
    // The slots are dense, but the physical layout identity must also advance.
    let wrong_layout = SchemaRowLayout::single_version(
        before.persisted_snapshot().row_layout().current_version(),
        after.row_layout().field_to_slot().to_vec(),
    );
    let mismatched = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        after.version(),
        after.entity_path().to_string(),
        after.entity_name().to_string(),
        after.primary_key_field_ids().to_vec(),
        wrong_layout,
        after.fields().to_vec(),
        after.indexes().to_vec(),
    )
    .with_constraint_catalog(after.constraint_catalog().clone());
    let error = validate_sql_ddl_field_drop_metadata_change(
        before.persisted_snapshot(),
        &mismatched,
        target,
    )
    .expect_err("unchanged physical layout identity must reject");
    assert_eq!(error.class(), ErrorClass::Unsupported);
}

#[test]
fn field_drop_rejects_constraint_and_allocator_drift() {
    let (before, derivation) = field_drop_fixture();
    let after = derivation.accepted_after().persisted_snapshot();
    let target = derivation
        .admission()
        .field_drop_target()
        .expect("drop target");
    for mismatched in [
        after
            .clone()
            .with_constraint_catalog(before.persisted_snapshot().constraint_catalog().clone()),
        after
            .clone()
            .with_relation_id_allocator(RelationIdAllocator::new(7)),
    ] {
        let error = validate_sql_ddl_field_drop_metadata_change(
            before.persisted_snapshot(),
            &mismatched,
            target,
        )
        .expect_err("unrelated or unremapped catalog state must reject");
        assert_eq!(error.class(), ErrorClass::Unsupported);
    }
}

#[test]
fn field_drop_rejects_target_from_another_field() {
    let (before, derivation) = field_drop_fixture();
    let mismatched = derivation.accepted_after().persisted_snapshot();
    let target = derivation
        .admission()
        .field_drop_target()
        .expect("drop target");
    // Reusing the after-image as the before-image resolves the same dense ID to
    // the retained field, so its name no longer agrees with the admitted target.
    let error = validate_sql_ddl_field_drop_metadata_change(
        mismatched,
        before.persisted_snapshot(),
        target,
    )
    .expect_err("a rebound dense ID must not authorize dropping a different field");
    assert_eq!(error.class(), ErrorClass::Unsupported);
}
