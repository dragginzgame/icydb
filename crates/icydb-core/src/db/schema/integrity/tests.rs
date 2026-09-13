use crate::db::schema::{
    AcceptedFieldKind, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
    PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    PersistedRelationEdgeSnapshot, RelationId, ScalarCodec, SchemaFieldSlot, SchemaIndexId,
    SchemaInsertDefault, SchemaRowLayout,
    integrity::{
        schema_snapshot_index_integrity_detail, schema_snapshot_relation_integrity_detail,
    },
};

fn field_contract() -> (SchemaRowLayout, Vec<PersistedFieldSnapshot>) {
    (
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    )
}

fn index(id: u32, ordinal: u16, name: &str, field_id: u32) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(id).unwrap(),
        ordinal,
        name.into(),
        "test::Store".into(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(field_id),
            SchemaFieldSlot::new(0),
            vec!["id".into()],
            AcceptedFieldKind::Nat64,
            false,
        )]),
        None,
    )
}

fn relation(id: u32, name: &str, field_id: u32) -> PersistedRelationEdgeSnapshot {
    PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(id).unwrap(),
        name.into(),
        "test::Target".into(),
        vec![FieldId::new(field_id)],
    )
}

#[test]
fn index_integrity_keeps_one_ordinal_sequence_across_active_and_candidate_entries() {
    let (layout, fields) = field_contract();
    let indexes = [
        index(1, 1, "first", 1),
        index(2, 2, "second", 1),
        index(3, 3, "third", 1),
    ];
    for split in 0..=indexes.len() {
        assert_eq!(
            schema_snapshot_index_integrity_detail(
                "test",
                &layout,
                &fields,
                &indexes[..split],
                &indexes[split..],
            ),
            None,
            "valid entries must admit at split {split}"
        );
    }
    assert_eq!(
        schema_snapshot_index_integrity_detail("test", &layout, &fields, &[], &[]),
        None
    );
}

#[test]
fn index_integrity_rejects_conflicts_and_malformed_keys_on_either_side_of_the_split() {
    let (layout, fields) = field_contract();
    for invalid in [
        index(1, 2, "second", 1), // Duplicate stable identity.
        index(2, 2, "first", 1),  // Duplicate name.
        index(2, 1, "second", 1), // Duplicate ordinal.
        index(2, 3, "second", 1), // Gap in the combined ordinal sequence.
        index(2, 2, "second", 2), // Key references a field outside authority.
    ] {
        let indexes = [index(1, 1, "first", 1), invalid];
        for split in 0..=indexes.len() {
            assert_eq!(
                schema_snapshot_index_integrity_detail(
                    "test",
                    &layout,
                    &fields,
                    &indexes[..split],
                    &indexes[split..],
                ),
                Some(()),
                "invalid entries must reject at split {split}"
            );
        }
    }
}

#[test]
fn relation_integrity_admits_active_and_candidate_entries_without_an_ordinal_requirement() {
    let (layout, fields) = field_contract();
    let relations = [
        relation(3, "third", 1),
        relation(1, "first", 1),
        relation(2, "second", 1),
    ];
    for split in 0..=relations.len() {
        assert_eq!(
            schema_snapshot_relation_integrity_detail(
                "test",
                &layout,
                &fields,
                &relations[..split],
                &relations[split..],
            ),
            None,
            "valid entries must admit at split {split}"
        );
    }
    assert_eq!(
        schema_snapshot_relation_integrity_detail("test", &layout, &fields, &[], &[]),
        None
    );
}

#[test]
fn relation_integrity_rejects_conflicts_and_invalid_sources_on_either_side_of_the_split() {
    let (layout, fields) = field_contract();
    for invalid in [
        relation(1, "second", 1), // Duplicate stable identity.
        relation(2, "first", 1),  // Duplicate name.
        relation(2, "second", 2), // Source references a field outside authority.
    ] {
        let relations = [relation(1, "first", 1), invalid];
        for split in 0..=relations.len() {
            assert_eq!(
                schema_snapshot_relation_integrity_detail(
                    "test",
                    &layout,
                    &fields,
                    &relations[..split],
                    &relations[split..],
                ),
                Some(()),
                "invalid entries must reject at split {split}"
            );
        }
    }
}
