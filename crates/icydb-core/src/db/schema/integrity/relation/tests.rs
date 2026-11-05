use super::*;
use crate::db::schema::{
    FieldId, FieldStorageDecode, LeafCodec, RelationId, ScalarCodec, SchemaFieldSlot,
    SchemaInsertDefault, empty_accepted_enum_catalog_for_tests,
};

#[test]
fn nested_relation_terminal_is_derived_from_catalog_authority() {
    let fields = vec![PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "target_id".to_string(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    )];
    let layout = SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]);
    let valid = PersistedRelationEdgeSnapshot::new_nested(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "target_id".to_string(),
        "schema::integrity::Target".to_string(),
        FieldId::new(1),
        vec![PersistedRelationPathStepSnapshot::OptionalSome],
    );
    assert_eq!(
        schema_snapshot_relation_integrity_detail(
            &layout,
            &fields,
            std::slice::from_ref(&valid),
            &[],
        ),
        None,
    );
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let composite_catalog = AcceptedCompositeCatalog::empty();
    let terminal = accepted_relation_source_terminal(
        &fields,
        valid.source(),
        &enum_catalog,
        &composite_catalog,
    )
    .expect("the optional step should derive its scalar terminal");
    assert_eq!(terminal.kind(), &AcceptedFieldKind::Nat64);
    assert!(!terminal.nullable());

    let malformed = PersistedRelationEdgeSnapshot::new_nested(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "target_id".to_string(),
        "schema::integrity::Target".to_string(),
        FieldId::new(1),
        vec![PersistedRelationPathStepSnapshot::EnterNamed],
    );
    assert!(
        accepted_relation_source_terminal(
            &fields,
            malformed.source(),
            &enum_catalog,
            &composite_catalog,
        )
        .is_none(),
    );
}
