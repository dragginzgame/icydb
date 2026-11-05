use super::*;

#[test]
fn narrow_sql_integer_contracts_match_accepted_kind_widths() {
    for (name, expected) in [
        ("int8", ScalarCodec::Int8),
        ("int16", ScalarCodec::Int16),
        ("int32", ScalarCodec::Int32),
        ("nat8", ScalarCodec::Nat8),
        ("nat16", ScalarCodec::Nat16),
        ("nat32", ScalarCodec::Nat32),
    ] {
        let contract = resolve_sql_ddl_field_type_contract(name).unwrap();
        assert_eq!(contract.leaf_codec(), LeafCodec::Scalar(expected));
        assert_eq!(
            contract.leaf_codec(),
            contract
                .kind()
                .leaf_codec_for_storage(contract.storage_decode())
        );
        let relation = AcceptedFieldKind::Relation {
            target_path: "tests::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "tests::Store".to_string(),
            key_kind: Box::new(contract.kind().clone()),
        };
        assert_eq!(
            relation.leaf_codec_for_storage(FieldStorageDecode::ByKind),
            contract.leaf_codec()
        );
    }
}
