use super::*;
use crate::{
    db::schema::{
        AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract, AcceptedSchemaRevision,
        AcceptedValueCatalogHandle, FieldStorageDecode, LeafCodec, TestEnumDefinition,
        TestEnumVariant, build_accepted_enum_catalog_for_tests,
    },
    value::{ValueEnum, ValueTag},
};

fn unit_definition() -> TestEnumDefinition {
    TestEnumDefinition::new("index::Status", vec![TestEnumVariant::unit("Ready")])
}

fn payload_definition() -> TestEnumDefinition {
    TestEnumDefinition::new(
        "index::Payload",
        vec![
            TestEnumVariant::unit("Empty"),
            TestEnumVariant::payload(
                "Value",
                AcceptedFieldKind::Nat64,
                FieldStorageDecode::ByKind,
            ),
        ],
    )
}

#[test]
fn scalar_index_components_do_not_request_enum_authority() {
    for kind in [None, Some(&AcceptedFieldKind::Nat64)] {
        for value in [Value::Nat64(7), Value::Null] {
            let mut lookups = 0;
            let encoded = encode_accepted_index_component(kind, &value, || {
                lookups += 1;
                None
            })
            .unwrap();
            assert_eq!(encoded, encode_value_index_component_ref(&value).unwrap());
            assert_eq!(lookups, 0);
        }
    }
}

#[test]
fn accepted_index_leaf_uses_catalog_ids_for_unit_enum_key() {
    let catalog = build_accepted_enum_catalog_for_tests(&[unit_definition()])
        .expect("unit enum catalog should build");
    let type_id = catalog
        .type_id("index::Status")
        .expect("status type should exist");
    let handle = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let kind = AcceptedFieldKind::Enum { type_id };
    let field = AcceptedFieldDecodeContract::new(
        "status",
        &kind,
        false,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    let persistence = AcceptedFieldPersistenceContract::new_for_tests(&handle, field)
        .expect("accepted index field should match the catalog");
    let value = Value::Enum(ValueEnum::test_unit(1, 1));

    // Even NULL must not bypass a missing enum ownership contract.
    for source in [&value, &Value::Null] {
        let error = encode_accepted_index_component(Some(&kind), source, || None).unwrap_err();
        let expected = InternalError::index_unsupported();
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }

    assert_eq!(
        encode_accepted_index_component(Some(&kind), &value, || {
            Some(persistence.admission_contract().clone())
        })
        .expect("accepted unit enum should produce an index component"),
        Some(vec![ValueTag::Enum.to_u8(), 1, 0, 0, 0, 1, 0, 0, 0, 1, 0]),
    );
}

#[test]
fn accepted_index_leaf_rejects_payload_enum_without_stable_key_capability() {
    let catalog = build_accepted_enum_catalog_for_tests(&[payload_definition()])
        .expect("payload enum catalog should build");
    let type_id = catalog
        .type_id("index::Payload")
        .expect("payload type should exist");
    let handle = AcceptedValueCatalogHandle::new_for_tests(
        catalog,
        crate::db::schema::AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let kind = AcceptedFieldKind::Enum { type_id };
    let field = AcceptedFieldDecodeContract::new(
        "payload",
        &kind,
        false,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    let persistence = AcceptedFieldPersistenceContract::new_for_tests(&handle, field)
        .expect("accepted index field should match the catalog");
    // Capability belongs to the complete definition, not the selected variant.
    for value in [
        Value::Enum(ValueEnum::test_unit(1, 1)),
        Value::Enum(ValueEnum::test_payload(1, 2, Value::Nat64(7))),
    ] {
        assert!(
            encode_accepted_index_component(Some(&kind), &value, || {
                Some(persistence.admission_contract().clone())
            })
            .is_err(),
            "payload enums must remain outside canonical index-key capability",
        );
    }
}
