//! Owned admitted-value handoff and its normalization/persistence boundaries.

use super::*;
use crate::{
    db::{
        data::{
            StructuralRowContract, decode_runtime_value_from_row_contract,
            encode_canonical_value_for_accepted_field_contract,
        },
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            AcceptedValueAdmissionContract, FieldId, LeafCodec, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
};

fn catalog() -> AcceptedValueCatalogHandle {
    AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    )
}

fn nested_kind() -> AcceptedFieldKind {
    AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob {
            max_len: Some(4096),
        }),
    }))
}

fn nested_input(items: usize, bytes: usize) -> InputValue {
    InputValue::list(
        (0..items)
            .map(|_| {
                InputValue::map(vec![(
                    InputValue::text("payload".into()),
                    InputValue::blob(vec![7; bytes]),
                )])
            })
            .collect(),
    )
}

fn admission(
    catalog: &AcceptedValueCatalogHandle,
    kind: AcceptedFieldKind,
) -> AcceptedValueAdmissionContract<'_> {
    let contract = AcceptedValueContract::from_accepted_field(
        catalog,
        &kind,
        FieldStorageDecode::CatalogValue,
    )
    .unwrap();
    AcceptedValueAdmissionContract::owned(catalog, contract, true)
}

fn row_contract(
    catalog: AcceptedValueCatalogHandle,
    kind: AcceptedFieldKind,
) -> StructuralRowContract {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "payload".into(),
            SchemaFieldSlot::new(1),
            kind,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::OwnedHandoff".into(),
        "OwnedHandoff".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(catalog),
    )
}

#[test]
fn owned_handoff_consumption_preserves_nested_heap_backing() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    let value = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".into()),
        Value::Blob(vec![7; 128]),
    )])]);
    // Record every heap-owning layer while the source allocations are still live.
    let backing = |value: &Value| {
        let Value::List(items) = value else {
            panic!("list")
        };
        let Value::Map(entries) = &items[0] else {
            panic!("map")
        };
        let (Value::Text(key), Value::Blob(bytes)) = &entries[0] else {
            panic!("entry")
        };
        (
            items.as_ptr(),
            entries.as_ptr(),
            key.as_ptr(),
            bytes.as_ptr(),
        )
    };
    let before = backing(&value);
    let admitted = admission
        .admit_canonical(value, &mut ValueAdmissionBudget::standard())
        .unwrap();
    let value = admitted.into_value();
    assert_eq!(backing(&value), before);
}

#[test]
fn owned_handoff_normalization_and_persistence_preserve_nested_and_null_values() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    let row = row_contract(catalog.clone(), nested_kind());
    for input in [
        nested_input(3, 128),
        InputValue::list(Vec::new()),
        InputValue::null(),
    ] {
        let value = admission
            .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
            .unwrap();
        let encoded = encode_value(&row, &value);
        assert_eq!(
            decode_runtime_value_from_row_contract(&row, 1, &encoded).unwrap(),
            value
        );
    }
}

#[test]
fn owned_handoff_rejects_invalid_input_and_persisted_values_before_returning() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    for (input, expected) in [
        (InputValue::nat64(1), ValueAdmissionError::TypeMismatch),
        (nested_input(1, 4097), ValueAdmissionError::ScalarConstraint),
    ] {
        assert_eq!(
            admission
                .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                .unwrap_err(),
            expected
        );
    }
    let valid = admission
        .normalize_input_to_runtime(nested_input(1, 128), &mut ValueAdmissionBudget::standard())
        .unwrap();
    let row = row_contract(catalog.clone(), nested_kind());
    let mut truncated = encode_value(&row, &valid);
    truncated.pop();
    let wrong_kind = encode_value(
        &row_contract(catalog.clone(), AcceptedFieldKind::Nat64),
        &Value::Nat64(1),
    );
    let oversized = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".into()),
        Value::Blob(vec![7; 4097]),
    )])]);
    let wider_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob { max_len: None }),
    }));
    let oversized = encode_value(&row_contract(catalog.clone(), wider_kind), &oversized);
    for encoded in [truncated, wrong_kind, oversized] {
        let error = decode_runtime_value_from_row_contract(&row, 1, &encoded)
            .expect_err("strict persisted admission");
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

fn encode_value(row: &StructuralRowContract, value: &Value) -> Vec<u8> {
    encode_canonical_value_for_accepted_field_contract(
        row.required_accepted_field_persistence_contract(1).unwrap(),
        value,
    )
    .unwrap()
}
