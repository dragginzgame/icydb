//! Mutation output owns selected cached values in accepted snapshot order.

use super::*;
use crate::{
    db::{
        data::canonical_row_from_runtime_value_source_with_accepted_contract,
        schema::{
            AcceptedCompositeCatalog, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
};
use std::borrow::Cow;

fn snapshot(blob_limit: u32) -> AcceptedSchemaSnapshot {
    let field = |id, name: &str, slot, kind, nullable, storage, codec| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.into(),
            SchemaFieldSlot::new(slot),
            kind,
            Vec::new(),
            nullable,
            SchemaInsertDefault::None,
            storage,
            codec,
        )
    };
    // Keep canonical dense field order with the primary key after the payload.
    let fields = vec![
        field(
            1,
            "payload",
            0,
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: Some(16) }),
                value: Box::new(AcceptedFieldKind::Blob {
                    max_len: Some(blob_limit),
                }),
            })),
            true,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
        field(
            2,
            "id",
            1,
            AcceptedFieldKind::Nat64,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        field(
            3,
            "label",
            2,
            AcceptedFieldKind::Text { max_len: Some(16) },
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ),
    ];
    AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::MutationOutput".into(),
        "MutationOutput".into(),
        FieldId::new(2),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .expect("mutation output fixture must satisfy accepted schema integrity")
}

fn contract(descriptor: &AcceptedRowLayoutRuntimeContract<'_>) -> StructuralRowContract {
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    StructuralRowContract::from_accepted_decode_contract(
        "tests::MutationOutput",
        descriptor.row_decode_contract(catalog),
    )
}

fn payload(bytes: usize) -> Value {
    Value::List(vec![Value::Map(vec![(
        Value::Text("chunk".into()),
        Value::Blob(vec![7; bytes]),
    )])])
}

fn row(contract: &StructuralRowContract, payload: &Value) -> RawRow {
    let values = [
        payload.clone(),
        Value::Nat64(7),
        Value::Text("label".into()),
    ];
    canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row()
}

fn backing(value: &Value, pointers: &mut Vec<*const u8>) {
    match value {
        Value::List(values) => {
            pointers.push(values.as_ptr().cast());
            for value in values {
                backing(value, pointers);
            }
        }
        Value::Map(entries) => {
            pointers.push(entries.as_ptr().cast());
            for (key, value) in entries {
                backing(key, pointers);
                backing(value, pointers);
            }
        }
        Value::Text(text) => pointers.push(text.as_ptr()),
        Value::Blob(bytes) => pointers.push(bytes.as_ptr()),
        _ => {}
    }
}

#[test]
fn mutation_output_moves_heap_backing_in_accepted_field_order() {
    let snapshot = snapshot(4096);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&snapshot).unwrap();
    let field_slots: Vec<_> = descriptor
        .fields()
        .iter()
        .map(|field| usize::from(field.slot().get()))
        .collect();
    let contract = contract(&descriptor);
    let payload = payload(1024);
    let row = row(&contract, &payload);
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
            .unwrap();
    let key = data_key_from_validated_reader(crate::types::EntityTag::new(17), &reader).unwrap();
    reader.validate_primary_key(&key).unwrap();
    let mut before = Vec::new();
    for field in descriptor.fields() {
        backing(
            reader
                .required_cached_value(usize::from(field.slot().get()))
                .unwrap(),
            &mut before,
        );
    }

    let values = into_mutation_output_values(reader, &field_slots).unwrap();
    assert_eq!(
        values,
        vec![payload, Value::Nat64(7), Value::Text("label".into())]
    );
    let mut after = Vec::new();
    for value in &values {
        backing(value, &mut after);
    }
    assert_eq!(before, after);
    assert_eq!(values.capacity(), descriptor.fields().len());
}

#[test]
fn mutation_output_materializes_cold_nullable_and_empty_fields() {
    let snapshot = snapshot(4096);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&snapshot).unwrap();
    let field_slots: Vec<_> = descriptor
        .fields()
        .iter()
        .map(|field| usize::from(field.slot().get()))
        .collect();
    let contract = contract(&descriptor);
    for payload in [payload(8), Value::List(Vec::new()), Value::Null] {
        let row = row(&contract, &payload);
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        assert_eq!(
            into_mutation_output_values(reader, &field_slots).unwrap(),
            vec![payload, Value::Nat64(7), Value::Text("label".into())]
        );
    }
}

#[test]
fn mutation_output_preserves_eager_and_lazy_corruption_rejection() {
    let wide = snapshot(4096);
    let narrow = snapshot(8);
    let wide_descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&wide).unwrap();
    let narrow_descriptor =
        AcceptedRowLayoutRuntimeContract::from_accepted_schema(&narrow).unwrap();
    let field_slots: Vec<_> = narrow_descriptor
        .fields()
        .iter()
        .map(|field| usize::from(field.slot().get()))
        .collect();
    let wide_contract = contract(&wide_descriptor);
    let narrow_contract = contract(&narrow_descriptor);
    let row = row(&wide_contract, &payload(9));
    let eager =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &narrow_contract)
            .err()
            .expect("eager validation must reject the oversized nested blob");
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &narrow_contract).unwrap();
    let lazy = into_mutation_output_values(reader, &field_slots).unwrap_err();
    assert_eq!(eager.class(), ErrorClass::Corruption);
    assert_eq!(lazy.class(), eager.class());
}
