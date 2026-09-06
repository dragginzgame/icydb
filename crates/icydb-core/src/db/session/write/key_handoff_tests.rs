//! Key reconstruction preserves accepted slot order and eager row validation.

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
    types::EntityTag,
};
use std::borrow::Cow;

fn contract(key_slots: &[u16], payload_limit: u32) -> StructuralRowContract {
    let fields = (0..5_u16)
        .map(|slot| {
            let (kind, codec) = if slot == 4 {
                (
                    AcceptedFieldKind::Blob {
                        max_len: Some(payload_limit),
                    },
                    ScalarCodec::Blob,
                )
            } else {
                (AcceptedFieldKind::Nat64, ScalarCodec::Nat64)
            };
            PersistedFieldSnapshot::new_initial(
                FieldId::new(u32::from(slot) + 1),
                format!("field_{slot}"),
                SchemaFieldSlot::new(slot),
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(codec),
            )
        })
        .collect::<Vec<_>>();
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::KeyHandoff".into(),
        "KeyHandoff".into(),
        key_slots
            .iter()
            .map(|slot| FieldId::new(u32::from(*slot) + 1))
            .collect::<Vec<_>>(),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(catalog),
    )
}

fn row(contract: &StructuralRowContract, payload: usize) -> RawRow {
    let values = [
        Value::Nat64(7),
        Value::Nat64(19),
        Value::Nat64(31),
        Value::Nat64(43),
        Value::Blob(vec![9; payload]),
    ];
    canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row()
}

#[test]
fn reconstructed_key_preserves_accepted_component_order_and_bytes() {
    let entity = EntityTag::new(17);
    for (slots, expected) in [
        (vec![1], Value::Nat64(19)),
        (
            vec![1, 0],
            Value::List(vec![Value::Nat64(19), Value::Nat64(7)]),
        ),
        (
            vec![3, 1, 2, 0],
            Value::List(vec![
                Value::Nat64(43),
                Value::Nat64(19),
                Value::Nat64(31),
                Value::Nat64(7),
            ]),
        ),
    ] {
        let contract = contract(&slots, 1024);
        let row = row(&contract, 8);
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        let key = data_key_from_validated_reader(entity, &reader).unwrap();
        let expected = DecodedDataStoreKey::try_from_structural_key(entity, &expected).unwrap();
        assert_eq!(key, expected);
        assert_eq!(key.to_raw().unwrap(), expected.to_raw().unwrap());
        reader.validate_primary_key(&key).unwrap();
    }
}

#[test]
fn reconstructed_key_rejects_truncated_non_key_payload_before_returning() {
    let wide = contract(&[1, 0], 1024);
    let row = row(&wide, 9);
    let truncated = RawRow::try_new(row.as_bytes()[..row.len() - 1].to_vec()).unwrap();
    let error =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&truncated, &wide)
            .and_then(|reader| data_key_from_validated_reader(EntityTag::new(17), &reader))
            .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn reconstructed_key_leaves_reader_values_available_for_identity_checks() {
    let contract = contract(&[1], 1024);
    let row = row(&contract, 8);
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
            .unwrap();
    let identity_value = reader.required_cached_value(1).unwrap();
    let key = data_key_from_validated_reader(EntityTag::new(17), &reader).unwrap();

    assert_eq!(key.primary_key_runtime_value(), *identity_value);
    assert_eq!(reader.required_cached_value(1).unwrap(), identity_value);
    assert_eq!(
        reader.required_cached_value(4).unwrap(),
        &Value::Blob(vec![9; 8])
    );
    let repeated = data_key_from_validated_reader(EntityTag::new(17), &reader).unwrap();
    assert_eq!(key.to_raw().unwrap(), repeated.to_raw().unwrap());
}
