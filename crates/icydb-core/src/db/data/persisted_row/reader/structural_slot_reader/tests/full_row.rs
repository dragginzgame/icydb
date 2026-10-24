//! Full-row ownership transfer and executor admission boundaries.

use super::*;
use crate::{
    db::{
        data::{
            DecodedDataStoreKey, RawRow, SlotReader,
            persisted_row::{
                canonical::encode_canonical_value_for_accepted_field_contract,
                contract::emit_raw_row_from_slot_payloads,
            },
        },
        executor::terminal::RowLayout,
        key_taxonomy::PrimaryKeyComponent,
    },
    error::{ErrorClass, InternalError},
    types::EntityTag,
};

fn nested_kind(max_len: u32) -> AcceptedFieldKind {
    AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob {
            max_len: Some(max_len),
        }),
    }))
}

fn nested_value(bytes: usize) -> Value {
    Value::List(vec![Value::Map(vec![(
        Value::Text("payload".into()),
        Value::Blob(vec![7; bytes]),
    )])])
}

#[test]
fn mixed_slots_preserve_lazy_refusal_and_cached_value_handoffs() {
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let contract =
            payload_contract_with_storage(nested_kind(16), LeafCodec::Structural, false, storage);
        let values = [Value::Nat64(7), Value::Bool(true), nested_value(8)];
        let row =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        assert!(reader.get_scalar(2).unwrap().is_none());
        assert!(reader.get_scalar(0).unwrap().is_some());
        for (slot, value) in values.iter().enumerate() {
            let first = reader.required_cached_value(slot).unwrap();
            assert_eq!(first, value);
            assert!(std::ptr::eq(
                first,
                reader.required_cached_value(slot).unwrap()
            ));
            assert_eq!(reader.take_required_value(slot).unwrap(), *value);
            assert_eq!(reader.required_cached_value(slot).unwrap(), value);
        }
        let mut output = Vec::new();
        reader.decode_all_values_into(&mut output).unwrap();
        assert_eq!(output, values);

        // A corrupt untouched container must still admit selective scalar reads,
        // then refuse both lazy materialization and eager full-row validation.
        let mut slots = values
            .iter()
            .enumerate()
            .map(|(slot, value)| {
                encode_canonical_value_for_accepted_field_contract(
                    contract
                        .required_accepted_field_persistence_contract(slot)
                        .unwrap(),
                    value,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        slots[2].pop();
        let corrupt = emit_raw_row_from_slot_payloads(
            contract.current_layout_version(),
            contract.field_count(),
            &slots,
        )
        .unwrap()
        .into_raw_row();
        let lazy =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&corrupt, &contract).unwrap();
        assert_eq!(lazy.required_cached_value(1).unwrap(), &Value::Bool(true));
        assert_eq!(
            lazy.required_cached_value(2).unwrap_err().class(),
            ErrorClass::Corruption
        );
        let eager = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            &corrupt, &contract,
        );
        assert_eq!(eager.err().unwrap().class(), ErrorClass::Corruption);
    }
}

#[test]
fn full_row_handoff_moves_nested_heap_backing_and_reuses_output_capacity() {
    let contract = payload_contract(nested_kind(4096), LeafCodec::Structural, true);
    let values = [Value::Nat64(7), Value::Bool(true), nested_value(1024)];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
            .unwrap();
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
    let before = backing(reader.required_cached_value(2).unwrap());
    let mut output = Vec::with_capacity(8);
    output.push(Value::Text("stale output".into()));
    let buffer = output.as_ptr();
    reader.decode_all_values_into(&mut output).unwrap();
    assert_eq!(output, values);
    assert_eq!(backing(&output[2]), before);
    assert_eq!(output.as_ptr(), buffer);
}

#[test]
fn full_row_executor_preserves_nested_empty_null_and_scalar_values() {
    let contract = payload_contract(nested_kind(4096), LeafCodec::Structural, true);
    let layout = RowLayout::from_structural_row_contract(contract);
    let key = DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(7).into());
    let mut output = Vec::with_capacity(8);
    for payload in [nested_value(1024), Value::List(Vec::new()), Value::Null] {
        let values = [Value::Nat64(7), Value::Bool(false), payload];
        let row = canonical_row_from_runtime_value_source_with_accepted_contract(
            layout.contract(),
            |slot| Ok(Cow::Borrowed(&values[slot])),
        )
        .unwrap()
        .into_raw_row();
        layout
            .decode_full_value_row_from_data_key_into(&key, &row, &mut output)
            .unwrap();
        assert_eq!(output, values);
    }
}

#[test]
fn full_row_executor_rejects_invalid_payloads_and_keys_before_output() {
    let contract = payload_contract(nested_kind(8), LeafCodec::Structural, true);
    let wider = payload_contract(nested_kind(4096), LeafCodec::Structural, true);
    let values = [Value::Nat64(7), Value::Bool(true), nested_value(9)];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&wider, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    // The valid wider wire form violates the current accepted scalar bound.
    // Check both lazy consuming admission and the eager full-row executor.
    let mut output = vec![Value::Text("stale output".into())];
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
    assert_eq!(
        reader
            .decode_all_values_into(&mut output)
            .unwrap_err()
            .class(),
        ErrorClass::Corruption
    );
    assert!(output.is_empty());
    let truncated = RawRow::try_new(row.as_bytes()[..row.len() - 1].to_vec()).unwrap();
    let wrong_kind = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        LeafCodec::Structural,
        true,
    );
    for (contract, id, row) in [
        (contract, 7, &row),
        (wrong_kind, 7, &row),
        (wider.clone(), 8, &row),
        (wider, 7, &truncated),
    ] {
        output.push(Value::Bool(false));
        let layout = RowLayout::from_structural_row_contract(contract);
        let key =
            DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(id).into());
        let error = layout
            .decode_full_value_row_from_data_key_into(&key, row, &mut output)
            .unwrap_err();
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert!(output.is_empty());
    }
}

#[test]
fn full_row_handoff_moves_scalar_blob_backing() {
    let contract = selective_payload_contract();
    let values = [
        Value::Nat64(7),
        Value::Bool(true),
        Value::Blob(vec![9; 4096]),
    ];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
    let Value::Blob(bytes) = reader.required_cached_value(2).unwrap() else {
        panic!("blob")
    };
    let before = bytes.as_ptr();
    let mut output = Vec::new();
    reader.decode_all_values_into(&mut output).unwrap();
    assert_eq!(output, values);
    let Value::Blob(bytes) = &output[2] else {
        panic!("blob")
    };
    assert_eq!(bytes.as_ptr(), before);
}

#[test]
fn by_kind_full_rows_retain_one_materialization_and_move_container_backing() {
    let map = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob {
            max_len: Some(4096),
        }),
    };
    for (kind, populated, empty) in [
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(3), Value::Nat64(5)]),
            Value::List(Vec::new()),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(3), Value::Nat64(5)]),
            Value::List(Vec::new()),
        ),
        (
            map,
            Value::Map(vec![(
                Value::Text("payload".into()),
                Value::Blob(vec![7; 1024]),
            )]),
            Value::Map(Vec::new()),
        ),
        (
            nested_kind(4096),
            nested_value(1024),
            Value::List(Vec::new()),
        ),
    ] {
        let contract = payload_contract_with_storage(
            kind,
            LeafCodec::Structural,
            true,
            FieldStorageDecode::ByKind,
        );
        let layout = RowLayout::from_structural_row_contract(contract.clone());
        let key =
            DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(7).into());
        for payload in [populated, empty, Value::Null] {
            let values = [Value::Nat64(7), Value::Bool(true), payload];
            let row =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row();
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            let materialized = &reader.cached_values[2].materialized;
            assert!(materialized.get().is_none());
            reader.materialize_all_values().unwrap();
            assert_eq!(materialized.get(), Some(&values[2]));
            let backing = |value: &Value| match value {
                Value::List(items) => items.as_ptr().cast::<()>(),
                Value::Map(entries) => entries.as_ptr().cast::<()>(),
                Value::Null => std::ptr::null(),
                _ => panic!("container fixture"),
            };
            let pointer = backing(materialized.get().unwrap());
            reader.materialize_all_values().unwrap();
            assert_eq!(backing(materialized.get().unwrap()), pointer);
            reader.validate_primary_key(&key).unwrap();
            let mut output = Vec::with_capacity(8);
            output.push(Value::Text("stale".into()));
            let output_pointer = output.as_ptr();
            reader.decode_all_values_into(&mut output).unwrap();
            assert_eq!(output, values);
            assert_eq!(backing(&output[2]), pointer);
            assert_eq!(output.as_ptr(), output_pointer);
            layout
                .decode_full_value_row_from_data_key_into(&key, &row, &mut output)
                .unwrap();
            assert_eq!(output, values);
            let reencoded =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&output[slot]))
                })
                .unwrap()
                .into_raw_row();
            assert_eq!(reencoded, row);
        }
    }
}

#[test]
fn by_kind_full_row_corruption_keeps_eager_refusal_order_and_empty_output() {
    let signature = |error: &InternalError| {
        (
            error.class(),
            error.origin(),
            error.diagnostic_code(),
            error.diagnostic_facts(),
        )
    };
    for kind in [
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) })),
        AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) })),
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
            value: Box::new(AcceptedFieldKind::Blob { max_len: Some(8) }),
        },
    ] {
        let payload = match &kind {
            AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_) => {
                Value::List(vec![Value::Blob(vec![7; 8])])
            }
            AcceptedFieldKind::Map { .. } => Value::Map(vec![(
                Value::Text("payload".into()),
                Value::Blob(vec![7; 8]),
            )]),
            _ => unreachable!(),
        };
        let contract = payload_contract_with_storage(
            kind,
            LeafCodec::Structural,
            false,
            FieldStorageDecode::ByKind,
        );
        let values = [Value::Nat64(7), Value::Bool(true), payload];
        let mut slots = values
            .iter()
            .enumerate()
            .map(|(slot, value)| {
                encode_canonical_value_for_accepted_field_contract(
                    contract
                        .required_accepted_field_persistence_contract(slot)
                        .unwrap(),
                    value,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        // Keep the row envelope valid while truncating the final container item.
        slots[2].pop();
        let row = emit_raw_row_from_slot_payloads(contract.current_layout_version(), 3, &slots)
            .unwrap()
            .into_raw_row();
        let reference =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .err()
                .expect("the truncated container payload must refuse");
        for id in [7, 8] {
            let key =
                DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(id).into());
            let mut output = vec![Value::Text("stale".into())];
            let layout = RowLayout::from_structural_row_contract(contract.clone());
            let error = layout
                .decode_full_value_row_from_data_key_into(&key, &row, &mut output)
                .unwrap_err();
            assert_eq!(signature(&error), signature(&reference));
            assert!(output.is_empty());
        }
    }
}
