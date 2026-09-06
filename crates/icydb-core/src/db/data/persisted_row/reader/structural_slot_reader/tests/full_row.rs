//! Full-row ownership transfer and executor admission boundaries.

use super::*;
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow},
        executor::terminal::RowLayout,
        key_taxonomy::PrimaryKeyComponent,
    },
    error::ErrorClass,
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
fn full_row_handoff_moves_nested_heap_backing_and_reuses_output_capacity() {
    let contract = payload_contract(nested_kind(4096), LeafCodec::Structural, true);
    let values = [Value::Nat64(7), Value::Bool(true), nested_value(1024)];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
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
