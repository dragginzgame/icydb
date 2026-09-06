//! Strict admission remains part of canonical materialization, including rereads.

use super::*;
use crate::{
    db::data::{
        RawRow,
        persisted_row::{
            canonical::encode_canonical_value_for_accepted_field_contract,
            contract::emit_raw_row_from_slot_payloads,
        },
        structural_field::encode_canonical_value_storage_bytes,
    },
    error::ErrorClass,
};

// Keep the row envelope valid while injecting a deliberately unadmitted field.
fn row_with_payload(contract: &StructuralRowContract, payload: Vec<u8>) -> RawRow {
    let mut slots = [Value::Nat64(7), Value::Bool(true)]
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
    slots.push(payload);
    emit_raw_row_from_slot_payloads(contract.current_layout_version(), 3, &slots)
        .unwrap()
        .into_raw_row()
}

#[test]
fn canonical_materialization_rejects_unadmitted_values_without_caching_them() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) }));
    let contract = payload_contract(kind, LeafCodec::Structural, false);
    let encode = |value: Value| encode_canonical_value_storage_bytes(&value).unwrap();
    let valid = encode(Value::List(vec![Value::Blob(vec![7; 8])]));
    let mut truncated = valid.clone();
    truncated.pop();
    let mut trailing = valid;
    trailing.extend(encode(Value::Null));
    for payload in [
        encode(Value::Null),
        encode(Value::Nat64(7)),
        encode(Value::List(vec![Value::Nat64(7)])),
        encode(Value::List(vec![Value::Blob(vec![7; 9])])),
        truncated,
        trailing,
        vec![u8::MAX],
    ] {
        let row = row_with_payload(&contract, payload);
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        // Lazy readers still allow an unrelated scalar; eager readers must
        // reject the invalid field even when a consumer would not select it.
        assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(7));
        let eager =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .err()
                .expect("eager admission must reject the unselected invalid field");
        assert_eq!(eager.class(), ErrorClass::Corruption);
        for _ in 0..2 {
            let error = reader.required_cached_value(2).unwrap_err();
            assert_eq!(error.diagnostic_code(), eager.diagnostic_code());
            assert_eq!(error.class(), ErrorClass::Corruption);
            let error = reader.take_direct_projection_value(2).unwrap_err();
            assert_eq!(error.diagnostic_code(), eager.diagnostic_code());
        }
    }
}

#[test]
fn canonical_materialization_preserves_nullable_empty_and_populated_rereads() {
    let contract = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) })),
        LeafCodec::Structural,
        true,
    );
    for value in [
        Value::Null,
        Value::List(vec![]),
        Value::List(vec![Value::Blob(vec![7; 8])]),
    ] {
        let row = row_with_payload(
            &contract,
            encode_canonical_value_storage_bytes(&value).unwrap(),
        );
        let mut reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        assert_eq!(reader.required_cached_value(2).unwrap(), &value);
        let taken = reader.take_direct_projection_value(2).unwrap();
        assert_eq!(taken, value);
        assert_eq!(reader.required_cached_value(2).unwrap(), &taken);
    }
}
