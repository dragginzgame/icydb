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

#[test]
fn narrow_integer_rows_preserve_projection_nulls_and_encoded_savings() {
    use crate::db::data::CanonicalSlotReader;

    for (kind, codec, value, width, wide_kind, wide_codec) in [
        (
            AcceptedFieldKind::Int8,
            ScalarCodec::Int8,
            Value::Int64(-128),
            1,
            AcceptedFieldKind::Int64,
            ScalarCodec::Int64,
        ),
        (
            AcceptedFieldKind::Int16,
            ScalarCodec::Int16,
            Value::Int64(-32768),
            2,
            AcceptedFieldKind::Int64,
            ScalarCodec::Int64,
        ),
        (
            AcceptedFieldKind::Int32,
            ScalarCodec::Int32,
            Value::Int64(i64::from(i32::MIN)),
            4,
            AcceptedFieldKind::Int64,
            ScalarCodec::Int64,
        ),
        (
            AcceptedFieldKind::Nat8,
            ScalarCodec::Nat8,
            Value::Nat64(255),
            1,
            AcceptedFieldKind::Nat64,
            ScalarCodec::Nat64,
        ),
        (
            AcceptedFieldKind::Nat16,
            ScalarCodec::Nat16,
            Value::Nat64(65535),
            2,
            AcceptedFieldKind::Nat64,
            ScalarCodec::Nat64,
        ),
        (
            AcceptedFieldKind::Nat32,
            ScalarCodec::Nat32,
            Value::Nat64(u64::from(u32::MAX)),
            4,
            AcceptedFieldKind::Nat64,
            ScalarCodec::Nat64,
        ),
    ] {
        let contract = payload_contract(kind, LeafCodec::Scalar(codec), true);
        let wide = payload_contract(wide_kind, LeafCodec::Scalar(wide_codec), true);
        for payload in [value, Value::Null] {
            let values = [Value::Nat64(7), Value::Bool(true), payload];
            let encode = |contract: &StructuralRowContract| {
                canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row()
            };
            let row = encode(&contract);
            let wide_row = encode(&wide);
            let mut reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &row, &contract,
            )
            .unwrap();
            let null = matches!(values[2], Value::Null);
            assert_eq!(
                reader.required_bytes(2).unwrap().len(),
                2 + if null { 0 } else { width }
            );
            assert_eq!(wide_row.len() - row.len(), if null { 0 } else { 8 - width });
            assert_eq!(reader.required_cached_value(2).unwrap(), &values[2]);
            assert_eq!(reader.take_direct_projection_value(2).unwrap(), values[2]);
            assert_eq!(reader.required_cached_value(2).unwrap(), &values[2]);
            let mut decoded = Vec::new();
            reader.decode_all_values_into(&mut decoded).unwrap();
            assert_eq!(decoded, values);
        }
    }
}

#[test]
fn big_integer_rows_preserve_nested_projection_and_borrowed_skip_boundaries() {
    use crate::{
        db::data::{
            CanonicalSlotReader, ValueStorageView,
            structural_field::decode_canonical_value_storage_bytes,
        },
        types::{IntBig, NatBig},
    };

    for (kind, value) in [
        (
            AcceptedFieldKind::IntBig { max_bytes: 2 },
            Value::IntBig(IntBig::from(-8192)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 2 },
            Value::NatBig(NatBig::from(16383_u32)),
        ),
    ] {
        let map_kind = |max_bytes| AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::List(Box::new(match kind {
                AcceptedFieldKind::IntBig { .. } => AcceptedFieldKind::IntBig { max_bytes },
                _ => AcceptedFieldKind::NatBig { max_bytes },
            }))),
        };
        let contract = payload_contract(map_kind(2), LeafCodec::Structural, true);
        let narrow = payload_contract(map_kind(1), LeafCodec::Structural, true);
        let items = Value::List(vec![value]);
        let payload = Value::Map(vec![
            (Value::Text("a".into()), items.clone()),
            (Value::Text("b".into()), items.clone()),
        ]);
        let row = row_with_payload(
            &contract,
            encode_canonical_value_storage_bytes(&payload).unwrap(),
        );
        let mut reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        let bytes = reader.required_bytes(2).unwrap();
        let view = ValueStorageView::from_raw_validated(bytes).unwrap();
        // Looking up the second entry must skip the first compact bigint/list
        // without confusing magnitude bytes with structural tags.
        let second = view.map_text_key_bytes(b"b").unwrap().unwrap();
        assert_eq!(
            decode_canonical_value_storage_bytes(second.as_bytes()).unwrap(),
            items
        );
        assert_eq!(reader.take_direct_projection_value(2).unwrap(), payload);
        assert_eq!(reader.required_cached_value(2).unwrap(), &payload);
        assert!(
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &narrow)
                .is_err()
        );
    }
}

#[test]
fn fixed_scalar_rows_preserve_nested_projection_nulls_and_reencoding() {
    use crate::{
        db::data::CanonicalSlotReader,
        types::{Account, Date, Decimal, Principal, Subaccount, U256, Ulid},
    };

    for (kind, value, width) in [
        (AcceptedFieldKind::Date, Value::Date(Date::MIN), 5),
        (
            AcceptedFieldKind::Decimal { scale: 28 },
            Value::Decimal(Decimal::from_i128_with_scale(i128::MIN, 28)),
            18,
        ),
        (
            AcceptedFieldKind::Account,
            Value::Account(Account::new(Principal::MAX, Some(Subaccount::MAX))),
            63,
        ),
        (AcceptedFieldKind::Ulid, Value::Ulid(Ulid::MAX), 17),
        (AcceptedFieldKind::U256, Value::U256(U256::MAX), 33),
    ] {
        let contract = payload_contract(
            AcceptedFieldKind::List(Box::new(kind)),
            LeafCodec::Structural,
            true,
        );
        for payload in [Value::List(vec![value]), Value::Null] {
            let values = [Value::Nat64(7), Value::Bool(true), payload.clone()];
            let row =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row();
            let mut reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &row, &contract,
            )
            .unwrap();
            assert_eq!(
                reader.required_bytes(2).unwrap().len(),
                if payload == Value::Null { 1 } else { 5 + width }
            );
            assert_eq!(reader.take_direct_projection_value(2).unwrap(), payload);
            assert_eq!(reader.required_cached_value(2).unwrap(), &payload);
            let mut decoded = Vec::new();
            reader.decode_all_values_into(&mut decoded).unwrap();
            let reencoded =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&decoded[slot]))
                })
                .unwrap()
                .into_raw_row();
            assert_eq!(reencoded.as_bytes(), row.as_bytes());
        }
    }
}
