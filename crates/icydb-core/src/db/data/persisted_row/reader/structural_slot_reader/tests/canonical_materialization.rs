//! Strict admission remains part of canonical materialization, including rereads.

use super::*;
use crate::{
    db::data::{
        RawRow,
        persisted_row::{
            canonical::encode_canonical_value_for_accepted_field_contract,
            contract::emit_raw_row_from_slot_payloads,
            reader::direct::decode_sparse_required_slot_with_contract,
        },
        structural_field::encode_canonical_value_storage_bytes,
    },
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, PredicateProgram},
    db::{codec::decode_row_payload_bytes, key_taxonomy::PrimaryKeyComponent},
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
fn full_and_sparse_reads_reject_malformed_unselected_spans() {
    let contract = selective_payload_contract();
    let row = row_with_payload(
        &contract,
        encode_canonical_value_for_accepted_field_contract(
            contract
                .required_accepted_field_persistence_contract(2)
                .unwrap(),
            &Value::Blob(vec![7]),
        )
        .unwrap(),
    );
    let payload = decode_row_payload_bytes(row.as_bytes())
        .unwrap()
        .into_payload();
    let third_entry = row.len() - payload.len() + 2 + 2 * 8;
    let third_start = u32::from_be_bytes(payload[18..22].try_into().unwrap());
    let third_len = u32::from_be_bytes(payload[22..26].try_into().unwrap());
    // Corrupt an unselected field while keeping the envelope valid. Both
    // scanners must validate the whole physical table before returning a slot.
    for (start, len) in [
        (0_u32, 0_u32),
        (u32::MAX, 1),
        (0, u32::MAX),
        (0, third_len), // reordered/overlapping span within the data section
        (third_start - 1, third_len + 1), // overlap, still ending at the payload end
        (third_start + 1, third_len - 1), // gap, still ending at the payload end
        (third_start, third_len - 1), // unclaimed trailing byte
    ] {
        let mut bytes = row.as_bytes().to_vec();
        bytes[third_entry..third_entry + 4].copy_from_slice(&start.to_be_bytes());
        bytes[third_entry + 4..third_entry + 8].copy_from_slice(&len.to_be_bytes());
        let corrupt = RawRow::try_new(bytes).unwrap();
        let full = StructuralSlotReader::from_raw_row_with_borrowed_contract(&corrupt, &contract)
            .err()
            .expect("full scan must reject the malformed span");
        let sparse = decode_sparse_required_slot_with_contract(
            &corrupt,
            &contract,
            &PrimaryKeyComponent::Nat64(7).into(),
            0,
        )
        .unwrap_err();
        assert_eq!(full.class(), ErrorClass::Corruption);
        assert_eq!(sparse.class(), full.class());
        assert_eq!(sparse.diagnostic_code(), full.diagnostic_code());
    }
}

#[test]
fn by_kind_collection_reads_reject_invalid_unselected_items() {
    use crate::db::data::encode_structural_field_by_accepted_kind_bytes;

    for (kind, value) in [
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: Some(2) })),
            Value::List(vec![Value::Text("é🦀x".into())]),
        ),
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(2) })),
            Value::List(vec![Value::Blob(vec![1, 2, 3])]),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(2), Value::Nat64(1)]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Nat64),
                value: Box::new(AcceptedFieldKind::Bool),
            },
            Value::Map(vec![
                (Value::Nat64(1), Value::Bool(false)),
                (Value::Nat64(1), Value::Bool(true)),
            ]),
        ),
    ] {
        let contract = payload_contract_with_storage(
            kind.clone(),
            LeafCodec::Structural,
            false,
            FieldStorageDecode::ByKind,
        );
        let row = row_with_payload(
            &contract,
            encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap(),
        );
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(7));
        let eager =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .err()
                .expect("eager decoding must reject an invalid unselected item");
        assert_eq!(eager.class(), ErrorClass::Corruption);
        for _ in 0..2 {
            assert_eq!(
                reader
                    .required_cached_value(2)
                    .unwrap_err()
                    .diagnostic_code(),
                eager.diagnostic_code()
            );
            assert_eq!(
                reader
                    .take_direct_projection_value(2)
                    .unwrap_err()
                    .diagnostic_code(),
                eager.diagnostic_code()
            );
        }
    }
}

#[test]
fn scalar_row_reads_enforce_nullability_and_text_blob_limits() {
    use crate::db::data::persisted_row::{
        canonical::scalar_slot_value_ref_from_accepted_value,
        codec::{ScalarSlotValueRef, encode_scalar_slot_value},
        contract::decode_runtime_value_from_accepted_field_contract,
    };

    for max_len in [None, Some(0), Some(2)] {
        for nullable in [false, true] {
            for (kind, codec, values) in [
                (
                    AcceptedFieldKind::Text { max_len },
                    ScalarCodec::Text,
                    vec![
                        Value::Null,
                        Value::Text(String::new()),
                        Value::Text("é🦀".into()),
                        Value::Text("é🦀x".into()),
                    ],
                ),
                (
                    AcceptedFieldKind::Blob { max_len },
                    ScalarCodec::Blob,
                    vec![
                        Value::Null,
                        Value::Blob(vec![]),
                        Value::Blob(vec![1, 2]),
                        Value::Blob(vec![1, 2, 3]),
                    ],
                ),
            ] {
                let contract = payload_contract(kind, LeafCodec::Scalar(codec), nullable);
                for value in values {
                    let admitted = match &value {
                        Value::Null => nullable,
                        Value::Text(text) => {
                            max_len.is_none_or(|limit| text.chars().count() <= limit as usize)
                        }
                        Value::Blob(bytes) => {
                            max_len.is_none_or(|limit| bytes.len() <= limit as usize)
                        }
                        _ => unreachable!(),
                    };
                    // Bypass write admission to test untrusted persisted bytes.
                    let scalar = match &value {
                        Value::Null => ScalarSlotValueRef::Null,
                        _ => scalar_slot_value_ref_from_accepted_value(&value, codec).unwrap(),
                    };
                    let payload = encode_scalar_slot_value(scalar, codec).unwrap();
                    let owned = decode_runtime_value_from_accepted_field_contract(
                        contract.required_accepted_field_decode_contract(2).unwrap(),
                        &payload,
                    );
                    let row = row_with_payload(&contract, payload);
                    let mut reader =
                        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract)
                            .unwrap();
                    assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(7));
                    let eager = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                        &row, &contract,
                    );
                    let sparse = decode_sparse_required_slot_with_contract(
                        &row,
                        &contract,
                        &PrimaryKeyComponent::Nat64(7).into(),
                        2,
                    );
                    assert_eq!(eager.is_ok(), admitted);
                    assert_eq!(sparse.is_ok(), admitted);
                    if admitted {
                        assert_eq!(owned.unwrap(), value);
                        assert_eq!(reader.required_cached_value(2).unwrap(), &value);
                        assert_eq!(reader.take_direct_projection_value(2).unwrap(), value);
                    } else {
                        let error = owned.unwrap_err();
                        assert_eq!(error.class(), ErrorClass::Corruption);
                        for _ in 0..2 {
                            assert_eq!(
                                reader
                                    .required_cached_value(2)
                                    .unwrap_err()
                                    .diagnostic_code(),
                                error.diagnostic_code()
                            );
                            assert_eq!(
                                reader
                                    .take_direct_projection_value(2)
                                    .unwrap_err()
                                    .diagnostic_code(),
                                error.diagnostic_code()
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn decimal_row_reads_reject_wrong_scale_without_normalizing_stored_bytes() {
    use crate::{db::data::encode_structural_field_by_accepted_kind_bytes, types::Decimal};

    let kind = AcceptedFieldKind::Decimal { scale: 2 };
    let contract = payload_contract_with_storage(
        kind.clone(),
        LeafCodec::Structural,
        false,
        FieldStorageDecode::ByKind,
    );
    // Equal numeric values at a different scale are still noncanonical storage.
    for (mantissa, scale, admitted) in [(100, 2, true), (10, 1, false), (1000, 3, false)] {
        let value = Value::Decimal(Decimal::from_i128_with_scale(mantissa, scale));
        let row = row_with_payload(
            &contract,
            encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap(),
        );
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(7));
        let eager =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract);
        if admitted {
            assert!(eager.is_ok());
            assert_eq!(reader.required_cached_value(2).unwrap(), &value);
            assert_eq!(reader.take_direct_projection_value(2).unwrap(), value);
        } else {
            let eager = eager
                .err()
                .expect("eager reads must reject the invalid scale");
            assert_eq!(eager.class(), ErrorClass::Corruption);
            for _ in 0..2 {
                let error = reader.required_cached_value(2).unwrap_err();
                assert_eq!(error.diagnostic_code(), eager.diagnostic_code());
                assert_eq!(
                    reader.take_direct_projection_value(2).unwrap_err().class(),
                    ErrorClass::Corruption
                );
            }
        }
    }
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
fn collection_predicates_preserve_early_late_absent_empty_and_null_results() {
    let contract = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) })),
        LeafCodec::Structural,
        true,
    );
    for (value, membership, empty) in [
        (Value::Null, [false; 3], false),
        (Value::List(vec![]), [false; 3], true),
        (
            Value::List(vec![Value::Blob(vec![1]), Value::Blob(vec![2])]),
            [true, true, false],
            false,
        ),
    ] {
        let row = row_with_payload(
            &contract,
            encode_canonical_value_storage_bytes(&value).unwrap(),
        );
        let comparisons = [1, 2, 3]
            .into_iter()
            .zip(membership)
            .map(|(byte, expected)| {
                (
                    Predicate::Compare(ComparePredicate::with_coercion(
                        "snapshot",
                        CompareOp::Contains,
                        Value::Blob(vec![byte]),
                        CoercionId::Strict,
                    )),
                    expected,
                )
            });
        let emptiness = [
            (
                Predicate::IsEmpty {
                    field: "snapshot".into(),
                },
                empty,
            ),
            (
                Predicate::IsNotEmpty {
                    field: "snapshot".into(),
                },
                !empty,
            ),
        ];
        for (predicate, expected) in comparisons.chain(emptiness) {
            // A fresh reader prevents another predicate from warming this slot.
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            let program = PredicateProgram::compile_with_row_contract(&contract, &predicate);
            assert_eq!(
                program.eval_with_structural_slot_reader(&reader).unwrap(),
                expected
            );
        }
    }
}

#[test]
fn collection_predicates_reject_invalid_tail_after_matching_first_item() {
    let contract = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: Some(8) })),
        LeafCodec::Structural,
        false,
    );
    let first = Value::Blob(vec![1]);
    let encode = |tail| {
        encode_canonical_value_storage_bytes(&Value::List(vec![first.clone(), tail])).unwrap()
    };
    let valid = encode(Value::Blob(vec![2]));
    let mut truncated = valid.clone();
    truncated.pop();
    let mut trailing = valid;
    trailing.push(0);
    for payload in [
        encode(Value::Nat64(2)),
        encode(Value::Blob(vec![2; 9])),
        truncated,
        trailing,
    ] {
        let row = row_with_payload(&contract, payload);
        for predicate in [
            Predicate::Compare(ComparePredicate::with_coercion(
                "snapshot",
                CompareOp::Contains,
                first.clone(),
                CoercionId::Strict,
            )),
            Predicate::IsEmpty {
                field: "snapshot".into(),
            },
            Predicate::IsNotEmpty {
                field: "snapshot".into(),
            },
        ] {
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            let program = PredicateProgram::compile_with_row_contract(&contract, &predicate);
            let error = program
                .eval_with_structural_slot_reader(&reader)
                .expect_err("a boolean result must not hide invalid trailing items");
            assert_eq!(error.class(), ErrorClass::Corruption);
        }
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
