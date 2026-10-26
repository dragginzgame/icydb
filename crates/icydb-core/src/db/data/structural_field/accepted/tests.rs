use crate::{
    db::{
        data::{
            decode_structural_field_by_accepted_kind_bytes,
            encode_structural_field_by_accepted_kind_bytes,
            validate_structural_field_by_accepted_kind_bytes,
        },
        schema::AcceptedFieldKind,
    },
    types::Decimal,
    value::Value,
};

fn assert_accepted_roundtrip(kind: &AcceptedFieldKind, value: &Value) {
    let encoded = encode_structural_field_by_accepted_kind_bytes(kind, value)
        .expect("accepted payload should encode");
    let decoded = decode_structural_field_by_accepted_kind_bytes(&encoded, kind)
        .expect("accepted payload should decode");

    validate_structural_field_by_accepted_kind_bytes(&encoded, kind)
        .expect("accepted payload should validate");
    assert_eq!(decoded, *value);
}

fn assert_accepted_rejects(kind: &AcceptedFieldKind, raw_bytes: &[u8]) {
    assert!(decode_structural_field_by_accepted_kind_bytes(raw_bytes, kind).is_err());
    assert!(validate_structural_field_by_accepted_kind_bytes(raw_bytes, kind).is_err());
}

#[test]
fn collection_elements_reject_null_without_changing_root_null_admission() {
    for inner in [
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::Text { max_len: None },
        AcceptedFieldKind::Blob { max_len: None },
    ] {
        for (kind, value) in [
            (
                AcceptedFieldKind::List(Box::new(inner.clone())),
                Value::List(vec![Value::Null]),
            ),
            (
                AcceptedFieldKind::Set(Box::new(inner.clone())),
                Value::List(vec![Value::Null]),
            ),
            (
                AcceptedFieldKind::Map {
                    key: Box::new(inner.clone()),
                    value: Box::new(AcceptedFieldKind::Bool),
                },
                Value::Map(vec![(Value::Null, Value::Bool(true))]),
            ),
            (
                AcceptedFieldKind::Map {
                    key: Box::new(AcceptedFieldKind::Bool),
                    value: Box::new(inner.clone()),
                },
                Value::Map(vec![(Value::Bool(true), Value::Null)]),
            ),
        ] {
            // Null tokens are framed correctly but violate the element kind.
            let bytes = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
            assert_accepted_rejects(&kind, &bytes);
            let nested = AcceptedFieldKind::List(Box::new(kind));
            let bytes =
                encode_structural_field_by_accepted_kind_bytes(&nested, &Value::List(vec![value]))
                    .unwrap();
            assert_accepted_rejects(&nested, &bytes);
        }
    }
    let null_item = encode_structural_field_by_accepted_kind_bytes(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat128)),
        &Value::List(vec![Value::Null]),
    )
    .unwrap();
    assert_accepted_rejects(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::List(Box::new(
            AcceptedFieldKind::Bool,
        )))),
        &null_item,
    );
    // Root nullability belongs to the accepted field contract, not its kind.
    for kind in [AcceptedFieldKind::Int128, AcceptedFieldKind::Nat128] {
        assert_accepted_roundtrip(&kind, &Value::Null);
    }
}

#[test]
fn ordered_collections_reject_duplicates_and_reversed_persisted_values() {
    for (leaf, ordered) in [
        (
            AcceptedFieldKind::Int64,
            vec![Value::Int64(-1), Value::Int64(0), Value::Int64(1)],
        ),
        (
            AcceptedFieldKind::Nat64,
            vec![Value::Nat64(0), Value::Nat64(1), Value::Nat64(u64::MAX)],
        ),
        (
            AcceptedFieldKind::Text { max_len: None },
            vec![
                Value::Text(String::new()),
                Value::Text("a".into()),
                Value::Text("é".into()),
            ],
        ),
    ] {
        for is_map in [false, true] {
            let kind = if is_map {
                AcceptedFieldKind::Map {
                    key: Box::new(leaf.clone()),
                    value: Box::new(AcceptedFieldKind::Bool),
                }
            } else {
                AcceptedFieldKind::Set(Box::new(leaf.clone()))
            };
            let value_for = |items: Vec<Value>| {
                if is_map {
                    Value::Map(
                        items
                            .into_iter()
                            .enumerate()
                            .map(|(index, key)| (key, Value::Bool(index % 2 == 0)))
                            .collect(),
                    )
                } else {
                    Value::List(items)
                }
            };
            for items in [vec![], ordered[..1].to_vec(), ordered.clone()] {
                assert_accepted_roundtrip(&kind, &value_for(items));
            }
            let mut reversed = ordered.clone();
            reversed.reverse();
            for malformed in [
                reversed,
                vec![ordered[0].clone(), ordered[0].clone()],
                vec![ordered[0].clone(), ordered[1].clone(), ordered[1].clone()],
            ] {
                let value = value_for(malformed);
                // The low-level encoder preserves input order; accepted write
                // admission normally establishes this invariant before encoding.
                let bytes = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
                assert_accepted_rejects(&kind, &bytes);
                let nested_kind = AcceptedFieldKind::List(Box::new(kind.clone()));
                let bytes = encode_structural_field_by_accepted_kind_bytes(
                    &nested_kind,
                    &Value::List(vec![value]),
                )
                .unwrap();
                assert_accepted_rejects(&nested_kind, &bytes);
            }
            // Lists retain their input order and duplicates.
            let list = AcceptedFieldKind::List(Box::new(leaf.clone()));
            assert_accepted_roundtrip(
                &list,
                &Value::List(vec![
                    ordered[1].clone(),
                    ordered[0].clone(),
                    ordered[0].clone(),
                ]),
            );
        }
    }
}

#[test]
fn nested_text_and_blob_decoding_enforces_accepted_lengths() {
    for max_len in [None, Some(0), Some(2)] {
        for (kind, values) in [
            (
                AcceptedFieldKind::Text { max_len },
                vec![
                    Value::Text(String::new()),
                    Value::Text("é🦀".into()),
                    Value::Text("é🦀x".into()),
                ],
            ),
            (
                AcceptedFieldKind::Blob { max_len },
                vec![
                    Value::Blob(vec![]),
                    Value::Blob(vec![1, 2]),
                    Value::Blob(vec![1, 2, 3]),
                ],
            ),
        ] {
            for (index, value) in values.into_iter().enumerate() {
                let length = [0, 2, 3][index];
                let admitted = max_len.is_none_or(|limit| length <= limit);
                for (kind, value) in [
                    (kind.clone(), value.clone()),
                    (
                        AcceptedFieldKind::List(Box::new(kind.clone())),
                        Value::List(vec![value.clone()]),
                    ),
                    (
                        AcceptedFieldKind::Map {
                            key: Box::new(kind.clone()),
                            value: Box::new(AcceptedFieldKind::Bool),
                        },
                        Value::Map(vec![(value.clone(), Value::Bool(true))]),
                    ),
                    (
                        AcceptedFieldKind::Map {
                            key: Box::new(AcceptedFieldKind::Bool),
                            value: Box::new(kind.clone()),
                        },
                        Value::Map(vec![(Value::Bool(true), value)]),
                    ),
                ] {
                    let bytes =
                        encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
                    if admitted {
                        assert_accepted_roundtrip(&kind, &value);
                    } else {
                        assert_accepted_rejects(&kind, &bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn decimal_decode_requires_the_declared_scale_in_nested_values() {
    for scale in 0..=28 {
        let kind = AcceptedFieldKind::Decimal { scale };
        for stored_scale in 0..=28 {
            for mantissa in [i128::MIN, -10, 0, 10, i128::MAX] {
                let value = Value::Decimal(Decimal::from_i128_with_scale(mantissa, stored_scale));
                for (kind, value) in [
                    (kind.clone(), value.clone()),
                    (
                        AcceptedFieldKind::List(Box::new(kind.clone())),
                        Value::List(vec![value.clone()]),
                    ),
                    (
                        AcceptedFieldKind::Map {
                            key: Box::new(kind.clone()),
                            value: Box::new(kind.clone()),
                        },
                        Value::Map(vec![(value.clone(), value)]),
                    ),
                ] {
                    // The low-level encoder lets this fixture construct bytes
                    // that ordinary accepted-value admission would reject.
                    let bytes =
                        encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
                    if scale == stored_scale {
                        assert_accepted_roundtrip(&kind, &value);
                    } else {
                        assert_accepted_rejects(&kind, &bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn accepted_kind_codec_roundtrips_nested_collections() {
    let kind = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: None }),
        value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
    };
    let value = Value::Map(vec![
        (
            Value::Text("alpha".to_string()),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
        ),
        (
            Value::Text("beta".to_string()),
            Value::List(vec![Value::Nat64(3)]),
        ),
    ]);

    assert_accepted_roundtrip(&kind, &value);
}

#[test]
fn accepted_kind_codec_rejects_truncated_nested_collections() {
    let cases = [
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::Nat64),
            },
            Value::Map(vec![(Value::Text("alpha".to_string()), Value::Nat64(1))]),
        ),
    ];

    for (kind, value) in cases {
        let mut malformed = encode_structural_field_by_accepted_kind_bytes(&kind, &value)
            .expect("accepted payload should encode");
        malformed.pop();
        assert_accepted_rejects(&kind, malformed.as_slice());
    }
}

#[test]
fn accepted_kind_codec_roundtrips_relation_lists() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Relation {
        target_path: "tests::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    }));
    let value = Value::List(vec![
        Value::Ulid(crate::types::Ulid::from_u128(11)),
        Value::Ulid(crate::types::Ulid::from_u128(12)),
    ]);

    assert_accepted_roundtrip(&kind, &value);
}

// Both operations must accept the same wire language. Materialization may
// allocate output, but must not depend on a preceding validation-only walk.
fn assert_decode_validation_parity(kind: &AcceptedFieldKind, bytes: &[u8]) {
    assert_eq!(
        decode_structural_field_by_accepted_kind_bytes(bytes, kind).is_ok(),
        validate_structural_field_by_accepted_kind_bytes(bytes, kind).is_ok(),
        "decode/validation parity for {kind:?} and {bytes:?}",
    );
}

#[test]
fn accepted_by_kind_decode_and_validation_agree_on_mutated_wire() {
    let cases = [
        (AcceptedFieldKind::Nat8, Value::Nat64(255)),
        (AcceptedFieldKind::Int8, Value::Int64(-128)),
        (AcceptedFieldKind::Bool, Value::Bool(true)),
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("text".into()),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            Value::Blob(vec![1, 2, 3]),
        ),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            Value::Decimal(Decimal::from_i128_with_scale(123, 2)),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
            },
            Value::Map(vec![(
                Value::Text("key".into()),
                Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
            )]),
        ),
    ];
    for (kind, value) in cases {
        assert_accepted_roundtrip(&kind, &value);
        let wire = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
        for end in 0..wire.len() {
            assert_accepted_rejects(&kind, &wire[..end]);
        }
        let mut trailing = wire.clone();
        trailing.push(0);
        assert_accepted_rejects(&kind, &trailing);
        for index in 0..wire.len() {
            for byte in [0, 1, 127, 255] {
                let mut mutated = wire.clone();
                mutated[index] = byte;
                assert_decode_validation_parity(&kind, &mutated);
            }
        }
    }
}

#[test]
fn accepted_relation_null_filtering_still_checks_following_items() {
    let relation = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Relation {
        target_path: "tests::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: crate::testing::PROBE_ENTITY_TAG,
        target_store_path: "tests::TargetStore".into(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    }));
    let key = Value::Ulid(crate::types::Ulid::from_u128(11));
    // Encode as a plain list so a null relation item remains on the wire.
    let wire = encode_structural_field_by_accepted_kind_bytes(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Ulid)),
        &Value::List(vec![Value::Null, key.clone()]),
    )
    .unwrap();
    validate_structural_field_by_accepted_kind_bytes(&wire, &relation).unwrap();
    assert_eq!(
        decode_structural_field_by_accepted_kind_bytes(&wire, &relation).unwrap(),
        Value::List(vec![key])
    );
    for end in 0..wire.len() {
        assert_accepted_rejects(&relation, &wire[..end]);
    }
    let invalid = encode_structural_field_by_accepted_kind_bytes(
        &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        &Value::List(vec![Value::Null, Value::Nat64(11)]),
    )
    .unwrap();
    assert_accepted_rejects(&relation, &invalid);
}

#[test]
fn accepted_recursive_materialization_preserves_deep_siblings_and_boundaries() {
    let mut kind = AcceptedFieldKind::Bool;
    let mut value = Value::Bool(true);
    for depth in 0..62 {
        if depth % 2 == 0 {
            kind = AcceptedFieldKind::List(Box::new(kind));
            value = Value::List(vec![value]);
        } else {
            kind = AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Bool),
                value: Box::new(kind),
            };
            value = Value::Map(vec![(Value::Bool(false), value)]);
        }
    }
    let bytes = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
    assert_accepted_roundtrip(&kind, &value);
    for end in 0..bytes.len() {
        assert_accepted_rejects(&kind, &bytes[..end]);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_accepted_rejects(&kind, &trailing);
    let wrong_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Bool));
    assert_accepted_rejects(&wrong_kind, &bytes);
}
