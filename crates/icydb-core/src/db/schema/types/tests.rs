use super::*;

const fn enum_kind() -> AcceptedFieldKind {
    AcceptedFieldKind::Enum {
        type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
    }
}

#[test]
fn sql_assignment_results_require_exact_integer_conversion_and_accepted_width() {
    for kind in [
        AcceptedFieldKind::Int8,
        AcceptedFieldKind::Int16,
        AcceptedFieldKind::Int32,
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
    ] {
        let value = Value::Decimal(Decimal::new(120, 1));
        assert!(input_value_from_sql_assignment_result_for_persisted_kind(&kind, &value).is_some());
        assert!(input_value_from_strict_sql_literal_for_persisted_kind(&kind, &value).is_none());
        assert!(
            input_value_from_sql_assignment_result_for_persisted_kind(
                &kind,
                &Value::Decimal(Decimal::new(125, 1))
            )
            .is_none()
        );
    }
    for (kind, invalid) in [
        (AcceptedFieldKind::Int8, 128),
        (AcceptedFieldKind::Nat8, 256),
        (AcceptedFieldKind::Nat64, -1),
    ] {
        assert!(
            input_value_from_sql_assignment_result_for_persisted_kind(
                &kind,
                &Value::Decimal(Decimal::new(invalid, 0))
            )
            .is_none()
        );
    }
    let relation = AcceptedFieldKind::Relation {
        target_path: "Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "Store".into(),
        key_kind: Box::new(AcceptedFieldKind::Nat8),
    };
    assert_eq!(
        input_value_from_sql_assignment_result_for_persisted_kind(
            &relation,
            &Value::Decimal(Decimal::new(12, 0))
        ),
        Some(InputValue::nat64(12))
    );
    assert!(
        input_value_from_sql_assignment_result_for_persisted_kind(
            &relation,
            &Value::Decimal(Decimal::new(256, 0))
        )
        .is_none()
    );
}

#[test]
fn strict_wide_integer_literals_accept_only_scale_zero_decimal_atoms() {
    for integer in [0, i128::from(u64::MAX) + 1, i128::MAX] {
        let atom = Value::Decimal(Decimal::from_i128_with_scale(integer, 0));
        let natural = u128::try_from(integer).unwrap();
        for (kind, expected) in [
            (AcceptedFieldKind::Int128, Value::Int128(integer)),
            (AcceptedFieldKind::Nat128, Value::Nat128(natural)),
        ] {
            assert_eq!(
                input_value_from_strict_sql_literal_for_persisted_kind(&kind, &atom),
                InputValue::try_from_runtime_non_enum(&expected),
            );
            for invalid in [
                Value::Decimal(Decimal::new(10, 1)),
                Value::Decimal(Decimal::new(15, 1)),
                Value::Bool(true),
            ] {
                assert!(
                    input_value_from_strict_sql_literal_for_persisted_kind(&kind, &invalid)
                        .is_none()
                );
            }
        }
    }
    let negative = Value::Decimal(Decimal::from_i128_with_scale(i128::MIN, 0));
    assert_eq!(
        input_value_from_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Int128,
            &negative
        ),
        Some(InputValue::int128(i128::MIN)),
    );
    assert!(
        input_value_from_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Nat128,
            &negative
        )
        .is_none()
    );
    let positive = Value::Decimal(Decimal::new(128, 0));
    for kind in [AcceptedFieldKind::Int64, AcceptedFieldKind::Nat64] {
        assert!(input_value_from_strict_sql_literal_for_persisted_kind(&kind, &positive).is_none());
    }
}

#[test]
fn strict_decimal_integer_literals_use_exact_accepted_scale() {
    for scale in [0, 8, 28] {
        let kind = AcceptedFieldKind::Decimal { scale };
        for value in [
            Value::Int64(-1),
            Value::Int64(0),
            Value::Nat64(1),
            Value::Int128(1),
            Value::Nat128(1),
        ] {
            let expected = crate::db::numeric::coerce_numeric_decimal(&value)
                .unwrap()
                .scale_to_integer(scale)
                .unwrap();
            assert_eq!(
                canonicalize_strict_sql_literal_for_persisted_kind(&kind, &value),
                Some(Value::Decimal(
                    Decimal::try_from_i128_with_scale(expected, scale).unwrap()
                ))
            );
        }
        for value in [Value::Text("1".into()), Value::Bool(true)] {
            assert!(
                input_value_from_strict_sql_literal_for_persisted_kind(&kind, &value).is_none()
            );
        }
    }
    let kind = AcceptedFieldKind::Decimal { scale: 1 };
    for value in [Value::Int128(i128::MAX), Value::Nat128(u128::MAX)] {
        assert!(input_value_from_strict_sql_literal_for_persisted_kind(&kind, &value).is_none());
    }
    // Existing Decimal literals retain their exact representation.
    let value = Value::Decimal(Decimal::new(15, 1));
    assert_eq!(
        input_value_from_strict_sql_literal_for_persisted_kind(&kind, &value),
        Some(InputValue::decimal(Decimal::new(15, 1)))
    );
}

#[test]
fn persisted_field_type_projection_is_total_for_non_scalar_relation_keys() {
    let relation = AcceptedFieldKind::Relation {
        target_path: "test::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "test::Store".into(),
        key_kind: Box::new(AcceptedFieldKind::test_composite()),
    };

    assert_eq!(
        field_type_from_persisted_kind(&relation),
        FieldType::Composite,
    );
}

#[test]
fn persisted_scalar_field_types_reuse_accepted_scalar_classification() {
    let kinds = [
        AcceptedFieldKind::Account,
        AcceptedFieldKind::Blob { max_len: Some(8) },
        AcceptedFieldKind::Bool,
        AcceptedFieldKind::Date,
        AcceptedFieldKind::Decimal { scale: 2 },
        AcceptedFieldKind::Duration,
        enum_kind(),
        AcceptedFieldKind::Float32,
        AcceptedFieldKind::Float64,
        AcceptedFieldKind::Int8,
        AcceptedFieldKind::Int16,
        AcceptedFieldKind::Int32,
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::Principal,
        AcceptedFieldKind::Subaccount,
        AcceptedFieldKind::Text { max_len: Some(8) },
        AcceptedFieldKind::Timestamp,
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::Ulid,
        AcceptedFieldKind::Unit,
    ];

    for kind in kinds {
        let AcceptedFieldKindCategory::Scalar(expected) =
            classify_accepted_field_kind(&kind).category()
        else {
            panic!("scalar test kind must classify as scalar");
        };

        assert_eq!(
            field_type_from_persisted_kind(&kind),
            FieldType::Scalar(expected),
        );
    }
}

#[test]
fn strict_sql_target_typing_keeps_unit_enum_labels_unresolved() {
    assert_eq!(
        input_value_from_strict_sql_literal_for_persisted_kind(
            &enum_kind(),
            &Value::Text("Active".to_string()),
        ),
        Some(InputValue::loose_enum("Active")),
    );
}

#[test]
fn strict_sql_target_typing_defers_enum_label_validation_to_catalog_admission() {
    assert_eq!(
        input_value_from_strict_sql_literal_for_persisted_kind(&enum_kind(), &Value::Nat64(7),),
        None,
    );
    for variant in ["Missing", "Loaded"] {
        assert_eq!(
            input_value_from_strict_sql_literal_for_persisted_kind(
                &enum_kind(),
                &Value::Text(variant.to_string()),
            ),
            Some(InputValue::loose_enum(variant)),
        );
    }
}

#[test]
fn strict_sql_literal_canonicalization_enforces_explicit_integer_bounds() {
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Int8,
            &Value::Int64(i64::from(i8::MAX)),
        ),
        Some(Value::Int64(i64::from(i8::MAX))),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Int8,
            &Value::Int64(i64::from(i8::MAX) + 1),
        ),
        None,
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Nat8,
            &Value::Nat64(u64::from(u8::MAX)),
        ),
        Some(Value::Nat64(u64::from(u8::MAX))),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Nat8,
            &Value::Int64(-1),
        ),
        None,
    );
}

#[test]
fn strict_sql_literal_canonicalization_supports_128_bit_integer_bounds() {
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Int128,
            &Value::Text(i128::MAX.to_string()),
        ),
        Some(Value::Int128(i128::MAX)),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Int128,
            &Value::Text((u128::try_from(i128::MAX).expect("i128 max fits u128") + 1).to_string()),
        ),
        None,
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Nat128,
            &Value::Text(u128::MAX.to_string()),
        ),
        Some(Value::Nat128(u128::MAX)),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_persisted_kind(
            &AcceptedFieldKind::Nat128,
            &Value::Text("-1".to_string()),
        ),
        None,
    );
}

#[test]
fn string_backed_filter_atoms_rehydrate_to_exact_runtime_types() {
    let date = Date::try_new(2026, 8, 5).expect("test date should be valid");
    let subaccount = Subaccount::from_array([0xab; 32]);
    let cases = [
        (
            AcceptedFieldKind::Blob { max_len: Some(4) },
            Value::Text("000aff".to_string()),
            Value::Blob(vec![0x00, 0x0a, 0xff]),
        ),
        (
            AcceptedFieldKind::Date,
            Value::Text(date.to_string()),
            Value::Date(date),
        ),
        (
            AcceptedFieldKind::Duration,
            Value::Text("12345".to_string()),
            Value::Duration(Duration::from_millis(12_345)),
        ),
        (
            AcceptedFieldKind::Subaccount,
            Value::Text(subaccount.to_string()),
            Value::Subaccount(subaccount),
        ),
        (
            AcceptedFieldKind::Timestamp,
            Value::Text("-42".to_string()),
            Value::Timestamp(Timestamp::from_millis(-42)),
        ),
    ];

    for (kind, literal, expected) in cases {
        assert_eq!(
            crate::db::query::preparation::with_preparation_work(|work| {
                canonicalize_filter_literal_for_persisted_kind(&kind, &literal, work)
                    .expect("literal preparation")
                    .map(Cow::into_owned)
            }),
            Some(expected),
            "{kind:?} should rehydrate its string-backed filter literal",
        );
    }
}

#[test]
fn subaccount_relation_filters_reuse_the_exact_key_kind_canonicalizer() {
    let subaccount = Subaccount::from_array([0xcd; 32]);
    let relation = AcceptedFieldKind::Relation {
        target_path: "test::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "test::Store".into(),
        key_kind: Box::new(AcceptedFieldKind::Subaccount),
    };

    assert_eq!(
        crate::db::query::preparation::with_preparation_work(|work| {
            canonicalize_filter_literal_for_persisted_kind(
                &relation,
                &Value::Text(subaccount.to_string()),
                work,
            )
            .expect("literal preparation")
            .map(Cow::into_owned)
        }),
        Some(Value::Subaccount(subaccount)),
    );
}

#[test]
fn collection_contains_rehydrates_string_backed_elements() {
    let subaccount = Subaccount::from_array([0xef; 32]);
    let field_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Subaccount));

    assert_eq!(
        crate::db::query::preparation::with_preparation_work(|work| {
            canonicalize_filter_collection_element_for_persisted_kind(
                &field_kind,
                &Value::Text(subaccount.to_string()),
                work,
            )
            .expect("literal preparation")
            .map(Cow::into_owned)
        }),
        Some(Value::Subaccount(subaccount)),
    );
}

#[test]
fn malformed_string_backed_filter_atoms_fail_closed() {
    let malformed = [
        (
            AcceptedFieldKind::Blob { max_len: Some(1) },
            Value::Text("0001".to_string()),
        ),
        (
            AcceptedFieldKind::Date,
            Value::Text("2026-02-30".to_string()),
        ),
        (AcceptedFieldKind::Duration, Value::Text("-1".to_string())),
        (AcceptedFieldKind::Subaccount, Value::Text("ab".repeat(31))),
        (
            AcceptedFieldKind::Subaccount,
            Value::Text(format!("{}zz", "ab".repeat(31))),
        ),
        (
            AcceptedFieldKind::Timestamp,
            Value::Text("not-a-timestamp".to_string()),
        ),
    ];

    for (kind, literal) in malformed {
        assert_eq!(
            crate::db::query::preparation::with_preparation_work(|work| {
                canonicalize_filter_literal_for_persisted_kind(&kind, &literal, work)
                    .expect("literal preparation")
                    .map(Cow::into_owned)
            }),
            None,
            "{kind:?} should reject malformed string-backed filter literals",
        );
    }
}
