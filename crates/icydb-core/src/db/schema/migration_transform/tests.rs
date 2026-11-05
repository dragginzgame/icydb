use super::*;

#[test]
fn checked_cast_matrix_is_exact_for_sign_overflow_precision_and_null() {
    let targets = [
        AcceptedFieldKind::Int8,
        AcceptedFieldKind::Int16,
        AcceptedFieldKind::Int32,
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::Decimal { scale: 0 },
        AcceptedFieldKind::Decimal { scale: 28 },
    ];
    for target in targets {
        assert_eq!(
            checked_cast_value(&Value::Null, &target, true),
            Ok(Value::Null)
        );
        assert_eq!(
            checked_cast_value(&Value::Null, &target, false),
            Err(PersistedSchemaMigrationTransformReason::NullSource),
        );
    }
    // Target nullability must not bypass checks for non-NULL values.
    for nullable in [false, true] {
        assert_eq!(
            checked_cast_value(&Value::Int64(-1), &AcceptedFieldKind::Nat8, nullable),
            Err(PersistedSchemaMigrationTransformReason::NegativeToUnsigned),
        );
        assert_eq!(
            checked_cast_value(&Value::Nat64(256), &AcceptedFieldKind::Nat8, nullable),
            Err(PersistedSchemaMigrationTransformReason::Overflow),
        );
        assert_eq!(
            checked_cast_value(
                &Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(15, 1)),
                &AcceptedFieldKind::Decimal { scale: 0 },
                nullable,
            ),
            Err(PersistedSchemaMigrationTransformReason::PrecisionLoss),
        );
        assert_eq!(
            checked_cast_value(&Value::Int64(7), &AcceptedFieldKind::Int128, nullable),
            Ok(Value::Int128(7)),
        );
        assert_eq!(
            checked_cast_value(
                &Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(150, 2)),
                &AcceptedFieldKind::Decimal { scale: 1 },
                nullable,
            ),
            Ok(Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(
                15, 1
            ))),
        );
    }
}

#[test]
fn unsupported_cast_domains_remain_closed() {
    assert!(!supported_cast_source(&AcceptedFieldKind::Timestamp));
    assert!(!supported_cast_target(&AcceptedFieldKind::Float64));
    for nullable in [false, true] {
        assert_eq!(
            checked_cast_value(
                &Value::Text("7".to_string()),
                &AcceptedFieldKind::Int64,
                nullable
            ),
            Err(PersistedSchemaMigrationTransformReason::ValueContract),
        );
    }
}
