//! SQL typed column comparisons agree across predicate and expression execution.

use super::*;
use crate::types::{
    Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
    Ulid,
};

fn nullable_scalar_field(
    id: u32,
    name: &str,
    slot: u16,
    kind: AcceptedFieldKind,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.into(),
        SchemaFieldSlot::new(slot),
        kind.clone(),
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
    )
}

fn seed_scalar_pairs(
    kind: AcceptedFieldKind,
    first: InputValue,
    second: InputValue,
) -> DbSession<TestCanister> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            nullable_scalar_field(2, "left_value", 1, kind.clone()),
            nullable_scalar_field(3, "right_value", 2, kind),
        ],
        Vec::new(),
    );
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    for (id, left, right) in [
        (1, first.clone(), first.clone()),
        (2, first.clone(), second.clone()),
        (3, second, first.clone()),
        (4, InputValue::null(), first.clone()),
        (5, first, InputValue::null()),
        (6, InputValue::null(), InputValue::null()),
    ] {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    ("left_value".into(), DynamicWriteCell::Value(left)),
                    ("right_value".into(), DynamicWriteCell::Value(right)),
                ])],
            )
            .unwrap();
    }
    session
}

#[test]
fn sql_affine_integer_literals_match_scalar_grouped_and_update_routes() {
    let mut cases = Vec::new();
    for kind in [
        AcceptedFieldKind::Int8,
        AcceptedFieldKind::Int16,
        AcceptedFieldKind::Int32,
        AcceptedFieldKind::Int64,
    ] {
        cases.push((kind, InputValue::int64(2), InputValue::int64(3)));
    }
    for kind in [
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
    ] {
        cases.push((kind, InputValue::nat64(2), InputValue::nat64(3)));
    }
    cases.extend([
        (
            AcceptedFieldKind::Int128,
            InputValue::int128(2),
            InputValue::int128(3),
        ),
        (
            AcceptedFieldKind::Nat128,
            InputValue::nat128(2),
            InputValue::nat128(3),
        ),
        (
            AcceptedFieldKind::Float32,
            InputValue::float32(Float32::try_new(2.0).unwrap()),
            InputValue::float32(Float32::try_new(3.0).unwrap()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::float64(Float64::try_new(2.0).unwrap()),
            InputValue::float64(Float64::try_new(3.0).unwrap()),
        ),
        (
            AcceptedFieldKind::Decimal { scale: 0 },
            InputValue::decimal(Decimal::new(2, 0)),
            InputValue::decimal(Decimal::new(3, 0)),
        ),
        (
            AcceptedFieldKind::Timestamp,
            InputValue::timestamp(Timestamp::from_millis(2)),
            InputValue::timestamp(Timestamp::from_millis(3)),
        ),
        (
            AcceptedFieldKind::Duration,
            InputValue::duration(Duration::from_millis(2)),
            InputValue::duration(Duration::from_millis(3)),
        ),
    ]);
    for (kind, first, second) in cases {
        // Each accepted schema owns fresh thread-local storage and runtime state.
        crate::testing::run_isolated(move || {
            let session = seed_scalar_pairs(kind, first, second);
            for (condition, ids) in [
                ("left_value + 1 = 3", vec![1, 2, 5]),
                ("1 + left_value = 3", vec![1, 2, 5]),
                ("3 = left_value + 1", vec![1, 2, 5]),
                ("left_value - 1 = 1", vec![1, 2, 5]),
                ("1 = left_value - 1", vec![1, 2, 5]),
                ("left_value + 1 <> 3", vec![3]),
                ("NOT left_value + 1 = 3", vec![3]),
                ("left_value + 1 < 4", vec![1, 2, 5]),
                ("4 > 1 + left_value", vec![1, 2, 5]),
                ("left_value + 1 <= 4", vec![1, 2, 3, 5]),
                ("left_value + 1 > 3", vec![3]),
                ("left_value + 1 >= 4", vec![3]),
            ] {
                assert_affine_route_rows(&session, condition, &ids);
            }
        });
    }
}

#[test]
fn sql_affine_integer_boundary_and_decimal_controls_preserve_rows() {
    for (kind, first, second, condition) in [
        (
            AcceptedFieldKind::Int32,
            InputValue::int64(-2),
            InputValue::int64(-3),
            "left_value + 3 = 1",
        ),
        (
            AcceptedFieldKind::Int128,
            InputValue::int128(i128::from(i64::MAX) + 2),
            InputValue::int128(i128::from(i64::MAX) + 3),
            "left_value - 2 = 9223372036854775807",
        ),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            InputValue::decimal(Decimal::new(225, 2)),
            InputValue::decimal(Decimal::new(325, 2)),
            "left_value + 0.5 = 2.75",
        ),
    ] {
        crate::testing::run_isolated(move || {
            let session = seed_scalar_pairs(kind, first, second);
            assert_affine_route_rows(&session, condition, &[1, 2, 5]);
        });
    }
}

fn assert_affine_route_rows(session: &DbSession<TestCanister>, condition: &str, ids: &[u64]) {
    let expected = ids
        .iter()
        .map(|id| vec![OutputValue::nat64(*id)])
        .collect::<Vec<_>>();
    for _ in 0..2 {
        assert_eq!(
            projection_rows(
                session,
                &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
            ),
            expected,
            "scalar: {condition}"
        );
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(&format!(
                "SELECT id, COUNT(*) FROM PlannerRow WHERE {condition} GROUP BY id ORDER BY id"
            ))
            .unwrap()
        else {
            panic!("expected grouped rows");
        };
        assert_eq!(
            rows.iter()
                .map(|row| row
                    .group_key()
                    .iter()
                    .chain(row.aggregate_values())
                    .cloned()
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            ids.iter()
                .map(|id| vec![OutputValue::nat64(*id), OutputValue::nat64(1)])
                .collect::<Vec<_>>(),
            "grouped: {condition}"
        );
    }
    let read_rows = "SELECT id, left_value, right_value FROM PlannerRow ORDER BY id";
    let before = projection_rows(session, read_rows);
    let result = session
        .execute_trusted_sql_exact_update(
            &format!(
                "UPDATE PlannerRow SET right_value = left_value WHERE {condition} RETURNING id"
            ),
            6,
        )
        .unwrap();
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("expected update RETURNING rows");
    };
    assert_eq!(rows, expected, "update: {condition}");
    let result = session
        .execute_trusted_sql_prefix_update(&format!(
            "UPDATE PlannerRow SET right_value = left_value WHERE {condition} ORDER BY id ASC LIMIT 100 RETURNING id"
        ))
        .unwrap();
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("expected bounded update RETURNING rows");
    };
    assert_eq!(rows, expected, "bounded update: {condition}");
    let expected_rows = before
        .into_iter()
        .map(|mut row| {
            let crate::value::PublicValue::Nat64(id) = row[0].as_public() else {
                panic!("expected numeric primary key");
            };
            if ids.contains(id) {
                row[2] = row[1].clone();
            }
            row
        })
        .collect::<Vec<_>>();
    assert_eq!(projection_rows(session, read_rows), expected_rows);
}

#[test]
fn sql_affine_non_numeric_fields_reject_before_writes() {
    let session = seed_scalar_pairs(
        AcceptedFieldKind::Text { max_len: None },
        InputValue::text("a".into()),
        InputValue::text("b".into()),
    );
    let read = "SELECT id, left_value, right_value FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for (sql, code) in [
        (
            "SELECT id FROM PlannerRow WHERE left_value + 1 = 3",
            icydb_diagnostic_code::ErrorCode::QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE,
        ),
        (
            "SELECT id, COUNT(*) FROM PlannerRow WHERE left_value + 1 = 3 GROUP BY id",
            icydb_diagnostic_code::ErrorCode::QUERY_PLAN,
        ),
    ] {
        assert_eq!(
            session
                .execute_trusted_sql_query(sql)
                .unwrap_err()
                .diagnostic()
                .error_code(),
            code,
        );
    }
    let errors = [
        session
            .execute_trusted_sql_exact_update(
                "UPDATE PlannerRow SET right_value = left_value WHERE left_value + 1 = 3",
                6,
            )
            .unwrap_err(),
        session
            .execute_trusted_sql_prefix_update(
                "UPDATE PlannerRow SET right_value = left_value WHERE left_value + 1 = 3 ORDER BY id ASC LIMIT 100",
            )
            .unwrap_err(),
    ];
    for error in errors {
        assert_eq!(
            error.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::QUERY_PLAN,
        );
    }
    assert_eq!(projection_rows(&session, read), before);
}

#[test]
fn sql_nat128_column_equality_covers_the_full_domain_and_preserves_nulls() {
    let session = seed_scalar_pairs(
        AcceptedFieldKind::Nat128,
        InputValue::nat128(1_u128 << 127),
        InputValue::nat128(u128::MAX),
    );
    for (op, expected) in [("=", vec![1]), ("!=", vec![2, 3])] {
        for condition in [
            format!("left_value {op} right_value"),
            format!("(left_value {op} right_value) AND id + 0 = id"),
            format!("CASE WHEN left_value {op} right_value THEN true ELSE false END"),
        ] {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                ),
                expected
                    .iter()
                    .map(|id| vec![OutputValue::nat64(*id)])
                    .collect::<Vec<_>>(),
                "{condition}",
            );
        }
    }
    assert_eq!(
        projection_rows(
            &session,
            "SELECT left_value = right_value FROM PlannerRow ORDER BY id"
        ),
        [Some(true), Some(false), Some(false), None, None, None]
            .into_iter()
            .map(|value| vec![value.map_or_else(OutputValue::null, OutputValue::boolean)])
            .collect::<Vec<_>>(),
    );
}

#[test]
fn sql_nat128_column_ordering_covers_the_full_domain_across_execution_lanes() {
    assert_scalar_column_ordering(
        AcceptedFieldKind::Nat128,
        InputValue::nat128(1_u128 << 127),
        InputValue::nat128(u128::MAX),
    );
}

#[test]
fn sql_same_width_float_comparisons_preserve_finite_extremes_and_nulls() {
    let mut cases = Vec::new();
    for (left, right) in [
        (0.0, f32::from_bits(1)),
        (f32::from_bits(1), f32::from_bits(2)),
        (1.0e-30, f32::MAX),
        (f32::MIN, 0.0),
    ] {
        cases.push((
            AcceptedFieldKind::Float32,
            InputValue::float32(Float32::try_new(left).unwrap()),
            InputValue::float32(Float32::try_new(right).unwrap()),
        ));
    }
    for (left, right) in [
        (0.0, f64::from_bits(1)),
        (f64::from_bits(1), f64::from_bits(2)),
        (1.0e-30, f64::MAX),
        (f64::MIN, 0.0),
    ] {
        cases.push((
            AcceptedFieldKind::Float64,
            InputValue::float64(Float64::try_new(left).unwrap()),
            InputValue::float64(Float64::try_new(right).unwrap()),
        ));
    }
    for (kind, first, second) in cases {
        crate::testing::run_isolated({
            let (kind, first, second) = (kind.clone(), first.clone(), second.clone());
            move || assert_scalar_column_ordering(kind, first, second)
        });
        crate::testing::run_isolated(move || {
            let session = seed_scalar_pairs(kind, first, second);
            for (op, expected) in [("=", [true, false, false]), ("!=", [false, true, true])] {
                for condition in [
                    format!("left_value {op} right_value"),
                    format!("CASE WHEN left_value {op} right_value THEN true ELSE false END"),
                ] {
                    assert_eq!(
                        projection_rows(
                            &session,
                            &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                        ),
                        expected
                            .into_iter()
                            .enumerate()
                            .filter(|(_, matches)| *matches)
                            .map(|(index, _)| vec![OutputValue::nat64(
                                u64::try_from(index + 1).unwrap()
                            )])
                            .collect::<Vec<_>>(),
                        "{condition}"
                    );
                }
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT left_value {op} right_value FROM PlannerRow ORDER BY id")
                    ),
                    expected
                        .into_iter()
                        .map(|value| vec![OutputValue::boolean(value)])
                        .chain([
                            vec![OutputValue::null()],
                            vec![OutputValue::null()],
                            vec![OutputValue::null()]
                        ])
                        .collect::<Vec<_>>()
                );
            }
        });
    }
}

#[test]
fn sql_same_width_float_literals_agree_across_predicate_and_expression_routes() {
    for (kind, first, second, literal) in [
        (
            AcceptedFieldKind::Float32,
            InputValue::float32(Float32::try_new(1.0e-30).unwrap()),
            InputValue::float32(Float32::try_new(f32::MAX).unwrap()),
            "FLOAT32 '1e-30'",
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::float64(Float64::try_new(1.0e-30).unwrap()),
            InputValue::float64(Float64::try_new(f64::MAX).unwrap()),
            "FLOAT64 '1e-30'",
        ),
    ] {
        crate::testing::run_isolated(move || {
            let session = seed_scalar_pairs(kind, first, second);
            for (op, reversed, expected) in [
                ("=", "=", vec![1, 2, 5]),
                ("!=", "!=", vec![3]),
                ("<", ">", vec![]),
                ("<=", ">=", vec![1, 2, 5]),
                (">", "<", vec![3]),
                (">=", "<=", vec![1, 2, 3, 5]),
            ] {
                for condition in [
                    format!("left_value {op} {literal}"),
                    format!("{literal} {reversed} left_value"),
                    format!("CASE WHEN left_value {op} {literal} THEN true ELSE false END"),
                ] {
                    assert_eq!(
                        projection_rows(
                            &session,
                            &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                        ),
                        expected
                            .iter()
                            .map(|id| vec![OutputValue::nat64(*id)])
                            .collect::<Vec<_>>(),
                        "{condition}"
                    );
                }
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT left_value {op} {literal} FROM PlannerRow ORDER BY id")
                    ),
                    (1..=6)
                        .map(|id| vec![if [4, 6].contains(&id) {
                            OutputValue::null()
                        } else {
                            OutputValue::boolean(expected.contains(&id))
                        }])
                        .collect::<Vec<_>>()
                );
            }
        });
    }
}

#[test]
fn sql_issue_timestamp_column_equality_matches_execution_lanes() {
    assert_scalar_column_equality(
        AcceptedFieldKind::Timestamp,
        InputValue::timestamp(Timestamp::from_millis(-1)),
        InputValue::timestamp(Timestamp::from_millis(1)),
    );
}

#[test]
fn sql_issue_ulid_column_equality_matches_execution_lanes() {
    assert_scalar_column_equality(
        AcceptedFieldKind::Ulid,
        InputValue::ulid(Ulid::from_u128(1)),
        InputValue::ulid(Ulid::from_u128(2)),
    );
}

#[test]
fn sql_scalar_text_column_equality_preserves_unknown() {
    assert_scalar_column_equality(
        AcceptedFieldKind::Text { max_len: None },
        InputValue::text("a".into()),
        InputValue::text("b".into()),
    );
}

#[test]
fn sql_scalar_bool_column_equality_preserves_unknown() {
    assert_scalar_column_equality(
        AcceptedFieldKind::Bool,
        InputValue::boolean(false),
        InputValue::boolean(true),
    );
}

#[test]
fn sql_scalar_principal_column_equality_preserves_unknown() {
    assert_scalar_column_equality(
        AcceptedFieldKind::Principal,
        InputValue::principal(Principal::anonymous()),
        InputValue::principal(Principal::from_slice(&[])),
    );
}

fn assert_scalar_column_equality(kind: AcceptedFieldKind, first: InputValue, second: InputValue) {
    let session = seed_scalar_pairs(kind.clone(), first, second);
    for (predicate, expected) in [
        ("left_value = right_value", vec![1]),
        ("right_value = left_value", vec![1]),
        ("left_value != right_value", vec![2, 3]),
        ("right_value != left_value", vec![2, 3]),
        ("NOT left_value = right_value", vec![2, 3]),
        ("NOT left_value != right_value", vec![1]),
    ] {
        for condition in [
            predicate.to_string(),
            format!("({predicate}) AND id + 0 = id"),
        ] {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                ),
                expected
                    .iter()
                    .map(|id| vec![OutputValue::nat64(*id)])
                    .collect::<Vec<_>>(),
                "{kind:?}: {condition}"
            );
        }
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT COUNT(*) FROM PlannerRow WHERE {predicate}")
            ),
            vec![vec![OutputValue::nat64(
                u64::try_from(expected.len()).unwrap()
            )]]
        );
    }
    for op in ["=", "!="] {
        assert_eq!(
            session
                .execute_trusted_sql_query(&format!(
                    "SELECT id FROM PlannerRow WHERE left_value {op} id"
                ))
                .unwrap_err()
                .diagnostic()
                .error_code(),
            icydb_diagnostic_code::ErrorCode::QUERY_PLAN
        );
    }
    if kind == AcceptedFieldKind::Timestamp {
        assert_eq!(
            projection_rows(
                &session,
                "SELECT id FROM PlannerRow WHERE left_value < right_value ORDER BY id"
            ),
            vec![vec![OutputValue::nat64(2)]]
        );
    }
}

#[test]
fn sql_issue_field_ordering_text_control_preserves_unknown() {
    assert_scalar_column_ordering(
        AcceptedFieldKind::Text { max_len: None },
        InputValue::text("a".into()),
        InputValue::text("b".into()),
    );
}

#[test]
fn sql_issue_field_ordering_scalar_families_match_execution_lanes() {
    for (kind, first, second) in [
        (
            AcceptedFieldKind::Ulid,
            InputValue::ulid(Ulid::from_u128(1)),
            InputValue::ulid(Ulid::from_u128(2)),
        ),
        (
            AcceptedFieldKind::Date,
            InputValue::date(Date::EPOCH),
            InputValue::date(Date::try_new(2026, 1, 1).unwrap()),
        ),
        (
            AcceptedFieldKind::Duration,
            InputValue::duration(Duration::from_millis(1)),
            InputValue::duration(Duration::from_millis(2)),
        ),
        (
            AcceptedFieldKind::Principal,
            InputValue::principal(Principal::from_slice(&[])),
            InputValue::principal(Principal::anonymous()),
        ),
        (
            AcceptedFieldKind::Subaccount,
            InputValue::subaccount(Subaccount::from_array([0; 32])),
            InputValue::subaccount(Subaccount::from_array([1; 32])),
        ),
        (
            AcceptedFieldKind::Account,
            InputValue::account(Account::from_owner_and_subaccount(
                Principal::anonymous(),
                Some(Subaccount::from_array([0; 32])),
            )),
            InputValue::account(Account::from_owner_and_subaccount(
                Principal::anonymous(),
                Some(Subaccount::from_array([1; 32])),
            )),
        ),
        (
            AcceptedFieldKind::Bool,
            InputValue::boolean(false),
            InputValue::boolean(true),
        ),
        (
            AcceptedFieldKind::U256,
            InputValue::u256(U256::ONE),
            InputValue::u256(U256::MAX),
        ),
        (
            AcceptedFieldKind::Int64,
            InputValue::int64(-1),
            InputValue::int64(1),
        ),
        (
            AcceptedFieldKind::Timestamp,
            InputValue::timestamp(Timestamp::from_millis(-1)),
            InputValue::timestamp(Timestamp::from_millis(1)),
        ),
    ] {
        // Each schema needs fresh thread-local authority, as in the maintained
        // multi-schema session fixtures.
        crate::testing::run_isolated(move || assert_scalar_column_ordering(kind, first, second));
    }
}

fn assert_scalar_column_ordering(kind: AcceptedFieldKind, first: InputValue, second: InputValue) {
    let session = seed_scalar_pairs(kind.clone(), first, second);
    for (predicate, expected) in [
        ("left_value < right_value", vec![2]),
        ("left_value <= right_value", vec![1, 2]),
        ("left_value > right_value", vec![3]),
        ("left_value >= right_value", vec![1, 3]),
        ("right_value < left_value", vec![3]),
        ("right_value <= left_value", vec![1, 3]),
        ("right_value > left_value", vec![2]),
        ("right_value >= left_value", vec![1, 2]),
        ("left_value BETWEEN left_value AND right_value", vec![1, 2]),
        ("left_value NOT BETWEEN left_value AND right_value", vec![3]),
        ("right_value BETWEEN left_value AND right_value", vec![1, 2]),
        (
            "right_value NOT BETWEEN left_value AND right_value",
            vec![3],
        ),
        ("left_value BETWEEN right_value AND right_value", vec![1]),
        (
            "left_value NOT BETWEEN right_value AND right_value",
            vec![2, 3],
        ),
    ] {
        let conditions = [
            predicate.to_string(),
            format!("({predicate}) AND id + 0 = id"),
            format!("CASE WHEN {predicate} THEN true ELSE false END"),
        ];
        for condition in conditions {
            for _ in 0..2 {
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                    ),
                    expected
                        .iter()
                        .map(|id| vec![OutputValue::nat64(*id)])
                        .collect::<Vec<_>>(),
                    "{kind:?}: {condition}",
                );
            }
        }
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(&format!(
                "SELECT id, COUNT(*) FROM PlannerRow WHERE {predicate} GROUP BY id ORDER BY id"
            ))
            .unwrap()
        else {
            panic!("grouped scalar comparison");
        };
        assert_eq!(
            rows.iter()
                .map(|row| row
                    .group_key()
                    .iter()
                    .chain(row.aggregate_values())
                    .cloned()
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|id| vec![OutputValue::nat64(*id), OutputValue::nat64(1)])
                .collect::<Vec<_>>(),
            "grouped {kind:?}: {predicate}",
        );
    }
}

#[test]
fn sql_issue_field_ordering_rejects_cross_kind_expression_operands() {
    let session = seed_scalar_pairs(
        AcceptedFieldKind::Date,
        InputValue::date(Date::EPOCH),
        InputValue::date(Date::try_new(2026, 1, 1).unwrap()),
    );
    for (condition, code) in [
        (
            "left_value < id",
            icydb_diagnostic_code::ErrorCode::QUERY_PLAN,
        ),
        (
            "left_value < id AND id + 0 = id",
            icydb_diagnostic_code::ErrorCode::QUERY_PROJECTION_BINARY_OPERANDS_INCOMPATIBLE,
        ),
        (
            "CASE WHEN left_value < id THEN true ELSE false END",
            icydb_diagnostic_code::ErrorCode::QUERY_PLAN,
        ),
    ] {
        assert_eq!(
            session
                .execute_trusted_sql_query(&format!("SELECT id FROM PlannerRow WHERE {condition}"))
                .unwrap_err()
                .diagnostic()
                .error_code(),
            code,
            "{condition}",
        );
    }
}

#[test]
fn sql_issue_case_compound_conditions_preserve_rows_and_null_results() {
    let session = seed_scalar_pairs(
        AcceptedFieldKind::Text { max_len: None },
        InputValue::text("a".into()),
        InputValue::text("b".into()),
    );
    for (condition, expected) in [
        (
            "CASE WHEN left_value <= right_value AND right_value >= left_value THEN true ELSE false END",
            vec![1, 2],
        ),
        (
            "CASE WHEN left_value < right_value OR left_value > right_value THEN true ELSE false END",
            vec![2, 3],
        ),
        (
            "CASE WHEN NOT (left_value < right_value OR left_value > right_value) THEN true ELSE NULL END",
            vec![1],
        ),
        (
            "CASE WHEN left_value <= right_value THEN NULL WHEN right_value >= left_value THEN true ELSE false END",
            vec![],
        ),
        (
            "CASE WHEN left_value > right_value THEN false WHEN left_value <= right_value THEN true ELSE NULL END",
            vec![1, 2],
        ),
        (
            "CASE WHEN left_value <= right_value THEN CASE WHEN right_value >= left_value THEN true ELSE NULL END ELSE false END",
            vec![1, 2],
        ),
        (
            "CASE WHEN id > 0 THEN left_value <= right_value AND right_value >= left_value ELSE false END",
            vec![1, 2],
        ),
        (
            "CASE WHEN id < 0 THEN false ELSE left_value < right_value OR left_value > right_value END",
            vec![2, 3],
        ),
        (
            "CASE WHEN CASE WHEN left_value < right_value OR left_value > right_value THEN true ELSE false END THEN true ELSE NULL END",
            vec![2, 3],
        ),
    ] {
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                ),
                expected
                    .iter()
                    .map(|id| vec![OutputValue::nat64(*id)])
                    .collect::<Vec<_>>(),
                "{condition}",
            );
            let SqlStatementResult::Grouped { rows, .. } = session
                .execute_trusted_sql_query(&format!(
                    "SELECT id, COUNT(*) FROM PlannerRow WHERE {condition} GROUP BY id ORDER BY id"
                ))
                .unwrap()
            else {
                panic!("grouped CASE predicate");
            };
            assert_eq!(
                rows.iter()
                    .map(|row| row.group_key().to_vec())
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|id| vec![OutputValue::nat64(*id)])
                    .collect::<Vec<_>>(),
                "grouped {condition}",
            );
        }
    }
}
