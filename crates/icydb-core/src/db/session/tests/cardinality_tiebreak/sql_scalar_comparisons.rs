//! SQL typed column comparisons agree across predicate and expression execution.

use super::*;
use crate::types::{
    Account, Date, Duration, IntBig, NatBig, Principal, Subaccount, Timestamp, U256, Ulid,
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
            AcceptedFieldKind::IntBig { max_bytes: 64 },
            InputValue::int_big(
                "170141183460469231731687303715884105728"
                    .parse::<IntBig>()
                    .unwrap(),
            ),
            InputValue::int_big(
                "170141183460469231731687303715884105729"
                    .parse::<IntBig>()
                    .unwrap(),
            ),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 64 },
            InputValue::nat_big(
                "340282366920938463463374607431768211456"
                    .parse::<NatBig>()
                    .unwrap(),
            ),
            InputValue::nat_big(
                "340282366920938463463374607431768211457"
                    .parse::<NatBig>()
                    .unwrap(),
            ),
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
        std::thread::spawn(move || assert_scalar_column_ordering(kind, first, second))
            .join()
            .unwrap();
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
