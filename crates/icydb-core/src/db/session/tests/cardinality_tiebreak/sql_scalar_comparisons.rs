//! SQL typed column equality agrees across predicate and expression execution.

use super::*;
use crate::types::{Principal, Timestamp, Ulid};

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
