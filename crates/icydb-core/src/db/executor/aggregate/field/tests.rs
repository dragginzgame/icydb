use super::{
    AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
    compare_entities_by_orderable_field, compare_entities_for_field_extrema,
    compare_orderable_field_values, extract_numeric_field_decimal,
    resolve_any_aggregate_target_slot, resolve_numeric_aggregate_target_slot,
    resolve_orderable_aggregate_target_slot,
};
use crate::{
    db::direction::Direction,
    model::field::FieldKind,
    types::{Decimal, Ulid},
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

static SCORE_LIST_KIND: FieldKind = FieldKind::Uint;

crate::test_canister! {
    ident = AggregateFieldCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = AggregateFieldStore,
    canister = AggregateFieldCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct AggregateFieldEntity {
    id: Ulid,
    rank: u32,
    label: String,
    scores: Vec<u32>,
}

crate::test_entity_schema! {
    ident = AggregateFieldEntity,
    id = Ulid,
    id_field = id,
    entity_name = "AggregateFieldEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
        ("scores", FieldKind::List(&SCORE_LIST_KIND)),
    ],
    indexes = [],
    store = AggregateFieldStore,
    canister = AggregateFieldCanister,
}

#[test]
fn resolve_orderable_target_slot_accepts_scalar_field() {
    let slot = resolve_orderable_aggregate_target_slot::<AggregateFieldEntity>("rank")
        .expect("rank should be accepted as orderable target");

    assert!(matches!(slot.kind, FieldKind::Uint));
}

#[test]
fn resolve_orderable_target_slot_matches_schema_index() {
    let slot = resolve_orderable_aggregate_target_slot::<AggregateFieldEntity>("rank")
        .expect("rank slot should resolve");

    assert_eq!(slot.index, 1);
    assert!(matches!(slot.kind, FieldKind::Uint));
}

#[test]
fn resolve_any_target_slot_supports_non_orderable_field_kind() {
    let slot = resolve_any_aggregate_target_slot::<AggregateFieldEntity>("scores")
        .expect("any-target slot should resolve list field");

    assert_eq!(slot.index, 3);
    assert!(matches!(slot.kind, FieldKind::List(_)));
}

#[test]
fn resolve_orderable_target_slot_rejects_unknown_field() {
    let err = resolve_orderable_aggregate_target_slot::<AggregateFieldEntity>("missing_field")
        .expect_err("unknown target field must be rejected");

    assert!(matches!(err, AggregateFieldValueError::UnknownField { .. }));
}

#[test]
fn resolve_orderable_target_slot_rejects_non_orderable_field_kind() {
    let err = resolve_orderable_aggregate_target_slot::<AggregateFieldEntity>("scores")
        .expect_err("list field should be rejected for field aggregates");

    assert!(matches!(
        err,
        AggregateFieldValueError::UnsupportedFieldKind { .. }
    ));
}

#[test]
fn compare_orderable_field_values_rejects_mismatched_variants() {
    let err = compare_orderable_field_values("rank", &Value::Uint(7), &Value::Text("x".into()))
        .expect_err("mismatched value variants must be rejected");

    assert!(matches!(
        err,
        AggregateFieldValueError::IncomparableFieldValues { .. }
    ));
}

#[test]
fn compare_entities_by_orderable_field_returns_deterministic_ordering() {
    let low = AggregateFieldEntity {
        id: Ulid::from_u128(1),
        rank: 10,
        label: "low".into(),
        scores: vec![1, 2],
    };
    let high = AggregateFieldEntity {
        id: Ulid::from_u128(2),
        rank: 20,
        label: "high".into(),
        scores: vec![3, 4],
    };

    let asc = compare_entities_by_orderable_field(
        &low,
        &high,
        "rank",
        FieldSlot {
            index: 1,
            kind: FieldKind::Uint,
        },
    )
    .expect("typed field comparison should succeed");
    let desc = apply_aggregate_direction(asc, Direction::Desc);

    assert_eq!(asc, Ordering::Less);
    assert_eq!(desc, Ordering::Greater);
}

#[test]
fn compare_entities_by_orderable_field_rejects_runtime_type_mismatch() {
    let left = AggregateFieldEntity {
        id: Ulid::from_u128(10),
        rank: 10,
        label: "left".into(),
        scores: vec![1, 2],
    };
    let right = AggregateFieldEntity {
        id: Ulid::from_u128(11),
        rank: 11,
        label: "right".into(),
        scores: vec![3, 4],
    };
    let err = compare_entities_by_orderable_field(
        &left,
        &right,
        "rank",
        // Deliberate mismatch: expected Text but runtime field emits Uint.
        FieldSlot {
            index: 1,
            kind: FieldKind::Text,
        },
    )
    .expect_err("runtime type mismatch must be rejected");

    assert!(matches!(
        err,
        AggregateFieldValueError::FieldValueTypeMismatch { .. }
    ));
}

#[test]
fn compare_entities_for_field_extrema_uses_pk_ascending_tie_break_in_asc() {
    let higher_id = AggregateFieldEntity {
        id: Ulid::from_u128(20),
        rank: 7,
        label: "higher".into(),
        scores: vec![1],
    };
    let lower_id = AggregateFieldEntity {
        id: Ulid::from_u128(10),
        rank: 7,
        label: "lower".into(),
        scores: vec![2],
    };

    let ordering = compare_entities_for_field_extrema(
        &higher_id,
        &lower_id,
        "rank",
        FieldSlot {
            index: 1,
            kind: FieldKind::Uint,
        },
        Direction::Asc,
    )
    .expect("field-extrema comparator should apply canonical PK tie-break");

    assert_eq!(ordering, Ordering::Greater);
}

#[test]
fn compare_entities_for_field_extrema_uses_pk_ascending_tie_break_in_desc() {
    let higher_id = AggregateFieldEntity {
        id: Ulid::from_u128(20),
        rank: 7,
        label: "higher".into(),
        scores: vec![1],
    };
    let lower_id = AggregateFieldEntity {
        id: Ulid::from_u128(10),
        rank: 7,
        label: "lower".into(),
        scores: vec![2],
    };

    let ordering = compare_entities_for_field_extrema(
        &higher_id,
        &lower_id,
        "rank",
        FieldSlot {
            index: 1,
            kind: FieldKind::Uint,
        },
        Direction::Desc,
    )
    .expect("field-extrema comparator should apply canonical PK tie-break");

    assert_eq!(ordering, Ordering::Greater);
}

#[test]
fn resolve_numeric_target_slot_accepts_numeric_field() {
    let slot = resolve_numeric_aggregate_target_slot::<AggregateFieldEntity>("rank")
        .expect("numeric target field should be accepted");

    assert!(matches!(slot.kind, FieldKind::Uint));
}

#[test]
fn resolve_numeric_target_slot_rejects_non_numeric_field() {
    let err = resolve_numeric_aggregate_target_slot::<AggregateFieldEntity>("label")
        .expect_err("text field should be rejected for numeric aggregates");

    assert!(matches!(
        err,
        AggregateFieldValueError::UnsupportedFieldKind { .. }
    ));
}

#[test]
fn extract_numeric_field_decimal_coerces_numeric_values() {
    let entity = AggregateFieldEntity {
        id: Ulid::from_u128(30),
        rank: 42,
        label: "num".into(),
        scores: vec![1, 2],
    };

    let value = extract_numeric_field_decimal(
        &entity,
        "rank",
        FieldSlot {
            index: 1,
            kind: FieldKind::Uint,
        },
    )
    .expect("numeric field extraction should succeed");

    assert_eq!(value, Decimal::from_num(42u64).expect("u64 -> decimal"));
}
