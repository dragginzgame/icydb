//! Module: db::executor::aggregate::field::tests
//! Covers field-target aggregate behavior and extrema field extraction.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    AggregateFieldKindCode, AggregateFieldValueError, AggregateValueKindCode, FieldSlot,
    apply_aggregate_direction, compare_orderable_field_values,
    extract_numeric_field_decimal_with_slot_reader, extract_orderable_field_value_with_slot_reader,
    resolve_any_aggregate_target_slot_from_fields,
    resolve_numeric_aggregate_target_slot_from_fields,
    resolve_numeric_aggregate_target_slot_from_planner_slot,
    resolve_orderable_aggregate_target_slot_from_fields,
};
use crate::{
    db::{
        direction::Direction, numeric::compare_numeric_order,
        query::plan::FieldSlot as PlannedFieldSlot, schema::AcceptedFieldKind,
    },
    entity::EntityDeclaration,
    model::field::FieldKind,
    traits::{
        AuthoredFieldProjection as AuthoredFieldProjectionTrait,
        FieldProjection as FieldProjectionTrait,
    },
    types::{Decimal, Ulid},
    value::{InputValue, Value},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cmp::Ordering;

static SCORE_LIST_KIND: FieldKind = FieldKind::Nat64;

fn compare_entity_slot(
    left: &AggregateFieldEntity,
    right: &AggregateFieldEntity,
    field_slot: FieldSlot,
) -> Result<Ordering, AggregateFieldValueError> {
    let left_value = extract_orderable_field_value_with_slot_reader(field_slot, &mut |index| {
        left.get_value_by_index(index)
    })?;
    let right_value = extract_orderable_field_value_with_slot_reader(field_slot, &mut |index| {
        right.get_value_by_index(index)
    })?;

    compare_orderable_field_values(&left_value, &right_value)
}

fn compare_entity_field_extrema(
    left: &AggregateFieldEntity,
    right: &AggregateFieldEntity,
    field_slot: FieldSlot,
    direction: Direction,
) -> Result<Ordering, AggregateFieldValueError> {
    let field_order = compare_entity_slot(left, right, field_slot)?;
    let directional_field_order = apply_aggregate_direction(field_order, direction);
    if directional_field_order != Ordering::Equal {
        return Ok(directional_field_order);
    }

    compare_orderable_field_values(&Value::Ulid(left.id), &Value::Ulid(right.id))
}

crate::test_canister! {
    ident = AggregateFieldCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = AggregateFieldStore,
    canister = AggregateFieldCanister,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct AggregateFieldEntity {
    id: Ulid,
    rank: u32,
    label: String,
    scores: Vec<u32>,
}

#[test]
fn authored_field_projection_preserves_stable_slots_and_cardinality() {
    let entity = AggregateFieldEntity {
        id: Ulid::nil(),
        rank: 7,
        label: "alpha".to_string(),
        scores: vec![3, 5],
    };

    assert_eq!(
        entity.get_input_value_by_index(0),
        Some(InputValue::Ulid(Ulid::nil())),
    );
    assert_eq!(
        entity.get_input_value_by_index(1),
        Some(InputValue::Nat64(7)),
    );
    assert_eq!(
        entity.get_input_value_by_index(2),
        Some(InputValue::Text("alpha".to_string())),
    );
    assert_eq!(
        entity.get_input_value_by_index(3),
        Some(InputValue::List(vec![
            InputValue::Nat64(3),
            InputValue::Nat64(5),
        ])),
    );
    assert_eq!(entity.get_input_value_by_index(4), None);
}

crate::test_entity! {
    ident = AggregateFieldEntity,
    entity_name = "AggregateFieldEntity",
    tag = crate::testing::AGGREGATE_FIELD_ENTITY_TAG,
    store = AggregateFieldStore,
    canister = AggregateFieldCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { rank: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { scores: Vec<u32> => FieldKind::List(&SCORE_LIST_KIND) },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[test]
fn resolve_orderable_target_slot_accepts_scalar_field() {
    let slot = resolve_orderable_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "rank",
    )
    .expect("rank should be accepted as orderable target");

    assert_eq!(slot.diagnostic_kind(), AggregateFieldKindCode::NAT64);
}

#[test]
fn resolve_orderable_target_slot_matches_schema_index() {
    let slot = resolve_orderable_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "rank",
    )
    .expect("rank slot should resolve");

    assert_eq!(slot.index, 1);
    assert_eq!(slot.diagnostic_kind(), AggregateFieldKindCode::NAT64);
}

#[test]
fn resolve_any_target_slot_supports_non_orderable_field_kind() {
    let slot = resolve_any_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "scores",
    )
    .expect("any-target slot should resolve list field");

    assert_eq!(slot.index, 3);
    assert_eq!(slot.diagnostic_kind(), AggregateFieldKindCode::LIST);
}

#[test]
fn resolve_orderable_target_slot_rejects_unknown_field() {
    let err = resolve_orderable_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "missing_field",
    )
    .expect_err("unknown target field must be rejected");

    std::assert_matches!(err, AggregateFieldValueError::UnknownField);
}

#[test]
fn resolve_orderable_target_slot_rejects_non_orderable_field_kind() {
    let err = resolve_orderable_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "scores",
    )
    .expect_err("list field should be rejected for field aggregates");

    std::assert_matches!(
        err,
        AggregateFieldValueError::UnsupportedFieldKind {
            slot_index: 3,
            kind: AggregateFieldKindCode::LIST,
        }
    );
}

#[test]
fn compare_orderable_field_values_rejects_mismatched_variants() {
    let err = compare_orderable_field_values(&Value::Nat64(7), &Value::Text("x".into()))
        .expect_err("mismatched value variants must be rejected");

    std::assert_matches!(
        err,
        AggregateFieldValueError::IncomparableFieldValues {
            left: AggregateValueKindCode::NAT64,
            right: AggregateValueKindCode::TEXT,
        }
    );
}

#[test]
fn compare_orderable_field_values_uses_shared_numeric_widen_authority() {
    let left = Value::Int64(7);
    let right = Value::Nat64(7);

    let ordering =
        compare_orderable_field_values(&left, &right).expect("numeric compare should work");

    assert_eq!(
        Some(ordering),
        compare_numeric_order(&left, &right),
        "aggregate field comparator should align with shared numeric comparator",
    );
}

#[test]
fn compare_orderable_field_values_falls_back_to_strict_for_non_numeric_values() {
    let ordering = compare_orderable_field_values(
        &Value::Text("a".to_string()),
        &Value::Text("b".to_string()),
    )
    .expect("text compare should fall back to strict ordering");

    assert_eq!(ordering, Ordering::Less);
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

    let asc = compare_entity_slot(
        &low,
        &high,
        FieldSlot::from_test_model_kind(1, FieldKind::Nat64),
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
    let err = compare_entity_slot(
        &left,
        &right,
        // Deliberate mismatch: expected Text but runtime field emits Nat.
        FieldSlot::from_test_model_kind(1, FieldKind::Text { max_len: None }),
    )
    .expect_err("runtime type mismatch must be rejected");

    std::assert_matches!(
        err,
        AggregateFieldValueError::FieldValueTypeMismatch {
            slot_index: 1,
            expected: AggregateFieldKindCode::TEXT,
            found: AggregateValueKindCode::NAT64,
        }
    );
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

    let ordering = compare_entity_field_extrema(
        &higher_id,
        &lower_id,
        FieldSlot::from_test_model_kind(1, FieldKind::Nat64),
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

    let ordering = compare_entity_field_extrema(
        &higher_id,
        &lower_id,
        FieldSlot::from_test_model_kind(1, FieldKind::Nat64),
        Direction::Desc,
    )
    .expect("field-extrema comparator should apply canonical PK tie-break");

    assert_eq!(ordering, Ordering::Greater);
}

#[test]
fn resolve_numeric_target_slot_accepts_numeric_field() {
    let slot = resolve_numeric_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "rank",
    )
    .expect("numeric target field should be accepted");

    assert_eq!(slot.diagnostic_kind(), AggregateFieldKindCode::NAT64);
}

#[test]
fn resolve_numeric_planner_slot_requires_accepted_kind_authority() {
    let err = resolve_numeric_aggregate_target_slot_from_planner_slot(
        &PlannedFieldSlot::from_model_kind(1, "rank", FieldKind::Nat64),
    )
    .expect_err("runtime aggregate slots must carry accepted kind authority");

    std::assert_matches!(
        err,
        AggregateFieldValueError::AcceptedContractUnavailable { slot_index: 1 }
    );
}

#[test]
fn resolve_numeric_planner_slot_preserves_unknown_field_taxonomy() {
    let err = resolve_numeric_aggregate_target_slot_from_planner_slot(
        &PlannedFieldSlot::from_test_slot(0, "missing"),
    )
    .expect_err("unresolved planner slot must remain an unknown field");

    std::assert_matches!(err, AggregateFieldValueError::UnknownField);
}

#[test]
fn resolve_numeric_planner_slot_uses_accepted_kind() {
    let err = resolve_numeric_aggregate_target_slot_from_planner_slot(
        &PlannedFieldSlot::from_test_accepted_kind(
            1,
            "rank",
            AcceptedFieldKind::Text { max_len: None },
        ),
    )
    .expect_err("accepted text authority must reject numeric aggregation");

    std::assert_matches!(
        err,
        AggregateFieldValueError::UnsupportedFieldKind {
            slot_index: 1,
            kind: AggregateFieldKindCode::TEXT,
        }
    );
}

#[test]
fn accepted_numeric_slot_uses_only_accepted_authority() {
    let slot = resolve_numeric_aggregate_target_slot_from_planner_slot(
        &PlannedFieldSlot::from_test_accepted_kind(1, "rank", AcceptedFieldKind::Nat8),
    )
    .expect("accepted numeric authority must select aggregate capability");

    let decimal =
        extract_numeric_field_decimal_with_slot_reader(slot, &mut |_| Some(Value::Nat64(7)))
            .expect("accepted runtime representation must drive extraction");

    assert_eq!(slot.diagnostic_kind(), AggregateFieldKindCode::NAT8);
    assert_eq!(decimal, Decimal::from_num(7u64).expect("u64 -> decimal"));
}

#[test]
fn resolve_numeric_target_slot_rejects_non_numeric_field() {
    let err = resolve_numeric_aggregate_target_slot_from_fields(
        AggregateFieldEntity::MODEL.fields(),
        "label",
    )
    .expect_err("text field should be rejected for numeric aggregates");

    std::assert_matches!(
        err,
        AggregateFieldValueError::UnsupportedFieldKind {
            slot_index: 2,
            kind: AggregateFieldKindCode::TEXT,
        }
    );
}

#[test]
fn extract_numeric_field_decimal_coerces_numeric_values() {
    let entity = AggregateFieldEntity {
        id: Ulid::from_u128(30),
        rank: 42,
        label: "num".into(),
        scores: vec![1, 2],
    };

    let value = extract_numeric_field_decimal_with_slot_reader(
        FieldSlot::from_test_model_kind(1, FieldKind::Nat64),
        &mut |index| entity.get_value_by_index(index),
    )
    .expect("numeric field extraction should succeed");

    assert_eq!(value, Decimal::from_num(42u64).expect("u64 -> decimal"));
}
