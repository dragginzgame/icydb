use crate::{
    db::{
        executor::aggregate::capability::{
            field_kind_supports_aggregate_ordering, field_kind_supports_numeric_aggregation,
        },
        query::plan::Direction,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// AggregateFieldValueError
///
/// Typed field-aggregate extraction/comparison errors used by aggregate
/// field-value helpers. These remain internal while field aggregates are scaffolded.
///

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, ThisError)]
pub(in crate::db::executor) enum AggregateFieldValueError {
    #[error("unknown aggregate target field: {field}")]
    UnknownField { field: String },

    #[error("aggregate target field does not support ordering: {field} kind={kind:?}")]
    UnsupportedFieldKind { field: String, kind: FieldKind },

    #[error("aggregate target field value missing on entity: {field}")]
    MissingFieldValue { field: String },

    #[error("aggregate target field value type mismatch: {field} kind={kind:?} value={value:?}")]
    FieldValueTypeMismatch {
        field: String,
        kind: FieldKind,
        value: Box<Value>,
    },

    #[error(
        "aggregate target field values are incomparable under strict ordering: {field} left={left:?} right={right:?}"
    )]
    IncomparableFieldValues {
        field: String,
        left: Box<Value>,
        right: Box<Value>,
    },
}

// Resolve one field model entry by name from an entity model.
fn field_model_by_name<'a>(model: &'a EntityModel, field: &str) -> Option<&'a FieldModel> {
    model
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
}

// Resolve one field model entry by name and return its stable slot index.
fn field_model_with_index<'a>(
    model: &'a EntityModel,
    field: &str,
) -> Option<(usize, &'a FieldModel)> {
    model
        .fields
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name == field)
}

///
/// FieldSlot
///
/// Stable aggregate field projection descriptor resolved once at setup.
///
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct FieldSlot {
    pub index: usize,
    pub kind: FieldKind,
}

// Return true when one runtime value matches the declared field kind shape.
#[cfg_attr(not(test), allow(dead_code))]
fn field_kind_matches_value(kind: &FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Account, Value::Account(_))
        | (FieldKind::Blob, Value::Blob(_))
        | (FieldKind::Bool, Value::Bool(_))
        | (FieldKind::Date, Value::Date(_))
        | (FieldKind::Decimal { .. }, Value::Decimal(_))
        | (FieldKind::Duration, Value::Duration(_))
        | (FieldKind::Enum { .. }, Value::Enum(_))
        | (FieldKind::Float32, Value::Float32(_))
        | (FieldKind::Float64, Value::Float64(_))
        | (FieldKind::Int, Value::Int(_))
        | (FieldKind::Int128, Value::Int128(_))
        | (FieldKind::IntBig, Value::IntBig(_))
        | (FieldKind::Principal, Value::Principal(_))
        | (FieldKind::Subaccount, Value::Subaccount(_))
        | (FieldKind::Text, Value::Text(_))
        | (FieldKind::Timestamp, Value::Timestamp(_))
        | (FieldKind::Uint, Value::Uint(_))
        | (FieldKind::Uint128, Value::Uint128(_))
        | (FieldKind::UintBig, Value::UintBig(_))
        | (FieldKind::Ulid, Value::Ulid(_))
        | (FieldKind::Unit, Value::Unit)
        | (FieldKind::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
        (FieldKind::Relation { key_kind, .. }, value) => field_kind_matches_value(key_kind, value),
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| field_kind_matches_value(inner, item)),
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            entries.iter().all(|(entry_key, entry_value)| {
                field_kind_matches_value(key, entry_key)
                    && field_kind_matches_value(value, entry_value)
            })
        }
        _ => false,
    }
}

/// Validate one aggregate target field against schema/runtime ordering constraints.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn validate_orderable_aggregate_target_field<E: EntityKind>(
    target_field: &str,
) -> Result<FieldKind, AggregateFieldValueError> {
    let Some(field) = field_model_by_name(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_aggregate_ordering(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(field.kind)
}

/// Resolve one orderable aggregate target field into a stable projection slot.
pub(in crate::db::executor) fn resolve_orderable_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_aggregate_ordering(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Resolve one aggregate target field into a stable projection slot.
pub(in crate::db::executor) fn resolve_any_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Validate one aggregate target field against numeric aggregate constraints.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn validate_numeric_aggregate_target_field<E: EntityKind>(
    target_field: &str,
) -> Result<FieldKind, AggregateFieldValueError> {
    let Some(field) = field_model_by_name(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_numeric_aggregation(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(field.kind)
}

/// Resolve one numeric aggregate target field into a stable projection slot.
pub(in crate::db::executor) fn resolve_numeric_aggregate_target_slot<E: EntityKind>(
    target_field: &str,
) -> Result<FieldSlot, AggregateFieldValueError> {
    let Some((index, field)) = field_model_with_index(E::MODEL, target_field) else {
        return Err(AggregateFieldValueError::UnknownField {
            field: target_field.to_string(),
        });
    };
    if !field_kind_supports_numeric_aggregation(&field.kind) {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field.kind,
        });
    }

    Ok(FieldSlot {
        index,
        kind: field.kind,
    })
}

/// Extract one field value from an entity and enforce the declared runtime field kind.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn extract_orderable_field_value<E: EntityKind + EntityValue>(
    entity: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Value, AggregateFieldValueError> {
    let Some(value) = entity.get_value_by_index(field_slot.index) else {
        return Err(AggregateFieldValueError::MissingFieldValue {
            field: target_field.to_string(),
        });
    };
    if !field_kind_matches_value(&field_slot.kind, &value) {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch {
            field: target_field.to_string(),
            kind: field_slot.kind,
            value: Box::new(value),
        });
    }

    Ok(value)
}

/// Extract one numeric field value as `Decimal` for aggregate arithmetic.
pub(in crate::db::executor) fn extract_numeric_field_decimal<E: EntityKind + EntityValue>(
    entity: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Decimal, AggregateFieldValueError> {
    let value = extract_orderable_field_value(entity, target_field, field_slot)?;
    if !value.supports_numeric_coercion() {
        return Err(AggregateFieldValueError::UnsupportedFieldKind {
            field: target_field.to_string(),
            kind: field_slot.kind,
        });
    }
    let Some(decimal) = value.to_numeric_decimal() else {
        return Err(AggregateFieldValueError::FieldValueTypeMismatch {
            field: target_field.to_string(),
            kind: field_slot.kind,
            value: Box::new(value),
        });
    };

    Ok(decimal)
}

/// Compare two extracted field values under strict same-variant ordering semantics.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn compare_orderable_field_values(
    target_field: &str,
    left: &Value,
    right: &Value,
) -> Result<Ordering, AggregateFieldValueError> {
    let Some(ordering) = Value::strict_order_cmp(left, right) else {
        return Err(AggregateFieldValueError::IncomparableFieldValues {
            field: target_field.to_string(),
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
        });
    };

    Ok(ordering)
}

/// Compare two entities by one orderable aggregate field and return base ascending ordering.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn compare_entities_by_orderable_field<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Ordering, AggregateFieldValueError> {
    let left_value = extract_orderable_field_value(left, target_field, field_slot)?;
    let right_value = extract_orderable_field_value(right, target_field, field_slot)?;

    compare_orderable_field_values(target_field, &left_value, &right_value)
}

/// Compare two entities for field-extrema selection with deterministic tie-break semantics.
///
/// Contract:
/// - primary comparison follows aggregate `direction` over the target field value.
/// - ties always break on canonical primary-key ascending order.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) fn compare_entities_for_field_extrema<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    target_field: &str,
    field_slot: FieldSlot,
    direction: Direction,
) -> Result<Ordering, AggregateFieldValueError> {
    let field_order = compare_entities_by_orderable_field(left, right, target_field, field_slot)?;
    let directional_field_order = apply_aggregate_direction(field_order, direction);
    if directional_field_order != Ordering::Equal {
        return Ok(directional_field_order);
    }

    let left_id = left.id().as_value();
    let right_id = right.id().as_value();

    compare_orderable_field_values(E::MODEL.primary_key.name, &left_id, &right_id)
}

/// Apply aggregate direction to one base ordering result.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) const fn apply_aggregate_direction(
    ordering: Ordering,
    direction: Direction,
) -> Ordering {
    match direction {
        Direction::Asc => ordering,
        Direction::Desc => ordering.reverse(),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
        compare_entities_by_orderable_field, compare_entities_for_field_extrema,
        compare_orderable_field_values, extract_numeric_field_decimal,
        resolve_any_aggregate_target_slot, resolve_orderable_aggregate_target_slot,
        validate_numeric_aggregate_target_field, validate_orderable_aggregate_target_field,
    };
    use crate::{
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
    fn validate_orderable_target_field_accepts_scalar_field() {
        let kind = validate_orderable_aggregate_target_field::<AggregateFieldEntity>("rank")
            .expect("rank should be accepted as orderable target");

        assert!(matches!(kind, FieldKind::Uint));
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
    fn validate_orderable_target_field_rejects_unknown_field() {
        let err =
            validate_orderable_aggregate_target_field::<AggregateFieldEntity>("missing_field")
                .expect_err("unknown target field must be rejected");

        assert!(matches!(err, AggregateFieldValueError::UnknownField { .. }));
    }

    #[test]
    fn validate_orderable_target_field_rejects_non_orderable_field_kind() {
        let err = validate_orderable_aggregate_target_field::<AggregateFieldEntity>("scores")
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
        let desc = apply_aggregate_direction(asc, crate::db::query::plan::Direction::Desc);

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
            crate::db::query::plan::Direction::Asc,
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
            crate::db::query::plan::Direction::Desc,
        )
        .expect("field-extrema comparator should apply canonical PK tie-break");

        assert_eq!(ordering, Ordering::Greater);
    }

    #[test]
    fn validate_numeric_target_field_accepts_numeric_field() {
        let kind = validate_numeric_aggregate_target_field::<AggregateFieldEntity>("rank")
            .expect("numeric target field should be accepted");

        assert!(matches!(kind, FieldKind::Uint));
    }

    #[test]
    fn validate_numeric_target_field_rejects_non_numeric_field() {
        let err = validate_numeric_aggregate_target_field::<AggregateFieldEntity>("label")
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
}
