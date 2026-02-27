use crate::{model::field::FieldKind, traits::EntityKind};

/// Return true when the field kind is eligible for deterministic aggregate ordering.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_aggregate_ordering(
    kind: &FieldKind,
) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => true,
        FieldKind::Relation { key_kind, .. } => field_kind_supports_aggregate_ordering(key_kind),
        FieldKind::Blob
        | FieldKind::List(_)
        | FieldKind::Set(_)
        | FieldKind::Map { .. }
        | FieldKind::Structured { .. } => false,
    }
}

/// Return true when the field kind supports numeric aggregate arithmetic.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_numeric_aggregation(
    kind: &FieldKind,
) -> bool {
    match kind {
        FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig => true,
        FieldKind::Relation { key_kind, .. } => field_kind_supports_numeric_aggregation(key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Enum { .. }
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Ulid
        | FieldKind::Unit => false,
    }
}

#[must_use]
pub(in crate::db::executor) fn field_is_orderable<E: EntityKind>(field: &str) -> bool {
    let Some(field_model) = E::MODEL
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
    else {
        return false;
    };

    field_kind_supports_aggregate_ordering(&field_model.kind)
}
