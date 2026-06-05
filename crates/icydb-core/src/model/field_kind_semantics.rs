//! Module: model::field_kind_semantics
//! Responsibility: runtime field-kind semantic classification and lossless literal canonicalization for runtime `FieldKind`.
//! Does not own: predicate normalization or executor policy.
//! Boundary: one semantic spine that adjacent layers consume instead of rebuilding ad hoc field-kind ladders.

use crate::{
    model::field::FieldKind,
    types::{Account, Decimal, Float32, Float64, IntBig, NatBig, Principal, Ulid},
    value::{Value, ValueEnum},
};
use std::str::FromStr;

///
/// FieldKindNumericClass
///
/// Runtime model-owned numeric family projection for one field kind.
/// This keeps narrow-vs-wide-vs-float-vs-decimal distinctions explicit so
/// consumers can answer capability questions without rebuilding exact kind ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FieldKindNumericClass {
    Signed64,
    Unsigned64,
    SignedWide,
    UnsignedWide,
    FloatLike,
    DecimalLike,
    DurationLike,
    TimestampLike,
}

///
/// FieldKindScalarClass
///
/// Runtime model-owned scalar semantic family for one field kind.
/// This is the coarse semantic layer that downstream capability answers are
/// derived from instead of matching directly on `FieldKind` everywhere.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FieldKindScalarClass {
    Boolean,
    Numeric(FieldKindNumericClass),
    Text,
    OrderedOpaque,
    Opaque,
}

///
/// FieldKindCategory
///
/// Runtime model-owned top-level field-kind category for semantic classification.
/// Relations keep their referenced scalar class explicit so consumers can
/// recurse semantically without hand-rolling relation-key ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FieldKindCategory {
    Scalar(FieldKindScalarClass),
    Relation(FieldKindScalarClass),
    Collection,
    Structured { queryable: bool },
}

impl FieldKindCategory {
    /// Return true when this category participates in numeric aggregates.
    #[must_use]
    const fn supports_aggregate_numeric(self) -> bool {
        matches!(
            self,
            Self::Scalar(FieldKindScalarClass::Numeric(_))
                | Self::Relation(FieldKindScalarClass::Numeric(_))
        )
    }

    /// Return true when this category supports deterministic aggregate ordering.
    #[must_use]
    const fn supports_aggregate_ordering(self) -> bool {
        match self {
            Self::Scalar(class) | Self::Relation(class) => scalar_class_supports_ordering(class),
            Self::Collection | Self::Structured { .. } => false,
        }
    }

    /// Return true when this category participates in predicate numeric widening.
    #[must_use]
    const fn supports_predicate_numeric_widen(self) -> bool {
        matches!(
            self,
            Self::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Signed64
                    | FieldKindNumericClass::Unsigned64
                    | FieldKindNumericClass::FloatLike
                    | FieldKindNumericClass::DecimalLike,
            )) | Self::Relation(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Signed64
                    | FieldKindNumericClass::Unsigned64
                    | FieldKindNumericClass::FloatLike
                    | FieldKindNumericClass::DecimalLike,
            ))
        )
    }
}

///
/// FieldKindSemantics
///
/// Runtime model-owned semantic contract for one `FieldKind`.
/// Consumers read capabilities and coarse family identity from this contract
/// instead of rebuilding interpretation ladders locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FieldKindSemantics {
    category: FieldKindCategory,
}

impl FieldKindSemantics {
    /// Build one runtime model-owned field-kind semantic contract.
    #[must_use]
    const fn new(category: FieldKindCategory) -> Self {
        Self { category }
    }

    /// Return the coarse semantic category for this field kind.
    #[must_use]
    const fn category(self) -> FieldKindCategory {
        self.category
    }

    /// Return true when this field kind participates in numeric aggregates.
    #[must_use]
    pub(crate) const fn supports_aggregate_numeric(self) -> bool {
        self.category.supports_aggregate_numeric()
    }

    /// Return true when this field kind supports deterministic aggregate ordering.
    #[must_use]
    pub(crate) const fn supports_aggregate_ordering(self) -> bool {
        self.category.supports_aggregate_ordering()
    }

    /// Return true when this field kind participates in predicate numeric widening.
    #[must_use]
    pub(crate) const fn supports_predicate_numeric_widen(self) -> bool {
        self.category.supports_predicate_numeric_widen()
    }
}

/// Return true when one single grouped field kind already arrives in canonical
/// grouped-equality form.
#[must_use]
pub(crate) const fn field_kind_has_identity_group_canonical_form(kind: FieldKind) -> bool {
    !matches!(
        kind,
        FieldKind::Decimal { .. }
            | FieldKind::Enum { .. }
            | FieldKind::Relation { .. }
            | FieldKind::List(_)
            | FieldKind::Set(_)
            | FieldKind::Map { .. }
            | FieldKind::Structured { .. }
            | FieldKind::Unit
    )
}

/// Canonicalize one grouped-key compare literal against one grouped field kind
/// when the Int<->Nat conversion is lossless and unambiguous.
///
/// Both fluent grouped `HAVING` and SQL grouped `HAVING` bind through this
/// helper so those two surfaces cannot drift on grouped-key numeric literal
/// normalization again.
#[must_use]
pub(crate) fn canonicalize_grouped_having_numeric_literal_for_field_kind(
    field_kind: Option<FieldKind>,
    value: &Value,
) -> Option<Value> {
    canonicalize_lossless_field_literal_for_kind(field_kind?, value, false)
}

/// Convert one parsed strict SQL literal into the exact runtime `Value`
/// variant required by the field kind when that conversion is lossless and
/// unambiguous.
///
/// This keeps SQL string tokens usable for scalar key types like `Ulid`
/// without widening text coercion across the general predicate surface.
#[must_use]
pub(crate) fn canonicalize_strict_sql_literal_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    canonicalize_strict_sql_literal_for_kind_impl(*kind, value)
}

/// Convert one frontend filter literal into the exact runtime `Value` variant
/// required by the field kind when that conversion is lossless and
/// unambiguous.
///
/// This keeps the public filter wire contract string-backed while the
/// schema-aware query boundary still rehydrates typed IDs and numerics before
/// planner validation consumes the predicate.
#[must_use]
pub(crate) fn canonicalize_filter_literal_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    canonicalize_lossless_field_literal_for_kind(*kind, value, true)
}

/// Classify one runtime `FieldKind` through the runtime model-owned semantic contract.
#[must_use]
pub(crate) const fn classify_field_kind(kind: &FieldKind) -> FieldKindSemantics {
    match kind {
        FieldKind::Account
        | FieldKind::Date
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::OrderedOpaque,
        )),
        FieldKind::Blob { .. } => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Opaque))
        }
        FieldKind::Bool => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Boolean))
        }
        FieldKind::Decimal { .. } => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::DecimalLike),
        )),
        FieldKind::Duration => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::DurationLike),
        )),
        FieldKind::Int8 | FieldKind::Int16 | FieldKind::Int32 | FieldKind::Int64 => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Signed64,
            )))
        }
        FieldKind::Int128 | FieldKind::IntBig { .. } => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::SignedWide,
            )))
        }
        FieldKind::Timestamp => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::TimestampLike),
        )),
        FieldKind::Nat8 | FieldKind::Nat16 | FieldKind::Nat32 | FieldKind::Nat64 => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Unsigned64,
            )))
        }
        FieldKind::Nat128 | FieldKind::NatBig { .. } => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::UnsignedWide,
            )))
        }
        FieldKind::Enum { .. } | FieldKind::Text { .. } => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Text))
        }
        FieldKind::Float32 | FieldKind::Float64 => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::FloatLike,
            )))
        }
        FieldKind::Relation { key_kind, .. } => FieldKindSemantics::new(
            FieldKindCategory::Relation(classify_relation_scalar_class(key_kind)),
        ),
        FieldKind::List(_) | FieldKind::Map { .. } | FieldKind::Set(_) => {
            FieldKindSemantics::new(FieldKindCategory::Collection)
        }
        FieldKind::Structured { queryable } => {
            FieldKindSemantics::new(FieldKindCategory::Structured {
                queryable: *queryable,
            })
        }
    }
}

// Reduce one relation key kind onto the scalar semantic class that adjacent
// planner/executor capabilities are allowed to consume.
const fn classify_relation_scalar_class(kind: &FieldKind) -> FieldKindScalarClass {
    match classify_field_kind(kind).category() {
        FieldKindCategory::Scalar(class) | FieldKindCategory::Relation(class) => class,
        FieldKindCategory::Collection | FieldKindCategory::Structured { .. } => {
            FieldKindScalarClass::Opaque
        }
    }
}

// Keep ordering eligibility derived from one scalar semantic family instead of
// rebuilding ad hoc field-kind allowlists at each consumer.
const fn scalar_class_supports_ordering(class: FieldKindScalarClass) -> bool {
    !matches!(class, FieldKindScalarClass::Opaque)
}

// Canonicalize one lossless field-key literal while keeping the grouped-key
// numeric path and SQL strict-literal path on one recursive field-kind owner.
#[expect(clippy::too_many_lines)]
fn canonicalize_lossless_field_literal_for_kind(
    kind: FieldKind,
    value: &Value,
    allow_text_ulid: bool,
) -> Option<Value> {
    match kind {
        FieldKind::Account => match value {
            Value::Account(inner) => Some(Value::Account(*inner)),
            Value::Text(inner) => Account::from_str(inner).ok().map(Value::Account),
            _ => None,
        },
        FieldKind::Bool => match value {
            Value::Bool(inner) => Some(Value::Bool(*inner)),
            _ => None,
        },
        FieldKind::Decimal { .. } => match value {
            Value::Decimal(inner) => Some(Value::Decimal(*inner)),
            Value::Text(inner) => Decimal::from_str(inner).ok().map(Value::Decimal),
            _ => None,
        },
        FieldKind::Enum { .. } => match value {
            Value::Enum(inner) => Some(Value::Enum(inner.clone())),
            Value::Text(inner) => Some(Value::Enum(ValueEnum::loose(inner))),
            _ => None,
        },
        FieldKind::Float32 => match value {
            Value::Float32(inner) => Some(Value::Float32(*inner)),
            Value::Text(inner) => inner
                .parse::<f32>()
                .ok()
                .and_then(Float32::try_new)
                .map(Value::Float32),
            _ => None,
        },
        FieldKind::Float64 => match value {
            Value::Float64(inner) => Some(Value::Float64(*inner)),
            Value::Text(inner) => inner
                .parse::<f64>()
                .ok()
                .and_then(Float64::try_new)
                .map(Value::Float64),
            _ => None,
        },
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_lossless_field_literal_for_kind(*key_kind, value, allow_text_ulid)
        }
        FieldKind::Int64 => canonicalize_int_literal(value, i64::MIN, i64::MAX),
        FieldKind::Int8 => canonicalize_int_literal(value, i64::from(i8::MIN), i64::from(i8::MAX)),
        FieldKind::Int16 => {
            canonicalize_int_literal(value, i64::from(i16::MIN), i64::from(i16::MAX))
        }
        FieldKind::Int32 => {
            canonicalize_int_literal(value, i64::from(i32::MIN), i64::from(i32::MAX))
        }
        FieldKind::Int128 => match value {
            Value::Int128(inner) => Some(Value::Int128(*inner)),
            Value::Text(inner) => inner.parse::<i128>().ok().map(Value::Int128),
            _ => None,
        },
        FieldKind::IntBig { .. } => match value {
            Value::IntBig(inner) => Some(Value::IntBig(inner.clone())),
            Value::Text(inner) => IntBig::from_str(inner).ok().map(Value::IntBig),
            _ => None,
        },
        FieldKind::List(inner) | FieldKind::Set(inner) => match value {
            Value::List(values) => Some(Value::List(
                values
                    .iter()
                    .map(|item| {
                        canonicalize_lossless_field_literal_for_kind(*inner, item, allow_text_ulid)
                            .unwrap_or_else(|| item.clone())
                    })
                    .collect(),
            )),
            _ => None,
        },
        FieldKind::Principal => match value {
            Value::Principal(inner) => Some(Value::Principal(*inner)),
            Value::Text(inner) => Principal::from_str(inner).ok().map(Value::Principal),
            _ => None,
        },
        FieldKind::Text { .. } => match value {
            Value::Text(inner) => Some(Value::Text(inner.clone())),
            _ => None,
        },
        FieldKind::Nat64 => canonicalize_nat_literal(value, u64::MAX),
        FieldKind::Nat8 => canonicalize_nat_literal(value, u64::from(u8::MAX)),
        FieldKind::Nat16 => canonicalize_nat_literal(value, u64::from(u16::MAX)),
        FieldKind::Nat32 => canonicalize_nat_literal(value, u64::from(u32::MAX)),
        FieldKind::Nat128 => match value {
            Value::Nat128(inner) => Some(Value::Nat128(*inner)),
            Value::Text(inner) => inner.parse::<u128>().ok().map(Value::Nat128),
            _ => None,
        },
        FieldKind::NatBig { .. } => match value {
            Value::NatBig(inner) => Some(Value::NatBig(inner.clone())),
            Value::Text(inner) => NatBig::from_str(inner).ok().map(Value::NatBig),
            _ => None,
        },
        FieldKind::Unit => match value {
            Value::Null | Value::Unit => Some(Value::Unit),
            _ => None,
        },
        FieldKind::Ulid if allow_text_ulid => match value {
            Value::Text(inner) => inner.parse::<Ulid>().ok().map(Value::Ulid),
            Value::Ulid(inner) => Some(Value::Ulid(*inner)),
            _ => None,
        },
        _ => None,
    }
}

fn canonicalize_int_literal(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        Value::Text(inner) => inner.parse::<i64>().ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

fn canonicalize_nat_literal(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        Value::Text(inner) => inner.parse::<u64>().ok()?,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

fn canonicalize_int_strict_literal(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

fn canonicalize_nat_strict_literal(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

// Keep strict SQL literal canonicalization on its original narrow contract:
// it only upgrades parsed numeric tokens onto exact integer field kinds and
// adds the explicit text-to-ULID escape hatch that SQL literal syntax needs.
fn canonicalize_strict_sql_literal_for_kind_impl(kind: FieldKind, value: &Value) -> Option<Value> {
    match kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_strict_sql_literal_for_kind_impl(*key_kind, value)
        }
        FieldKind::Int64 => canonicalize_int_strict_literal(value, i64::MIN, i64::MAX),
        FieldKind::Int8 => {
            canonicalize_int_strict_literal(value, i64::from(i8::MIN), i64::from(i8::MAX))
        }
        FieldKind::Int16 => {
            canonicalize_int_strict_literal(value, i64::from(i16::MIN), i64::from(i16::MAX))
        }
        FieldKind::Int32 => {
            canonicalize_int_strict_literal(value, i64::from(i32::MIN), i64::from(i32::MAX))
        }
        FieldKind::Nat64 => canonicalize_nat_strict_literal(value, u64::MAX),
        FieldKind::Nat8 => canonicalize_nat_strict_literal(value, u64::from(u8::MAX)),
        FieldKind::Nat16 => canonicalize_nat_strict_literal(value, u64::from(u16::MAX)),
        FieldKind::Nat32 => canonicalize_nat_strict_literal(value, u64::from(u32::MAX)),
        FieldKind::Ulid => match value {
            Value::Text(inner) => inner.parse::<Ulid>().ok().map(Value::Ulid),
            _ => None,
        },
        FieldKind::List(inner) | FieldKind::Set(inner) => match value {
            Value::List(values) => values
                .iter()
                .map(|item| canonicalize_strict_sql_literal_for_kind_impl(*inner, item))
                .collect::<Option<Vec<_>>>()
                .map(Value::List),
            _ => None,
        },
        _ => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
