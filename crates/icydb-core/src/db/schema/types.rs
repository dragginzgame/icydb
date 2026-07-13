//! Module: db::schema::types
//! Responsibility: compact predicate-schema type system for validation and coercion checks.
//! Does not own: planner route selection or runtime predicate execution behavior.
//! Boundary: defines scalar/field type compatibility surfaces used by predicate validation.

#[cfg(feature = "sql")]
use crate::types::{IntBig, NatBig, Ulid};
#[cfg(feature = "sql")]
use crate::value::{InputValue, InputValueEnum};
use crate::{
    db::schema::{
        AcceptedFieldKind, AcceptedFieldKindCategory, AcceptedScalarClass,
        classify_accepted_field_kind,
    },
    model::field::FieldKind,
    value::RuntimeValueKind,
    value::{CoercionFamily, Value},
};
use std::fmt;

///
/// ScalarType
///
/// Internal scalar classification used by predicate validation.
/// This is deliberately *smaller* than the full schema/type system
/// and exists only to support:
/// - coercion rules
/// - literal compatibility checks
/// - operator validity (ordering, equality)
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScalarType {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    SignedNumeric,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    UnsignedNumeric,
    Nat128,
    NatBig,
    Ulid,
    Unit,
}

// The scalar registry is keyed by runtime `Value` variant names. Keep that
// value vocabulary local and project broad fixed-width schema families onto
// explicit signed/unsigned predicate classifications.
macro_rules! scalar_type_variant {
    (Int) => {
        ScalarType::SignedNumeric
    };
    (Nat) => {
        ScalarType::UnsignedNumeric
    };
    ($scalar:ident) => {
        ScalarType::$scalar
    };
}

// Local helpers to expand the scalar registry into match arms.
macro_rules! scalar_coercion_family_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( scalar_type_variant!($scalar) => $coercion_family, )*
        }
    };
}

macro_rules! scalar_matches_value_from_registry {
    ( @args $self:expr, $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        matches!(
            ($self, $value),
            $( (scalar_type_variant!($scalar), $value_pat) )|*
        )
    };
}

macro_rules! scalar_supports_numeric_coercion_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( scalar_type_variant!($scalar) => $supports_numeric_coercion, )*
        }
    };
}

macro_rules! scalar_is_keyable_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( scalar_type_variant!($scalar) => $is_keyable, )*
        }
    };
}

macro_rules! scalar_supports_ordering_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( scalar_type_variant!($scalar) => $supports_ordering, )*
        }
    };
}

impl ScalarType {
    #[must_use]
    pub(crate) const fn coercion_family(&self) -> CoercionFamily {
        scalar_registry!(scalar_coercion_family_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn is_orderable(&self) -> bool {
        // Predicate-level ordering gate.
        // Delegates to registry-backed supports_ordering.
        self.supports_ordering()
    }

    #[must_use]
    pub(crate) const fn matches_value(&self, value: &Value) -> bool {
        scalar_registry!(scalar_matches_value_from_registry, self, value)
    }

    #[must_use]
    pub(crate) const fn supports_numeric_coercion(&self) -> bool {
        scalar_registry!(scalar_supports_numeric_coercion_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn is_keyable(&self) -> bool {
        scalar_registry!(scalar_is_keyable_from_registry, self)
    }

    #[must_use]
    pub(crate) const fn supports_ordering(&self) -> bool {
        scalar_registry!(scalar_supports_ordering_from_registry, self)
    }
}

///
/// FieldType
///
/// Reduced runtime type representation used exclusively for predicate validation.
/// This intentionally drops:
/// - record structure
/// - tuple structure
/// - validator/sanitizer metadata
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldType {
    Scalar(ScalarType),
    List(Box<Self>),
    Set(Box<Self>),
    Map { key: Box<Self>, value: Box<Self> },
    Structured { queryable: bool },
}

impl FieldType {
    #[must_use]
    pub(crate) const fn value_kind(&self) -> RuntimeValueKind {
        match self {
            Self::Scalar(_) => RuntimeValueKind::Atomic,
            Self::List(_) | Self::Set(_) => RuntimeValueKind::Structured { queryable: true },
            Self::Map { .. } => RuntimeValueKind::Structured { queryable: false },
            Self::Structured { queryable } => RuntimeValueKind::Structured {
                queryable: *queryable,
            },
        }
    }

    #[must_use]
    pub(crate) const fn coercion_family(&self) -> Option<CoercionFamily> {
        match self {
            Self::Scalar(inner) => Some(inner.coercion_family()),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(CoercionFamily::Collection),
            Self::Structured { .. } => None,
        }
    }

    #[must_use]
    pub(crate) const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarType::Text))
    }

    #[must_use]
    pub(crate) const fn is_bool(&self) -> bool {
        matches!(self, Self::Scalar(ScalarType::Bool))
    }

    #[must_use]
    pub(crate) const fn is_collection(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_) | Self::Map { .. })
    }

    #[must_use]
    pub(crate) const fn is_list_like(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub(crate) const fn is_orderable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_orderable(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn is_keyable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_keyable(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn supports_numeric_coercion(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.supports_numeric_coercion(),
            _ => false,
        }
    }
}

pub(crate) fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Scalar(inner) => inner.matches_value(literal),
        FieldType::List(element) | FieldType::Set(element) => match literal {
            Value::List(items) => items.iter().all(|item| literal_matches_type(item, element)),
            _ => false,
        },
        FieldType::Map { key, value } => match literal {
            Value::Map(entries) => {
                if Value::validate_map_entries(entries.as_slice()).is_err() {
                    return false;
                }

                entries.iter().all(|(entry_key, entry_value)| {
                    literal_matches_type(entry_key, key) && literal_matches_type(entry_value, value)
                })
            }
            _ => false,
        },
        FieldType::Structured { .. } => {
            // NOTE: non-queryable structured field types never match predicate literals.
            false
        }
    }
}

pub(crate) fn field_type_from_model_kind(kind: &FieldKind) -> FieldType {
    match kind {
        FieldKind::Account => FieldType::Scalar(ScalarType::Account),
        FieldKind::Blob { .. } => FieldType::Scalar(ScalarType::Blob),
        FieldKind::Bool => FieldType::Scalar(ScalarType::Bool),
        FieldKind::Date => FieldType::Scalar(ScalarType::Date),
        FieldKind::Decimal { .. } => FieldType::Scalar(ScalarType::Decimal),
        FieldKind::Duration => FieldType::Scalar(ScalarType::Duration),
        FieldKind::Enum { .. } => FieldType::Scalar(ScalarType::Enum),
        FieldKind::Float32 => FieldType::Scalar(ScalarType::Float32),
        FieldKind::Float64 => FieldType::Scalar(ScalarType::Float64),
        FieldKind::Int8 | FieldKind::Int16 | FieldKind::Int32 | FieldKind::Int64 => {
            FieldType::Scalar(ScalarType::SignedNumeric)
        }
        FieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        FieldKind::IntBig { .. } => FieldType::Scalar(ScalarType::IntBig),
        FieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        FieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        FieldKind::Text { .. } => FieldType::Scalar(ScalarType::Text),
        FieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        FieldKind::Nat8 | FieldKind::Nat16 | FieldKind::Nat32 | FieldKind::Nat64 => {
            FieldType::Scalar(ScalarType::UnsignedNumeric)
        }
        FieldKind::Nat128 => FieldType::Scalar(ScalarType::Nat128),
        FieldKind::NatBig { .. } => FieldType::Scalar(ScalarType::NatBig),
        FieldKind::Ulid => FieldType::Scalar(ScalarType::Ulid),
        FieldKind::Unit => FieldType::Scalar(ScalarType::Unit),
        FieldKind::Relation { key_kind, .. } => field_type_from_model_kind(key_kind),
        FieldKind::List(inner) => FieldType::List(Box::new(field_type_from_model_kind(inner))),
        FieldKind::Set(inner) => FieldType::Set(Box::new(field_type_from_model_kind(inner))),
        FieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_model_kind(key)),
            value: Box::new(field_type_from_model_kind(value)),
        },
        FieldKind::Structured { queryable } => FieldType::Structured {
            queryable: *queryable,
        },
    }
}

/// Canonicalize one strict SQL literal against accepted persisted field metadata.
#[cfg(feature = "sql")]
#[must_use]
pub(in crate::db) fn canonicalize_strict_sql_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Relation(_) => {
            let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
                unreachable!("persisted kind invariant")
            };

            canonicalize_strict_sql_literal_for_persisted_kind(key_kind, value)
        }
        AcceptedFieldKindCategory::Collection => match kind {
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match value {
                Value::List(values) => values
                    .iter()
                    .map(|item| canonicalize_strict_sql_literal_for_persisted_kind(inner, item))
                    .collect::<Option<Vec<_>>>()
                    .map(Value::List),
                _ => None,
            },
            AcceptedFieldKind::Map { .. } => None,
            _ => unreachable!("persisted kind invariant"),
        },
        AcceptedFieldKindCategory::Structured { .. }
        | AcceptedFieldKindCategory::Scalar(
            AcceptedScalarClass::Account
            | AcceptedScalarClass::Blob
            | AcceptedScalarClass::Bool
            | AcceptedScalarClass::Date
            | AcceptedScalarClass::Decimal
            | AcceptedScalarClass::Duration
            | AcceptedScalarClass::Enum
            | AcceptedScalarClass::Float32
            | AcceptedScalarClass::Float64
            | AcceptedScalarClass::Principal
            | AcceptedScalarClass::Subaccount
            | AcceptedScalarClass::Text
            | AcceptedScalarClass::Timestamp
            | AcceptedScalarClass::Unit,
        ) => None,
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Signed64) => {
            canonicalize_signed64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unsigned64) => {
            canonicalize_unsigned64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Signed128) => {
            canonicalize_int128_persisted_literal(value)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::SignedBig) => {
            let AcceptedFieldKind::IntBig { max_bytes } = kind else {
                unreachable!("persisted kind invariant")
            };

            canonicalize_int_big_persisted_literal(value, *max_bytes)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unsigned128) => {
            canonicalize_nat128_persisted_literal(value)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::UnsignedBig) => {
            let AcceptedFieldKind::NatBig { max_bytes } = kind else {
                unreachable!("persisted kind invariant")
            };

            canonicalize_nat_big_persisted_literal(value, *max_bytes)
        }
        AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Ulid) => match value {
            Value::Text(inner) => inner.parse::<Ulid>().ok().map(Value::Ulid),
            _ => None,
        },
    }
}

/// Target-type one strict SQL literal against accepted persisted metadata.
///
/// Enum labels remain unresolved authored input until catalog admission. Other
/// field kinds retain the existing strict SQL canonicalization rules.
#[cfg(feature = "sql")]
#[must_use]
pub(in crate::db) fn input_value_from_strict_sql_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<InputValue> {
    if matches!(kind, AcceptedFieldKind::Enum { .. }) {
        let Value::Text(variant_name) = value else {
            return None;
        };
        return Some(InputValue::Enum(InputValueEnum::loose(
            variant_name.clone(),
        )));
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
        .unwrap_or_else(|| value.clone());
    literal_matches_type(&normalized, &field_type_from_persisted_kind(kind))
        .then(|| InputValue::try_from_runtime_non_enum(&normalized))
        .flatten()
}

#[cfg(feature = "sql")]
fn canonicalize_signed64_persisted_literal(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Int64 => canonicalize_int_persisted_literal(value, i64::MIN, i64::MAX),
        AcceptedFieldKind::Int8 => {
            canonicalize_int_persisted_literal(value, i64::from(i8::MIN), i64::from(i8::MAX))
        }
        AcceptedFieldKind::Int16 => {
            canonicalize_int_persisted_literal(value, i64::from(i16::MIN), i64::from(i16::MAX))
        }
        AcceptedFieldKind::Int32 => {
            canonicalize_int_persisted_literal(value, i64::from(i32::MIN), i64::from(i32::MAX))
        }
        _ => unreachable!("persisted kind invariant"),
    }
}

#[cfg(feature = "sql")]
fn canonicalize_unsigned64_persisted_literal(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Nat64 => canonicalize_nat_persisted_literal(value, u64::MAX),
        AcceptedFieldKind::Nat8 => canonicalize_nat_persisted_literal(value, u64::from(u8::MAX)),
        AcceptedFieldKind::Nat16 => canonicalize_nat_persisted_literal(value, u64::from(u16::MAX)),
        AcceptedFieldKind::Nat32 => canonicalize_nat_persisted_literal(value, u64::from(u32::MAX)),
        _ => unreachable!("persisted kind invariant"),
    }
}

pub(in crate::db) fn field_type_from_persisted_kind(kind: &AcceptedFieldKind) -> FieldType {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Scalar(class) => {
            debug_assert!(semantics.is_scalar());
            debug_assert_eq!(semantics.is_signed_numeric(), scalar_class_is_signed(class));
            debug_assert_eq!(
                semantics.is_unsigned_numeric(),
                scalar_class_is_unsigned(class)
            );
            return FieldType::Scalar(scalar_type_from_persisted_class(class));
        }
        AcceptedFieldKindCategory::Relation(Some(class)) => {
            debug_assert!(!semantics.is_scalar());
            debug_assert_eq!(semantics.is_signed_numeric(), scalar_class_is_signed(class));
            debug_assert_eq!(
                semantics.is_unsigned_numeric(),
                scalar_class_is_unsigned(class)
            );
            return FieldType::Scalar(scalar_type_from_persisted_class(class));
        }
        AcceptedFieldKindCategory::Relation(None) => {
            let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
                unreachable!("persisted kind invariant")
            };

            return field_type_from_persisted_kind(key_kind);
        }
        AcceptedFieldKindCategory::Collection => {
            debug_assert!(semantics.is_collection());
        }
        AcceptedFieldKindCategory::Structured { .. } => {
            debug_assert!(semantics.is_structured());
        }
    }

    match kind {
        AcceptedFieldKind::List(inner) => {
            FieldType::List(Box::new(field_type_from_persisted_kind(inner)))
        }
        AcceptedFieldKind::Set(inner) => {
            FieldType::Set(Box::new(field_type_from_persisted_kind(inner)))
        }
        AcceptedFieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_persisted_kind(key)),
            value: Box::new(field_type_from_persisted_kind(value)),
        },
        AcceptedFieldKind::Structured { queryable } => FieldType::Structured {
            queryable: *queryable,
        },
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::Relation { .. } => {
            unreachable!("persisted kind invariant")
        }
    }
}

const fn scalar_class_is_signed(class: AcceptedScalarClass) -> bool {
    matches!(
        class,
        AcceptedScalarClass::Signed64
            | AcceptedScalarClass::Signed128
            | AcceptedScalarClass::SignedBig
    )
}

const fn scalar_class_is_unsigned(class: AcceptedScalarClass) -> bool {
    matches!(
        class,
        AcceptedScalarClass::Unsigned64
            | AcceptedScalarClass::Unsigned128
            | AcceptedScalarClass::UnsignedBig
    )
}

const fn scalar_type_from_persisted_class(class: AcceptedScalarClass) -> ScalarType {
    match class {
        AcceptedScalarClass::Account => ScalarType::Account,
        AcceptedScalarClass::Blob => ScalarType::Blob,
        AcceptedScalarClass::Bool => ScalarType::Bool,
        AcceptedScalarClass::Date => ScalarType::Date,
        AcceptedScalarClass::Decimal => ScalarType::Decimal,
        AcceptedScalarClass::Duration => ScalarType::Duration,
        AcceptedScalarClass::Enum => ScalarType::Enum,
        AcceptedScalarClass::Float32 => ScalarType::Float32,
        AcceptedScalarClass::Float64 => ScalarType::Float64,
        AcceptedScalarClass::Signed64 => ScalarType::SignedNumeric,
        AcceptedScalarClass::Signed128 => ScalarType::Int128,
        AcceptedScalarClass::SignedBig => ScalarType::IntBig,
        AcceptedScalarClass::Principal => ScalarType::Principal,
        AcceptedScalarClass::Subaccount => ScalarType::Subaccount,
        AcceptedScalarClass::Text => ScalarType::Text,
        AcceptedScalarClass::Timestamp => ScalarType::Timestamp,
        AcceptedScalarClass::Unsigned64 => ScalarType::UnsignedNumeric,
        AcceptedScalarClass::Unsigned128 => ScalarType::Nat128,
        AcceptedScalarClass::UnsignedBig => ScalarType::NatBig,
        AcceptedScalarClass::Ulid => ScalarType::Ulid,
        AcceptedScalarClass::Unit => ScalarType::Unit,
    }
}

#[cfg(feature = "sql")]
fn canonicalize_int_persisted_literal(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

#[cfg(feature = "sql")]
fn canonicalize_nat_persisted_literal(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

#[cfg(feature = "sql")]
fn canonicalize_int128_persisted_literal(value: &Value) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => i128::from(*inner),
        Value::Nat64(inner) => i128::from(*inner),
        Value::Int128(inner) => *inner,
        Value::Nat128(inner) => i128::try_from(*inner).ok()?,
        Value::IntBig(inner) => inner.to_i128()?,
        Value::NatBig(inner) => i128::try_from(inner.to_u128()?).ok()?,
        Value::Text(inner) => inner.parse::<i128>().ok()?,
        _ => return None,
    };

    Some(Value::Int128(value))
}

#[cfg(feature = "sql")]
fn canonicalize_nat128_persisted_literal(value: &Value) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u128::try_from(*inner).ok()?,
        Value::Nat64(inner) => u128::from(*inner),
        Value::Int128(inner) => u128::try_from(*inner).ok()?,
        Value::Nat128(inner) => *inner,
        Value::IntBig(inner) => inner.to_string().parse::<u128>().ok()?,
        Value::NatBig(inner) => inner.to_u128()?,
        Value::Text(inner) => inner.parse::<u128>().ok()?,
        _ => return None,
    };

    Some(Value::Nat128(value))
}

#[cfg(feature = "sql")]
fn canonicalize_int_big_persisted_literal(value: &Value, max_bytes: u32) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => IntBig::from(*inner),
        Value::Nat64(inner) => IntBig::from_bigint((*inner).into()),
        Value::IntBig(inner) => inner.clone(),
        Value::Text(inner) => inner.parse::<IntBig>().ok()?,
        _ => return None,
    };

    (value.to_leb128().len() <= max_bytes as usize).then_some(Value::IntBig(value))
}

#[cfg(feature = "sql")]
fn canonicalize_nat_big_persisted_literal(value: &Value, max_bytes: u32) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => NatBig::from(u64::try_from(*inner).ok()?),
        Value::Nat64(inner) => NatBig::from(*inner),
        Value::NatBig(inner) => inner.clone(),
        Value::Text(inner) => inner.parse::<NatBig>().ok()?,
        _ => return None,
    };

    (value.to_leb128().len() <= max_bytes as usize).then_some(Value::NatBig(value))
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(inner) => write!(f, "{inner:?}"),
            Self::List(inner) => write!(f, "List<{inner}>"),
            Self::Set(inner) => write!(f, "Set<{inner}>"),
            Self::Map { key, value } => write!(f, "Map<{key}, {value}>"),
            Self::Structured { queryable } => {
                write!(f, "Structured<queryable={queryable}>")
            }
        }
    }
}

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::*;

    const fn enum_kind() -> AcceptedFieldKind {
        AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
        }
    }

    #[test]
    fn strict_sql_target_typing_keeps_unit_enum_labels_unresolved() {
        assert_eq!(
            input_value_from_strict_sql_literal_for_persisted_kind(
                &enum_kind(),
                &Value::Text("Active".to_string()),
            ),
            Some(InputValue::Enum(InputValueEnum::loose("Active"))),
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
                Some(InputValue::Enum(InputValueEnum::loose(variant))),
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
                &Value::Text(
                    (u128::try_from(i128::MAX).expect("i128 max fits u128") + 1).to_string()
                ),
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
    fn strict_sql_literal_canonicalization_enforces_big_integer_byte_bounds() {
        assert_eq!(
            canonicalize_strict_sql_literal_for_persisted_kind(
                &AcceptedFieldKind::IntBig { max_bytes: 1 },
                &Value::Text("0".to_string()),
            ),
            Some(Value::IntBig(IntBig::from(0_i64))),
        );
        assert_eq!(
            canonicalize_strict_sql_literal_for_persisted_kind(
                &AcceptedFieldKind::IntBig { max_bytes: 1 },
                &Value::Text("128".to_string()),
            ),
            None,
        );
        assert_eq!(
            canonicalize_strict_sql_literal_for_persisted_kind(
                &AcceptedFieldKind::NatBig { max_bytes: 1 },
                &Value::Text("-1".to_string()),
            ),
            None,
        );
    }
}
