//! Module: db::schema::types
//! Responsibility: compact predicate-schema type system for validation and coercion checks.
//! Does not own: planner route selection or runtime predicate execution behavior.
//! Boundary: defines scalar/field type compatibility surfaces used by predicate validation.

#[cfg(any(test, feature = "sql"))]
use crate::value::{InputValue, InputValueEnum};
use crate::{
    db::{
        codec::hex::decode_hex_bounded,
        schema::{
            AcceptedFieldKind, AcceptedFieldKindCategory, MAX_ACCEPTED_RECURSIVE_DEPTH,
            classify_accepted_field_kind, composite_catalog::AcceptedCompositeCatalog,
        },
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
    value::{CoercionFamily, Value},
};
use icydb_schema::{ScalarCoercionFamily, ScalarKind};
use std::fmt;
use std::str::FromStr;

const fn scalar_coercion_family(kind: ScalarKind) -> CoercionFamily {
    match kind.coercion_family() {
        ScalarCoercionFamily::Numeric => CoercionFamily::Numeric,
        ScalarCoercionFamily::Textual => CoercionFamily::Textual,
        ScalarCoercionFamily::Identifier => CoercionFamily::Identifier,
        ScalarCoercionFamily::Enum => CoercionFamily::Enum,
        ScalarCoercionFamily::Blob => CoercionFamily::Blob,
        ScalarCoercionFamily::Bool => CoercionFamily::Bool,
        ScalarCoercionFamily::Unit => CoercionFamily::Unit,
    }
}

macro_rules! scalar_kind_matches_value_from_registry {
    ( @args $kind:expr, $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        matches!(
            ($kind, $value),
            $( (ScalarKind::$scalar, $value_pat) )|*
        )
    };
}

const fn scalar_kind_matches_value(kind: ScalarKind, value: &Value) -> bool {
    scalar_registry!(scalar_kind_matches_value_from_registry, kind, value)
}

///
/// FieldType
///
/// Reduced runtime type representation used exclusively for predicate validation.
/// This intentionally drops:
/// - record structure
/// - tuple structure
/// - validator/normalizer metadata
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldType {
    Scalar(ScalarKind),
    List(Box<Self>),
    Set(Box<Self>),
    Map { key: Box<Self>, value: Box<Self> },
    Composite,
}

impl FieldType {
    #[must_use]
    pub(crate) const fn is_queryable(&self) -> bool {
        matches!(self, Self::Scalar(_) | Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub(crate) const fn coercion_family(&self) -> Option<CoercionFamily> {
        match self {
            Self::Scalar(inner) => Some(scalar_coercion_family(*inner)),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(CoercionFamily::Collection),
            Self::Composite => None,
        }
    }

    #[must_use]
    pub(crate) const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarKind::Text))
    }

    #[must_use]
    pub(crate) const fn is_bool(&self) -> bool {
        matches!(self, Self::Scalar(ScalarKind::Bool))
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
            Self::Scalar(inner) => inner.supports_ordering(),
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
        FieldType::Scalar(inner) => scalar_kind_matches_value(*inner, literal),
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
        FieldType::Composite => {
            // NOTE: exact composite field types never match predicate literals.
            false
        }
    }
}

/// Canonicalize one strict SQL literal against accepted persisted field metadata.
#[must_use]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn canonicalize_strict_sql_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Relation(_) => {
            let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
                return None;
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
            _ => None,
        },
        AcceptedFieldKindCategory::Composite
        | AcceptedFieldKindCategory::Scalar(
            ScalarKind::Account
            | ScalarKind::Blob
            | ScalarKind::Bool
            | ScalarKind::Date
            | ScalarKind::Decimal
            | ScalarKind::Duration
            | ScalarKind::Enum
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Principal
            | ScalarKind::Subaccount
            | ScalarKind::Text
            | ScalarKind::Timestamp
            | ScalarKind::Unit,
        ) => None,
        AcceptedFieldKindCategory::Scalar(ScalarKind::Int) => {
            canonicalize_signed64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Nat) => {
            canonicalize_unsigned64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Int128) => {
            canonicalize_int128_persisted_literal(value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::IntBig) => {
            let AcceptedFieldKind::IntBig { max_bytes } = kind else {
                return None;
            };

            canonicalize_int_big_persisted_literal(value, *max_bytes)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Nat128) => {
            canonicalize_nat128_persisted_literal(value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::NatBig) => {
            let AcceptedFieldKind::NatBig { max_bytes } = kind else {
                return None;
            };

            canonicalize_nat_big_persisted_literal(value, *max_bytes)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::U256) => canonicalize_u256_literal(value),
        AcceptedFieldKindCategory::Scalar(ScalarKind::Ulid) => match value {
            Value::Text(inner) => inner.parse::<Ulid>().ok().map(Value::Ulid),
            _ => None,
        },
    }
}

/// Canonicalize one string-backed public filter literal through accepted kind
/// authority.
///
/// Unlike strict SQL literals, public filter numerics arrive as text so their
/// Candid shape stays stable across narrow and wide numeric field kinds.
#[must_use]
pub(in crate::db) fn canonicalize_filter_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            canonicalize_filter_literal_for_persisted_kind(key_kind, value)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match value {
            Value::List(values) => values
                .iter()
                .map(|item| canonicalize_filter_literal_for_persisted_kind(inner, item))
                .collect::<Option<Vec<_>>>()
                .map(Value::List),
            _ => None,
        },
        AcceptedFieldKind::Map { .. } | AcceptedFieldKind::Composite { .. } => None,
        _ => canonicalize_filter_scalar_literal(kind, value),
    }
}

/// Canonicalize one collection-containment literal through the field's
/// accepted element kind.
#[must_use]
pub(in crate::db) fn canonicalize_filter_collection_element_for_persisted_kind(
    field_kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match field_kind {
        AcceptedFieldKind::List(element_kind) | AcceptedFieldKind::Set(element_kind) => {
            canonicalize_filter_literal_for_persisted_kind(element_kind, value)
        }
        _ => None,
    }
}

fn canonicalize_filter_scalar_literal(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Account => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Account(inner) => Some(*inner),
                _ => None,
            },
            Account::from_str,
            Value::Account,
        ),
        AcceptedFieldKind::Bool => match value {
            Value::Bool(inner) => Some(Value::Bool(*inner)),
            _ => None,
        },
        AcceptedFieldKind::Decimal { .. } => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Decimal(inner) => Some(*inner),
                _ => None,
            },
            Decimal::from_str,
            Value::Decimal,
        ),
        AcceptedFieldKind::Float32 => match value {
            Value::Float32(inner) => Some(Value::Float32(*inner)),
            Value::Text(inner) => inner
                .parse::<f32>()
                .ok()
                .and_then(Float32::try_new)
                .map(Value::Float32),
            _ => None,
        },
        AcceptedFieldKind::Float64 => match value {
            Value::Float64(inner) => Some(Value::Float64(*inner)),
            Value::Text(inner) => inner
                .parse::<f64>()
                .ok()
                .and_then(Float64::try_new)
                .map(Value::Float64),
            _ => None,
        },
        AcceptedFieldKind::Principal => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Principal(inner) => Some(*inner),
                _ => None,
            },
            Principal::from_str,
            Value::Principal,
        ),
        AcceptedFieldKind::Text { .. } => match value {
            Value::Text(inner) => Some(Value::Text(inner.clone())),
            _ => None,
        },
        AcceptedFieldKind::Ulid => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Ulid(inner) => Some(*inner),
                _ => None,
            },
            Ulid::from_str,
            Value::Ulid,
        ),
        AcceptedFieldKind::Unit => match value {
            Value::Null | Value::Unit => Some(Value::Unit),
            _ => None,
        },
        AcceptedFieldKind::U256 => canonicalize_u256_literal(value),
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. } => canonicalize_filter_numeric_literal(kind, value),
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp => canonicalize_filter_string_backed_atom(kind, value),
        AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. } => None,
    }
}

fn canonicalize_filter_string_backed_atom(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Blob { max_len } => {
            let max_len = max_len
                .and_then(|max_len| usize::try_from(max_len).ok())
                .unwrap_or(usize::MAX);
            match value {
                Value::Blob(inner) if inner.len() <= max_len => Some(Value::Blob(inner.clone())),
                Value::Text(inner) => decode_hex_bounded(inner, max_len).map(Value::Blob),
                _ => None,
            }
        }
        AcceptedFieldKind::Date => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Date(inner) => Some(*inner),
                _ => None,
            },
            |value| Date::parse(value).ok_or(()),
            Value::Date,
        ),
        AcceptedFieldKind::Duration => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Duration(inner) => Some(*inner),
                _ => None,
            },
            Duration::parse_flexible,
            Value::Duration,
        ),
        AcceptedFieldKind::Subaccount => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Subaccount(inner) => Some(*inner),
                _ => None,
            },
            |value| {
                let bytes = decode_hex_bounded(value, 32).ok_or(())?;
                <[u8; 32]>::try_from(bytes)
                    .map(Subaccount::from_array)
                    .map_err(|_| ())
            },
            Value::Subaccount,
        ),
        AcceptedFieldKind::Timestamp => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Timestamp(inner) => Some(*inner),
                _ => None,
            },
            Timestamp::parse_flexible,
            Value::Timestamp,
        ),
        _ => None,
    }
}

fn canonicalize_filter_numeric_literal(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Int8 => {
            canonicalize_filter_int(value, i64::from(i8::MIN), i64::from(i8::MAX))
        }
        AcceptedFieldKind::Int16 => {
            canonicalize_filter_int(value, i64::from(i16::MIN), i64::from(i16::MAX))
        }
        AcceptedFieldKind::Int32 => {
            canonicalize_filter_int(value, i64::from(i32::MIN), i64::from(i32::MAX))
        }
        AcceptedFieldKind::Int64 => canonicalize_filter_int(value, i64::MIN, i64::MAX),
        AcceptedFieldKind::Int128 => match value {
            Value::Int128(inner) => Some(Value::Int128(*inner)),
            Value::Text(inner) => inner.parse::<i128>().ok().map(Value::Int128),
            _ => None,
        },
        AcceptedFieldKind::IntBig { max_bytes } => {
            let parsed = canonicalize_text_or_exact(
                value,
                |value| match value {
                    Value::IntBig(inner) => Some(inner.clone()),
                    _ => None,
                },
                IntBig::from_str,
                Value::IntBig,
            )?;
            canonicalize_int_big_persisted_literal(&parsed, *max_bytes)
        }
        AcceptedFieldKind::Nat8 => canonicalize_filter_nat(value, u64::from(u8::MAX)),
        AcceptedFieldKind::Nat16 => canonicalize_filter_nat(value, u64::from(u16::MAX)),
        AcceptedFieldKind::Nat32 => canonicalize_filter_nat(value, u64::from(u32::MAX)),
        AcceptedFieldKind::Nat64 => canonicalize_filter_nat(value, u64::MAX),
        AcceptedFieldKind::Nat128 => match value {
            Value::Nat128(inner) => Some(Value::Nat128(*inner)),
            Value::Text(inner) => inner.parse::<u128>().ok().map(Value::Nat128),
            _ => None,
        },
        AcceptedFieldKind::NatBig { max_bytes } => {
            let parsed = canonicalize_text_or_exact(
                value,
                |value| match value {
                    Value::NatBig(inner) => Some(inner.clone()),
                    _ => None,
                },
                NatBig::from_str,
                Value::NatBig,
            )?;
            canonicalize_nat_big_persisted_literal(&parsed, *max_bytes)
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. } => None,
    }
}

fn canonicalize_u256_literal(value: &Value) -> Option<Value> {
    match value {
        Value::U256(value) => Some(Value::U256(*value)),
        Value::Nat64(value) => Some(Value::U256(U256::from(*value))),
        Value::Nat128(value) => Some(Value::U256(U256::from(*value))),
        Value::Text(value) => value.parse().ok().map(Value::U256),
        _ => None,
    }
}

fn canonicalize_text_or_exact<T, E>(
    value: &Value,
    exact: impl FnOnce(&Value) -> Option<T>,
    parse: impl FnOnce(&str) -> Result<T, E>,
    wrap: impl FnOnce(T) -> Value,
) -> Option<Value> {
    if let Some(exact) = exact(value) {
        return Some(wrap(exact));
    }
    match value {
        Value::Text(inner) => parse(inner).ok().map(wrap),
        _ => None,
    }
}

fn canonicalize_filter_int(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        Value::Text(inner) => inner.parse::<i64>().ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

fn canonicalize_filter_nat(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        Value::Text(inner) => inner.parse::<u64>().ok()?,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

/// Target-type one strict SQL literal against accepted persisted metadata.
///
/// Enum labels remain unresolved authored input until catalog admission. Other
/// field kinds retain the existing strict SQL canonicalization rules.
#[must_use]
#[cfg(any(test, feature = "sql"))]
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

#[cfg(any(test, feature = "sql"))]
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
        _ => None,
    }
}

#[cfg(any(test, feature = "sql"))]
fn canonicalize_unsigned64_persisted_literal(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Nat64 => canonicalize_nat_persisted_literal(value, u64::MAX),
        AcceptedFieldKind::Nat8 => canonicalize_nat_persisted_literal(value, u64::from(u8::MAX)),
        AcceptedFieldKind::Nat16 => canonicalize_nat_persisted_literal(value, u64::from(u16::MAX)),
        AcceptedFieldKind::Nat32 => canonicalize_nat_persisted_literal(value, u64::from(u32::MAX)),
        _ => None,
    }
}

pub(in crate::db) fn field_type_from_persisted_kind(kind: &AcceptedFieldKind) -> FieldType {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => field_type_from_persisted_kind(key_kind),
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
        AcceptedFieldKind::Composite { .. } => FieldType::Composite,
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
        | AcceptedFieldKind::U256 => scalar_field_type_from_persisted_kind(kind),
    }
}

/// Project one accepted persisted kind into its runtime query-value shape.
///
/// Accepted newtypes are nominal write/admission contracts, but their admitted
/// row values use the recursively unwrapped scalar or collection shape. Query
/// planning must inspect that value shape while records, tuples, missing
/// definitions, and recursive wrapper cycles remain opaque and fail closed.
#[must_use]
pub(in crate::db) fn query_field_kind_from_persisted_kind(
    kind: &AcceptedFieldKind,
    composite_catalog: &AcceptedCompositeCatalog,
) -> AcceptedFieldKind {
    query_field_kind_at_depth(kind, composite_catalog, 0).unwrap_or_else(|| kind.clone())
}

fn query_field_kind_at_depth(
    kind: &AcceptedFieldKind,
    composite_catalog: &AcceptedCompositeCatalog,
    depth: usize,
) -> Option<AcceptedFieldKind> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return None;
    }
    let next_depth = depth.saturating_add(1);

    match kind {
        AcceptedFieldKind::Composite { .. } => {
            let resolved = composite_catalog.resolve_newtype_value_kind(kind)?;
            query_field_kind_at_depth(&resolved, composite_catalog, next_depth)
        }
        AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => Some(AcceptedFieldKind::Relation {
            target_path: target_path.clone(),
            target_entity_name: target_entity_name.clone(),
            target_entity_tag: *target_entity_tag,
            target_store_path: target_store_path.clone(),
            key_kind: Box::new(query_field_kind_at_depth(
                key_kind,
                composite_catalog,
                next_depth,
            )?),
        }),
        AcceptedFieldKind::List(inner) => Some(AcceptedFieldKind::List(Box::new(
            query_field_kind_at_depth(inner, composite_catalog, next_depth)?,
        ))),
        AcceptedFieldKind::Set(inner) => Some(AcceptedFieldKind::Set(Box::new(
            query_field_kind_at_depth(inner, composite_catalog, next_depth)?,
        ))),
        AcceptedFieldKind::Map { key, value } => Some(AcceptedFieldKind::Map {
            key: Box::new(query_field_kind_at_depth(
                key,
                composite_catalog,
                next_depth,
            )?),
            value: Box::new(query_field_kind_at_depth(
                value,
                composite_catalog,
                next_depth,
            )?),
        }),
        _ => Some(kind.clone()),
    }
}

fn scalar_field_type_from_persisted_kind(kind: &AcceptedFieldKind) -> FieldType {
    let AcceptedFieldKindCategory::Scalar(kind) = classify_accepted_field_kind(kind).category()
    else {
        debug_assert!(false, "scalar accepted field kind must classify as scalar");
        return FieldType::Composite;
    };

    FieldType::Scalar(kind)
}

#[cfg(any(test, feature = "sql"))]
fn canonicalize_int_persisted_literal(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

#[cfg(any(test, feature = "sql"))]
fn canonicalize_nat_persisted_literal(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

#[cfg(any(test, feature = "sql"))]
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

#[cfg(any(test, feature = "sql"))]
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
            Self::Composite => f.write_str("Composite"),
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
    fn persisted_field_type_projection_is_total_for_non_scalar_relation_keys() {
        let relation = AcceptedFieldKind::Relation {
            target_path: "test::Target".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "test::Store".into(),
            key_kind: Box::new(AcceptedFieldKind::test_composite()),
        };

        assert_eq!(
            field_type_from_persisted_kind(&relation),
            FieldType::Composite,
        );
    }

    #[test]
    fn persisted_scalar_field_types_reuse_accepted_scalar_classification() {
        let kinds = [
            AcceptedFieldKind::Account,
            AcceptedFieldKind::Blob { max_len: Some(8) },
            AcceptedFieldKind::Bool,
            AcceptedFieldKind::Date,
            AcceptedFieldKind::Decimal { scale: 2 },
            AcceptedFieldKind::Duration,
            enum_kind(),
            AcceptedFieldKind::Float32,
            AcceptedFieldKind::Float64,
            AcceptedFieldKind::Int8,
            AcceptedFieldKind::Int16,
            AcceptedFieldKind::Int32,
            AcceptedFieldKind::Int64,
            AcceptedFieldKind::Int128,
            AcceptedFieldKind::IntBig { max_bytes: 8 },
            AcceptedFieldKind::Principal,
            AcceptedFieldKind::Subaccount,
            AcceptedFieldKind::Text { max_len: Some(8) },
            AcceptedFieldKind::Timestamp,
            AcceptedFieldKind::Nat8,
            AcceptedFieldKind::Nat16,
            AcceptedFieldKind::Nat32,
            AcceptedFieldKind::Nat64,
            AcceptedFieldKind::Nat128,
            AcceptedFieldKind::NatBig { max_bytes: 8 },
            AcceptedFieldKind::Ulid,
            AcceptedFieldKind::Unit,
        ];

        for kind in kinds {
            let AcceptedFieldKindCategory::Scalar(expected) =
                classify_accepted_field_kind(&kind).category()
            else {
                panic!("scalar test kind must classify as scalar");
            };

            assert_eq!(
                field_type_from_persisted_kind(&kind),
                FieldType::Scalar(expected),
            );
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

    #[test]
    fn string_backed_filter_atoms_rehydrate_to_exact_runtime_types() {
        let date = Date::try_new(2026, 8, 5).expect("test date should be valid");
        let subaccount = Subaccount::from_array([0xab; 32]);
        let cases = [
            (
                AcceptedFieldKind::Blob { max_len: Some(4) },
                Value::Text("000aff".to_string()),
                Value::Blob(vec![0x00, 0x0a, 0xff]),
            ),
            (
                AcceptedFieldKind::Date,
                Value::Text(date.to_string()),
                Value::Date(date),
            ),
            (
                AcceptedFieldKind::Duration,
                Value::Text("12345".to_string()),
                Value::Duration(Duration::from_millis(12_345)),
            ),
            (
                AcceptedFieldKind::Subaccount,
                Value::Text(subaccount.to_string()),
                Value::Subaccount(subaccount),
            ),
            (
                AcceptedFieldKind::Timestamp,
                Value::Text("-42".to_string()),
                Value::Timestamp(Timestamp::from_millis(-42)),
            ),
        ];

        for (kind, literal, expected) in cases {
            assert_eq!(
                canonicalize_filter_literal_for_persisted_kind(&kind, &literal),
                Some(expected),
                "{kind:?} should rehydrate its string-backed filter literal",
            );
        }
    }

    #[test]
    fn subaccount_relation_filters_reuse_the_exact_key_kind_canonicalizer() {
        let subaccount = Subaccount::from_array([0xcd; 32]);
        let relation = AcceptedFieldKind::Relation {
            target_path: "test::Target".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "test::Store".into(),
            key_kind: Box::new(AcceptedFieldKind::Subaccount),
        };

        assert_eq!(
            canonicalize_filter_literal_for_persisted_kind(
                &relation,
                &Value::Text(subaccount.to_string()),
            ),
            Some(Value::Subaccount(subaccount)),
        );
    }

    #[test]
    fn collection_contains_rehydrates_string_backed_elements() {
        let subaccount = Subaccount::from_array([0xef; 32]);
        let field_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Subaccount));

        assert_eq!(
            canonicalize_filter_collection_element_for_persisted_kind(
                &field_kind,
                &Value::Text(subaccount.to_string()),
            ),
            Some(Value::Subaccount(subaccount)),
        );
    }

    #[test]
    fn malformed_string_backed_filter_atoms_fail_closed() {
        let malformed = [
            (
                AcceptedFieldKind::Blob { max_len: Some(1) },
                Value::Text("0001".to_string()),
            ),
            (
                AcceptedFieldKind::Date,
                Value::Text("2026-02-30".to_string()),
            ),
            (AcceptedFieldKind::Duration, Value::Text("-1".to_string())),
            (AcceptedFieldKind::Subaccount, Value::Text("ab".repeat(31))),
            (
                AcceptedFieldKind::Subaccount,
                Value::Text(format!("{}zz", "ab".repeat(31))),
            ),
            (
                AcceptedFieldKind::Timestamp,
                Value::Text("not-a-timestamp".to_string()),
            ),
        ];

        for (kind, literal) in malformed {
            assert_eq!(
                canonicalize_filter_literal_for_persisted_kind(&kind, &literal),
                None,
                "{kind:?} should reject malformed string-backed filter literals",
            );
        }
    }
}
