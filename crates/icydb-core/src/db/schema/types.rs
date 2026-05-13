//! Module: db::schema::types
//! Responsibility: compact predicate-schema type system for validation and coercion checks.
//! Does not own: planner route selection or runtime predicate execution behavior.
//! Boundary: defines scalar/field type compatibility surfaces used by predicate validation.

use crate::{
    db::schema::PersistedFieldKind,
    model::field::FieldKind,
    traits::RuntimeValueKind,
    types::Ulid,
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
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Nat,
    Nat128,
    NatBig,
    Ulid,
    Unit,
}

// Local helpers to expand the scalar registry into match arms.
macro_rules! scalar_coercion_family_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $coercion_family, )*
        }
    };
}

macro_rules! scalar_matches_value_from_registry {
    ( @args $self:expr, $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        matches!(
            ($self, $value),
            $( (ScalarType::$scalar, $value_pat) )|*
        )
    };
}

macro_rules! scalar_supports_numeric_coercion_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_numeric_coercion, )*
        }
    };
}

macro_rules! scalar_is_keyable_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $is_keyable, )*
        }
    };
}

macro_rules! scalar_supports_ordering_from_registry {
    ( @args $self:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $self {
            $( ScalarType::$scalar => $supports_ordering, )*
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
        FieldKind::Int => FieldType::Scalar(ScalarType::Int),
        FieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        FieldKind::IntBig => FieldType::Scalar(ScalarType::IntBig),
        FieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        FieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        FieldKind::Text { .. } => FieldType::Scalar(ScalarType::Text),
        FieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        FieldKind::Nat => FieldType::Scalar(ScalarType::Nat),
        FieldKind::Nat128 => FieldType::Scalar(ScalarType::Nat128),
        FieldKind::NatBig => FieldType::Scalar(ScalarType::NatBig),
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
#[must_use]
pub(in crate::db) fn canonicalize_strict_sql_literal_for_persisted_kind(
    kind: &PersistedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        PersistedFieldKind::Relation { key_kind, .. } => {
            canonicalize_strict_sql_literal_for_persisted_kind(key_kind, value)
        }
        PersistedFieldKind::Int => match value {
            Value::Nat(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        PersistedFieldKind::Nat => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Nat),
            _ => None,
        },
        PersistedFieldKind::Ulid => match value {
            Value::Text(inner) => Ulid::from_str(inner).ok().map(Value::Ulid),
            _ => None,
        },
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => match value {
            Value::List(values) => values
                .iter()
                .map(|item| canonicalize_strict_sql_literal_for_persisted_kind(inner, item))
                .collect::<Option<Vec<_>>>()
                .map(Value::List),
            _ => None,
        },
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Enum { .. }
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig
        | PersistedFieldKind::Unit
        | PersistedFieldKind::Map { .. }
        | PersistedFieldKind::Structured { .. } => None,
    }
}

pub(in crate::db) fn field_type_from_persisted_kind(kind: &PersistedFieldKind) -> FieldType {
    match kind {
        PersistedFieldKind::Account => FieldType::Scalar(ScalarType::Account),
        PersistedFieldKind::Blob { .. } => FieldType::Scalar(ScalarType::Blob),
        PersistedFieldKind::Bool => FieldType::Scalar(ScalarType::Bool),
        PersistedFieldKind::Date => FieldType::Scalar(ScalarType::Date),
        PersistedFieldKind::Decimal { .. } => FieldType::Scalar(ScalarType::Decimal),
        PersistedFieldKind::Duration => FieldType::Scalar(ScalarType::Duration),
        PersistedFieldKind::Enum { .. } => FieldType::Scalar(ScalarType::Enum),
        PersistedFieldKind::Float32 => FieldType::Scalar(ScalarType::Float32),
        PersistedFieldKind::Float64 => FieldType::Scalar(ScalarType::Float64),
        PersistedFieldKind::Int => FieldType::Scalar(ScalarType::Int),
        PersistedFieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        PersistedFieldKind::IntBig => FieldType::Scalar(ScalarType::IntBig),
        PersistedFieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        PersistedFieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        PersistedFieldKind::Text { .. } => FieldType::Scalar(ScalarType::Text),
        PersistedFieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        PersistedFieldKind::Nat => FieldType::Scalar(ScalarType::Nat),
        PersistedFieldKind::Nat128 => FieldType::Scalar(ScalarType::Nat128),
        PersistedFieldKind::NatBig => FieldType::Scalar(ScalarType::NatBig),
        PersistedFieldKind::Ulid => FieldType::Scalar(ScalarType::Ulid),
        PersistedFieldKind::Unit => FieldType::Scalar(ScalarType::Unit),
        PersistedFieldKind::Relation { key_kind, .. } => field_type_from_persisted_kind(key_kind),
        PersistedFieldKind::List(inner) => {
            FieldType::List(Box::new(field_type_from_persisted_kind(inner)))
        }
        PersistedFieldKind::Set(inner) => {
            FieldType::Set(Box::new(field_type_from_persisted_kind(inner)))
        }
        PersistedFieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_persisted_kind(key)),
            value: Box::new(field_type_from_persisted_kind(value)),
        },
        PersistedFieldKind::Structured { queryable } => FieldType::Structured {
            queryable: *queryable,
        },
    }
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
