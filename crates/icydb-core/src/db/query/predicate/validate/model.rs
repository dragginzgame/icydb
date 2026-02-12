use crate::{
    model::field::EntityFieldKind,
    traits::FieldValueKind,
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
pub enum ScalarType {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    E8s,
    E18s,
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Uint,
    Uint128,
    UintBig,
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
    pub const fn coercion_family(&self) -> CoercionFamily {
        scalar_registry!(scalar_coercion_family_from_registry, self)
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        // Predicate-level ordering gate.
        // Delegates to registry-backed supports_ordering.
        self.supports_ordering()
    }

    #[must_use]
    pub const fn matches_value(&self, value: &Value) -> bool {
        scalar_registry!(scalar_matches_value_from_registry, self, value)
    }

    #[must_use]
    pub const fn supports_numeric_coercion(&self) -> bool {
        scalar_registry!(scalar_supports_numeric_coercion_from_registry, self)
    }

    #[must_use]
    pub const fn is_keyable(&self) -> bool {
        scalar_registry!(scalar_is_keyable_from_registry, self)
    }

    #[must_use]
    pub const fn supports_ordering(&self) -> bool {
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
pub enum FieldType {
    Scalar(ScalarType),
    List(Box<Self>),
    Set(Box<Self>),
    Map { key: Box<Self>, value: Box<Self> },
    Structured { queryable: bool },
}

impl FieldType {
    #[must_use]
    pub const fn value_kind(&self) -> FieldValueKind {
        match self {
            Self::Scalar(_) => FieldValueKind::Atomic,
            Self::List(_) | Self::Set(_) => FieldValueKind::Structured { queryable: true },
            Self::Map { .. } => FieldValueKind::Structured { queryable: false },
            Self::Structured { queryable } => FieldValueKind::Structured {
                queryable: *queryable,
            },
        }
    }

    #[must_use]
    pub const fn coercion_family(&self) -> Option<CoercionFamily> {
        match self {
            Self::Scalar(inner) => Some(inner.coercion_family()),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(CoercionFamily::Collection),
            Self::Structured { .. } => None,
        }
    }

    #[must_use]
    pub const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarType::Text))
    }

    #[must_use]
    pub const fn is_collection(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_) | Self::Map { .. })
    }

    #[must_use]
    pub const fn is_list_like(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub const fn is_orderable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_orderable(),
            _ => false,
        }
    }

    #[must_use]
    pub const fn is_keyable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_keyable(),
            _ => false,
        }
    }

    #[must_use]
    pub const fn supports_numeric_coercion(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.supports_numeric_coercion(),
            _ => false,
        }
    }
}

pub fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
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

pub fn field_type_from_model_kind(kind: &EntityFieldKind) -> FieldType {
    match kind {
        EntityFieldKind::Account => FieldType::Scalar(ScalarType::Account),
        EntityFieldKind::Blob => FieldType::Scalar(ScalarType::Blob),
        EntityFieldKind::Bool => FieldType::Scalar(ScalarType::Bool),
        EntityFieldKind::Date => FieldType::Scalar(ScalarType::Date),
        EntityFieldKind::Decimal => FieldType::Scalar(ScalarType::Decimal),
        EntityFieldKind::Duration => FieldType::Scalar(ScalarType::Duration),
        EntityFieldKind::Enum => FieldType::Scalar(ScalarType::Enum),
        EntityFieldKind::E8s => FieldType::Scalar(ScalarType::E8s),
        EntityFieldKind::E18s => FieldType::Scalar(ScalarType::E18s),
        EntityFieldKind::Float32 => FieldType::Scalar(ScalarType::Float32),
        EntityFieldKind::Float64 => FieldType::Scalar(ScalarType::Float64),
        EntityFieldKind::Int => FieldType::Scalar(ScalarType::Int),
        EntityFieldKind::Int128 => FieldType::Scalar(ScalarType::Int128),
        EntityFieldKind::IntBig => FieldType::Scalar(ScalarType::IntBig),
        EntityFieldKind::Principal => FieldType::Scalar(ScalarType::Principal),
        EntityFieldKind::Subaccount => FieldType::Scalar(ScalarType::Subaccount),
        EntityFieldKind::Text => FieldType::Scalar(ScalarType::Text),
        EntityFieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        EntityFieldKind::Uint => FieldType::Scalar(ScalarType::Uint),
        EntityFieldKind::Uint128 => FieldType::Scalar(ScalarType::Uint128),
        EntityFieldKind::UintBig => FieldType::Scalar(ScalarType::UintBig),
        EntityFieldKind::Ulid => FieldType::Scalar(ScalarType::Ulid),
        EntityFieldKind::Unit => FieldType::Scalar(ScalarType::Unit),
        EntityFieldKind::Relation { key_kind, .. } => field_type_from_model_kind(key_kind),
        EntityFieldKind::List(inner) => {
            FieldType::List(Box::new(field_type_from_model_kind(inner)))
        }
        EntityFieldKind::Set(inner) => FieldType::Set(Box::new(field_type_from_model_kind(inner))),
        EntityFieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_model_kind(key)),
            value: Box::new(field_type_from_model_kind(value)),
        },
        EntityFieldKind::Structured { queryable } => FieldType::Structured {
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
