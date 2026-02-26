use crate::{
    model::field::FieldKind,
    traits::FieldValueKind,
    value::{CoercionFamily, Value},
};
use std::fmt;
use thiserror::Error as ThisError;

///
/// CoercionId
///
/// Identifier for an explicit comparison coercion policy.
///
/// Coercions express *how* values may be compared, not whether a comparison
/// is valid for a given field. Validation and planning enforce legality.
///
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoercionId {
    Strict,
    NumericWiden,
    TextCasefold,
    CollectionElement,
}

impl CoercionId {
    /// Stable tag used by plan hash encodings (fingerprint/continuation).
    #[must_use]
    pub const fn plan_hash_tag(self) -> u8 {
        match self {
            Self::Strict => 0x01,
            Self::NumericWiden => 0x02,
            Self::TextCasefold => 0x04,
            Self::CollectionElement => 0x05,
        }
    }
}

///
/// UnsupportedQueryFeature
///
/// Policy-level query features intentionally rejected by the engine.
///
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum UnsupportedQueryFeature {
    #[error(
        "map field '{field}' is not queryable; map predicates are disabled. model queryable attributes as scalar/indexed fields or list entries"
    )]
    MapPredicate { field: String },
}

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
    pub(crate) const fn value_kind(&self) -> FieldValueKind {
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

pub(super) fn field_type_from_model_kind(kind: &FieldKind) -> FieldType {
    match kind {
        FieldKind::Account => FieldType::Scalar(ScalarType::Account),
        FieldKind::Blob => FieldType::Scalar(ScalarType::Blob),
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
        FieldKind::Text => FieldType::Scalar(ScalarType::Text),
        FieldKind::Timestamp => FieldType::Scalar(ScalarType::Timestamp),
        FieldKind::Uint => FieldType::Scalar(ScalarType::Uint),
        FieldKind::Uint128 => FieldType::Scalar(ScalarType::Uint128),
        FieldKind::UintBig => FieldType::Scalar(ScalarType::UintBig),
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
