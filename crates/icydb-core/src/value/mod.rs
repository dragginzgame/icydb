//! Module: value
//!
//! Responsibility: canonical dynamic value representation plus storage-key helpers.
//! Does not own: planner semantics or db-level decode policy.
//! Boundary: shared value/domain surface used by query, executor, and storage layers.

mod coercion;
mod compare;
mod hash;
mod rank;
mod storage_key;
mod tag;
mod wire;

#[cfg(test)]
mod tests;

use crate::{
    model::field::{FieldKind, FieldStorageDecode},
    prelude::*,
    traits::{EnumValue, FieldTypeMeta, FieldValue, NumericValue, Repr},
    types::*,
};
use candid::CandidType;
use serde::{Deserialize, Serialize, Serializer};
use serde_bytes::Bytes;
use std::{cmp::Ordering, fmt};

// re-exports
pub use coercion::{CoercionFamily, CoercionFamilyExt};
pub(crate) use hash::ValueHashWriter;
pub(crate) use hash::hash_value;
#[cfg(test)]
pub(crate) use hash::with_test_hash_override;
pub use storage_key::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError};
pub use tag::ValueTag;

//
// CONSTANTS
//

const F64_SAFE_I64: i64 = 1i64 << 53;
const F64_SAFE_U64: u64 = 1u64 << 53;
const F64_SAFE_I128: i128 = 1i128 << 53;
const F64_SAFE_U128: u128 = 1u128 << 53;
pub(crate) const VALUE_WIRE_TYPE_NAME: &str = "Value";
pub(crate) const VALUE_WIRE_VARIANT_LABELS: &[&str] = &[
    "Account",
    "Blob",
    "Bool",
    "Date",
    "Decimal",
    "Duration",
    "Enum",
    "Float32",
    "Float64",
    "Int",
    "Int128",
    "IntBig",
    "List",
    "Map",
    "Null",
    "Principal",
    "Subaccount",
    "Text",
    "Timestamp",
    "Uint",
    "Uint128",
    "UintBig",
    "Ulid",
    "Unit",
];

//
// NumericRepr
//

enum NumericRepr {
    Decimal(Decimal),
    F64(f64),
    None,
}

// Name and discriminant owner for the stable `Value` serde wire shape.
#[derive(Clone, Copy)]
pub(crate) enum ValueWireVariant {
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
    List,
    Map,
    Null,
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

impl ValueWireVariant {
    // Return the stable serde discriminant index for this `Value` variant.
    pub(crate) const fn index(self) -> u32 {
        match self {
            Self::Account => 0,
            Self::Blob => 1,
            Self::Bool => 2,
            Self::Date => 3,
            Self::Decimal => 4,
            Self::Duration => 5,
            Self::Enum => 6,
            Self::Float32 => 7,
            Self::Float64 => 8,
            Self::Int => 9,
            Self::Int128 => 10,
            Self::IntBig => 11,
            Self::List => 12,
            Self::Map => 13,
            Self::Null => 14,
            Self::Principal => 15,
            Self::Subaccount => 16,
            Self::Text => 17,
            Self::Timestamp => 18,
            Self::Uint => 19,
            Self::Uint128 => 20,
            Self::UintBig => 21,
            Self::Ulid => 22,
            Self::Unit => 23,
        }
    }

    // Return the stable serde variant label for this `Value` variant.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Account => "Account",
            Self::Blob => "Blob",
            Self::Bool => "Bool",
            Self::Date => "Date",
            Self::Decimal => "Decimal",
            Self::Duration => "Duration",
            Self::Enum => "Enum",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Int => "Int",
            Self::Int128 => "Int128",
            Self::IntBig => "IntBig",
            Self::List => "List",
            Self::Map => "Map",
            Self::Null => "Null",
            Self::Principal => "Principal",
            Self::Subaccount => "Subaccount",
            Self::Text => "Text",
            Self::Timestamp => "Timestamp",
            Self::Uint => "Uint",
            Self::Uint128 => "Uint128",
            Self::UintBig => "UintBig",
            Self::Ulid => "Ulid",
            Self::Unit => "Unit",
        }
    }

    // Resolve one stable serde variant label back to its runtime discriminant.
    pub(crate) fn from_label(label: &str) -> Option<Self> {
        match label {
            "Account" => Some(Self::Account),
            "Blob" => Some(Self::Blob),
            "Bool" => Some(Self::Bool),
            "Date" => Some(Self::Date),
            "Decimal" => Some(Self::Decimal),
            "Duration" => Some(Self::Duration),
            "Enum" => Some(Self::Enum),
            "Float32" => Some(Self::Float32),
            "Float64" => Some(Self::Float64),
            "Int" => Some(Self::Int),
            "Int128" => Some(Self::Int128),
            "IntBig" => Some(Self::IntBig),
            "List" => Some(Self::List),
            "Map" => Some(Self::Map),
            "Null" => Some(Self::Null),
            "Principal" => Some(Self::Principal),
            "Subaccount" => Some(Self::Subaccount),
            "Text" => Some(Self::Text),
            "Timestamp" => Some(Self::Timestamp),
            "Uint" => Some(Self::Uint),
            "Uint128" => Some(Self::Uint128),
            "UintBig" => Some(Self::UintBig),
            "Ulid" => Some(Self::Ulid),
            "Unit" => Some(Self::Unit),
            _ => None,
        }
    }
}

// Serialize one non-unit `Value` variant through the stable wire descriptor.
fn serialize_value_newtype_variant<S, T>(
    serializer: S,
    variant: ValueWireVariant,
    value: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized + Serialize,
{
    serializer.serialize_newtype_variant(
        VALUE_WIRE_TYPE_NAME,
        variant.index(),
        variant.label(),
        value,
    )
}

// Serialize one unit `Value` variant through the stable wire descriptor.
fn serialize_value_unit_variant<S>(
    serializer: S,
    variant: ValueWireVariant,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_unit_variant(VALUE_WIRE_TYPE_NAME, variant.index(), variant.label())
}

//
// TextMode
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextMode {
    Cs, // case-sensitive
    Ci, // case-insensitive
}

//
// MapValueError
//
// Invariant violations for `Value::Map` construction/normalization.
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapValueError {
    EmptyKey {
        index: usize,
    },
    NonScalarKey {
        index: usize,
        key: Value,
    },
    NonScalarValue {
        index: usize,
        value: Value,
    },
    DuplicateKey {
        left_index: usize,
        right_index: usize,
    },
}

impl std::fmt::Display for MapValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyKey { index } => write!(f, "map key at index {index} must be non-null"),
            Self::NonScalarKey { index, key } => {
                write!(f, "map key at index {index} is not scalar: {key:?}")
            }
            Self::NonScalarValue { index, value } => {
                write!(
                    f,
                    "map value at index {index} is not scalar/ref-like: {value:?}"
                )
            }
            Self::DuplicateKey {
                left_index,
                right_index,
            } => write!(
                f,
                "map contains duplicate keys at normalized positions {left_index} and {right_index}"
            ),
        }
    }
}

impl std::error::Error for MapValueError {}

//
// SchemaInvariantError
//
// Invariant violations encountered while materializing schema/runtime values.
//

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaInvariantError {
    InvalidMapValue(MapValueError),
}

impl std::fmt::Display for SchemaInvariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMapValue(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SchemaInvariantError {}

impl From<MapValueError> for SchemaInvariantError {
    fn from(value: MapValueError) -> Self {
        Self::InvalidMapValue(value)
    }
}

//
// Value
// can be used in WHERE statements
//
// Null        → the field’s value is Option::None (i.e., SQL NULL).
// Unit        → internal placeholder for RHS; not a real value.
//

#[derive(CandidType, Clone, Eq, PartialEq)]
pub enum Value {
    Account(Account),
    Blob(Vec<u8>),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(ValueEnum),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Int128(Int128),
    IntBig(Int),
    /// Ordered list of values.
    /// Used for many-cardinality transport.
    /// List order is preserved for normalization and fingerprints.
    List(Vec<Self>),
    /// Canonical deterministic map representation.
    ///
    /// - Maps are unordered values; insertion order is discarded.
    /// - Entries are always sorted by canonical key order and keys are unique.
    /// - Map fields remain non-queryable and persist as atomic value replacements.
    /// - Persistence treats map fields as atomic value replacements per row save.
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    Uint(u64),
    Uint128(Nat128),
    UintBig(Nat),
    Ulid(Ulid),
    Unit,
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Account(value) => f.debug_tuple("Account").field(value).finish(),
            Self::Blob(value) => write!(f, "Blob({} bytes)", value.len()),
            Self::Bool(value) => f.debug_tuple("Bool").field(value).finish(),
            Self::Date(value) => f.debug_tuple("Date").field(value).finish(),
            Self::Decimal(value) => f.debug_tuple("Decimal").field(value).finish(),
            Self::Duration(value) => f.debug_tuple("Duration").field(value).finish(),
            Self::Enum(value) => f.debug_tuple("Enum").field(value).finish(),
            Self::Float32(value) => f.debug_tuple("Float32").field(value).finish(),
            Self::Float64(value) => f.debug_tuple("Float64").field(value).finish(),
            Self::Int(value) => f.debug_tuple("Int").field(value).finish(),
            Self::Int128(value) => f.debug_tuple("Int128").field(value).finish(),
            Self::IntBig(value) => f.debug_tuple("IntBig").field(value).finish(),
            Self::List(value) => f.debug_tuple("List").field(value).finish(),
            Self::Map(value) => f.debug_tuple("Map").field(value).finish(),
            Self::Null => f.write_str("Null"),
            Self::Principal(value) => f.debug_tuple("Principal").field(value).finish(),
            Self::Subaccount(value) => f.debug_tuple("Subaccount").field(value).finish(),
            Self::Text(value) => f.debug_tuple("Text").field(value).finish(),
            Self::Timestamp(value) => f.debug_tuple("Timestamp").field(value).finish(),
            Self::Uint(value) => f.debug_tuple("Uint").field(value).finish(),
            Self::Uint128(value) => f.debug_tuple("Uint128").field(value).finish(),
            Self::UintBig(value) => f.debug_tuple("UintBig").field(value).finish(),
            Self::Ulid(value) => f.debug_tuple("Ulid").field(value).finish(),
            Self::Unit => f.write_str("Unit"),
        }
    }
}

impl FieldTypeMeta for Value {
    const KIND: FieldKind = FieldKind::Structured { queryable: false };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl Value {
    pub const __KIND: FieldKind = FieldKind::Structured { queryable: false };
    pub const __STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Keep `Value` serialization aligned with the structural value-storage
        // decoder so persisted `FieldStorageDecode::Value` slots can roundtrip
        // every nested variant through fresh write + immediate readback.
        match self {
            Self::Account(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Account, value)
            }
            Self::Blob(value) => serialize_value_newtype_variant(
                serializer,
                ValueWireVariant::Blob,
                &Bytes::new(value.as_slice()),
            ),
            Self::Bool(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Bool, value)
            }
            Self::Date(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Date, value)
            }
            Self::Decimal(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Decimal, value)
            }
            Self::Duration(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Duration, value)
            }
            Self::Enum(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Enum, value)
            }
            Self::Float32(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Float32, &value.get())
            }
            Self::Float64(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Float64, &value.get())
            }
            Self::Int(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Int, value)
            }
            Self::Int128(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Int128, value)
            }
            Self::IntBig(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::IntBig, value)
            }
            Self::List(items) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::List, items)
            }
            Self::Map(entries) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Map, entries)
            }
            Self::Null => serialize_value_unit_variant(serializer, ValueWireVariant::Null),
            Self::Principal(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Principal, value)
            }
            Self::Subaccount(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Subaccount, value)
            }
            Self::Text(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Text, value)
            }
            Self::Timestamp(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Timestamp, value)
            }
            Self::Uint(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Uint, value)
            }
            Self::Uint128(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Uint128, value)
            }
            Self::UintBig(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::UintBig, value)
            }
            Self::Ulid(value) => {
                serialize_value_newtype_variant(serializer, ValueWireVariant::Ulid, value)
            }
            Self::Unit => serialize_value_unit_variant(serializer, ValueWireVariant::Unit),
        }
    }
}

// Local helpers to expand the scalar registry into match arms.
macro_rules! value_is_numeric_from_registry {
    ( @args $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $value {
            $( $value_pat => $is_numeric, )*
            _ => false,
        }
    };
}

macro_rules! value_supports_numeric_coercion_from_registry {
    ( @args $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $value {
            $( $value_pat => $supports_numeric_coercion, )*
            _ => false,
        }
    };
}

macro_rules! value_storage_key_case {
    ( $value:expr, Unit, true ) => {
        if let Value::Unit = $value {
            Some(StorageKey::Unit)
        } else {
            None
        }
    };
    ( $value:expr, $scalar:ident, true ) => {
        if let Value::$scalar(v) = $value {
            Some(StorageKey::$scalar(*v))
        } else {
            None
        }
    };
    ( $value:expr, $scalar:ident, false ) => {
        None
    };
}

macro_rules! value_storage_key_from_registry {
    ( @args $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:tt, is_storage_key_encodable = $is_storage_key_encodable:tt) ),* $(,)? ) => {
        {
            let mut key = None;
            $(
                match key {
                    Some(_) => {}
                    None => {
                        key = value_storage_key_case!($value, $scalar, $is_storage_key_encodable);
                    }
                }
            )*
            key
        }
    };
}

macro_rules! value_coercion_family_from_registry {
    ( @args $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_storage_key_encodable = $is_storage_key_encodable:expr) ),* $(,)? ) => {
        match $value {
            $( $value_pat => $coercion_family, )*
            Value::List(_) => CoercionFamily::Collection,
            Value::Map(_) => CoercionFamily::Collection,
            Value::Null => CoercionFamily::Null,
        }
    };
}

impl Value {
    ///
    /// CONSTRUCTION
    ///

    /// Build a `Value::List` from a list literal.
    ///
    /// Intended for tests and inline construction.
    /// Requires `Clone` because items are borrowed.
    pub fn from_slice<T>(items: &[T]) -> Self
    where
        T: Into<Self> + Clone,
    {
        Self::List(items.iter().cloned().map(Into::into).collect())
    }

    /// Build a `Value::List` from owned items.
    ///
    /// This is the canonical constructor for query / DTO boundaries.
    pub fn from_list<T>(items: Vec<T>) -> Self
    where
        T: Into<Self>,
    {
        Self::List(items.into_iter().map(Into::into).collect())
    }

    /// Build a canonical `Value::Map` from owned key/value entries.
    ///
    /// Invariants are validated and entries are normalized:
    /// - keys must be scalar and non-null
    /// - values may be scalar or structured
    /// - entries are sorted by canonical key order
    /// - duplicate keys are rejected
    pub fn from_map(entries: Vec<(Self, Self)>) -> Result<Self, MapValueError> {
        let normalized = Self::normalize_map_entries(entries)?;
        Ok(Self::Map(normalized))
    }

    /// Validate map entry invariants without changing order.
    pub fn validate_map_entries(entries: &[(Self, Self)]) -> Result<(), MapValueError> {
        for (index, (key, _value)) in entries.iter().enumerate() {
            if matches!(key, Self::Null) {
                return Err(MapValueError::EmptyKey { index });
            }
            if !key.is_scalar() {
                return Err(MapValueError::NonScalarKey {
                    index,
                    key: key.clone(),
                });
            }
        }

        Ok(())
    }

    // Compare two map entries by canonical key order.
    pub(crate) fn compare_map_entry_keys(left: &(Self, Self), right: &(Self, Self)) -> Ordering {
        Self::canonical_cmp_key(&left.0, &right.0)
    }

    // Sort map entries in canonical key order without changing ownership.
    pub(crate) fn sort_map_entries_in_place(entries: &mut [(Self, Self)]) {
        entries.sort_by(Self::compare_map_entry_keys);
    }

    // Return `true` when map entries are already in strict canonical order and
    // therefore contain no duplicate canonical keys.
    pub(crate) fn map_entries_are_strictly_canonical(entries: &[(Self, Self)]) -> bool {
        entries.windows(2).all(|pair| {
            let [left, right] = pair else {
                return true;
            };

            Self::compare_map_entry_keys(left, right) == Ordering::Less
        })
    }

    /// Normalize map entries into canonical deterministic order.
    pub fn normalize_map_entries(
        mut entries: Vec<(Self, Self)>,
    ) -> Result<Vec<(Self, Self)>, MapValueError> {
        Self::validate_map_entries(&entries)?;
        Self::sort_map_entries_in_place(entries.as_mut_slice());

        for i in 1..entries.len() {
            let (left_key, _) = &entries[i - 1];
            let (right_key, _) = &entries[i];
            if Self::canonical_cmp_key(left_key, right_key) == Ordering::Equal {
                return Err(MapValueError::DuplicateKey {
                    left_index: i - 1,
                    right_index: i,
                });
            }
        }

        Ok(entries)
    }

    /// Build a `Value::Enum` from a domain enum using its explicit mapping.
    pub fn from_enum<E: EnumValue>(value: E) -> Self {
        Self::Enum(value.to_value_enum())
    }

    /// Build a strict enum value using the canonical path of `E`.
    #[must_use]
    pub fn enum_strict<E: Path>(variant: &str) -> Self {
        Self::Enum(ValueEnum::strict::<E>(variant))
    }

    ///
    /// TYPES
    ///

    /// Returns true if the value is one of the numeric-like variants
    /// supported by numeric comparison/ordering.
    #[must_use]
    pub const fn is_numeric(&self) -> bool {
        scalar_registry!(value_is_numeric_from_registry, self)
    }

    /// Returns true when numeric coercion/comparison is explicitly allowed.
    #[must_use]
    pub const fn supports_numeric_coercion(&self) -> bool {
        scalar_registry!(value_supports_numeric_coercion_from_registry, self)
    }

    /// Returns true if the value is Text.
    #[must_use]
    pub const fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    /// Returns true if the value is Unit (used for presence/null comparators).
    #[must_use]
    pub const fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    #[must_use]
    pub const fn is_scalar(&self) -> bool {
        match self {
            // definitely not scalar:
            Self::List(_) | Self::Map(_) | Self::Unit => false,
            _ => true,
        }
    }

    /// Stable canonical variant tag used by hash/fingerprint encodings.
    #[must_use]
    pub(crate) const fn canonical_tag(&self) -> ValueTag {
        tag::canonical_tag(self)
    }

    /// Stable canonical rank used by all cross-variant ordering surfaces.
    #[must_use]
    pub(crate) const fn canonical_rank(&self) -> u8 {
        rank::canonical_rank(self)
    }

    /// Total canonical comparator used by planner/predicate/fingerprint surfaces.
    #[must_use]
    pub(crate) fn canonical_cmp(left: &Self, right: &Self) -> Ordering {
        compare::canonical_cmp(left, right)
    }

    /// Total canonical comparator used for map-key normalization.
    #[must_use]
    pub fn canonical_cmp_key(left: &Self, right: &Self) -> Ordering {
        compare::canonical_cmp_key(left, right)
    }

    /// Total canonical comparator for one map entry `(key, value)`.
    ///
    /// This keeps map-entry ordering aligned across normalization, hashing,
    /// and fingerprint-adjacent surfaces.
    #[must_use]
    pub(crate) fn canonical_cmp_map_entry(
        left_key: &Self,
        left_value: &Self,
        right_key: &Self,
        right_value: &Self,
    ) -> Ordering {
        Self::canonical_cmp_key(left_key, right_key)
            .then_with(|| Self::canonical_cmp(left_value, right_value))
    }

    /// Build one borrowed canonical map-entry order for hashing and
    /// fingerprint-adjacent encoding surfaces.
    #[must_use]
    pub(crate) fn ordered_map_entries(entries: &[(Self, Self)]) -> Vec<&(Self, Self)> {
        let mut ordered = entries.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            Self::canonical_cmp_map_entry(&left.0, &left.1, &right.0, &right.1)
        });

        ordered
    }

    /// Strict comparator for identical orderable variants.
    ///
    /// Returns `None` for mismatched or non-orderable variants.
    #[must_use]
    pub(crate) fn strict_order_cmp(left: &Self, right: &Self) -> Option<Ordering> {
        compare::strict_order_cmp(left, right)
    }

    fn numeric_repr(&self) -> NumericRepr {
        // Numeric comparison eligibility is registry-authoritative.
        if !self.supports_numeric_coercion() {
            return NumericRepr::None;
        }

        if let Some(d) = self.to_decimal() {
            return NumericRepr::Decimal(d);
        }
        if let Some(f) = self.to_f64_lossless() {
            return NumericRepr::F64(f);
        }
        NumericRepr::None
    }

    ///
    /// CONVERSION
    ///

    /// NOTE:
    /// `Unit` is intentionally treated as a valid storage key and indexable,
    /// used for singleton tables and synthetic identity entities.
    /// Only `Null` is non-indexable.
    #[must_use]
    pub const fn as_storage_key(&self) -> Option<StorageKey> {
        scalar_registry!(value_storage_key_from_registry, self)
    }

    #[must_use]
    pub const fn as_text(&self) -> Option<&str> {
        if let Self::Text(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_list(&self) -> Option<&[Self]> {
        if let Self::List(xs) = self {
            Some(xs.as_slice())
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_map(&self) -> Option<&[(Self, Self)]> {
        if let Self::Map(entries) = self {
            Some(entries.as_slice())
        } else {
            None
        }
    }

    fn to_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Decimal(d) => d.try_to_decimal(),
            Self::Duration(d) => d.try_to_decimal(),
            Self::Float64(f) => f.try_to_decimal(),
            Self::Float32(f) => f.try_to_decimal(),
            Self::Int(i) => i.try_to_decimal(),
            Self::Int128(i) => i.try_to_decimal(),
            Self::IntBig(i) => i.try_to_decimal(),
            Self::Timestamp(t) => t.try_to_decimal(),
            Self::Uint(u) => u.try_to_decimal(),
            Self::Uint128(u) => u.try_to_decimal(),
            Self::UintBig(u) => u.try_to_decimal(),

            _ => None,
        }
    }

    // Internal numeric coercion helper for aggregate arithmetic.
    pub(crate) fn to_numeric_decimal(&self) -> Option<Decimal> {
        self.to_decimal()
    }

    // it's lossless, trust me bro
    #[expect(clippy::cast_precision_loss)]
    fn to_f64_lossless(&self) -> Option<f64> {
        match self {
            Self::Duration(d) if d.repr() <= F64_SAFE_U64 => Some(d.repr() as f64),
            Self::Float64(f) => Some(f.get()),
            Self::Float32(f) => Some(f64::from(f.get())),
            Self::Int(i) if (-F64_SAFE_I64..=F64_SAFE_I64).contains(i) => Some(*i as f64),
            Self::Int128(i) if (-F64_SAFE_I128..=F64_SAFE_I128).contains(&i.get()) => {
                Some(i.get() as f64)
            }
            Self::IntBig(i) => i.to_i128().and_then(|v| {
                (-F64_SAFE_I128..=F64_SAFE_I128)
                    .contains(&v)
                    .then_some(v as f64)
            }),
            Self::Timestamp(t) if (-F64_SAFE_I64..=F64_SAFE_I64).contains(&t.repr()) => {
                Some(t.repr() as f64)
            }
            Self::Uint(u) if *u <= F64_SAFE_U64 => Some(*u as f64),
            Self::Uint128(u) if u.get() <= F64_SAFE_U128 => Some(u.get() as f64),
            Self::UintBig(u) => u
                .to_u128()
                .and_then(|v| (v <= F64_SAFE_U128).then_some(v as f64)),

            _ => None,
        }
    }

    /// Cross-type numeric comparison; returns None if non-numeric.
    #[must_use]
    pub fn cmp_numeric(&self, other: &Self) -> Option<Ordering> {
        if !self.supports_numeric_coercion() || !other.supports_numeric_coercion() {
            return None;
        }

        match (self.numeric_repr(), other.numeric_repr()) {
            (NumericRepr::Decimal(a), NumericRepr::Decimal(b)) => a.partial_cmp(&b),
            (NumericRepr::F64(a), NumericRepr::F64(b)) => a.partial_cmp(&b),
            _ => None,
        }
    }

    ///
    /// TEXT COMPARISON
    ///

    fn fold_ci(s: &str) -> std::borrow::Cow<'_, str> {
        if s.is_ascii() {
            return std::borrow::Cow::Owned(s.to_ascii_lowercase());
        }
        // NOTE: Unicode fallback — temporary to_lowercase for non‑ASCII.
        // Future: replace with proper NFKC + full casefold when available.
        std::borrow::Cow::Owned(s.to_lowercase())
    }

    fn text_with_mode(s: &'_ str, mode: TextMode) -> std::borrow::Cow<'_, str> {
        match mode {
            TextMode::Cs => std::borrow::Cow::Borrowed(s),
            TextMode::Ci => Self::fold_ci(s),
        }
    }

    fn text_op(
        &self,
        other: &Self,
        mode: TextMode,
        f: impl Fn(&str, &str) -> bool,
    ) -> Option<bool> {
        let (a, b) = (self.as_text()?, other.as_text()?);
        let a = Self::text_with_mode(a, mode);
        let b = Self::text_with_mode(b, mode);
        Some(f(&a, &b))
    }

    fn ci_key(&self) -> Option<String> {
        match self {
            Self::Text(s) => Some(Self::fold_ci(s).into_owned()),
            Self::Ulid(u) => Some(u.to_string().to_ascii_lowercase()),
            Self::Principal(p) => Some(p.to_string().to_ascii_lowercase()),
            Self::Account(a) => Some(a.to_string().to_ascii_lowercase()),
            _ => None,
        }
    }

    fn eq_ci(a: &Self, b: &Self) -> bool {
        if let (Some(ak), Some(bk)) = (a.ci_key(), b.ci_key()) {
            return ak == bk;
        }

        a == b
    }

    fn normalize_list_ref(v: &Self) -> Vec<&Self> {
        match v {
            Self::List(vs) => vs.iter().collect(),
            v => vec![v],
        }
    }

    fn contains_by<F>(&self, needle: &Self, eq: F) -> Option<bool>
    where
        F: Fn(&Self, &Self) -> bool,
    {
        self.as_list()
            .map(|items| items.iter().any(|v| eq(v, needle)))
    }

    #[expect(clippy::unnecessary_wraps)]
    fn contains_any_by<F>(&self, needles: &Self, eq: F) -> Option<bool>
    where
        F: Fn(&Self, &Self) -> bool,
    {
        let needles = Self::normalize_list_ref(needles);
        match self {
            Self::List(items) => Some(needles.iter().any(|n| items.iter().any(|v| eq(v, n)))),
            scalar => Some(needles.iter().any(|n| eq(scalar, n))),
        }
    }

    #[expect(clippy::unnecessary_wraps)]
    fn contains_all_by<F>(&self, needles: &Self, eq: F) -> Option<bool>
    where
        F: Fn(&Self, &Self) -> bool,
    {
        let needles = Self::normalize_list_ref(needles);
        match self {
            Self::List(items) => Some(needles.iter().all(|n| items.iter().any(|v| eq(v, n)))),
            scalar => Some(needles.len() == 1 && eq(scalar, needles[0])),
        }
    }

    fn in_list_by<F>(&self, haystack: &Self, eq: F) -> Option<bool>
    where
        F: Fn(&Self, &Self) -> bool,
    {
        if let Self::List(items) = haystack {
            Some(items.iter().any(|h| eq(h, self)))
        } else {
            None
        }
    }

    /// Case-sensitive/insensitive equality check for text-like values.
    #[must_use]
    pub fn text_eq(&self, other: &Self, mode: TextMode) -> Option<bool> {
        self.text_op(other, mode, |a, b| a == b)
    }

    /// Check whether `other` is a substring of `self` under the given text mode.
    #[must_use]
    pub fn text_contains(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        self.text_op(needle, mode, |a, b| a.contains(b))
    }

    /// Check whether `self` starts with `other` under the given text mode.
    #[must_use]
    pub fn text_starts_with(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        self.text_op(needle, mode, |a, b| a.starts_with(b))
    }

    /// Check whether `self` ends with `other` under the given text mode.
    #[must_use]
    pub fn text_ends_with(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        self.text_op(needle, mode, |a, b| a.ends_with(b))
    }

    ///
    /// EMPTY
    ///

    #[must_use]
    pub const fn is_empty(&self) -> Option<bool> {
        match self {
            Self::List(xs) => Some(xs.is_empty()),
            Self::Map(entries) => Some(entries.is_empty()),
            Self::Text(s) => Some(s.is_empty()),
            Self::Blob(b) => Some(b.is_empty()),

            //  fields represented as Value::Null:
            Self::Null => Some(true),

            _ => None,
        }
    }

    /// Logical negation of [`is_empty`](Self::is_empty).
    #[must_use]
    pub fn is_not_empty(&self) -> Option<bool> {
        self.is_empty().map(|b| !b)
    }

    ///
    /// COLLECTIONS
    ///

    /// Returns true if `self` contains `needle` (or equals it for scalars).
    #[must_use]
    pub fn contains(&self, needle: &Self) -> Option<bool> {
        self.contains_by(needle, |a, b| a == b)
    }

    /// Returns true if any item in `needles` matches a member of `self`.
    #[must_use]
    pub fn contains_any(&self, needles: &Self) -> Option<bool> {
        self.contains_any_by(needles, |a, b| a == b)
    }

    /// Returns true if every item in `needles` matches a member of `self`.
    #[must_use]
    pub fn contains_all(&self, needles: &Self) -> Option<bool> {
        self.contains_all_by(needles, |a, b| a == b)
    }

    /// Returns true if `self` exists inside the provided list.
    #[must_use]
    pub fn in_list(&self, haystack: &Self) -> Option<bool> {
        self.in_list_by(haystack, |a, b| a == b)
    }

    /// Case-insensitive `contains` supporting text and identifier variants.
    #[must_use]
    pub fn contains_ci(&self, needle: &Self) -> Option<bool> {
        match self {
            Self::List(_) => self.contains_by(needle, Self::eq_ci),
            _ => Some(Self::eq_ci(self, needle)),
        }
    }

    /// Case-insensitive variant of [`contains_any`](Self::contains_any).
    #[must_use]
    pub fn contains_any_ci(&self, needles: &Self) -> Option<bool> {
        self.contains_any_by(needles, Self::eq_ci)
    }

    /// Case-insensitive variant of [`contains_all`](Self::contains_all).
    #[must_use]
    pub fn contains_all_ci(&self, needles: &Self) -> Option<bool> {
        self.contains_all_by(needles, Self::eq_ci)
    }

    /// Case-insensitive variant of [`in_list`](Self::in_list).
    #[must_use]
    pub fn in_list_ci(&self, haystack: &Self) -> Option<bool> {
        self.in_list_by(haystack, Self::eq_ci)
    }
}

impl FieldValue for Value {
    fn kind() -> crate::traits::FieldValueKind {
        crate::traits::FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        self.clone()
    }

    fn from_value(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

#[macro_export]
macro_rules! impl_from_for {
    ( $( $type:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl From<$type> for Value {
                fn from(v: $type) -> Self {
                    Self::$variant(v.into())
                }
            }
        )*
    };
}

impl_from_for! {
    Account    => Account,
    Date       => Date,
    Decimal    => Decimal,
    Duration   => Duration,
    bool       => Bool,
    i8         => Int,
    i16        => Int,
    i32        => Int,
    i64        => Int,
    i128       => Int128,
    Int        => IntBig,
    Principal  => Principal,
    Subaccount => Subaccount,
    &str       => Text,
    String     => Text,
    Timestamp  => Timestamp,
    u8         => Uint,
    u16        => Uint,
    u32        => Uint,
    u64        => Uint,
    u128       => Uint128,
    Nat        => UintBig,
    Ulid       => Ulid,
}

impl CoercionFamilyExt for Value {
    /// Returns the coercion-routing family for this value.
    ///
    /// NOTE:
    /// This does NOT imply numeric, arithmetic, ordering, or keyability support.
    /// All scalar capabilities are registry-driven.
    fn coercion_family(&self) -> CoercionFamily {
        scalar_registry!(value_coercion_family_from_registry, self)
    }
}

impl From<Vec<Self>> for Value {
    fn from(vec: Vec<Self>) -> Self {
        Self::List(vec)
    }
}

impl TryFrom<Vec<(Self, Self)>> for Value {
    type Error = SchemaInvariantError;

    fn try_from(entries: Vec<(Self, Self)>) -> Result<Self, Self::Error> {
        Self::from_map(entries).map_err(Self::Error::from)
    }
}

impl From<()> for Value {
    fn from((): ()) -> Self {
        Self::Unit
    }
}

// NOTE:
// Value::partial_cmp is NOT the canonical ordering for database semantics.
// Some orderable scalar types (e.g. Account, Unit) intentionally do not
// participate here. Use canonical_cmp / strict ordering for ORDER BY,
// planning, and key-range validation.
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::Date(a), Self::Date(b)) => a.partial_cmp(b),
            (Self::Decimal(a), Self::Decimal(b)) => a.partial_cmp(b),
            (Self::Duration(a), Self::Duration(b)) => a.partial_cmp(b),
            (Self::Enum(a), Self::Enum(b)) => a.partial_cmp(b),
            (Self::Float32(a), Self::Float32(b)) => a.partial_cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.partial_cmp(b),
            (Self::Int(a), Self::Int(b)) => a.partial_cmp(b),
            (Self::Int128(a), Self::Int128(b)) => a.partial_cmp(b),
            (Self::IntBig(a), Self::IntBig(b)) => a.partial_cmp(b),
            (Self::Principal(a), Self::Principal(b)) => a.partial_cmp(b),
            (Self::Subaccount(a), Self::Subaccount(b)) => a.partial_cmp(b),
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
            (Self::Uint(a), Self::Uint(b)) => a.partial_cmp(b),
            (Self::Uint128(a), Self::Uint128(b)) => a.partial_cmp(b),
            (Self::UintBig(a), Self::UintBig(b)) => a.partial_cmp(b),
            (Self::Ulid(a), Self::Ulid(b)) => a.partial_cmp(b),
            (Self::Map(a), Self::Map(b)) => {
                for ((left_key, left_value), (right_key, right_value)) in a.iter().zip(b.iter()) {
                    let key_cmp = Self::canonical_cmp_key(left_key, right_key);
                    if key_cmp != Ordering::Equal {
                        return Some(key_cmp);
                    }

                    match left_value.partial_cmp(right_value) {
                        Some(Ordering::Equal) => {}
                        non_eq => return non_eq,
                    }
                }
                a.len().partial_cmp(&b.len())
            }

            // Cross-type comparisons: no ordering
            _ => None,
        }
    }
}

//
// ValueEnum
// handles the Enum case; `path` is optional to allow strict (typed) or loose matching.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, PartialOrd, Serialize)]
pub struct ValueEnum {
    variant: String,
    path: Option<String>,
    payload: Option<Box<Value>>,
}

impl ValueEnum {
    /// Build a strict enum value matching the provided variant and path.
    #[must_use]
    pub fn new(variant: &str, path: Option<&str>) -> Self {
        Self {
            variant: variant.to_string(),
            path: path.map(ToString::to_string),
            payload: None,
        }
    }

    /// Build a strict enum value using the canonical path of `E`.
    #[must_use]
    pub fn strict<E: Path>(variant: &str) -> Self {
        Self::new(variant, Some(E::PATH))
    }

    /// Build a strict enum value from a domain enum using its explicit mapping.
    #[must_use]
    pub fn from_enum<E: EnumValue>(value: E) -> Self {
        value.to_value_enum()
    }

    /// Build an enum value with an unresolved path for filter construction.
    /// Query normalization resolves this to the schema enum path before validation.
    #[must_use]
    pub fn loose(variant: &str) -> Self {
        Self::new(variant, None)
    }

    /// Attach an enum payload (used for data-carrying variants).
    #[must_use]
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = Some(Box::new(payload));
        self
    }

    #[must_use]
    pub fn variant(&self) -> &str {
        &self.variant
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_deref()
    }

    pub(crate) fn set_path(&mut self, path: Option<String>) {
        self.path = path;
    }
}
