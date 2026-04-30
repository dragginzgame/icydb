//! Module: value
//!
//! Responsibility: canonical dynamic value representation plus storage-key helpers.
//! Does not own: planner semantics or db-level decode policy.
//! Boundary: shared value/domain surface used by query, executor, and storage layers.

mod canonical;
mod coercion;
mod compare;
mod hash;
mod input;
pub mod map;
pub mod ops;
mod output;
mod rank;
pub mod semantics;
mod storage_key;
mod storage_key_runtime;
mod tag;
mod wire;

#[cfg(test)]
mod tests;

use crate::{
    model::field::{FieldKind, FieldStorageDecode},
    prelude::*,
    traits::{EnumValue, FieldTypeMeta, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueMeta},
    types::*,
};
use candid::CandidType;
use serde::Deserialize;
use std::{cmp::Ordering, fmt};

// re-exports
pub(crate) use canonical::canonicalize_value_set;
pub use coercion::{CoercionFamily, CoercionFamilyExt};
#[cfg(test)]
pub(crate) use hash::with_test_hash_override;
pub(crate) use hash::{ValueHashWriter, hash_single_list_identity_canonical_value, hash_value};
pub use input::{InputValue, InputValueEnum};
pub use map::{MapValueError, SchemaInvariantError};
pub use output::{OutputValue, OutputValueEnum};
pub use storage_key::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError};
pub(crate) use storage_key_runtime::{
    storage_key_as_runtime_value, storage_key_from_runtime_value,
};
pub use tag::ValueTag;

//
// CONSTANTS
//

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

//
// TextMode
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextMode {
    Cs, // case-sensitive
    Ci, // case-insensitive
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
        let normalized = map::normalize_map_entries(entries)?;
        Ok(Self::Map(normalized))
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

    ///
    /// CONVERSION
    ///

    /// NOTE:
    /// `Unit` is intentionally treated as a valid storage key and indexable,
    /// used for singleton tables and synthetic identity entities.
    /// Only `Null` is non-indexable.
    #[must_use]
    pub const fn as_storage_key(&self) -> Option<StorageKey> {
        match self {
            Self::Account(value) => Some(StorageKey::Account(*value)),
            Self::Int(value) => Some(StorageKey::Int(*value)),
            Self::Principal(value) => Some(StorageKey::Principal(*value)),
            Self::Subaccount(value) => Some(StorageKey::Subaccount(*value)),
            Self::Timestamp(value) => Some(StorageKey::Timestamp(*value)),
            Self::Uint(value) => Some(StorageKey::Uint(*value)),
            Self::Ulid(value) => Some(StorageKey::Ulid(*value)),
            Self::Unit => Some(StorageKey::Unit),
            _ => None,
        }
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
}

impl RuntimeValueMeta for Value {
    fn kind() -> crate::traits::RuntimeValueKind {
        crate::traits::RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl RuntimeValueDecode for Value {
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

//
// ValueEnum
// handles the Enum case; `path` is optional to allow strict (typed) or loose matching.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, PartialOrd)]
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
