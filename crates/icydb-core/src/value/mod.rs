//! Module: value
//!
//! Responsibility: canonical dynamic values plus typed runtime conversion.
//! Does not own: planner semantics, primary-key encoding, or persisted decode policy.
//! Boundary: shared value/domain surface used by query, executor, and storage layers.
//!
//! `Value` is the runtime canonical value model. Public canister/query boundaries
//! should prefer `InputValue` for caller-supplied literals and `OutputValue` for
//! result payloads, so API surfaces do not depend on runtime execution internals.

mod canonical;
mod canonical_enum;
mod coercion;
mod compare;
mod hash;
mod input;
mod map;
pub(crate) mod ops;
mod output;
mod rank;
mod runtime;
mod semantics;
mod tag;
mod wire;

#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;

use crate::types::*;
use serde::{Deserialize, Deserializer, de};
use std::{cmp::Ordering, fmt};

// re-exports
pub(crate) use canonical::canonicalize_value_set;
pub(crate) use canonical_enum::{CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId};
pub use coercion::{CoercionFamily, CoercionFamilyExt};
#[cfg(test)]
pub(crate) use hash::with_test_hash_override;
pub(crate) use hash::{ValueHashWriter, hash_single_list_identity_canonical_value, hash_value};
pub use input::{InputValue, InputValueEnum};
pub use map::{MapValueError, SchemaInvariantError};
pub use output::{OutputValue, OutputValueEnum, render_output_value_text};
pub use runtime::{
    Collection, MapCollection, RuntimeEnumContext, RuntimeEnumSelection, RuntimeValueDecode,
    RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, runtime_value_btree_map_from_value,
    runtime_value_btree_set_from_value, runtime_value_collection_to_value,
    runtime_value_from_value, runtime_value_from_value_with_enum_context,
    runtime_value_from_value_with_optional_enum_context, runtime_value_from_vec_into,
    runtime_value_from_vec_into_btree_map, runtime_value_from_vec_into_btree_set,
    runtime_value_into, runtime_value_map_collection_to_value, runtime_value_to_value,
    runtime_value_vec_from_value,
};
pub use tag::ValueTag;

//
// CONSTANTS
//

const VALUE_WIRE_TYPE_NAME: &str = "Value";
const VALUE_WIRE_VARIANT_LABELS: &[&str] = &[
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
    "Nat",
    "Nat128",
    "NatBig",
    "Ulid",
    "Unit",
];

// Name and discriminant owner for the stable `Value` serde wire shape.
#[derive(Clone, Copy)]
enum ValueWireVariant {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    Int64,
    Int128,
    IntBig,
    List,
    Map,
    Null,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Nat64,
    Nat128,
    NatBig,
    Ulid,
    Unit,
}

impl ValueWireVariant {
    // Resolve one stable serde variant label back to its runtime discriminant.
    fn from_label(label: &str) -> Option<Self> {
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
            "Int" => Some(Self::Int64),
            "Int128" => Some(Self::Int128),
            "IntBig" => Some(Self::IntBig),
            "List" => Some(Self::List),
            "Map" => Some(Self::Map),
            "Null" => Some(Self::Null),
            "Principal" => Some(Self::Principal),
            "Subaccount" => Some(Self::Subaccount),
            "Text" => Some(Self::Text),
            "Timestamp" => Some(Self::Timestamp),
            "Nat" => Some(Self::Nat64),
            "Nat128" => Some(Self::Nat128),
            "NatBig" => Some(Self::NatBig),
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
//
// Runtime-only dynamic value used by query evaluation, SQL expressions,
// projection materialization, predicates, cursor payloads, and intermediate
// execution state.
//
// Value is intentionally not a persisted field type. Schema persistence must
// admit it through an accepted field contract before selecting a storage codec.
//
// Null        → the field’s value is Option::None (i.e., SQL NULL).
// Unit        → internal placeholder for RHS; not a real value.
//
#[derive(Clone, Eq, PartialEq)]
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
    Int64(i64),
    Int128(i128),
    IntBig(IntBig),
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
    Nat64(u64),
    Nat128(u128),
    NatBig(NatBig),
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
            Self::Int64(value) => f.debug_tuple("Int64").field(value).finish(),
            Self::Int128(value) => f.debug_tuple("Int128").field(value).finish(),
            Self::IntBig(value) => f.debug_tuple("IntBig").field(value).finish(),
            Self::List(value) => f.debug_tuple("List").field(value).finish(),
            Self::Map(value) => f.debug_tuple("Map").field(value).finish(),
            Self::Null => f.write_str("Null"),
            Self::Principal(value) => f.debug_tuple("Principal").field(value).finish(),
            Self::Subaccount(value) => f.debug_tuple("Subaccount").field(value).finish(),
            Self::Text(value) => f.debug_tuple("Text").field(value).finish(),
            Self::Timestamp(value) => f.debug_tuple("Timestamp").field(value).finish(),
            Self::Nat64(value) => f.debug_tuple("Nat64").field(value).finish(),
            Self::Nat128(value) => f.debug_tuple("Nat128").field(value).finish(),
            Self::NatBig(value) => f.debug_tuple("NatBig").field(value).finish(),
            Self::Ulid(value) => f.debug_tuple("Ulid").field(value).finish(),
            Self::Unit => f.write_str("Unit"),
        }
    }
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

    /// Return whether this runtime value contains canonical enum identity.
    #[must_use]
    pub(crate) fn contains_enum(&self) -> bool {
        match self {
            Self::Enum(_) => true,
            Self::List(values) => values.iter().any(Self::contains_enum),
            Self::Map(entries) => entries
                .iter()
                .any(|(key, value)| key.contains_enum() || value.contains_enum()),
            _ => false,
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
    pub(crate) fn canonical_cmp_key(left: &Self, right: &Self) -> Ordering {
        compare::canonical_cmp_key(left, right)
    }

    ///
    /// CONVERSION
    ///

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
    i8         => Int64,
    i16        => Int64,
    i32        => Int64,
    i64        => Int64,
    i128       => Int128,
    IntBig     => IntBig,
    Principal  => Principal,
    Subaccount => Subaccount,
    &str       => Text,
    String     => Text,
    Timestamp  => Timestamp,
    u8         => Nat64,
    u16        => Nat64,
    u32        => Nat64,
    u64        => Nat64,
    u128       => Nat128,
    NatBig     => NatBig,
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
// Canonical store-local enum identity. Names exist only at input/output boundaries.
//

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct ValueEnum(CanonicalEnumValue<Value>);

impl ValueEnum {
    #[cfg(test)]
    pub(crate) const fn test_unit(type_id: u32, variant_id: u32) -> Self {
        Self::new(
            EnumTypeId::new(type_id).expect("test enum type ID must be non-zero"),
            EnumVariantId::new(variant_id).expect("test enum variant ID must be non-zero"),
            CanonicalEnumBody::Unit,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_payload(type_id: u32, variant_id: u32, payload: Value) -> Self {
        Self::new(
            EnumTypeId::new(type_id).expect("test enum type ID must be non-zero"),
            EnumVariantId::new(variant_id).expect("test enum variant ID must be non-zero"),
            CanonicalEnumBody::Payload(Box::new(payload)),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_payload(self, payload: Value) -> Self {
        Self::new(
            self.type_id(),
            self.variant_id(),
            CanonicalEnumBody::Payload(Box::new(payload)),
        )
    }

    #[must_use]
    pub(crate) const fn from_canonical(value: CanonicalEnumValue<Value>) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn new(
        type_id: EnumTypeId,
        variant_id: EnumVariantId,
        body: CanonicalEnumBody<Value>,
    ) -> Self {
        Self(CanonicalEnumValue::new(type_id, variant_id, body))
    }

    #[must_use]
    pub(crate) const fn canonical(&self) -> &CanonicalEnumValue<Value> {
        &self.0
    }

    #[must_use]
    pub(crate) const fn type_id(&self) -> EnumTypeId {
        self.0.type_id()
    }

    #[must_use]
    pub(crate) const fn variant_id(&self) -> EnumVariantId {
        self.0.variant_id()
    }

    #[must_use]
    pub(crate) const fn body(&self) -> &CanonicalEnumBody<Value> {
        self.0.body()
    }

    #[must_use]
    pub(crate) fn payload(&self) -> Option<&Value> {
        match self.body() {
            CanonicalEnumBody::Unit => None,
            CanonicalEnumBody::Payload(payload) => Some(payload.as_ref()),
        }
    }
}

impl<'de> Deserialize<'de> for ValueEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (type_id, variant_id, payload): (u32, u32, Option<Box<Value>>) =
            Deserialize::deserialize(deserializer)?;
        let type_id = EnumTypeId::new(type_id)
            .ok_or_else(|| de::Error::custom("enum type ID must be non-zero"))?;
        let variant_id = EnumVariantId::new(variant_id)
            .ok_or_else(|| de::Error::custom("enum variant ID must be non-zero"))?;
        let body = payload.map_or(CanonicalEnumBody::Unit, CanonicalEnumBody::Payload);
        Ok(Self::new(type_id, variant_id, body))
    }
}
