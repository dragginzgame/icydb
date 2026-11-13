//! Module: value::tag
//! Responsibility: canonical discriminant tags for dynamic values.
//! Does not own: same-variant semantics or wire payload encoding.
//! Boundary: stable tag contract used by hashing, ordering, and value metadata.

use crate::value::Value;

///
/// ValueTag
///
/// Stable canonical value-variant tag used by hashing and ordering surfaces.
///
/// IMPORTANT:
/// Tag values are part of stable hashing and ordering behavior; change only as an
/// explicit breaking value-semantics migration.
///
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTag {
    /// Canonical discriminant for [`Value::Account`].
    Account = 1,
    /// Canonical discriminant for [`Value::Blob`].
    Blob = 2,
    /// Canonical discriminant for [`Value::Bool`].
    Bool = 3,
    /// Canonical discriminant for [`Value::Date`].
    Date = 4,
    /// Canonical discriminant for [`Value::Decimal`].
    Decimal = 5,
    /// Canonical discriminant for [`Value::Duration`].
    Duration = 6,
    /// Canonical discriminant for [`Value::Enum`].
    Enum = 7,
    /// Canonical discriminant for [`Value::Float32`].
    Float32 = 8,
    /// Canonical discriminant for [`Value::Float64`].
    Float64 = 9,
    /// Canonical discriminant for [`Value::Int64`].
    Int64 = 10,
    /// Canonical discriminant for [`Value::Int128`].
    Int128 = 11,
    /// Canonical discriminant for [`Value::List`].
    List = 12,
    /// Canonical discriminant for [`Value::Map`].
    Map = 13,
    /// Canonical discriminant for [`Value::Null`].
    Null = 14,
    /// Canonical discriminant for [`Value::Principal`].
    Principal = 15,
    /// Canonical discriminant for [`Value::Subaccount`].
    Subaccount = 16,
    /// Canonical discriminant for [`Value::Text`].
    Text = 17,
    /// Canonical discriminant for [`Value::Timestamp`].
    Timestamp = 18,
    /// Canonical discriminant for [`Value::Nat64`].
    Nat64 = 19,
    /// Canonical discriminant for [`Value::Nat128`].
    Nat128 = 20,
    /// Canonical discriminant for [`Value::Ulid`].
    Ulid = 21,
    /// Canonical discriminant for [`Value::Unit`].
    Unit = 22,
    /// Canonical discriminant for [`Value::U256`].
    U256 = 23,
}

impl ValueTag {
    /// Stable wire/hash byte tag for this variant.
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        self as u8
    }

    /// Stable human-readable value kind label for diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
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
            Self::Int64 => "Int64",
            Self::Int128 => "Int128",
            Self::List => "List",
            Self::Map => "Map",
            Self::Null => "Null",
            Self::Principal => "Principal",
            Self::Subaccount => "Subaccount",
            Self::Text => "Text",
            Self::Timestamp => "Timestamp",
            Self::Nat64 => "Nat64",
            Self::Nat128 => "Nat128",
            Self::Ulid => "Ulid",
            Self::Unit => "Unit",
            Self::U256 => "U256",
        }
    }
}

/// Stable canonical variant tag used by hash/fingerprint encodings.
#[must_use]
pub(super) const fn canonical_tag(value: &Value) -> ValueTag {
    match value {
        Value::Account(_) => ValueTag::Account,
        Value::Blob(_) => ValueTag::Blob,
        Value::Bool(_) => ValueTag::Bool,
        Value::Date(_) => ValueTag::Date,
        Value::Decimal(_) => ValueTag::Decimal,
        Value::Duration(_) => ValueTag::Duration,
        Value::Enum(_) => ValueTag::Enum,
        Value::Float32(_) => ValueTag::Float32,
        Value::Float64(_) => ValueTag::Float64,
        Value::Int64(_) => ValueTag::Int64,
        Value::Int128(_) => ValueTag::Int128,
        Value::List(_) => ValueTag::List,
        Value::Map(_) => ValueTag::Map,
        Value::Null => ValueTag::Null,
        Value::Principal(_) => ValueTag::Principal,
        Value::Subaccount(_) => ValueTag::Subaccount,
        Value::Text(_) => ValueTag::Text,
        Value::Timestamp(_) => ValueTag::Timestamp,
        Value::Nat64(_) => ValueTag::Nat64,
        Value::Nat128(_) => ValueTag::Nat128,
        Value::Ulid(_) => ValueTag::Ulid,
        Value::Unit => ValueTag::Unit,
        Value::U256(_) => ValueTag::U256,
    }
}
