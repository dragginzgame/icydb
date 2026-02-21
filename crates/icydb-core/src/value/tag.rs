use crate::value::Value;

///
/// ValueTag
///
/// Stable canonical value-variant tag used by hashing and ordering surfaces.
///
/// IMPORTANT:
/// Tag values are part of stable behavior and must remain fixed after 0.7.
///
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTag {
    Account = 1,
    Blob = 2,
    Bool = 3,
    Date = 4,
    Decimal = 5,
    Duration = 6,
    Enum = 7,
    Float32 = 8,
    Float64 = 9,
    Int = 10,
    Int128 = 11,
    IntBig = 12,
    List = 13,
    Map = 14,
    Null = 15,
    Principal = 16,
    Subaccount = 17,
    Text = 18,
    Timestamp = 19,
    Uint = 20,
    Uint128 = 21,
    UintBig = 22,
    Ulid = 23,
    Unit = 24,
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
        Value::Int(_) => ValueTag::Int,
        Value::Int128(_) => ValueTag::Int128,
        Value::IntBig(_) => ValueTag::IntBig,
        Value::List(_) => ValueTag::List,
        Value::Map(_) => ValueTag::Map,
        Value::Null => ValueTag::Null,
        Value::Principal(_) => ValueTag::Principal,
        Value::Subaccount(_) => ValueTag::Subaccount,
        Value::Text(_) => ValueTag::Text,
        Value::Timestamp(_) => ValueTag::Timestamp,
        Value::Uint(_) => ValueTag::Uint,
        Value::Uint128(_) => ValueTag::Uint128,
        Value::UintBig(_) => ValueTag::UintBig,
        Value::Ulid(_) => ValueTag::Ulid,
        Value::Unit => ValueTag::Unit,
    }
}
