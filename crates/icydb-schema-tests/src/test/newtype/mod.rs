pub mod maths;

pub use crate::prelude::*;

/// --------------------
/// Primitive Newtypes
/// --------------------

#[newtype(primitive = "Account", item(prim = "Account"))]
pub struct AccountN {}

#[newtype(primitive = "Bool", item(prim = "Bool"))]
pub struct BoolN {}

#[newtype(primitive = "Date", item(prim = "Date"))]
pub struct DateN {}

#[newtype(item(prim = "Decimal", scale = 18), primitive = "Decimal")]
pub struct DecimalN {}

#[newtype(item(prim = "Duration"), primitive = "Duration")]
pub struct DurationN {}

#[newtype(primitive = "Float32", item(prim = "Float32"))]
pub struct Float32N {}

#[newtype(primitive = "Float64", item(prim = "Float64"))]
pub struct Float64N {}

#[newtype(primitive = "Int", item(prim = "Int"))]
pub struct IntN {}

#[newtype(primitive = "Int8", item(prim = "Int8"))]
pub struct Int8N {}

#[newtype(primitive = "Int16", item(prim = "Int16"))]
pub struct Int16N {}

#[newtype(primitive = "Int32", item(prim = "Int32"))]
pub struct Int32N {}

#[newtype(primitive = "Int64", item(prim = "Int64"))]
pub struct Int64N {}

#[newtype(primitive = "Int128", item(prim = "Int128"))]
pub struct Int128N {}

#[newtype(primitive = "Nat", item(prim = "Nat"))]
pub struct NatN {}

#[newtype(primitive = "Nat8", item(prim = "Nat8"))]
pub struct Nat8N {}

#[newtype(primitive = "Nat16", item(prim = "Nat16"))]
pub struct Nat16N {}

#[newtype(primitive = "Nat32", item(prim = "Nat32"))]
pub struct Nat32N {}

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Nat64N {}

#[newtype(primitive = "Nat128", item(prim = "Nat128"))]
pub struct Nat128N {}

#[newtype(primitive = "Principal", item(prim = "Principal"))]
pub struct PrincipalN {}

#[newtype(primitive = "Subaccount", item(prim = "Subaccount"))]
pub struct SubaccountN {}

#[newtype(primitive = "Text", item(prim = "Text"))]
pub struct TextN {}

#[newtype(primitive = "Timestamp", item(prim = "Timestamp"))]
pub struct TimestampN {}

#[newtype(primitive = "Ulid", item(prim = "Ulid"))]
pub struct UlidN {}

#[newtype(primitive = "Unit", item(prim = "Unit"))]
pub struct UnitN {}

///
/// Wrapped
///

#[newtype(primitive = "Float32", item(is = "Float32N"))]
pub struct Float32W {}

#[newtype(primitive = "Float32", item(is = "Float32W"))]
pub struct Float32WW {}

#[newtype(primitive = "Nat32", item(is = "Nat32N"))]
pub struct Nat32W {}

#[newtype(primitive = "Nat32", item(is = "Nat32W"))]
pub struct Nat32WW {}

/// --------------------
/// Defaulted Newtypes
/// --------------------
///
/// These all have a default value suitable for quick initialization.
/// Each mirrors its non-default counterpart above.

#[newtype(primitive = "Account", item(prim = "Account"))]
pub struct AccountD {}

#[newtype(primitive = "Bool", item(prim = "Bool"), default = true)]
pub struct BoolD {}

#[newtype(
    primitive = "Date",
    item(prim = "Date"),
    default = "icydb::types::Date::EPOCH"
)]
pub struct DateD {}

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal", scale = 18),
    default = 0.0
)]
pub struct DecimalD {}

#[newtype(primitive = "Duration", item(prim = "Duration"), default = 0u64)]
pub struct DurationD {}

#[newtype(primitive = "Float32", item(prim = "Float32"), default = 0)]
pub struct Float32D {}

#[newtype(primitive = "Float64", item(prim = "Float64"), default = 0)]
pub struct Float64D {}

#[newtype(primitive = "Int", item(prim = "Int"), default = 0)]
pub struct IntD {}

#[newtype(primitive = "Int128", item(prim = "Int128"), default = 0)]
pub struct Int128D {}

#[newtype(primitive = "Nat", item(prim = "Nat"), default = 0u64)]
pub struct NatD {}

#[newtype(primitive = "Nat32", item(prim = "Nat32"), default = 0u32)]
pub struct Nat32D {}

#[newtype(primitive = "Nat64", item(prim = "Nat64"), default = 0u64)]
pub struct Nat64D;

#[newtype(primitive = "Nat128", item(prim = "Nat128"), default = 0u128)]
pub struct Nat128D;

#[newtype(
    primitive = "Principal",
    item(prim = "Principal"),
    default = "icydb::types::Principal::anonymous"
)]
pub struct PrincipalD;

#[newtype(primitive = "Subaccount", item(prim = "Subaccount"))]
pub struct SubaccountD;

#[newtype(primitive = "Text", item(prim = "Text"), default = "\"\"")]
pub struct TextD;

#[newtype(
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    default = "icydb::types::Timestamp::EPOCH"
)]
pub struct TimestampD;

#[newtype(
    primitive = "Ulid",
    item(prim = "Ulid"),
    default = "icydb::types::Ulid::generate"
)]
pub struct UlidD;
